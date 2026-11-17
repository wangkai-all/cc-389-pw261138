"""Catalog service: ingest routing, query routing, shared-dimension admin,
comparison queries, dashboard state, isolation self-checks.

Routing rules
-------------
* tenant state "legacy"   -> queries/ingest hit legacy.db (shared tables)
* tenant state "migrated" -> queries/ingest hit tenants/<T>/catalog.db
* staging catalogs are *never* returned by the query router; only the
  migrator opens them, and only for the owning tenant.

Shared dimensions are kept as ONE read-only versioned store (decision:
no per-tenant copies). Deletes/revisions/ACL tightening therefore
propagate to every tenant immediately and uniformly.
"""
import os
import re
import shutil
import threading

from . import store
from .store import db, dumps, loads, now_iso

TENANT_RE = re.compile(r"^[A-Z0-9][A-Z0-9_-]{0,63}$")

SHARED_PREFIX = "SHARED/"
PRIV_PREFIX = "PRIV/"

# ref classification results
OK_OWN = "ok_own"
OK_SHARED = "ok_shared"
CROSS_TENANT = "cross_tenant_ref"
DANGLING = "dangling_ref"


class CatalogError(Exception):
    status = 500


class BadRequest(CatalogError):
    status = 400


class NotFound(CatalogError):
    status = 404


class Conflict(CatalogError):
    status = 409


def parse_priv_owner(key):
    """Return owning tenant of a PRIV/<TENANT>/<name> key, else None."""
    parts = key.split("/")
    if len(parts) >= 3 and parts[0] == "PRIV" and parts[1]:
        return parts[1]
    return None


class CatalogService:
    def __init__(self, root):
        self.paths = store.Paths(root)
        store.init_all(self.paths)
        self.lock = threading.RLock()

    # ------------------------------------------------------------------
    # validation & routing
    # ------------------------------------------------------------------
    def check_tenant(self, tenant):
        if not isinstance(tenant, str) or not TENANT_RE.match(tenant):
            raise BadRequest(f"invalid tenant name: {tenant!r}")
        return tenant

    def tenant_state(self, tenant):
        self.check_tenant(tenant)
        with db(self.paths.registry, wal=True) as c:
            row = c.execute(
                "SELECT state FROM tenants WHERE tenant=?", (tenant,)
            ).fetchone()
        return row["state"] if row else "legacy"

    def _ensure_tenant(self, c, tenant):
        c.execute(
            "INSERT OR IGNORE INTO tenants(tenant, state) VALUES(?, 'legacy')",
            (tenant,),
        )

    def live_catalog_path(self, tenant):
        """The only path a query for `tenant` may open besides legacy/shared.

        Staging paths are deliberately never returned here.
        """
        self.check_tenant(tenant)
        return self.paths.tenant_live(tenant)

    # ------------------------------------------------------------------
    # sequences & markers
    # ------------------------------------------------------------------
    def _next_seq(self, c):
        c.execute("UPDATE meta SET value = CAST(value AS INTEGER) + 1 WHERE key='seq'")
        return int(c.execute("SELECT value FROM meta WHERE key='seq'").fetchone()[0])

    def _next_fact_no(self, c, tenant):
        key = f"fact_no:{tenant}"
        c.execute(
            "INSERT OR IGNORE INTO meta(key, value) VALUES(?, '0')", (key,)
        )
        c.execute(
            "UPDATE meta SET value = CAST(value AS INTEGER) + 1 WHERE key=?",
            (key,),
        )
        return int(c.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()[0])

    def new_fact_id(self, tenant):
        self.check_tenant(tenant)
        with self.lock, db(self.paths.registry, wal=True) as c:
            return f"TRK-{tenant}-{self._next_fact_no(c, tenant):06d}"

    # ------------------------------------------------------------------
    # shared dimension store (read-only to tenants; admin ops below)
    # ------------------------------------------------------------------
    def resolve_shared(self, c_shared, key, tenant):
        """Latest-version resolution with tombstone + ACL semantics."""
        row = c_shared.execute(
            "SELECT * FROM dims WHERE dim_key=? ORDER BY version DESC LIMIT 1",
            (key,),
        ).fetchone()
        if row is None:
            return {"status": "missing", "key": key}
        if row["tombstoned"]:
            return {"status": "tombstoned", "key": key, "version": row["version"]}
        allowed = loads(row["allowed_tenants"]) if row["allowed_tenants"] else None
        if allowed is not None and tenant not in allowed:
            return {
                "status": "access_revoked",
                "key": key,
                "version": row["version"],
            }
        return {
            "status": "ok",
            "key": key,
            "version": row["version"],
            "payload": loads(row["payload"]),
        }

    def upsert_shared(self, key, payload, allowed=None):
        with self.lock, db(self.paths.shared, wal=True) as c:
            row = c.execute(
                "SELECT MAX(version) AS v FROM dims WHERE dim_key=?", (key,)
            ).fetchone()
            version = (row["v"] or 0) + 1
            c.execute(
                "INSERT INTO dims(dim_key, version, payload, tombstoned,"
                " allowed_tenants, updated_at) VALUES(?,?,?,0,?,?)",
                (key, version, dumps(payload),
                 dumps(allowed) if allowed is not None else None, now_iso()),
            )
            return {"key": key, "version": version}

    def revise_shared(self, key, payload):
        """New version; visible to every tenant's next query immediately."""
        with self.lock, db(self.paths.shared, wal=True) as c:
            row = c.execute(
                "SELECT * FROM dims WHERE dim_key=? ORDER BY version DESC LIMIT 1",
                (key,),
            ).fetchone()
            if row is None:
                raise NotFound(f"shared dim not found: {key}")
            version = row["version"] + 1
            c.execute(
                "INSERT INTO dims(dim_key, version, payload, tombstoned,"
                " allowed_tenants, updated_at) VALUES(?,?,?,0,?,?)",
                (key, version, dumps(payload), row["allowed_tenants"], now_iso()),
            )
            return {"key": key, "version": version}

    def tombstone_shared(self, key):
        """Delete = tombstone: stops resolving for all tenants at once;
        stored historical refs are reported as tombstoned, not rewritten."""
        with self.lock, db(self.paths.shared, wal=True) as c:
            row = c.execute(
                "SELECT * FROM dims WHERE dim_key=? ORDER BY version DESC LIMIT 1",
                (key,),
            ).fetchone()
            if row is None:
                raise NotFound(f"shared dim not found: {key}")
            version = row["version"] + 1
            c.execute(
                "INSERT INTO dims(dim_key, version, payload, tombstoned,"
                " allowed_tenants, updated_at) VALUES(?,?,?,1,?,?)",
                (key, version, row["payload"], row["allowed_tenants"], now_iso()),
            )
            return {"key": key, "version": version, "tombstoned": True}

    def tighten_shared(self, key, allowed):
        """Permission tightening = new version with an ACL; excluded tenants
        stop resolving the dim on their very next query."""
        if not isinstance(allowed, list) or not allowed:
            raise BadRequest("allowed must be a non-empty list of tenants")
        for t in allowed:
            self.check_tenant(t)
        with self.lock, db(self.paths.shared, wal=True) as c:
            row = c.execute(
                "SELECT * FROM dims WHERE dim_key=? ORDER BY version DESC LIMIT 1",
                (key,),
            ).fetchone()
            if row is None:
                raise NotFound(f"shared dim not found: {key}")
            version = row["version"] + 1
            c.execute(
                "INSERT INTO dims(dim_key, version, payload, tombstoned,"
                " allowed_tenants, updated_at) VALUES(?,?,?,?,?,?)",
                (key, version, row["payload"], row["tombstoned"],
                 dumps(sorted(allowed)), now_iso()),
            )
            return {"key": key, "version": version, "allowed": sorted(allowed)}

    def list_shared_dims(self):
        with db(self.paths.shared, wal=True) as c:
            rows = c.execute(
                "SELECT d.* FROM dims d JOIN ("
                "  SELECT dim_key, MAX(version) v FROM dims GROUP BY dim_key"
                ") m ON d.dim_key=m.dim_key AND d.version=m.v ORDER BY d.dim_key"
            ).fetchall()
        return [
            {
                "key": r["dim_key"],
                "version": r["version"],
                "tombstoned": bool(r["tombstoned"]),
                "allowed": loads(r["allowed_tenants"]) if r["allowed_tenants"] else None,
                "payload": loads(r["payload"]),
            }
            for r in rows
        ]

    # ------------------------------------------------------------------
    # reference classification (the anti "filter-and-copy" core)
    # ------------------------------------------------------------------
    def classify_ref_legacy(self, c_legacy, c_shared, tenant, ref):
        """Classify a ref found on a legacy row owned by `tenant`."""
        if ref.startswith(SHARED_PREFIX):
            r = self.resolve_shared(c_shared, ref, tenant)
            if r["status"] == "ok":
                return OK_SHARED, f"shared v{r['version']}"
            return OK_SHARED, f"shared ref kept, resolves as {r['status']}"
        owner = parse_priv_owner(ref)
        if owner == tenant:
            row = c_legacy.execute(
                "SELECT 1 FROM private_dims WHERE dim_key=? AND tenant=?",
                (ref, tenant),
            ).fetchone()
            if row:
                return OK_OWN, "own private dim"
            return DANGLING, f"own private dim {ref} not found"
        if owner is not None:
            return CROSS_TENANT, f"private dim owned by {owner}"
        return DANGLING, f"unresolvable ref {ref}"

    def classify_ref_isolated(self, c_tenant, c_shared, tenant, ref):
        """Classify a ref in an isolated tenant catalog context."""
        if ref.startswith(SHARED_PREFIX):
            r = self.resolve_shared(c_shared, ref, tenant)
            if r["status"] == "ok":
                return OK_SHARED, f"shared v{r['version']}"
            return OK_SHARED, f"shared ref kept, resolves as {r['status']}"
        owner = parse_priv_owner(ref)
        if owner == tenant:
            row = c_tenant.execute(
                "SELECT 1 FROM dims WHERE dim_key=?", (ref,)
            ).fetchone()
            if row:
                return OK_OWN, "own private dim"
            return DANGLING, f"own private dim {ref} not found"
        if owner is not None:
            return CROSS_TENANT, f"private dim owned by {owner}"
        return DANGLING, f"unresolvable ref {ref}"

    # ------------------------------------------------------------------
    # ingest (routed; continuously available during migration)
    # ------------------------------------------------------------------
    def ingest_batch(self, tenant, facts, batch_id=None):
        """Ingest one batch. Always lands in the tenant's *current* store:
        legacy while not migrated (migrator catch-up replays it into
        staging), isolated catalog after cutover."""
        self.check_tenant(tenant)
        if not facts:
            raise BadRequest("batch must contain at least one fact")
        with self.lock:
            state = self.tenant_state(tenant)
            with db(self.paths.registry, wal=True) as rc:
                self._ensure_tenant(rc, tenant)
                seq = self._next_seq(rc)
                batch_id = batch_id or f"B-{tenant}-{seq:04d}"
                ts = now_iso()
                if state == "legacy":
                    with db(self.paths.legacy, wal=True) as c:
                        for f in facts:
                            c.execute(
                                "INSERT OR REPLACE INTO facts(fact_id, tenant,"
                                " payload, refs, batch_id, seq, migrated_out,"
                                " ingested_at) VALUES(?,?,?,?,?,?,0,?)",
                                (f["fact_id"], tenant, dumps(f["payload"]),
                                 dumps(f["refs"]), batch_id, seq, ts),
                            )
                        c.execute(
                            "INSERT OR REPLACE INTO batches(batch_id, tenant,"
                            " seq, fact_count, received_at) VALUES(?,?,?,?,?)",
                            (batch_id, tenant, seq, len(facts), ts),
                        )
                    dest = "legacy"
                else:
                    live = self.live_catalog_path(tenant)
                    if not os.path.exists(live):
                        raise Conflict(
                            f"tenant {tenant} migrated but catalog is missing")
                    quarantined = self._ingest_isolated(
                        live, tenant, facts, batch_id, seq, ts)
                    dest = "isolated"
                rc.execute(
                    "INSERT OR REPLACE INTO batch_log(batch_id, tenant, seq,"
                    " destination, fact_count, received_at) VALUES(?,?,?,?,?,?)",
                    (batch_id, tenant, seq, dest, len(facts), ts),
                )
        out = {"batch_id": batch_id, "tenant": tenant, "seq": seq,
               "destination": dest, "fact_count": len(facts)}
        if dest == "isolated" and quarantined:
            out["quarantined"] = quarantined
        return out

    def _ingest_isolated(self, live, tenant, facts, batch_id, seq, ts):
        """Post-migration ingest keeps the same reference hygiene: bad refs
        are quarantined, never silently carried."""
        quarantined = []
        with db(live) as tc, db(self.paths.shared, wal=True) as sc:
            for f in facts:
                kept, dropped = [], []
                for ref in f["refs"]:
                    kind, detail = self.classify_ref_isolated(tc, sc, tenant, ref)
                    if kind in (OK_OWN, OK_SHARED):
                        kept.append(ref)
                    else:
                        dropped.append((ref, kind, detail))
                tc.execute(
                    "INSERT OR REPLACE INTO facts(fact_id, payload, refs,"
                    " batch_id, origin_seq, ingested_at) VALUES(?,?,?,?,?,?)",
                    (f["fact_id"], dumps(f["payload"]), dumps(kept),
                     batch_id, seq, ts),
                )
                for ref, kind, detail in dropped:
                    tc.execute(
                        "INSERT OR REPLACE INTO quarantine(fact_id, ref_key,"
                        " reason, detail) VALUES(?,?,?,?)",
                        (f["fact_id"], ref, kind, detail),
                    )
                    quarantined.append({"fact_id": f["fact_id"], "ref": ref,
                                        "reason": kind})
            tc.execute(
                "INSERT OR REPLACE INTO applied_batches(batch_id, origin_seq,"
                " applied_at) VALUES(?,?,?)",
                (batch_id, seq, ts),
            )
        return quarantined

    def seed_private_dims(self, tenant, dims):
        """Seed helper: tenant-private dims live in the legacy shared table
        until the tenant migrates."""
        self.check_tenant(tenant)
        with self.lock, db(self.paths.legacy, wal=True) as c:
            for key, payload in dims.items():
                owner = parse_priv_owner(key)
                if owner != tenant:
                    raise BadRequest(f"dim {key} is not owned by {tenant}")
                c.execute(
                    "INSERT OR REPLACE INTO private_dims(dim_key, tenant,"
                    " payload) VALUES(?,?,?)",
                    (key, tenant, dumps(payload)),
                )

    # ------------------------------------------------------------------
    # queries
    # ------------------------------------------------------------------
    def query(self, tenant):
        """Tenant-scoped query, routed by tenant state. Never touches any
        staging catalog, never opens another tenant's files."""
        self.check_tenant(tenant)
        with self.lock:
            state = self.tenant_state(tenant)
            if state == "legacy":
                return self._query_legacy(tenant)
            return self._query_isolated(tenant)

    def _query_legacy(self, tenant):
        with db(self.paths.legacy, wal=True) as lc, \
                db(self.paths.shared, wal=True) as sc:
            rows = lc.execute(
                "SELECT * FROM facts WHERE tenant=? AND migrated_out=0"
                " ORDER BY seq, fact_id", (tenant,),
            ).fetchall()
            facts = [self._present_fact_legacy(lc, sc, tenant, r) for r in rows]
        return {"tenant": tenant, "served_by": "legacy", "facts": facts}

    def _present_fact_legacy(self, lc, sc, tenant, row):
        refs = []
        for ref in loads(row["refs"]):
            kind, detail = self.classify_ref_legacy(lc, sc, tenant, ref)
            entry = {"key": ref}
            if kind == OK_OWN:
                payload = lc.execute(
                    "SELECT payload FROM private_dims WHERE dim_key=? AND tenant=?",
                    (ref, tenant)).fetchone()["payload"]
                entry.update(status="ok", kind="own", payload=loads(payload))
            elif kind == OK_SHARED:
                r = self.resolve_shared(sc, ref, tenant)
                entry.update(status=r["status"], kind="shared",
                             version=r.get("version"), payload=r.get("payload"))
            elif kind == CROSS_TENANT:
                # the pre-existing error, visible but NOT resolved: we never
                # read the other tenant's dim payload
                entry.update(status="cross_tenant", kind="error", detail=detail)
            else:
                entry.update(status="dangling", kind="error", detail=detail)
            refs.append(entry)
        return {"fact_id": row["fact_id"], "payload": loads(row["payload"]),
                "batch_id": row["batch_id"], "refs": refs}

    def _query_isolated(self, tenant):
        live = self.live_catalog_path(tenant)
        if not os.path.exists(live):
            raise Conflict(f"tenant {tenant} migrated but catalog is missing")
        with db(live) as tc, db(self.paths.shared, wal=True) as sc:
            rows = tc.execute(
                "SELECT * FROM facts ORDER BY origin_seq, fact_id").fetchall()
            qrows = tc.execute("SELECT * FROM quarantine").fetchall()
            qmap = {}
            for q in qrows:
                qmap.setdefault(q["fact_id"], []).append(
                    {"key": q["ref_key"], "reason": q["reason"],
                     "detail": q["detail"]})
            facts = []
            for row in rows:
                refs = []
                for ref in loads(row["refs"]):
                    entry = {"key": ref}
                    if ref.startswith(SHARED_PREFIX):
                        r = self.resolve_shared(sc, ref, tenant)
                        entry.update(status=r["status"], kind="shared",
                                     version=r.get("version"),
                                     payload=r.get("payload"))
                    else:
                        d = tc.execute(
                            "SELECT payload FROM dims WHERE dim_key=?",
                            (ref,)).fetchone()
                        if d:
                            entry.update(status="ok", kind="own",
                                         payload=loads(d["payload"]))
                        else:
                            entry.update(status="dangling", kind="error")
                    refs.append(entry)
                facts.append({
                    "fact_id": row["fact_id"],
                    "payload": loads(row["payload"]),
                    "batch_id": row["batch_id"],
                    "refs": refs,
                    "quarantined_refs": qmap.get(row["fact_id"], []),
                })
        return {"tenant": tenant, "served_by": "isolated", "facts": facts}

    def compare(self, tenant):
        """对照查询: run the tenant's view against the historical legacy
        rows and the isolated catalog; the only allowed differences are
        exactly the quarantined references."""
        self.check_tenant(tenant)
        if self.tenant_state(tenant) != "migrated":
            return {"tenant": tenant, "compared": False,
                    "reason": "tenant is not migrated"}
        with db(self.paths.legacy, wal=True) as lc:
            legacy_rows = lc.execute(
                "SELECT * FROM facts WHERE tenant=? ORDER BY fact_id",
                (tenant,)).fetchall()
        live = self.live_catalog_path(tenant)
        with db(live) as tc:
            new_rows = tc.execute("SELECT * FROM facts ORDER BY fact_id").fetchall()
            qrows = tc.execute("SELECT * FROM quarantine").fetchall()
        quarantined = {(q["fact_id"], q["ref_key"]) for q in qrows}
        legacy_map = {r["fact_id"]: set(loads(r["refs"])) for r in legacy_rows}
        new_map = {r["fact_id"]: (set(loads(r["refs"])), r["batch_id"])
                   for r in new_rows}
        # batches ingested after cutover land only in the isolated catalog;
        # they are expected additions, not migration discrepancies
        with db(self.paths.registry, wal=True) as rc:
            post_cutover = {r["batch_id"] for r in rc.execute(
                "SELECT batch_id FROM batch_log WHERE tenant=?"
                " AND destination='isolated'", (tenant,))}

        unexpected, expected = [], []
        for fid in sorted(set(legacy_map) | set(new_map)):
            if fid not in new_map:
                unexpected.append({"fact_id": fid, "issue": "missing_in_isolated"})
                continue
            if fid not in legacy_map:
                if new_map[fid][1] in post_cutover:
                    expected.append({"fact_id": fid,
                                     "issue": "post_cutover_batch"})
                else:
                    unexpected.append({"fact_id": fid,
                                       "issue": "extra_in_isolated"})
                continue
            new_refs = new_map[fid][0]
            removed = legacy_map[fid] - new_refs
            added = new_refs - legacy_map[fid]
            for ref in sorted(removed):
                item = {"fact_id": fid, "ref": ref}
                if (fid, ref) in quarantined:
                    expected.append({**item, "issue": "quarantined_ref_removed"})
                else:
                    unexpected.append({**item, "issue": "ref_lost_without_quarantine"})
            for ref in sorted(added):
                unexpected.append({"fact_id": fid, "ref": ref,
                                   "issue": "ref_added"})
        return {
            "tenant": tenant,
            "compared": True,
            "legacy_facts": len(legacy_map),
            "isolated_facts": len(new_map),
            "expected_differences": expected,
            "unexpected_differences": unexpected,
            "match": not unexpected,
        }

    # ------------------------------------------------------------------
    # dashboard / admin
    # ------------------------------------------------------------------
    def dashboard(self):
        with db(self.paths.registry, wal=True) as rc:
            tenants = [r["tenant"] for r in rc.execute(
                "SELECT tenant FROM tenants ORDER BY tenant")]
            states = {r["tenant"]: r["state"] for r in rc.execute(
                "SELECT tenant, state FROM tenants")}
            migrations = [dict(r) for r in rc.execute(
                "SELECT * FROM migrations ORDER BY created_at DESC, id DESC")]
            batch_log = [dict(r) for r in rc.execute(
                "SELECT * FROM batch_log ORDER BY seq DESC LIMIT 20")]
        out_tenants = []
        for t in tenants:
            with db(self.paths.legacy, wal=True) as lc:
                legacy_count = lc.execute(
                    "SELECT COUNT(*) n FROM facts WHERE tenant=? AND migrated_out=0",
                    (t,)).fetchone()["n"]
            live = self.paths.tenant_live(t)
            isolated_count = None
            quarantine_count = None
            if os.path.exists(live):
                with db(live) as tc:
                    isolated_count = tc.execute(
                        "SELECT COUNT(*) n FROM facts").fetchone()["n"]
                    quarantine_count = tc.execute(
                        "SELECT COUNT(*) n FROM quarantine").fetchone()["n"]
            active = next((m for m in migrations
                           if m["tenant"] == t and m["state"] in
                           ("COPYING", "CATCHING_UP")), None)
            last = next((m for m in migrations if m["tenant"] == t), None)
            active_view = None
            if active:
                active_view = self._migration_view(active)
                active_view["lag"] = self._lag(active)
            out_tenants.append({
                "tenant": t,
                "state": states[t],
                "legacy_facts": legacy_count,
                "isolated_facts": isolated_count,
                "quarantined_refs": quarantine_count,
                "active_migration": active_view,
                "last_migration": self._migration_view(last) if last else None,
            })
        return {"tenants": out_tenants,
                "shared_dims": self.list_shared_dims(),
                "recent_batches": batch_log}

    def _migration_view(self, m):
        return {"id": m["id"], "tenant": m["tenant"], "state": m["state"],
                "high_water": m["high_water"],
                "conflicts": loads(m["conflicts"]),
                "created_at": m["created_at"], "updated_at": m["updated_at"]}

    def _lag(self, migration_row):
        """Batches in legacy not yet replayed into the staging catalog."""
        staging = migration_row["staging_path"]
        if not staging or not os.path.exists(staging):
            return 0
        tenant = migration_row["tenant"]
        with db(self.paths.legacy, wal=True) as lc, db(staging) as tc:
            applied = {r["batch_id"] for r in
                       tc.execute("SELECT batch_id FROM applied_batches")}
            all_ids = {r["batch_id"] for r in lc.execute(
                "SELECT batch_id FROM batches WHERE tenant=?", (tenant,))}
        return len(all_ids - applied)

    def reset(self):
        """Back to an empty catalog (used by the demo and the verifier)."""
        with self.lock:
            if os.path.exists(self.paths.root):
                shutil.rmtree(self.paths.root)
            store.init_all(self.paths)

    # ------------------------------------------------------------------
    # isolation self-checks (also exposed via the API for the web UI)
    # ------------------------------------------------------------------
    def isolation_check(self):
        checks = []

        def check(name, fn):
            try:
                fn()
                checks.append({"name": name, "pass": True})
            except Exception as e:  # noqa: BLE001 - report anything
                checks.append({"name": name, "pass": False, "detail": str(e)})

        def bad_names_rejected():
            for bad in ("../GLOBEX", "ACME/../x", "", "a", "AC ME", "AC.ME"):
                try:
                    self.check_tenant(bad)
                except BadRequest:
                    continue
                raise AssertionError(f"tenant name accepted: {bad!r}")

        def paths_stay_in_tenant_dir():
            for t in ("ACME", "GLOBEX"):
                p = os.path.realpath(self.live_catalog_path(t))
                base = os.path.realpath(self.paths.tenant_dir(t))
                assert p.startswith(base + os.sep), p
                assert "staging_" not in os.path.basename(p)

        def queries_never_leak_foreign_markers():
            with db(self.paths.registry, wal=True) as rc:
                tenants = [r["tenant"] for r in
                           rc.execute("SELECT tenant FROM tenants")]
            for t in tenants:
                res = self.query(t)
                others = [o for o in tenants if o != t]
                for f in res["facts"]:
                    for o in others:
                        assert not f["fact_id"].startswith(f"TRK-{o}-"), (
                            f"{t} query returned {o} fact {f['fact_id']}")
                    for ref in f["refs"]:
                        if ref.get("kind") == "own" or ref.get("payload"):
                            owner = parse_priv_owner(ref["key"])
                            assert owner in (None, t), (
                                f"{t} query resolved dim payload of {owner}")

        check("tenant-name validation rejects traversal", bad_names_rejected)
        check("live catalog path stays inside tenant dir",
              paths_stay_in_tenant_dir)
        check("queries never leak other tenants' markers",
              queries_never_leak_foreign_markers)
        return {"checks": checks,
                "pass": all(c["pass"] for c in checks)}
