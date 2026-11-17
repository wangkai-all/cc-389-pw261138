"""Migrator: preview -> copy (with reference-closure validation) ->
catch-up of continuously arriving batches -> verified cutover, with
rollback for incomplete migrations.

Why not "filter by tenant and copy": rows in the shared table can carry
references to *other* tenants' private dimensions (pre-existing data
errors). A naive `WHERE tenant=?` copy would carry those cross-tenant
references into the new isolated catalog. Every copied row therefore
goes through reference-closure validation; bad references are moved to
a quarantine table (and the migration's conflict report), never into
the tenant's validated ref set.
"""
import os

from . import store
from .service import (CROSS_TENANT, DANGLING, OK_OWN, OK_SHARED, Conflict,
                      NotFound)
from .store import db, dumps, loads, now_iso

ACTIVE_STATES = ("COPYING", "CATCHING_UP")


class Migrator:
    def __init__(self, service):
        self.svc = service
        self.paths = service.paths

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------
    def active_migration(self, tenant):
        with db(self.paths.registry, wal=True) as c:
            row = c.execute(
                "SELECT * FROM migrations WHERE tenant=? AND state IN"
                " ('COPYING','CATCHING_UP') ORDER BY created_at DESC LIMIT 1",
                (tenant,),
            ).fetchone()
        return dict(row) if row else None

    def _require_active(self, tenant):
        m = self.active_migration(tenant)
        if m is None:
            raise Conflict(f"tenant {tenant} has no incomplete migration")
        return m

    def _update_migration(self, c, mid, **fields):
        sets = ", ".join(f"{k}=?" for k in fields) + ", updated_at=?"
        vals = list(fields.values()) + [now_iso(), mid]
        c.execute(f"UPDATE migrations SET {sets} WHERE id=?", vals)

    def lag(self, tenant):
        """Batches received in legacy but not yet replayed into staging."""
        m = self.active_migration(tenant)
        if m is None:
            return 0
        return self.svc._lag(m)

    # ------------------------------------------------------------------
    # 1. preview (dry-run, writes nothing)
    # ------------------------------------------------------------------
    def preview(self, tenant):
        self.svc.check_tenant(tenant)
        report = {"tenant": tenant, "dry_run": True, "fact_count": 0,
                  "own_dims": 0, "refs": {OK_OWN: 0, OK_SHARED: 0,
                                          CROSS_TENANT: 0, DANGLING: 0},
                  "conflicts": [], "warnings": []}
        with db(self.paths.legacy, wal=True) as lc, \
                db(self.paths.shared, wal=True) as sc:
            rows = lc.execute(
                "SELECT * FROM facts WHERE tenant=? AND migrated_out=0"
                " ORDER BY seq", (tenant,),
            ).fetchall()
            report["own_dims"] = lc.execute(
                "SELECT COUNT(*) n FROM private_dims WHERE tenant=?",
                (tenant,)).fetchone()["n"]
            for row in rows:
                report["fact_count"] += 1
                for ref in loads(row["refs"]):
                    kind, detail = self.svc.classify_ref_legacy(
                        lc, sc, tenant, ref)
                    report["refs"][kind] += 1
                    if kind in (CROSS_TENANT, DANGLING):
                        report["conflicts"].append({
                            "fact_id": row["fact_id"], "ref": ref,
                            "reason": kind, "detail": detail})
                    elif kind == OK_SHARED and "resolves as" in detail:
                        report["warnings"].append({
                            "fact_id": row["fact_id"], "ref": ref,
                            "reason": detail})
        return report

    # ------------------------------------------------------------------
    # 2. start: create staging catalog + validated initial copy
    # ------------------------------------------------------------------
    def start(self, tenant):
        self.svc.check_tenant(tenant)
        with self.svc.lock:
            if self.svc.tenant_state(tenant) != "legacy":
                raise Conflict(f"tenant {tenant} is already migrated")
            if self.active_migration(tenant):
                raise Conflict(f"tenant {tenant} already has an active migration")
            with db(self.paths.registry, wal=True) as rc:
                n = rc.execute(
                    "SELECT COUNT(*) n FROM migrations WHERE tenant=?",
                    (tenant,)).fetchone()["n"]
                mid = f"MIG-{tenant}-{n + 1}"
                staging = self.paths.tenant_staging(tenant, mid)
                ts = now_iso()
                rc.execute(
                    "INSERT INTO migrations(id, tenant, state, high_water,"
                    " conflicts, staging_path, created_at, updated_at)"
                    " VALUES(?,?, 'COPYING', 0, '[]', ?, ?, ?)",
                    (mid, tenant, staging, ts, ts),
                )
                high_water = int(rc.execute(
                    "SELECT value FROM meta WHERE key='seq'").fetchone()[0])
            try:
                conflicts = self._initial_copy(tenant, staging)
            except Exception:
                if os.path.exists(staging):
                    os.remove(staging)
                with db(self.paths.registry, wal=True) as rc:
                    self._update_migration(rc, mid, state="FAILED")
                raise
            with db(self.paths.registry, wal=True) as rc:
                self._update_migration(
                    rc, mid, state="CATCHING_UP", high_water=high_water,
                    conflicts=dumps(conflicts))
        return {"migration_id": mid, "tenant": tenant, "state": "CATCHING_UP",
                "high_water": high_water, "conflicts": conflicts,
                "lag": self.lag(tenant)}

    def _initial_copy(self, tenant, staging):
        store.init_tenant_db(staging)
        conflicts = []
        with db(self.paths.legacy, wal=True) as lc, \
                db(self.paths.shared, wal=True) as sc, \
                db(staging) as tc:
            # tenant's own private dimensions move into the isolated catalog
            for d in lc.execute(
                    "SELECT * FROM private_dims WHERE tenant=?", (tenant,)):
                tc.execute(
                    "INSERT OR REPLACE INTO dims(dim_key, payload) VALUES(?,?)",
                    (d["dim_key"], d["payload"]))
            rows = lc.execute(
                "SELECT * FROM facts WHERE tenant=? AND migrated_out=0"
                " ORDER BY seq", (tenant,),
            ).fetchall()
            for row in rows:
                conflicts += self._copy_one(lc, sc, tc, tenant, row)
            # every batch received up to the high-water mark is covered by
            # the initial copy; only newer batches need catch-up
            for b in lc.execute(
                    "SELECT * FROM batches WHERE tenant=?", (tenant,)):
                tc.execute(
                    "INSERT OR IGNORE INTO applied_batches(batch_id,"
                    " origin_seq, applied_at) VALUES(?,?,?)",
                    (b["batch_id"], b["seq"], now_iso()))
        return conflicts

    def _copy_one(self, lc, sc, tc, tenant, row):
        """Validate one legacy row's references and copy it into staging.
        Returns conflict records for refs that were refused."""
        conflicts = []
        kept = []
        for ref in loads(row["refs"]):
            kind, detail = self.svc.classify_ref_legacy(lc, sc, tenant, ref)
            if kind in (OK_OWN, OK_SHARED):
                kept.append(ref)
            else:
                tc.execute(
                    "INSERT OR REPLACE INTO quarantine(fact_id, ref_key,"
                    " reason, detail) VALUES(?,?,?,?)",
                    (row["fact_id"], ref, kind, detail))
                conflicts.append({"fact_id": row["fact_id"], "ref": ref,
                                  "reason": kind, "detail": detail})
        tc.execute(
            "INSERT OR REPLACE INTO facts(fact_id, payload, refs, batch_id,"
            " origin_seq, ingested_at) VALUES(?,?,?,?,?,?)",
            (row["fact_id"], row["payload"], dumps(kept), row["batch_id"],
             row["seq"], row["ingested_at"]),
        )
        return conflicts

    # ------------------------------------------------------------------
    # 3. catch-up: replay batches that arrived after the high-water mark
    # ------------------------------------------------------------------
    def catchup(self, tenant):
        self.svc.check_tenant(tenant)
        with self.svc.lock:
            m = self._require_active(tenant)
            staging = m["staging_path"]
            applied, new_conflicts = [], []
            with db(self.paths.legacy, wal=True) as lc, \
                    db(self.paths.shared, wal=True) as sc, \
                    db(staging) as tc:
                done = {r["batch_id"] for r in tc.execute(
                    "SELECT batch_id FROM applied_batches")}
                pending = lc.execute(
                    "SELECT * FROM batches WHERE tenant=? ORDER BY seq",
                    (tenant,)).fetchall()
                for b in pending:
                    if b["batch_id"] in done:
                        continue
                    rows = lc.execute(
                        "SELECT * FROM facts WHERE batch_id=? AND tenant=?"
                        " ORDER BY seq", (b["batch_id"], tenant)).fetchall()
                    for row in rows:
                        new_conflicts += self._copy_one(lc, sc, tc, tenant, row)
                    tc.execute(
                        "INSERT OR IGNORE INTO applied_batches(batch_id,"
                        " origin_seq, applied_at) VALUES(?,?,?)",
                        (b["batch_id"], b["seq"], now_iso()))
                    applied.append(b["batch_id"])
            if new_conflicts:
                with db(self.paths.registry, wal=True) as rc:
                    cur = loads(rc.execute(
                        "SELECT conflicts FROM migrations WHERE id=?",
                        (m["id"],)).fetchone()["conflicts"])
                    self._update_migration(
                        rc, m["id"], conflicts=dumps(cur + new_conflicts))
            return {"migration_id": m["id"], "applied_batches": applied,
                    "new_conflicts": new_conflicts, "lag": self.lag(tenant)}

    # ------------------------------------------------------------------
    # 4. cutover: final catch-up + verification + atomic switch
    # ------------------------------------------------------------------
    def cutover(self, tenant):
        self.svc.check_tenant(tenant)
        with self.svc.lock:
            m = self._require_active(tenant)
            self.catchup(tenant)  # final replay under the same lock
            if self.lag(tenant) != 0:
                raise Conflict(f"tenant {tenant} still has pending batches")
            staging = m["staging_path"]
            with db(self.paths.legacy, wal=True) as lc, db(staging) as tc:
                legacy_count = lc.execute(
                    "SELECT COUNT(*) n FROM facts WHERE tenant=?"
                    " AND migrated_out=0", (tenant,)).fetchone()["n"]
                staged_count = tc.execute(
                    "SELECT COUNT(*) n FROM facts").fetchone()["n"]
                orphan_quarantine = tc.execute(
                    "SELECT COUNT(*) n FROM quarantine q WHERE NOT EXISTS"
                    " (SELECT 1 FROM facts f WHERE f.fact_id=q.fact_id)"
                ).fetchone()["n"]
            if staged_count != legacy_count:
                raise Conflict(
                    f"verification failed: legacy has {legacy_count} facts,"
                    f" staging has {staged_count}")
            if orphan_quarantine:
                raise Conflict("verification failed: orphan quarantine rows")
            live = self.paths.tenant_live(tenant)
            os.replace(staging, live)  # staging -> live, same tenant dir
            with db(self.paths.registry, wal=True) as rc:
                rc.execute("UPDATE tenants SET state='migrated' WHERE tenant=?",
                           (tenant,))
                self._update_migration(rc, m["id"], state="DONE")
            with db(self.paths.legacy, wal=True) as lc:
                lc.execute(
                    "UPDATE facts SET migrated_out=1 WHERE tenant=?", (tenant,))
            return {"migration_id": m["id"], "tenant": tenant, "state": "DONE",
                    "facts_migrated": staged_count}

    # ------------------------------------------------------------------
    # 5. rollback: only for incomplete migrations
    # ------------------------------------------------------------------
    def rollback(self, tenant):
        self.svc.check_tenant(tenant)
        with self.svc.lock:
            m = self._require_active(tenant)
            staging = m["staging_path"]
            if os.path.exists(staging):
                os.remove(staging)
            with db(self.paths.registry, wal=True) as rc:
                self._update_migration(rc, m["id"], state="ROLLED_BACK",
                                       staging_path=None)
            # legacy rows were never touched (migrated_out is only set at
            # cutover), so the tenant simply keeps querying legacy; batches
            # received during the attempt remain safely in legacy.
            return {"migration_id": m["id"], "tenant": tenant,
                    "state": "ROLLED_BACK"}
