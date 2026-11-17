"""Storage layer: paths, schema, connection helpers.

Layout under the data root:
  legacy.db            - pre-migration shared tables (facts tagged by tenant,
                         tenant-private dims, ingest batches)
  shared_dims.db       - read-only shared dimension store (versioned,
                         tombstones, per-tenant ACL)
  registry.db          - control plane: tenant states, migrations, batch log,
                         global sequence
  tenants/<T>/catalog.db            - live isolated catalog (post-cutover)
  tenants/<T>/staging_<mid>.db      - temporary migration catalog (never
                                      served to queries)
"""
import json
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone


def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class Paths:
    def __init__(self, root):
        self.root = root
        self.legacy = os.path.join(root, "legacy.db")
        self.shared = os.path.join(root, "shared_dims.db")
        self.registry = os.path.join(root, "registry.db")
        self.tenants_dir = os.path.join(root, "tenants")

    def tenant_dir(self, tenant):
        return os.path.join(self.tenants_dir, tenant)

    def tenant_live(self, tenant):
        return os.path.join(self.tenant_dir(tenant), "catalog.db")

    def tenant_staging(self, tenant, migration_id):
        return os.path.join(self.tenant_dir(tenant), f"staging_{migration_id}.db")


def _connect(path, wal):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = sqlite3.connect(path, timeout=30)
    conn.row_factory = sqlite3.Row
    if wal:
        conn.execute("PRAGMA journal_mode=WAL")
    return conn


@contextmanager
def db(path, wal=False):
    """Open a connection, commit on success, always close."""
    conn = _connect(path, wal)
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


LEGACY_SCHEMA = """
CREATE TABLE IF NOT EXISTS facts (
  fact_id      TEXT PRIMARY KEY,        -- traceable marker, e.g. TRK-ACME-000001
  tenant       TEXT NOT NULL,
  payload      TEXT NOT NULL,           -- JSON
  refs         TEXT NOT NULL,           -- JSON array of dimension keys
  batch_id     TEXT NOT NULL,
  seq          INTEGER NOT NULL,        -- global ingest sequence
  migrated_out INTEGER NOT NULL DEFAULT 0,
  ingested_at  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_facts_tenant ON facts(tenant, migrated_out);
CREATE TABLE IF NOT EXISTS private_dims (
  dim_key TEXT NOT NULL,                -- PRIV/<TENANT>/<name>
  tenant  TEXT NOT NULL,                -- owning tenant
  payload TEXT NOT NULL,
  PRIMARY KEY (dim_key, tenant)
);
CREATE TABLE IF NOT EXISTS batches (
  batch_id   TEXT PRIMARY KEY,          -- B-<TENANT>-<seq>
  tenant     TEXT NOT NULL,
  seq        INTEGER NOT NULL,
  fact_count INTEGER NOT NULL,
  received_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_batches_tenant_seq ON batches(tenant, seq);
"""

SHARED_SCHEMA = """
CREATE TABLE IF NOT EXISTS dims (
  dim_key         TEXT NOT NULL,        -- SHARED/<domain>/<name>
  version         INTEGER NOT NULL,
  payload         TEXT NOT NULL,
  tombstoned      INTEGER NOT NULL DEFAULT 0,
  allowed_tenants TEXT,                 -- JSON array; NULL = all tenants
  updated_at      TEXT NOT NULL,
  PRIMARY KEY (dim_key, version)
);
"""

TENANT_SCHEMA = """
CREATE TABLE IF NOT EXISTS facts (
  fact_id     TEXT PRIMARY KEY,
  payload     TEXT NOT NULL,
  refs        TEXT NOT NULL,            -- JSON array of *validated* refs
  batch_id    TEXT NOT NULL,
  origin_seq  INTEGER NOT NULL,         -- legacy ingest seq (idempotent catch-up)
  ingested_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS dims (       -- the tenant's own private dims, isolated
  dim_key TEXT PRIMARY KEY,
  payload TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS quarantine ( -- refs refused during migration/ingest
  fact_id TEXT NOT NULL,
  ref_key TEXT NOT NULL,
  reason  TEXT NOT NULL,                -- cross_tenant_ref | dangling_ref
  detail  TEXT NOT NULL,
  PRIMARY KEY (fact_id, ref_key)
);
CREATE TABLE IF NOT EXISTS applied_batches (
  batch_id   TEXT PRIMARY KEY,
  origin_seq INTEGER NOT NULL,
  applied_at TEXT NOT NULL
);
"""

REGISTRY_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS tenants (
  tenant TEXT PRIMARY KEY,
  state  TEXT NOT NULL DEFAULT 'legacy' -- legacy | migrated
);
CREATE TABLE IF NOT EXISTS migrations (
  id           TEXT PRIMARY KEY,        -- MIG-<tenant>-<n>
  tenant       TEXT NOT NULL,
  state        TEXT NOT NULL,           -- COPYING | CATCHING_UP | DONE |
                                        -- ROLLED_BACK | FAILED
  high_water   INTEGER NOT NULL DEFAULT 0,
  conflicts    TEXT NOT NULL DEFAULT '[]',
  staging_path TEXT,
  created_at   TEXT NOT NULL,
  updated_at   TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS batch_log (  -- audit log of every received batch
  batch_id    TEXT PRIMARY KEY,
  tenant      TEXT NOT NULL,
  seq         INTEGER NOT NULL,
  destination TEXT NOT NULL,            -- legacy | isolated
  fact_count  INTEGER NOT NULL,
  received_at TEXT NOT NULL
);
"""


def init_all(paths):
    os.makedirs(paths.tenants_dir, exist_ok=True)
    with db(paths.legacy, wal=True) as c:
        c.executescript(LEGACY_SCHEMA)
    with db(paths.shared, wal=True) as c:
        c.executescript(SHARED_SCHEMA)
    with db(paths.registry, wal=True) as c:
        c.executescript(REGISTRY_SCHEMA)
        c.execute("INSERT OR IGNORE INTO meta(key, value) VALUES('seq', '0')")


def init_tenant_db(path):
    with db(path) as c:
        c.executescript(TENANT_SCHEMA)


def dumps(obj):
    return json.dumps(obj, ensure_ascii=False, sort_keys=True)


def loads(text):
    return json.loads(text)
