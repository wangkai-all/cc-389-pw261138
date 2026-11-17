"""Seed data: fictional organizations with traceable markers, plus
deliberately planted cross-tenant reference errors so the migrator's
conflict detection has something real to catch.

Markers
-------
* facts:        TRK-<TENANT>-<seq>   (globally traceable)
* batches:      B-<TENANT>-<seq>
* private dims: PRIV-<TENANT>-<name> stored as PRIV/<TENANT>/<name>
* shared dims:  SHARED/<domain>/<name>
"""
from .service import CatalogService

TENANTS = ["ACME", "GLOBEX", "INITECH"]

SHARED_DIMS = {
    "SHARED/GEO/CN": {"name": "China", "region": "APAC"},
    "SHARED/GEO/US": {"name": "United States", "region": "NA"},
    "SHARED/UNIT/KG": {"name": "kilogram"},
    "SHARED/UNIT/M": {"name": "metre"},
}

PRIVATE_DIMS = {
    "ACME": {
        "PRIV/ACME/WH-SHANGHAI": {"name": "ACME Shanghai warehouse"},
        "PRIV/ACME/SKU-1001": {"name": "ACME anvil, 20kg"},
    },
    "GLOBEX": {
        "PRIV/GLOBEX/WH-NEWYORK": {"name": "Globex New York warehouse"},
        "PRIV/GLOBEX/SKU-9001": {"name": "Globex flux capacitor"},
    },
    "INITECH": {
        "PRIV/INITECH/WH-AUSTIN": {"name": "Initech Austin warehouse"},
    },
}

# (refs) per fact; TRK-ACME-000003 / TRK-ACME-000005 / TRK-INITECH-000002
# carry planted errors on purpose.
SEED_FACTS = {
    "ACME": [
        ["SHARED/GEO/CN", "PRIV/ACME/WH-SHANGHAI"],
        ["SHARED/UNIT/KG", "PRIV/ACME/SKU-1001"],
        ["SHARED/GEO/CN", "PRIV/GLOBEX/WH-NEWYORK"],   # cross-tenant error
        ["SHARED/GEO/US"],
        ["PRIV/ACME/NOPE"],                            # dangling error
        ["SHARED/UNIT/M", "PRIV/ACME/WH-SHANGHAI"],
    ],
    "GLOBEX": [
        ["SHARED/GEO/US", "PRIV/GLOBEX/WH-NEWYORK"],
        ["SHARED/UNIT/KG", "PRIV/GLOBEX/SKU-9001"],
        ["SHARED/GEO/CN"],
    ],
    "INITECH": [
        ["SHARED/GEO/US", "PRIV/INITECH/WH-AUSTIN"],
        ["PRIV/ACME/SKU-1001"],                        # cross-tenant error
    ],
}

# deterministic target for planted errors in generated batches
_ERROR_TARGET = {"ACME": "PRIV/GLOBEX/WH-NEWYORK",
                 "GLOBEX": "PRIV/ACME/SKU-1001",
                 "INITECH": "PRIV/ACME/SKU-1001"}
_OK_REFS = {"ACME": ["SHARED/GEO/CN", "PRIV/ACME/WH-SHANGHAI"],
            "GLOBEX": ["SHARED/GEO/US", "PRIV/GLOBEX/WH-NEWYORK"],
            "INITECH": ["SHARED/GEO/US", "PRIV/INITECH/WH-AUSTIN"]}


def _fact(svc, tenant, refs, note):
    fid = svc.new_fact_id(tenant)
    return {"fact_id": fid,
            "payload": {"marker": fid, "org": tenant, "note": note},
            "refs": list(refs)}


def seed_demo(svc: CatalogService):
    """Reset to an empty catalog, then load the fictional orgs."""
    svc.reset()
    for key, payload in SHARED_DIMS.items():
        svc.upsert_shared(key, payload)
    for tenant in TENANTS:
        svc.seed_private_dims(tenant, PRIVATE_DIMS[tenant])
        facts = [_fact(svc, tenant, refs, "seed")
                 for refs in SEED_FACTS[tenant]]
        svc.ingest_batch(tenant, facts)
    return {"tenants": TENANTS,
            "facts_per_tenant": {t: len(SEED_FACTS[t]) for t in TENANTS}}


def gen_batch(svc: CatalogService, tenant: str, n=2, plant_error=False):
    """Generate and ingest a new batch for a tenant (used by the UI button
    and by the verifier to simulate continuously arriving data)."""
    svc.check_tenant(tenant)
    facts = []
    for i in range(n):
        refs = list(_OK_REFS.get(tenant, ["SHARED/GEO/US"]))
        facts.append(_fact(svc, tenant, refs, "live-batch"))
    if plant_error:
        target = _ERROR_TARGET.get(tenant, "PRIV/UNKNOWN/FOREIGN-DIM")
        facts.append(_fact(svc, tenant, [target],
                           "planted-cross-tenant-error"))
    return svc.ingest_batch(tenant, facts)
