#!/usr/bin/env python3
"""End-to-end verification, starting from an EMPTY catalog directory.

Covers: seeding fictional orgs with traceable markers, conflict preview,
migration of one tenant while new batches keep arriving, verified cutover,
comparison queries, rollback of an incomplete migration, shared-dimension
propagation (revise / tombstone / ACL tightening), tenant-isolation
boundaries, and an HTTP smoke test of the real server.

Run:  python3 scripts/verify_e2e.py
"""
import json
import os
import shutil
import sys
import tempfile
import threading
import urllib.request
from http.server import ThreadingHTTPServer

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from catalog.migrator import Migrator
from catalog.seed import gen_batch, seed_demo
from catalog.service import BadRequest, CatalogService, Conflict
import server as server_mod

RESULTS = []


def check(name, cond, detail=""):
    RESULTS.append((name, bool(cond), detail))
    mark = "PASS" if cond else "FAIL"
    line = f"[{mark}] {name}"
    if detail:
        line += f"  -- {detail}"
    print(line)


def main():
    data_dir = tempfile.mkdtemp(prefix="catalog_verify_")
    print(f"verify data dir (starts empty): {data_dir}\n")
    svc = CatalogService(data_dir)
    mig = Migrator(svc)

    # ---- phase 0: empty catalog -------------------------------------
    dash = svc.dashboard()
    check("空目录初始化后无租户", dash["tenants"] == [])
    check("空目录初始化后无共享维度", dash["shared_dims"] == [])

    # ---- phase 1: seed fictional orgs --------------------------------
    seed_demo(svc)
    dash = svc.dashboard()
    tenants = {t["tenant"]: t for t in dash["tenants"]}
    check("种子租户 ACME/GLOBEX/INITECH 就绪",
          sorted(tenants) == ["ACME", "GLOBEX", "INITECH"])
    check("ACME 初始 6 条事实(legacy)", tenants["ACME"]["legacy_facts"] == 6)
    check("共享维度 4 个", len(dash["shared_dims"]) == 4)

    # ---- phase 2: preview is a dry-run --------------------------------
    rep = mig.preview("ACME")
    conflicts = {(c["fact_id"], c["ref"], c["reason"]) for c in rep["conflicts"]}
    check("预览发现预置跨租户错误引用",
          ("TRK-ACME-000003", "PRIV/GLOBEX/WH-NEWYORK", "cross_tenant_ref")
          in conflicts, json.dumps(rep["conflicts"], ensure_ascii=False))
    check("预览发现预置悬空引用",
          ("TRK-ACME-000005", "PRIV/ACME/NOPE", "dangling_ref") in conflicts)
    check("预览恰好报告 2 个冲突", len(rep["conflicts"]) == 2)
    check("预览不写库: 无 staging 文件",
          not os.path.exists(svc.paths.tenant_dir("ACME")))
    check("预览不写库: ACME 仍在 legacy",
          svc.tenant_state("ACME") == "legacy")

    # ---- phase 3: migrate ACME while batches keep arriving ------------
    start = mig.start("ACME")
    mid = start["migration_id"]
    check("迁移启动后进入追平状态", start["state"] == "CATCHING_UP")
    staging = svc.paths.tenant_staging("ACME", mid)
    check("staging 临时目录已建立", os.path.exists(staging))

    # new batches arrive for the migrating tenant AND for another tenant
    b1 = gen_batch(svc, "ACME", n=2)
    b2 = gen_batch(svc, "ACME", n=1, plant_error=True)   # new cross-tenant error
    b3 = gen_batch(svc, "GLOBEX", n=2)
    check("迁移期间新批次持续写入 legacy",
          b1["destination"] == b2["destination"] == b3["destination"] == "legacy")
    check("追平前存在滞后批次", mig.lag("ACME") == 2, f"lag={mig.lag('ACME')}")

    # old queries during migration: still legacy, still consistent
    during = svc.query("ACME")
    check("迁移中旧查询仍走 legacy", during["served_by"] == "legacy")
    check("迁移中旧查询立即可见新批次",
          any(f["batch_id"] == b1["batch_id"] for f in during["facts"]))

    cu = mig.catchup("ACME")
    check("追平后滞后为 0", cu["lag"] == 0)
    check("追平应用了 2 个新批次", len(cu["applied_batches"]) == 2)
    check("追平捕获新批次里的跨租户错误",
          any(c["reason"] == "cross_tenant_ref" for c in cu["new_conflicts"]),
          json.dumps(cu["new_conflicts"], ensure_ascii=False))

    done = mig.cutover("ACME")
    check("切换完成", done["state"] == "DONE" and done["facts_migrated"] == 10)
    check("切换后 staging 已改名上线",
          not os.path.exists(staging)
          and os.path.exists(svc.paths.tenant_live("ACME")))
    check("切换后租户状态为 migrated", svc.tenant_state("ACME") == "migrated")

    # ---- phase 4: post-cutover queries & boundaries --------------------
    qa = svc.query("ACME")
    check("ACME 查询改由隔离目录服务", qa["served_by"] == "isolated")
    ids = {f["fact_id"] for f in qa["facts"]}
    check("ACME 10 条事实全部迁移(含追平批次)",
          len(ids) == 10 and "TRK-ACME-000001" in ids)
    carried = [r["key"] for f in qa["facts"] for r in f["refs"]]
    check("跨租户错误引用未被带入隔离目录",
          not any(r.startswith("PRIV/GLOBEX") for r in carried))
    quar = [(f["fact_id"], q["key"])
            for f in qa["facts"] for q in f["quarantined_refs"]]
    check("3 个错误引用全部进入隔离区(2 预置 + 1 追平)", len(quar) == 3,
          json.dumps(quar, ensure_ascii=False))

    qg = svc.query("GLOBEX")
    check("GLOBEX 仍由 legacy 服务", qg["served_by"] == "legacy")
    check("GLOBEX 结果不含 ACME 标记",
          all(not f["fact_id"].startswith("TRK-ACME") for f in qg["facts"]))
    check("GLOBEX 迁移期间收到的新批次也在 legacy",
          any(f["batch_id"] == b3["batch_id"] for f in qg["facts"]))

    # post-cutover ingest routes to the isolated catalog
    b4 = gen_batch(svc, "ACME", n=1)
    check("切换后 ACME 新批次写入隔离目录", b4["destination"] == "isolated")
    qa2 = svc.query("ACME")
    check("切换后新批次立即可查", len(qa2["facts"]) == 11)

    # ---- phase 5: comparison query --------------------------------------
    cmp_res = svc.compare("ACME")
    check("对照查询: 结构一致(无意外差异)", cmp_res["match"],
          json.dumps(cmp_res["unexpected_differences"], ensure_ascii=False))
    check("对照查询: 预期差异==隔离区引用数",
          len([d for d in cmp_res["expected_differences"]
               if d["issue"] == "quarantined_ref_removed"]) == 3)
    check("对照查询: 切换后批次计为预期新增",
          any(d["issue"] == "post_cutover_batch"
              for d in cmp_res["expected_differences"]))

    # ---- phase 6: rollback of an incomplete migration --------------------
    mig.start("INITECH")
    b5 = gen_batch(svc, "INITECH", n=2)          # arrives during migration
    mig.catchup("INITECH")
    ini_staging = svc.paths.tenant_staging(
        "INITECH", mig.active_migration("INITECH")["id"])
    check("回退前 staging 存在", os.path.exists(ini_staging))
    rb = mig.rollback("INITECH")
    check("回退完成", rb["state"] == "ROLLED_BACK")
    check("回退后 staging 已删除", not os.path.exists(ini_staging))
    check("回退后 INITECH 仍在 legacy", svc.tenant_state("INITECH") == "legacy")
    qi = svc.query("INITECH")
    check("回退后数据零丢失(含迁移期间批次)",
          any(f["batch_id"] == b5["batch_id"] for f in qi["facts"]))
    try:
        mig.rollback("INITECH")
        check("重复回退被拒绝", False)
    except Conflict:
        check("重复回退被拒绝", True)

    # ---- phase 7: shared-dimension propagation ---------------------------
    svc.revise_shared("SHARED/GEO/CN", {"name": "China", "region": "APAC",
                                        "note": "rev-2"})
    ra = svc.query("ACME")
    rg = svc.query("GLOBEX")

    def shared_status(res, key):
        for f in res["facts"]:
            for r in f["refs"]:
                if r["key"] == key:
                    return r
        return None

    sa = shared_status(ra, "SHARED/GEO/CN")
    sg = shared_status(rg, "SHARED/GEO/CN")
    check("修订对已隔离租户即时生效(v2)",
          sa and sa["version"] == 2 and sa["payload"].get("note") == "rev-2")
    check("修订对 legacy 租户即时生效(v2)", sg and sg["version"] == 2)

    svc.tombstone_shared("SHARED/UNIT/KG")
    check("删除(墓碑)对已隔离租户生效",
          shared_status(svc.query("ACME"), "SHARED/UNIT/KG")["status"]
          == "tombstoned")
    check("删除(墓碑)对 legacy 租户生效",
          shared_status(svc.query("GLOBEX"), "SHARED/UNIT/KG")["status"]
          == "tombstoned")

    svc.tighten_shared("SHARED/GEO/CN", ["ACME"])
    check("权限收紧: ACME 仍可解析",
          shared_status(svc.query("ACME"), "SHARED/GEO/CN")["status"] == "ok")
    check("权限收紧: GLOBEX 立即被拒",
          shared_status(svc.query("GLOBEX"), "SHARED/GEO/CN")["status"]
          == "access_revoked")

    # ---- phase 8: isolation boundaries -----------------------------------
    for bad in ("../GLOBEX", "ACME/../x", "GLOBEX%00"):
        try:
            svc.query(bad)
            check(f"非法租户名被拒绝: {bad!r}", False)
        except BadRequest:
            check(f"非法租户名被拒绝: {bad!r}", True)

    # while GLOBEX is mid-migration, other tenants must not see its staging
    mig.start("GLOBEX")
    gmid = mig.active_migration("GLOBEX")["id"]
    gstaging = svc.paths.tenant_staging("GLOBEX", gmid)
    check("GLOBEX staging 已建立", os.path.exists(gstaging))
    qg2 = svc.query("GLOBEX")
    check("迁移中 GLOBEX 旧查询仍走 legacy(不读临时目录)",
          qg2["served_by"] == "legacy")
    qa3 = svc.query("ACME")
    check("他租户(ACME)查询不受 GLOBEX 迁移影响",
          qa3["served_by"] == "isolated"
          and all(not f["fact_id"].startswith("TRK-GLOBEX")
                  for f in qa3["facts"]))
    qi3 = svc.query("INITECH")
    check("他租户(INITECH)查询不含 GLOBEX 标记",
          all(not f["fact_id"].startswith("TRK-GLOBEX")
              for f in qi3["facts"]))
    iso = svc.isolation_check()
    check("隔离自检全部通过", iso["pass"],
          json.dumps([c for c in iso["checks"] if not c["pass"]],
                     ensure_ascii=False))
    mig.rollback("GLOBEX")
    check("GLOBEX 迁移已回退, 状态恢复 legacy",
          svc.tenant_state("GLOBEX") == "legacy")

    # ---- phase 9: HTTP smoke test against the real server -----------------
    http_dir = tempfile.mkdtemp(prefix="catalog_http_")
    svc2 = CatalogService(http_dir)
    mig2 = Migrator(svc2)
    httpd = ThreadingHTTPServer(("127.0.0.1", 0),
                                server_mod.build_handler(svc2, mig2))
    port = httpd.server_address[1]
    threading.Thread(target=httpd.serve_forever, daemon=True).start()

    def http(path, method="GET", body=None):
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}{path}", method=method,
            data=json.dumps(body).encode() if body is not None else None,
            headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read())

    http("/api/seed", "POST")
    st = http("/api/state")
    check("HTTP: /api/state 返回 3 租户", len(st["tenants"]) == 3)
    http("/api/tenants/ACME/migrate", "POST")
    http("/api/tenants/ACME/cutover", "POST")
    q = http("/api/query?tenant=ACME")
    check("HTTP: 迁移+切换后 ACME 由隔离目录服务",
          q["served_by"] == "isolated")
    c = http("/api/compare?tenant=ACME")
    check("HTTP: 对照查询一致", c["compared"] and c["match"])
    httpd.shutdown()

    # ---- summary -----------------------------------------------------------
    failed = [r for r in RESULTS if not r[1]]
    print(f"\n{len(RESULTS) - len(failed)}/{len(RESULTS)} checks passed"
          f"  (data kept at {data_dir})")
    if failed:
        print("FAILED:")
        for name, _, detail in failed:
            print(f"  - {name} {detail}")
        sys.exit(1)
    print("ALL GREEN")


if __name__ == "__main__":
    main()
