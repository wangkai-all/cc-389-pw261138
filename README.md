# 租户隔离目录迁移系统

共享表改造为按租户隔离存储的完整实现：迁移器、目录服务、查询端与网页操作台。
纯 Python 标准库 + SQLite，无第三方依赖。

## 运行

```bash
python3 server.py          # http://127.0.0.1:8000 （数据写入 ./data）
python3 scripts/verify_e2e.py   # 端到端验证：从空目录起 58 项检查
```

网页操作台：填充演示数据 → 对租户「预览冲突」→「开始迁移」→ 迁移中可「发新批次 /
发带错批次」→「追平批次」→「切换」→「对照查询」；未完成的迁移可「回退」。

## 存储布局（data/）

| 文件 | 角色 |
|---|---|
| `legacy.db` | 遗留共享表：事实表（按租户打标）、各租户私有维度、批次日志 |
| `shared_dims.db` | 公共维度：单一只读共享存储（版本化 + 墓碑 + 租户 ACL） |
| `registry.db` | 控制面：租户状态（legacy/migrated）、迁移记录、批次审计、全局序号 |
| `tenants/<T>/staging_<迁移id>.db` | 迁移期临时目录，**任何查询都读不到** |
| `tenants/<T>/catalog.db` | 切换后的租户隔离目录（含隔离区 quarantine 表） |

## 关键决策

### 1. 公共维度：只读共享版本，不复制到各租户

选择**维护单一只读共享版本**而非逐租户复制。传播语义：

| 变更 | 实现 | 传播 |
|---|---|---|
| 修订 | 写入 version+1 | 所有租户下一次查询即解析到新版本；旧版本保留供审计 |
| 删除 | 写入墓碑版本 | 所有租户立即不可解析；已迁移事实里的历史引用标记为 `tombstoned`，不被静默改写 |
| 权限收紧 | 新版本携带 `allowed_tenants` ACL | 被排除租户下一次查询即得到 `access_revoked`，其余租户不受影响 |

不选复制的原因：删除与权限收紧需要可靠的逐租户删除扇出，任何滞后副本都会
继续放行已撤销的访问；单一共享存储使三类变更即时、一致地传到所有租户。

### 2. 不做「按租户过滤后复制」

共享表里存在跨租户错误引用（如 ACME 的行引用了 `PRIV/GLOBEX/...`）。直接
`WHERE tenant=?` 复制会把这些错误带进新的隔离目录。迁移器对每一行做
**引用闭包校验**：自有私有维度与共享维度才放行；跨租户引用与悬空引用进入
隔离区（quarantine）并记入冲突报告，绝不进入租户的已校验引用集。切换后的
持续写入走同一套校验。

### 3. 迁移协议

```
预览(干跑, 不落库) → 开始(建 staging + 高水位 + 校验拷贝) → 追平(重放高水位后
到达的新批次, 幂等) → 切换(最终追平 + 行数校验 + 原子改名 + 状态翻转)
```

- 迁移期间新批次持续写入 legacy，旧查询路径不变、立即可见；追平可反复执行。
- 切换顺序：最终追平 → 校验（行数一致、无孤儿隔离行）→ staging 改名上线 →
  租户状态置 migrated → legacy 行标记 `migrated_out`（保留可审计，不删除）。
- **回退**：仅对未完成迁移（CATCHING_UP）开放；删除 staging 即可——legacy
  从未被改动，迁移期间到达的批次零丢失。已切换的迁移不可回退。
- **对照查询**：同一租户在 legacy 历史视图与隔离目录间比对，唯一允许的差异
  是隔离区引用与切换后批次；其余任何差异都会使对照失败。

### 4. 隔离边界

- 查询路由只返回 legacy 或该租户自己的 `catalog.db`；staging 路径永不进入路由。
- 租户名严格校验（`^[A-Z0-9][A-Z0-9_-]{0,63}$`），阻断路径穿越。
- 查询结果绝不解析他租户私有维度的内容；跨租户引用仅作为错误元数据展示。
- `/api/isolation_check` 与验证脚本第 8 阶段持续验证以上边界。

## API 摘要

```
GET  /api/state                         仪表盘（租户/迁移/共享维度/批次）
POST /api/seed | /api/reset             填充虚构组织演示数据 | 回到空目录
POST /api/tenants/<T>/preview           冲突预览（干跑）
POST /api/tenants/<T>/migrate           开始迁移
POST /api/tenants/<T>/catchup           追平新批次
POST /api/tenants/<T>/cutover           校验并切换
POST /api/tenants/<T>/rollback          回退未完成迁移
POST /api/tenants/<T>/batches           接收新批次（plant_error 可注入错误引用）
GET  /api/query?tenant=<T>              租户查询（自动路由 legacy/isolated）
GET  /api/compare?tenant=<T>            对照查询
POST /api/shared_dims/<K>/revise        修订公共维度
POST /api/shared_dims/<K>/tombstone     删除（墓碑）
POST /api/shared_dims/<K>/acl           权限收紧
GET  /api/isolation_check               隔离边界自检
```

## 验证

`scripts/verify_e2e.py` 从空目录起步，使用虚构组织 ACME / GLOBEX / INITECH
与可追踪标记（`TRK-<租户>-<序号>`、`B-<租户>-<序号>`）完成：预置并捕获跨租户
错误引用、迁移中持续接收新批次并追平、切换、对照查询、回退未完成迁移、
公共维度三类变更的传播、以及「旧查询读不到他租户临时目录」的边界检查，
最后对真实 HTTP 服务做冒烟。当前 58/58 通过。
