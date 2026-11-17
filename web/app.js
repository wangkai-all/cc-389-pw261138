"use strict";

const $ = (sel) => document.querySelector(sel);

async function api(path, method = "GET", body = null) {
  const opts = { method, headers: {} };
  if (body !== null) {
    opts.headers["Content-Type"] = "application/json";
    opts.body = JSON.stringify(body);
  }
  const res = await fetch(path, opts);
  const data = await res.json();
  if (!res.ok) throw new Error(data.error || res.statusText);
  return data;
}

function log(msg, obj) {
  const el = $("#log");
  const line = `[${new Date().toLocaleTimeString()}] ${msg}` +
    (obj !== undefined ? "\n" + JSON.stringify(obj, null, 1) : "");
  el.textContent = line + "\n\n" + el.textContent;
}

function esc(s) {
  return String(s).replace(/[&<>"]/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
}

const STATE_LABEL = {
  legacy: ["遗留共享表", "badge gray"],
  migrated: ["已隔离", "badge green"],
};
const MIG_LABEL = {
  COPYING: "初始拷贝中",
  CATCHING_UP: "追平批次中",
  DONE: "已完成",
  ROLLED_BACK: "已回退",
  FAILED: "失败",
};
const REF_STATUS_LABEL = {
  ok: "正常",
  tombstoned: "已删除(墓碑)",
  access_revoked: "权限已收紧",
  cross_tenant: "跨租户错误",
  dangling: "悬空引用",
  missing: "不存在",
};

function badge(text, cls) {
  return `<span class="badge ${cls}">${esc(text)}</span>`;
}

async function refresh() {
  const state = await api("/api/state");
  renderTenants(state.tenants);
  renderSharedDims(state.shared_dims);
  renderBatches(state.recent_batches);
  const sel = $("#query-tenant");
  const cur = sel.value;
  sel.innerHTML = state.tenants
    .map((t) => `<option>${esc(t.tenant)}</option>`).join("");
  if (cur) sel.value = cur;
}

function renderTenants(tenants) {
  const tb = $("#tenants tbody");
  tb.innerHTML = tenants.map((t) => {
    const [label, cls] = STATE_LABEL[t.state] || [t.state, "badge gray"];
    const mig = t.active_migration || t.last_migration;
    const migText = mig
      ? `${esc(mig.id)} ${badge(MIG_LABEL[mig.state] || mig.state,
          mig.state === "DONE" ? "green" : mig.state === "CATCHING_UP" ? "amber" : "gray")}`
      : "—";
    const lag = t.active_migration ? t.active_migration.lag ?? "?" : "—";
    const active = !!t.active_migration;
    const migrated = t.state === "migrated";
    return `<tr>
      <td><b>${esc(t.tenant)}</b></td>
      <td>${badge(label, cls.replace("badge ", ""))}</td>
      <td>${t.legacy_facts}</td>
      <td>${t.isolated_facts ?? "—"}</td>
      <td>${t.quarantined_refs ?? "—"}</td>
      <td>${migText}</td>
      <td class="lag">${active ? lag : "—"}</td>
      <td class="ops">
        <button data-op="preview" data-t="${esc(t.tenant)}">预览冲突</button>
        ${!migrated && !active ? `<button data-op="migrate" data-t="${esc(t.tenant)}">开始迁移</button>` : ""}
        ${active ? `<button data-op="catchup" data-t="${esc(t.tenant)}">追平批次</button>
                    <button data-op="cutover" data-t="${esc(t.tenant)}">切换</button>
                    <button data-op="rollback" data-t="${esc(t.tenant)}" class="danger">回退</button>` : ""}
        <button data-op="batch" data-t="${esc(t.tenant)}">发新批次</button>
        <button data-op="batcherr" data-t="${esc(t.tenant)}" title="发送含跨租户错误引用的批次">发带错批次</button>
      </td>
    </tr>`;
  }).join("");
}

function renderSharedDims(dims) {
  $("#shared-dims tbody").innerHTML = dims.map((d) => `<tr>
    <td><code>${esc(d.key)}</code></td>
    <td>v${d.version}</td>
    <td>${d.tombstoned ? badge("已删除", "red") : badge("可用", "green")}</td>
    <td>${d.allowed ? esc(d.allowed.join(", ")) : "全部"}</td>
    <td><code>${esc(JSON.stringify(d.payload))}</code></td>
    <td class="ops">
      <button data-dim="${esc(d.key)}" data-op="revise">修订</button>
      <button data-dim="${esc(d.key)}" data-op="tombstone" class="danger">删除</button>
      <button data-dim="${esc(d.key)}" data-op="acl">收紧权限</button>
    </td>
  </tr>`).join("");
}

function renderBatches(batches) {
  $("#batches tbody").innerHTML = batches.map((b) => `<tr>
    <td><code>${esc(b.batch_id)}</code></td><td>${esc(b.tenant)}</td>
    <td>${b.seq}</td><td>${esc(b.destination)}</td><td>${b.fact_count}</td>
    <td>${esc(b.received_at)}</td>
  </tr>`).join("");
}

function renderPreview(report) {
  $("#preview-card").hidden = false;
  $("#preview-tenant").textContent = report.tenant;
  const r = report.refs;
  $("#preview-summary").innerHTML =
    `事实 ${report.fact_count} 条，私有维度 ${report.own_dims} 个；` +
    `引用：自有 ${r.ok_own}、共享 ${r.ok_shared}、` +
    badge(`跨租户错误 ${r.cross_tenant_ref}`, "red") + " " +
    badge(`悬空引用 ${r.dangling_ref}`, "red") +
    (report.warnings.length
      ? " " + badge(`警告 ${report.warnings.length}`, "amber") : "");
  const rows = report.conflicts.concat(report.warnings);
  $("#preview-table tbody").innerHTML = rows.map((c) => `<tr>
    <td><code>${esc(c.fact_id)}</code></td>
    <td><code>${esc(c.ref)}</code></td>
    <td>${esc(c.reason)}</td><td>${esc(c.detail || "")}</td>
  </tr>`).join("") || `<tr><td colspan="4">无冲突</td></tr>`;
}

function renderQuery(res) {
  const el = $("#query-result");
  const srcCls = res.served_by === "isolated" ? "green" : "gray";
  let html = `<p>来源：${badge(res.served_by, srcCls)}（${esc(res.tenant)}），
    共 ${res.facts.length} 条</p>`;
  html += `<table><thead><tr><th>事实标记</th><th>批次</th><th>内容</th>
    <th>引用解析</th><th>被隔离引用</th></tr></thead><tbody>`;
  for (const f of res.facts) {
    const refs = f.refs.map((r) => {
      const cls = r.status === "ok" ? "green"
        : (r.status === "cross_tenant" || r.status === "dangling") ? "red"
        : "amber";
      return `<div>${badge(REF_STATUS_LABEL[r.status] || r.status, cls)}
        <code>${esc(r.key)}</code>${r.version ? " v" + r.version : ""}</div>`;
    }).join("");
    const quar = (f.quarantined_refs || []).map((q) =>
      `<div>${badge(q.reason, "red")} <code>${esc(q.key)}</code></div>`).join("");
    html += `<tr><td><code>${esc(f.fact_id)}</code></td>
      <td><code>${esc(f.batch_id)}</code></td>
      <td><code>${esc(JSON.stringify(f.payload))}</code></td>
      <td>${refs}</td><td>${quar}</td></tr>`;
  }
  el.innerHTML = html + "</tbody></table>";
}

function renderCompare(res) {
  const el = $("#query-result");
  if (!res.compared) {
    el.innerHTML = `<p>${badge("无法对照", "amber")} ${esc(res.reason)}</p>`;
    return;
  }
  let html = `<p>${res.match ? badge("对照一致", "green") : badge("存在意外差异", "red")}
    ｜legacy ${res.legacy_facts} 条 / 隔离目录 ${res.isolated_facts} 条
    ｜预期差异（被隔离引用）${res.expected_differences.length} 处
    ｜意外差异 ${res.unexpected_differences.length} 处</p>`;
  const rows = res.unexpected_differences.concat(res.expected_differences);
  if (rows.length) {
    html += `<table><thead><tr><th>事实标记</th><th>引用</th><th>差异</th></tr></thead>
      <tbody>` + rows.map((d) => `<tr>
      <td><code>${esc(d.fact_id)}</code></td>
      <td><code>${esc(d.ref || "")}</code></td><td>${esc(d.issue)}</td>
    </tr>`).join("") + "</tbody></table>";
  }
  el.innerHTML = html;
}

async function run(op, fn) {
  try {
    const res = await fn();
    log(`${op} 成功`, res);
    return res;
  } catch (e) {
    log(`${op} 失败: ${e.message}`);
  } finally {
    await refresh();
  }
}

$("#tenants").addEventListener("click", async (e) => {
  const btn = e.target.closest("button");
  if (!btn) return;
  const t = btn.dataset.t, op = btn.dataset.op;
  if (op === "preview") {
    const rep = await run("preview", () => api(`/api/tenants/${t}/preview`, "POST"));
    if (rep) renderPreview(rep);
  } else if (op === "migrate") {
    await run("migrate", () => api(`/api/tenants/${t}/migrate`, "POST"));
  } else if (op === "catchup") {
    await run("catchup", () => api(`/api/tenants/${t}/catchup`, "POST"));
  } else if (op === "cutover") {
    await run("cutover", () => api(`/api/tenants/${t}/cutover`, "POST"));
  } else if (op === "rollback") {
    await run("rollback", () => api(`/api/tenants/${t}/rollback`, "POST"));
  } else if (op === "batch") {
    await run("batch", () => api(`/api/tenants/${t}/batches`, "POST", { n: 2 }));
  } else if (op === "batcherr") {
    await run("batch(err)", () =>
      api(`/api/tenants/${t}/batches`, "POST", { n: 1, plant_error: true }));
  }
});

$("#shared-dims").addEventListener("click", async (e) => {
  const btn = e.target.closest("button");
  if (!btn) return;
  const key = btn.dataset.dim, op = btn.dataset.op;
  if (op === "revise") {
    const payload = prompt("新内容（JSON）", '{"name":"revised"}');
    if (!payload) return;
    await run("revise", () =>
      api(`/api/shared_dims/${key}/revise`, "POST", { payload: JSON.parse(payload) }));
  } else if (op === "tombstone") {
    if (confirm(`确认删除（墓碑）${key}？所有租户立即不可解析。`))
      await run("tombstone", () => api(`/api/shared_dims/${key}/tombstone`, "POST"));
  } else if (op === "acl") {
    const allowed = prompt("允许可见的租户（逗号分隔）", "ACME");
    if (!allowed) return;
    await run("acl", () => api(`/api/shared_dims/${key}/acl`, "POST",
      { allowed: allowed.split(",").map((s) => s.trim()).filter(Boolean) }));
  }
});

$("#btn-seed").addEventListener("click", () =>
  run("seed", () => api("/api/seed", "POST")));
$("#btn-reset").addEventListener("click", () =>
  run("reset", () => api("/api/reset", "POST")));
$("#btn-refresh").addEventListener("click", refresh);
$("#btn-isolation").addEventListener("click", () =>
  run("isolation_check", () => api("/api/isolation_check")));
$("#btn-query").addEventListener("click", async () => {
  const t = $("#query-tenant").value;
  const res = await run("query", () => api(`/api/query?tenant=${t}`));
  if (res) renderQuery(res);
});
$("#btn-compare").addEventListener("click", async () => {
  const t = $("#query-tenant").value;
  const res = await run("compare", () => api(`/api/compare?tenant=${t}`));
  if (res) renderCompare(res);
});

refresh().catch((e) => log("初始加载失败: " + e.message));
