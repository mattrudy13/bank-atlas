// Bank Atlas Pipeline page: a run report built from site-data/pipeline_status.json.
import { loadJSON, el, fmtInt, fmtDate, fmtStampET } from "./common.js";

const $ = (sel) => document.querySelector(sel);
const LAYERS = ["bronze", "silver", "gold"];

loadJSON("pipeline_status").then(render).catch((err) => {
  console.error(err);
  $("#status").replaceChildren(el("p", { class: "error" }, "The run report didn't load. ", String(err.message)));
});

function render(s) {
  const passed = s.checks.filter((c) => c.passed).length;
  const ok = s.status === "passed" && passed === s.checks.length;

  const src = new Map((s.fdic_sources ?? []).map((x) => [x.endpoint, x.created_at]));
  $("#asof").replaceChildren(
    el("span", { class: "asof-item" }, `${fmtDate(src.get("locations"))} branches`),
    el("span", { class: "asof-item" }, `${fmtDate(src.get("sod"))} deposits`));

  $("#status").replaceChildren(
    el("span", { class: ok ? "status-badge" : "status-badge failed" }, ok ? "PASSED" : "FAILED"),
    el("span", { class: "status-text" }, `${passed} of ${s.checks.length} checks`));
  const lake = s.storage === "local" ? "local folder (R2 not set up yet)" : `Cloudflare R2 (${s.storage})`;
  $("#run-meta").replaceChildren(
    `Last run ${fmtStampET(s.finished_at)} · ${s.duration_seconds} s`, el("br"),
    `Lake: ${lake}`, el("br"),
    `Metro: ${s.metro?.name ?? ""} (CBSA ${s.metro?.cbsa ?? "?"})`);

  // Part I: one table, grouped by layer, rows and Delta version per table.
  const body = [];
  for (const layer of LAYERS) {
    const tables = s.tables.filter((t) => t.layer === layer);
    if (!tables.length) continue;
    body.push(el("tr", { class: "layer" }, el("td", { colspan: "3" }, `${layer} · ${fmtInt(tables.reduce((n, t) => n + t.row_count, 0))} rows`)));
    for (const t of tables) {
      body.push(el("tr", {}, el("td", {}, t.table_name), el("td", {}, fmtInt(t.row_count)), el("td", {}, `v${t.delta_version}`)));
    }
  }
  $("#tables").replaceChildren(
    el("table", { class: "tbl" },
      el("thead", {}, el("tr", {}, el("th", {}, "Table"), el("th", {}, "Rows"), el("th", {}, "Version"))),
      el("tbody", {}, ...body)),
    el("p", { class: "fine" }, "Bronze keeps every run's raw rows (row counts here are the latest run's); silver and gold are rebuilt from it."));

  // Part II: each check with its detail.
  $("#check-count").textContent = `${passed}/${s.checks.length} PASSED`;
  $("#checks").replaceChildren(...s.checks.map((c) =>
    el("div", { class: "check" },
      el("span", { class: c.passed ? "mark" : "mark bad", "aria-label": c.passed ? "Passed" : "Failed" }, c.passed ? "✓" : "✗"),
      el("span", {}, c.name.charAt(0).toUpperCase() + c.name.slice(1), el("small", {}, c.detail)))));

  // Part III: recent runs, newest first.
  const runs = s.recent_runs ?? [];
  $("#runs").replaceChildren(
    el("table", { class: "tbl" },
      el("thead", {}, el("tr", {}, el("th", {}, "Started"), el("th", {}, "Time"), el("th", {}, "Result"))),
      el("tbody", {}, ...runs.map((r) => el("tr", {},
        el("td", {}, fmtStampET(r.started_at)),
        el("td", {}, `${r.duration_seconds} s`),
        el("td", { style: r.status === "passed" ? "" : "color: var(--red)" },
          r.status === "passed" ? "✓ passed" : `✗ ${r.failed_checks?.length ?? 0} failed`))))));
}
