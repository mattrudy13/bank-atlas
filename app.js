// Bank Atlas map page: Hampton Roads branches, notes, deposit share and changes by year.
// Everything is computed in the browser from the pipeline's gold files in site-data/.
import { loadJSON, el, fmtInt, fmtMoney, fmtDate, fmtStampET, shortBankName } from "./common.js";

// The six banks named in the legend, by FDIC certificate number. Every other bank is gray.
const NAMED = new Map([
  [9846, { short: "Truist", color: "#7c3aed" }],
  [35095, { short: "TowneBank", color: "#2453c5" }],
  [3511, { short: "Wells Fargo", color: "#0f8a5f" }],
  [3510, { short: "Bank of America", color: "#c77700" }],
  [628, { short: "JPMorgan Chase", color: "#e0457b" }],
  [34589, { short: "Atlantic Union", color: "#111111" }],
]);
const OTHER_COLOR = "#a0a4a8";
const RED = "#c8102e";
const THEN_YEARS = [1994, 2006, 2016]; // Part II columns; the latest deposit year is added after loading
const DESKTOP_CHIPS = 7;               // areas shown as chips on desktop; the rest go in "More"
const PHONE = window.matchMedia("(max-width: 700px)");

const state = { area: "Metro", bank: null, selected: null, year: null };
let D;
let map, markerLayer;
const $ = (sel) => document.querySelector(sel);

init();

async function init() {
  try {
    const [branches, eventsByYear, deposits, status] = await Promise.all(
      ["branches_now", "branch_events_by_year", "deposit_share_by_year", "pipeline_status"].map(loadJSON));
    // The per-event list powers Part III's list; the page still works without it.
    const events = await loadJSON("branch_events").catch(() => null);
    D = prepare(branches, eventsByYear, deposits, status, events);
    state.year = D.eventYears.at(-1);
    buildMap();
    render({ fit: true });
  } catch (err) {
    console.error(err);
    $(".main").before(el("p", { class: "error" }, "The data didn't load. ", String(err.message)));
  }
}

function prepare(branches, eventsByYear, deposits, status, events) {
  const meta = branches.meta;
  const depIndex = new Map();
  for (const r of deposits.data) {
    const key = `${r.area}|${r.year}`;
    if (!depIndex.has(key)) depIndex.set(key, []);
    depIndex.get(key).push(r);
  }
  for (const rows of depIndex.values()) rows.sort((a, b) => a.rank - b.rank);
  const depYears = [...new Set(deposits.data.map((r) => r.year))].sort((a, b) => a - b);

  const firstEventYear = eventsByYear.meta?.first_year ?? 2000;
  const evIndex = new Map(eventsByYear.data.map((r) => [`${r.area}|${r.year}`, r]));
  const eventYears = [...new Set(eventsByYear.data.map((r) => r.year))].filter((y) => y >= firstEventYear).sort((a, b) => a - b);

  // Current name for each bank: the newest deposit row, then today's branch list wins.
  const bankName = new Map();
  for (const r of [...deposits.data].sort((a, b) => a.year - b.year)) bankName.set(r.cert, r.bank_name);
  for (const b of branches.data) bankName.set(b.cert, b.bank_name);

  return {
    meta,
    branches: branches.data,
    byUninum: new Map(branches.data.map((b) => [b.uninum, b])),
    depIndex, latest: depYears.at(-1),
    evIndex, eventYears,
    events: events?.data ?? null,
    status,
    bankName,
    noCallout: new Set(meta.no_callout_certs ?? status.no_callout_certs ?? []),
    branchesAsOf: (meta.fdic_data_as_of?.locations ?? "").slice(0, 10),
    eventsAsOf: (meta.fdic_data_as_of?.history ?? "").slice(0, 10),
  };
}

// ---------- Lookups ----------

const branchesIn = (area) => (area === "Metro" ? D.branches : D.branches.filter((b) => b.locality === area));
const depRows = (area, year) => D.depIndex.get(`${area}|${year}`) ?? [];
const sum = (xs, f) => xs.reduce((t, x) => t + (f(x) ?? 0), 0);
const colorOf = (cert) => NAMED.get(cert)?.color ?? OTHER_COLOR;
const shortOf = (cert) => NAMED.get(cert)?.short ?? shortBankName(D.bankName.get(cert) ?? `Bank ${cert}`);
const areaLabel = (area) => (area === "Metro" ? "ALL METRO" : area.toUpperCase());

// A branch counts as new if it opened in the year before the branch data's date.
function isNew(b) {
  if (!b.opened_date || !D.branchesAsOf) return false;
  const cutoff = new Date(D.branchesAsOf);
  cutoff.setFullYear(cutoff.getFullYear() - 1);
  return b.opened_date >= cutoff.toISOString().slice(0, 10);
}

// ---------- Render ----------

function render({ fit = false } = {}) {
  renderAsOf();
  renderLines();
  renderChips();
  drawMarkers();
  if (fit) fitArea();
  renderLegend();
  renderDetail();
  renderNotes();
  renderBanks();
  renderYears();
}

function renderAsOf() {
  $("#asof").replaceChildren(
    el("span", { class: "asof-item" }, `${fmtDate(D.branchesAsOf)} branches`),
    el("span", { class: "asof-item" }, `06/30/${D.latest} deposits`));
  const s = D.status;
  const passed = s.checks?.filter((c) => c.passed).length ?? 0;
  $("#foot-run").textContent = `Pipeline: last run ${fmtStampET(s.finished_at)} · ${passed}/${s.checks?.length ?? 0} checks passed →`;
}

function renderLines() {
  const here = branchesIn(state.area);
  const items = [
    ["1", "BRANCHES", "Branches today", fmtInt(here.length)],
    ["2", "BANKS", "Banks", fmtInt(new Set(here.map((b) => b.cert)).size)],
    ["3", "DEPSUM", `Deposits, 6/30/${D.latest}`, fmtMoney(sum(depRows(state.area, D.latest), (r) => r.deposits_thousands))],
    ["4", "BR1994", "Branches in 1994", fmtInt(sum(depRows(state.area, 1994), (r) => r.branches))],
  ];
  $("#lines").replaceChildren(...items.map(([n, code, name, value]) =>
    el("div", { class: "line-item" },
      el("div", {}, el("div", { class: "line-code" }, `LINE ${n} · ${code}`), el("div", { class: "line-name" }, name)),
      el("div", { class: "line-value" }, value))));
}

function renderChips() {
  const counts = new Map();
  for (const b of D.branches) counts.set(b.locality, (counts.get(b.locality) ?? 0) + 1);
  const areas = [...counts.keys()].sort((a, b) => counts.get(b) - counts.get(a) || a.localeCompare(b));
  const chip = (area, extra) => el("button", {
    type: "button", class: extra ? "chip extra" : "chip", "aria-pressed": String(state.area === area),
    onclick: () => setArea(area),
  }, area === "Metro" ? "All metro" : area);

  const shown = areas.slice(0, DESKTOP_CHIPS);
  const rest = areas.slice(DESKTOP_CHIPS);
  const more = el("select", { class: "chip-more", "aria-label": "More areas", onchange: (e) => e.target.value && setArea(e.target.value) },
    el("option", { value: "" }, rest.includes(state.area) ? "More" : "More…"),
    ...rest.map((a) => el("option", { value: a, selected: state.area === a }, `${a} (${counts.get(a)})`)));

  const nodes = [chip("Metro"), ...shown.map((a) => chip(a)), ...rest.map((a) => chip(a, true)), more];
  if (state.bank) {
    nodes.push(el("button", { type: "button", class: "bank-filter", onclick: () => setBank(null), "aria-label": `Show all banks (now only ${shortOf(state.bank)})` },
      `Only ${shortOf(state.bank)}`, el("span", { "aria-hidden": "true", style: "padding: 0 6px" }, "✕")));
  }
  $("#chips").replaceChildren(...nodes);
}

function setArea(area) {
  state.area = area;
  const sel = D.byUninum.get(state.selected);
  if (sel && area !== "Metro" && sel.locality !== area) state.selected = null;
  render({ fit: true });
}

function setBank(cert) {
  state.bank = cert;
  render({ fit: true });
}

// ---------- Map ----------

function buildMap() {
  map = L.map("map", { scrollWheelZoom: false, zoomSnap: 0.5 });
  // OpenStreetMap's own tiles need no key; style.css turns them gray to fit the look.
  L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png", {
    maxZoom: 19,
    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
  }).addTo(map);
  markerLayer = L.layerGroup().addTo(map);
  map.on("click", () => select(null));
}

function visibleBranches() {
  return branchesIn(state.area).filter((b) => b.latitude != null && (!state.bank || b.cert === state.bank));
}

function drawMarkers() {
  markerLayer.clearLayers();
  const r = PHONE.matches ? 6 : 7;
  // Gray dots go down first so the named banks sit on top.
  const list = visibleBranches().sort((a, b) => (NAMED.has(a.cert) ? 1 : 0) - (NAMED.has(b.cert) ? 1 : 0));
  for (const b of list) {
    const ll = [b.latitude, b.longitude];
    const color = colorOf(b.cert);
    if (isNew(b)) L.circleMarker(ll, { radius: r + 3.5, color, weight: 1.5, fill: false, interactive: false }).addTo(markerLayer);
    const m = L.circleMarker(ll, { radius: r, color: "#ffffff", weight: 1, fillColor: color, fillOpacity: 1, bubblingMouseEvents: false });
    m.on("click", () => select(b.uninum));
    m.on("add", () => {
      // Make each dot reachable by keyboard and named for screen readers.
      const node = m.getElement();
      node.setAttribute("tabindex", "0");
      node.setAttribute("role", "button");
      node.setAttribute("aria-label", `${shortOf(b.cert)}, ${b.branch_name}, ${b.city}`);
      node.addEventListener("keydown", (e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); select(b.uninum); } });
    });
    markerLayer.addLayer(m);
  }
  const sel = D.byUninum.get(state.selected);
  if (sel && list.includes(sel)) {
    L.circleMarker([sel.latitude, sel.longitude], { radius: r + 6, color: RED, weight: 2.5, fill: false, interactive: false }).addTo(markerLayer);
  }
}

function fitArea() {
  const pts = visibleBranches().map((b) => [b.latitude, b.longitude]);
  if (!pts.length) return;
  map.fitBounds(L.latLngBounds(pts), { padding: [24, 24], maxZoom: 14 });
}

function renderLegend() {
  const here = visibleBranches();
  const counts = new Map();
  for (const b of here) counts.set(b.cert, (counts.get(b.cert) ?? 0) + 1);
  const named = [...NAMED.keys()].filter((c) => counts.has(c)).sort((a, b) => counts.get(b) - counts.get(a));
  const others = [...counts.keys()].filter((c) => !NAMED.has(c));
  const items = named.map((c) => el("span", {}, el("span", { class: "dot", style: `background:${colorOf(c)}` }), shortOf(c), el("span", { class: "mono muted" }, counts.get(c))));
  if (others.length) {
    items.push(el("span", {}, el("span", { class: "dot", style: `background:${OTHER_COLOR}` }),
      `${others.length} other bank${others.length === 1 ? "" : "s"}`, el("span", { class: "mono muted" }, sum(others, (c) => counts.get(c)))));
  }
  if (here.some(isNew)) items.push(el("span", {}, el("span", { class: "dot dot-ring" }), "Opened in the last 12 months"));
  $("#legend").replaceChildren(...items);
}

// ---------- Selected branch ----------

function select(uninum) {
  state.selected = uninum;
  drawMarkers();
  renderDetail();
  const b = D.byUninum.get(uninum);
  if (b) focusBranch(b);
}

function focusBranch(b) {
  const ll = L.latLng(b.latitude, b.longitude);
  if (!PHONE.matches) {
    if (!map.getBounds().pad(-0.05).contains(ll)) map.panTo(ll);
    return;
  }
  // On a phone the sheet covers the bottom of the screen. Scroll the map to the
  // top of the screen, zoom in, and put the dot in the middle of the part of
  // the map the sheet doesn't cover.
  const box = map.getContainer();
  box.scrollIntoView({ block: "start", behavior: "smooth" });
  const zoom = Math.max(map.getZoom(), 13);
  const visible = Math.min(box.offsetHeight, window.innerHeight - $("#detail").offsetHeight);
  const wantY = visible > 80 ? visible / 2 : box.offsetHeight / 4;
  const point = map.project(ll, zoom).add([0, box.offsetHeight / 2 - wantY]);
  map.flyTo(map.unproject(point, zoom), zoom, { duration: 0.4 });
}

function renderDetail() {
  const box = $("#detail");
  const b = D.byUninum.get(state.selected);
  const head = el("div", { class: "part-head" },
    el("h2", { id: "pd" }, "Selected branch"),
    b && el("span", { class: "code" }, `UNINUM ${b.uninum}`),
    b && el("button", { type: "button", class: "detail-close", "aria-label": "Close", onclick: () => select(null) }, "✕"));
  if (!b) {
    box.classList.remove("open");
    box.replaceChildren(head, el("p", { class: "empty" }, "Tap a dot on the map to see that branch."));
    return;
  }

  const fields = [
    ["Opened", fmtDate(b.opened_date)],
    ["Type", b.branch_type ?? "—"],
    [`Deposits 6/30/${b.deposits_year ?? D.latest}`, b.deposits_thousands == null ? "Not in the June survey" : fmtMoney(b.deposits_thousands)],
  ];
  if (b.acquired_date) fields.push(["With this bank since", fmtDate(b.acquired_date)]);

  const mine = D.branches.filter((x) => x.cert === b.cert);
  const shareRow = depRows("Metro", D.latest).find((r) => r.cert === b.cert);
  const summary = [`${mine.length} branch${mine.length === 1 ? "" : "es"} in the metro`];
  if (shareRow) summary.push(`${shareRow.share_pct.toFixed(1)}% of metro deposits`);
  const earliest = mine.map((x) => x.opened_date).filter(Boolean).sort()[0];
  if (earliest && mine.length > 1 && Number(earliest.slice(0, 4)) >= Number(D.latest) - 10) {
    summary.push(`all opened since ${fmtDate(earliest)}`);
  }

  const address = `${b.address}, ${b.city}, ${b.state} ${b.zip ?? ""}`.trim();
  const onlyThis = state.bank === b.cert;
  box.replaceChildren(
    el("div", { class: "sheet-handle", "aria-hidden": "true" }),
    head,
    el("div", { class: "detail-body" },
      el("div", { class: "detail-bank" }, el("span", { class: "dot", style: `background:${colorOf(b.cert)}` }), shortBankName(b.bank_name)),
      el("div", { class: "detail-name" }, b.branch_name),
      el("div", { class: "detail-addr" }, address),
      el("div", { class: "fields" }, ...fields.map(([k, v]) =>
        el("div", { class: "field" }, el("div", { class: "label" }, k), el("div", { class: "field-value" }, v)))),
      b.opened_date && b.opened_date < "1960" &&
        el("p", { class: "fine" }, "FDIC's opened date for older offices is often the founding bank's date."),
      el("div", { class: "detail-summary" }, el("span", { class: "label muted" }, "Bank in the metro "), summary.join(" · ")),
      el("div", { class: "detail-actions" },
        el("button", { type: "button", class: "btn btn-dark", onclick: () => setBank(onlyThis ? null : b.cert) },
          onlyThis ? "Show all banks" : `Show only ${shortOf(b.cert)}`),
        el("a", { class: "btn", href: `https://maps.apple.com/?address=${encodeURIComponent(address)}`, target: "_blank", rel: "noopener" }, "Directions"))));
  box.classList.add("open");
}

// ---------- Notes ----------

// Automatic notes never name a bank in no_callout_certs.
// If such a bank would top a ranking, that note is skipped rather than naming the runner-up as "most".
function renderNotes() {
  const area = state.area;
  const notes = [];
  const today = branchesIn(area).length;
  const then = sum(depRows(area, 1994), (r) => r.branches);
  if (then) notes.push(`${fmtInt(today)} branches today, ${today <= then ? "down" : "up"} from ${fmtInt(then)} in 1994.`);

  const top5 = (year) => sum(depRows(area, year).slice(0, 5), (r) => r.share_pct);
  if (depRows(area, D.latest).length > 5 && depRows(area, 1994).length > 5) {
    const now = top5(D.latest), old = top5(1994);
    notes.push(`The five largest banks hold ${Math.round(now)}% of deposits, ${now >= old ? "up" : "down"} from ${Math.round(old)}% in 1994.`);
  }

  const since = D.eventYears.at(-1) - 10;
  if (D.events) {
    const opens = new Map();
    for (const e of D.events) {
      if (e.event !== "opened" || e.year < since || (area !== "Metro" && e.locality !== area)) continue;
      opens.set(e.cert, (opens.get(e.cert) ?? 0) + 1);
    }
    const ranked = [...opens.entries()].sort((a, b) => b[1] - a[1]);
    const [first, second] = ranked;
    if (first && first[1] >= 3 && (!second || first[1] > second[1]) && !D.noCallout.has(first[0])) {
      notes.push(`${shortOf(first[0])} opened ${first[1]} branches since ${since}, more than any other bank.`);
    }
  }

  const recent = D.eventYears.filter((y) => y >= since).map((y) => D.evIndex.get(`${area}|${y}`)).filter(Boolean);
  const worst = recent.reduce((best, r) => (r.closed > (best?.closed ?? 0) ? r : best), null);
  if (worst) notes.push(`Most closings in a single year since ${since}: ${worst.closed}, in ${worst.year}.`);

  $("#notes").replaceChildren(...notes.map((t) => el("li", {}, t)));
}

// ---------- Part II: then-and-now ----------

function renderBanks() {
  const area = state.area;
  const years = [...THEN_YEARS, D.latest];
  const shareIn = (year) => new Map(depRows(area, year).map((r) => [r.cert, r.share_pct]));
  const shares = years.map(shareIn);
  const top = depRows(area, D.latest).slice(0, 8);
  const f = (x) => (x == null ? "—" : x.toFixed(1));

  const head = el("tr", {}, el("th", {}, "Bank"), ...years.map((y, i) => el("th", { class: i === years.length - 1 ? "now" : null }, y)));
  const rows = top.map((r) => el("tr", {},
    el("td", { class: "bank" }, el("span", { class: "name" }, el("span", { class: "dot", style: `background:${colorOf(r.cert)}` }), shortOf(r.cert))),
    ...shares.map((m, i) => el("td", { class: i === years.length - 1 ? "now" : null }, f(m.get(r.cert))))));
  const total = (name, fn) => el("tr", { class: "total" }, el("td", { class: "bank" }, name),
    ...years.map((y, i) => el("td", { class: i === years.length - 1 ? "now" : null }, fn(y))));
  const totals = [
    total("Top 5 hold", (y) => (depRows(area, y).length ? `${sum(depRows(area, y).slice(0, 5), (r) => r.share_pct).toFixed(1)}%` : "—")),
    total("Banks here", (y) => fmtInt(depRows(area, y).length)),
    total("Deposits", (y) => fmtMoney(sum(depRows(area, y), (r) => r.deposits_thousands))),
  ];
  $("#p2-area").textContent = areaLabel(area);
  $("#banks").replaceChildren(
    el("table", { class: "share-table" },
      el("caption", { class: "visually-hidden" }, `Share of deposits for today's largest banks, ${years.join(", ")}`),
      el("thead", {}, head), el("tbody", {}, ...rows, ...totals)),
    el("p", { class: "fine" }, `Percent of deposits on June 30 of each year. Shares follow each bank's charter; "—" means that charter had no branches here that year, and banks that merged in show under their old names in earlier years.`));
}

// ---------- Part III: year strip + that year's list ----------

function renderYears() {
  const area = state.area;
  const rows = D.eventYears.map((y) => D.evIndex.get(`${area}|${y}`) ?? { year: y, opened: 0, closed: 0 });
  const max = Math.max(1, ...rows.map((r) => Math.max(r.opened, r.closed)));
  if (!D.eventYears.includes(state.year)) state.year = D.eventYears.at(-1);
  const cur = rows.find((r) => r.year === state.year);

  const strip = el("div", { class: "strip", role: "group", "aria-label": "Pick a year" }, ...rows.map((r) =>
    el("button", { type: "button", "aria-pressed": String(r.year === state.year), "aria-label": `${r.year}: ${r.opened} opened, ${r.closed} closed`, onclick: () => setYear(r.year) },
      el("span", { class: "up" }, el("span", { class: "bar-o", style: `height:${Math.round((r.opened / max) * 46)}px` })),
      el("span", { class: "axis" }),
      el("span", { class: "down" }, el("span", { class: "bar-c", style: `display:block;height:${Math.round((r.closed / max) * 82)}px` })))));
  const first = D.eventYears[0], last = D.eventYears.at(-1);
  const mid = D.eventYears[Math.floor(D.eventYears.length / 2)];
  const thisYear = state.year === Number(D.eventsAsOf.slice(0, 4));

  const idx = D.eventYears.indexOf(state.year);
  const stepper = el("div", { class: "stepper" },
    el("button", { type: "button", "aria-label": "Previous year", disabled: idx <= 0, onclick: () => setYear(D.eventYears[idx - 1]) }, "‹"),
    el("div", { class: "stepper-year" }, el("div", { class: "mono" }, state.year),
      el("div", { class: "muted", style: "font-size:12px" }, `${cur.opened} opened · ${cur.closed} closed${thisYear ? ` · through ${fmtDate(D.eventsAsOf, { year: false })}` : ""}`)),
    el("button", { type: "button", "aria-label": "Next year", disabled: idx >= D.eventYears.length - 1, onclick: () => setYear(D.eventYears[idx + 1]) }, "›"));

  $("#p3-area").textContent = areaLabel(area);
  $("#years").replaceChildren(
    el("div", { class: "legend-row" },
      el("span", {}, el("span", { class: "sq", style: "background:var(--blue)" }), "Opened (above)"),
      el("span", {}, el("span", { class: "sq", style: "background:var(--ink)" }), "Closed (below)")),
    strip,
    el("div", { class: "strip-scale" }, el("span", {}, first), el("span", {}, mid), el("span", {}, last)),
    stepper,
    el("div", { class: "events" }, ...eventRows(area, state.year)),
    el("p", { class: "fine" }, `FDIC's records of openings and closings start in mid-1999, so this begins in ${first}. Mergers and sales aren't counted here.`));
}

function setYear(y) {
  state.year = y;
  renderYears();
}

// One line per bank per day, so ten closings on one date read as one entry.
function eventRows(area, year) {
  if (!D.events) return [el("p", { class: "fine" }, "The list of individual openings and closings isn't in the site data yet.")];
  const groups = new Map();
  for (const e of D.events) {
    if (e.year !== year || (area !== "Metro" && e.locality !== area)) continue;
    const key = `${e.date}|${e.cert}|${e.event}`;
    if (!groups.has(key)) groups.set(key, { ...e, places: new Map() });
    const g = groups.get(key);
    g.places.set(e.city, (g.places.get(e.city) ?? 0) + 1);
  }
  if (!groups.size) return [el("p", { class: "fine" }, "No openings or closings this year.")];
  return [...groups.values()].sort((a, b) => a.date.localeCompare(b.date)).map((g) => {
    const n = sum([...g.places.values()], (v) => v);
    const where = [...g.places.entries()].map(([city, k]) => (k > 1 ? `${city} (${k})` : city)).join(", ");
    return el("div", { class: "event" },
      el("span", { class: "mono muted" }, fmtDate(g.date, { year: false })),
      el("span", { class: `event-type ${g.event}` }, `${g.event.toUpperCase()}${n > 1 ? ` ${n}` : ""}`),
      el("span", {}, el("b", {}, shortBankName(g.bank_name)), el("br"), el("span", { class: "event-where" }, where)));
  });
}

PHONE.addEventListener("change", () => D && render());
