// Shared helpers for the map page and the Pipeline page.

export async function loadJSON(name) {
  const res = await fetch(`site-data/${name}.json`, { cache: "no-cache" });
  if (!res.ok) throw new Error(`${name}.json: HTTP ${res.status}`);
  return res.json();
}

// Builds DOM nodes. Text always goes in as text nodes, never as HTML, so a
// bank or branch name can't inject markup.
export function el(tag, attrs = {}, ...children) {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(attrs)) {
    if (value == null || value === false) continue;
    if (key === "class") node.className = value;
    else if (key === "style") node.style.cssText = value;
    else if (key.startsWith("on")) node.addEventListener(key.slice(2), value);
    else node.setAttribute(key, value === true ? "" : value);
  }
  for (const child of children.flat()) {
    if (child == null || child === false) continue;
    node.append(child instanceof Node ? child : document.createTextNode(String(child)));
  }
  return node;
}

export const fmtInt = (n) => Number(n).toLocaleString("en-US");

// Deposits arrive in thousands of dollars, as FDIC reports them.
export function fmtMoney(thousands) {
  if (thousands == null) return "—";
  const dollars = thousands * 1000;
  if (dollars >= 1e9) return `$${(dollars / 1e9).toFixed(1)}B`;
  if (dollars >= 1e6) return `$${(dollars / 1e6).toFixed(1)}M`;
  return `$${Math.round(dollars / 1e3)}K`;
}

// "2026-02-06" → "02/06/2026". Date-only strings are read as calendar dates,
// not instants, so a time zone can't shift them by a day.
export function fmtDate(iso, { year = true } = {}) {
  if (!iso) return "—";
  const [y, m, d] = iso.slice(0, 10).split("-");
  return year ? `${m}/${d}/${y}` : `${m}/${d}`;
}

const ET = new Intl.DateTimeFormat("en-US", {
  timeZone: "America/New_York", weekday: "short", month: "2-digit", day: "2-digit", year: "numeric",
  hour: "numeric", minute: "2-digit"
});

// A full timestamp (e.g. a pipeline run) shown in Eastern time.
export function fmtStampET(iso) {
  if (!iso) return "—";
  return ET.format(new Date(iso)).replace(",", "") + " ET";
}

// "JPMorgan Chase Bank, National Association" → "JPMorgan Chase Bank".
export function shortBankName(name) {
  return String(name)
    .replace(/,?\s+(National Association|N\.A\.|Federal Savings Bank|FSB|F\.S\.B\.)$/i, "")
    .trim();
}
