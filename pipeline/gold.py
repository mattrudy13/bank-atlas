"""Gold: the small tables the site reads, as Delta and as JSON in site-data/.

build() only computes; publish() writes. run.py runs the checks in between,
so a failed check leaves the last good gold (Delta and JSON) untouched.
"""

from __future__ import annotations

import json
import os
from datetime import date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc

from pipeline import config, lake

BRANCHES_NOW = """
-- Every branch open today in the metro, with its bank and latest deposits.
WITH current_branches AS (
    SELECT *
    FROM dim_branch
    WHERE is_current
),
current_banks AS (
    SELECT cert, bank_name
    FROM dim_bank
    WHERE is_current
),
latest_deposits AS (
    -- Newest June 30 figure per branch. ROW_NUMBER ranks each branch's years
    -- newest first and QUALIFY keeps rank 1: one row per branch, no self-join.
    SELECT
        uninum,
        year AS deposits_year,
        deposits_thousands
    FROM fact_branch_deposits_year
    WHERE uninum IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY uninum ORDER BY year DESC) = 1
)
SELECT
    b.uninum,
    b.cert,
    k.bank_name,
    b.branch_name,
    b.address,
    b.city,
    b.state,
    b.zip,
    m.locality,
    b.county_fips,
    b.latitude,
    b.longitude,
    b.established_date AS opened_date,
    b.acquired_date,
    b.service_type_code AS branch_type_code,
    b.service_type AS branch_type,
    b.is_main_office,
    d.deposits_thousands,
    d.deposits_year
FROM current_branches AS b
JOIN dim_metro_county AS m
    ON m.county_fips = b.county_fips
LEFT JOIN current_banks AS k
    ON k.cert = b.cert
LEFT JOIN latest_deposits AS d
    ON d.uninum = b.uninum
ORDER BY m.locality, k.bank_name, b.branch_name, b.uninum
"""

BRANCH_EVENTS_BY_YEAR = """
-- Openings, closings, hand-overs and moves per year, for the whole metro and
-- each locality. The year x area grid makes quiet years show as zeros
-- instead of missing rows, so the site's charts need no gap-filling.
-- Starts in 2000: FDIC began recording openings, closings and moves in
-- Aug 1999, so earlier years would show false zeros.
WITH years AS (
    SELECT CAST(range AS INTEGER) AS year
    FROM range($first_year, $last_year + 1)
),
areas AS (
    SELECT 'Metro' AS area, NULL AS county_fips
    UNION ALL
    SELECT locality, county_fips
    FROM dim_metro_county
),
metro_events AS (
    SELECT
        e.event_year AS year,
        m.locality,
        e.event_type
    FROM fact_branch_event AS e
    JOIN dim_metro_county AS m
        ON m.county_fips = e.county_fips
    WHERE e.event_year BETWEEN $first_year AND $last_year
),
events_by_area AS (
    -- Each event counts once for its locality and once for the metro.
    SELECT year, 'Metro' AS area, event_type FROM metro_events
    UNION ALL
    SELECT year, locality AS area, event_type FROM metro_events
)
SELECT
    y.year,
    a.area,
    a.county_fips,
    COUNT(*) FILTER (WHERE x.event_type = 'opened') AS opened,
    COUNT(*) FILTER (WHERE x.event_type = 'closed') AS closed,
    COUNT(*) FILTER (WHERE x.event_type = 'changed_hands') AS changed_hands,
    COUNT(*) FILTER (WHERE x.event_type = 'moved') AS moved,
    COUNT(*) FILTER (WHERE x.event_type = 'opened')
        - COUNT(*) FILTER (WHERE x.event_type = 'closed') AS net_change
FROM years AS y
CROSS JOIN areas AS a
LEFT JOIN events_by_area AS x
    ON x.year = y.year
   AND x.area = a.area
GROUP BY y.year, a.area, a.county_fips
ORDER BY y.year, a.area = 'Metro' DESC, a.area
"""

DEPOSIT_SHARE_BY_YEAR = """
-- Each bank's share of deposits per year, for the metro and each locality.
-- By charter (CERT), as FDIC reports it, with the bank's name that year.
WITH metro_sod AS (
    SELECT
        d.year,
        m.locality,
        d.cert,
        d.bank_name,
        d.deposits_thousands
    FROM fact_branch_deposits_year AS d
    JOIN dim_metro_county AS m
        ON m.county_fips = d.county_fips
    WHERE d.year >= $first_year
),
by_area AS (
    SELECT year, 'Metro' AS area, cert, bank_name, deposits_thousands FROM metro_sod
    UNION ALL
    SELECT year, locality AS area, cert, bank_name, deposits_thousands FROM metro_sod
),
bank_totals AS (
    SELECT
        year,
        area,
        cert,
        MAX(bank_name) AS bank_name,  -- one name per charter per year
        -- DuckDB widens SUM(BIGINT) to a 128-bit integer; metro totals are
        -- ~$35B (35M thousands), far inside BIGINT.
        CAST(SUM(deposits_thousands) AS BIGINT) AS deposits_thousands,
        COUNT(*) AS branches
    FROM by_area
    GROUP BY year, area, cert
)
SELECT
    year,
    area,
    cert,
    bank_name,
    deposits_thousands,
    branches,
    -- A window SUM gives the area's total on every row without collapsing
    -- rows the way GROUP BY would. NULLIF guards an area with $0 deposits.
    100.0 * deposits_thousands
        / NULLIF(SUM(deposits_thousands) OVER (PARTITION BY year, area), 0) AS share_pct,
    RANK() OVER (PARTITION BY year, area ORDER BY deposits_thousands DESC) AS rank
FROM bank_totals
ORDER BY year, area = 'Metro' DESC, area, rank, cert
"""

BRANCH_EVENTS = """
-- Each metro branch opening and closing, oldest first: the list under the
-- site's year strip. Mergers, sales and moves are left out because no branch
-- appears or disappears; they stay in the yearly counts.
SELECT
    e.event_date AS "date",
    e.event_year AS "year",
    e.event_type AS event,
    e.cert,
    e.bank_name,  -- FDIC's name for the bank on the event date
    e.branch_name,
    e.city,
    m.locality,
    e.county_fips
FROM fact_branch_event AS e
JOIN dim_metro_county AS m
    ON m.county_fips = e.county_fips
WHERE e.event_type IN ('opened', 'closed')
  AND e.event_year BETWEEN $first_year AND $last_year
-- Tie-breakers keep the order stable run to run, so the JSON only changes
-- when the data does.
ORDER BY e.event_date, e.event_type, e.bank_name, e.branch_name, e.event_id
"""

GOLD_TABLES = ("branches_now", "branch_events_by_year", "branch_events", "deposit_share_by_year")


def title_case_city(data: pa.Table) -> pa.Table:
    """History has cities in capitals ("VIRGINIA BEACH"); match the other files."""
    i = data.schema.get_field_index("city")
    return data.set_column(i, "city", pc.utf8_title(data["city"]))


def build(con, run_ts: datetime) -> dict[str, pa.Table]:
    """Compute gold from the registered silver views. Writes nothing."""
    events_years = {"first_year": config.EVENTS_FIRST_YEAR, "last_year": run_ts.year}
    return {
        "branches_now": lake.query(con, BRANCHES_NOW),
        "branch_events_by_year": lake.query(con, BRANCH_EVENTS_BY_YEAR, events_years),
        "branch_events": title_case_city(lake.query(con, BRANCH_EVENTS, events_years)),
        "deposit_share_by_year": lake.query(con, DEPOSIT_SHARE_BY_YEAR, {"first_year": config.FIRST_YEAR}),
    }


def register(con, tables: dict[str, pa.Table]) -> None:
    """Expose computed (not yet published) gold to DuckDB as gold_<name>."""
    for name, data in tables.items():
        con.register(f"gold_{name}", data)


# --- JSON for the site -------------------------------------------------------------

# Decimal places per float column in JSON. Delta keeps full precision.
JSON_ROUNDING = {"latitude": 6, "longitude": 6, "share_pct": 3}


def _jsonable(value, column: str):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, float):
        return round(value, JSON_ROUNDING.get(column, 4))
    return value


def file_meta(con, run_ts: datetime, name: str, sources: dict) -> dict:
    """Metadata at the top of every gold JSON file."""
    localities = con.execute(
        "SELECT county_fips, locality, state FROM dim_metro_county ORDER BY locality"
    ).fetchall()
    # No run timestamp here: pipeline_status.json has it, and leaving it out
    # means a data file only changes (and gets committed) when its data does.
    meta = {
        "table": name,
        # When FDIC built each dataset (its API index time), e.g. SOD yearly.
        "fdic_data_as_of": {endpoint: s["created_at"] for endpoint, s in sources.items()},
        "metro": {"cbsa": config.METRO_CBSA, "name": config.METRO_NAME},
        "localities": [{"county_fips": f, "locality": n, "state": s} for f, n, s in localities],
        # The site shows these banks like any other but must never name them
        # in automatic callouts ("biggest gainer" and the like).
        "no_callout_certs": config.NO_CALLOUT_CERTS,
    }
    if name == "branches_now":
        meta["units"] = {"deposits_thousands": "US dollars, thousands, as of June 30 of deposits_year"}
    if name == "branch_events_by_year":
        meta["first_year"] = config.EVENTS_FIRST_YEAR
        meta["notes"] = (
            f"Starts in {config.EVENTS_FIRST_YEAR}, the first full year after FDIC began recording "
            f"branch openings, closings and moves ({config.OPEN_CLOSE_TRACKED_FROM}). changed_hands "
            "counts branches moved to another bank by merger (713) or sale (722), not as a "
            "closing plus an opening. area 'Metro' rows have county_fips null."
        )
    if name == "branch_events":
        meta["first_year"] = config.EVENTS_FIRST_YEAR
        meta["notes"] = (
            "One row per metro branch opening (FDIC code 711) or closing (721), oldest first. "
            "Mergers, sales and moves are not listed. bank_name is the bank's name on the event "
            "date; branch_name is as FDIC records it (often capitals)."
        )
    if name == "deposit_share_by_year":
        meta["units"] = {"deposits_thousands": "US dollars, thousands, as of June 30", "share_pct": "percent"}
        meta["notes"] = "Share of each area's deposits by bank charter (CERT), named as of that year."
    return meta


def render_json(meta: dict, data: pa.Table) -> str:
    """One row per line: small files and readable git diffs on refresh."""
    rows = [{k: _jsonable(v, k) for k, v in row.items()} for row in data.to_pylist()]
    lines = ["{", f'"meta": {json.dumps(meta, sort_keys=True)},', '"data": [']
    for i, row in enumerate(rows):
        sep = "," if i < len(rows) - 1 else ""
        lines.append(json.dumps(row, separators=(",", ":")) + sep)
    lines += ["]", "}"]
    return "\n".join(lines) + "\n"


def write_text_atomic(path: Path, text: str) -> None:
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(text)
    os.replace(tmp, path)  # atomic swap: the site never sees half a file


def publish(con, tables: dict[str, pa.Table], run_ts: datetime, run_id: str, sources: dict) -> None:
    """Overwrite gold Delta tables and site-data JSON. Only called after checks pass."""
    # Render every file first, so a serialization error stops the publish
    # before anything is written.
    rendered = {name: render_json(file_meta(con, run_ts, name, sources), data) for name, data in tables.items()}
    for name, data in tables.items():
        lake.write("gold", name, data, mode="overwrite", run_id=run_id)
    config.SITE_DATA_DIR.mkdir(parents=True, exist_ok=True)
    for name, text in rendered.items():
        write_text_atomic(config.SITE_DATA_DIR / f"{name}.json", text)
