"""Silver: typed, deduplicated tables, kept current with MERGE.

- dim_bank and dim_branch are SCD2: when a bank or branch changes (name,
  address, owner, type...), the old row is closed and a new one opened, so
  past states stay queryable, the same way the dimensions work at the bank.
- fact_branch_event and fact_branch_deposits_year are upserted on their keys.
- dim_metro_county is a small reference table, rewritten each run.

Each builder is a DuckDB query over the latest bronze snapshot; the MERGE
itself runs in delta-rs.
"""

from __future__ import annotations

from datetime import datetime

import pyarrow as pa

from pipeline import config, fetch, lake

# --- Shared SQL pieces -----------------------------------------------------------


def row_hash(columns: list[str]) -> str:
    """SQL for an md5 over the columns that, when changed, mean a new version.

    COALESCE matters: concat_ws skips NULLs, so without it ('a', NULL, 'b')
    and ('a', 'b', NULL) would hash the same.
    """
    parts = ", ".join(f"COALESCE(CAST({c} AS VARCHAR), '<null>')" for c in columns)
    return f"md5(concat_ws('|', {parts}))"


def ts_literal(ts: datetime) -> str:
    """A timestamp delta-rs's MERGE expressions accept."""
    return f"CAST('{ts.strftime('%Y-%m-%dT%H:%M:%S.%fZ')}' AS TIMESTAMP)"


def is_miscoded(county_fips: str, city: str) -> str:
    """SQL that's true when a row's county and city are in config.MISCODED_PLACES."""
    return f"""EXISTS (
            SELECT 1
            FROM miscoded_places AS m
            WHERE m.county_fips = {county_fips}
              AND m.city = UPPER(TRIM({city}))
        )"""


# --- Snapshots (latest bronze, typed) -------------------------------------------

DIM_BANK_COLUMNS = [
    "bank_name", "city", "state", "holding_company", "bank_class",
    "is_active", "established_date", "end_date", "website",
]

DIM_BANK_SNAPSHOT = f"""
-- One row per bank charter (CERT) from the newest institutions snapshot.
WITH typed AS (
    SELECT
        CAST(CERT AS BIGINT) AS cert,
        NAME AS bank_name,
        CITY AS city,
        STALP AS state,
        NULLIF(TRIM(NAMEHCR), '') AS holding_company,
        BKCLASS AS bank_class,
        ACTIVE = '1' AS is_active,
        CAST(TRY_STRPTIME(ESTYMD, '%m/%d/%Y') AS DATE) AS established_date,
        -- FDIC writes 12/31/9999 for "no end date"; NULL says that plainly.
        CASE
            WHEN ENDEFYMD LIKE '%9999' THEN NULL
            ELSE CAST(TRY_STRPTIME(ENDEFYMD, '%m/%d/%Y') AS DATE)
        END AS end_date,
        NULLIF(TRIM(WEBADDR), '') AS website,
        ID
    FROM bronze_institutions
),
deduped AS (
    -- Fetch already refuses repeated IDs; this keeps silver safe on its own.
    -- ROW_NUMBER numbers the rows within each CERT, and QUALIFY keeps the
    -- first, which is a dedupe without a self-join or GROUP BY on every column.
    SELECT *
    FROM typed
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cert ORDER BY ID) = 1
)
SELECT
    cert,
    {", ".join(DIM_BANK_COLUMNS)},
    {row_hash(DIM_BANK_COLUMNS)} AS row_hash
FROM deduped
"""

DIM_BRANCH_COLUMNS = [
    "cert", "branch_name", "branch_number", "address", "city", "state", "zip",
    "county_name", "county_fips", "cbsa_code", "latitude", "longitude",
    "service_type_code", "service_type", "is_main_office", "established_date",
    "acquired_date",
]

DIM_BRANCH_SNAPSHOT = f"""
-- One row per branch office (UNINUM, FDIC's permanent office ID) nationwide.
WITH typed AS (
    SELECT
        CAST(UNINUM AS BIGINT) AS uninum,
        CAST(CERT AS BIGINT) AS cert,
        OFFNAME AS branch_name,
        TRY_CAST(OFFNUM AS INTEGER) AS branch_number,
        ADDRESS AS address,
        CITY AS city,
        STALP AS state,
        ZIP AS zip,
        COUNTY AS county_name,
        LPAD(STCNTY, 5, '0') AS county_fips,
        NULLIF(TRIM(CBSA_NO), '') AS cbsa_code,
        -- FDIC uses 0 for "not geocoded", and 0,0 is in the Atlantic.
        -- Rounded to 6 places (~10 cm) so float noise isn't a "change".
        ROUND(NULLIF(TRY_CAST(LATITUDE AS DOUBLE), 0), 6) AS latitude,
        ROUND(NULLIF(TRY_CAST(LONGITUDE AS DOUBLE), 0), 6) AS longitude,
        TRY_CAST(SERVTYPE AS INTEGER) AS service_type_code,
        -- FDIC mixes "FULL SERVICE - ..." and "Full Service - ..." for the
        -- same code; one casing keeps the map legend to one entry per type.
        UPPER(LEFT(TRIM(SERVTYPE_DESC), 1)) || LOWER(SUBSTR(TRIM(SERVTYPE_DESC), 2)) AS service_type,
        MAINOFF = '1' AS is_main_office,
        CAST(TRY_STRPTIME(ESTYMD, '%m/%d/%Y') AS DATE) AS established_date,
        CASE
            WHEN ACQDATE LIKE '%9999' THEN NULL
            ELSE CAST(TRY_STRPTIME(ACQDATE, '%m/%d/%Y') AS DATE)
        END AS acquired_date,
        ID
    FROM bronze_locations
),
deduped AS (
    SELECT *
    FROM typed
    QUALIFY ROW_NUMBER() OVER (PARTITION BY uninum ORDER BY ID) = 1
)
SELECT
    uninum,
    {", ".join(DIM_BRANCH_COLUMNS)},
    {row_hash(DIM_BRANCH_COLUMNS)} AS row_hash
FROM deduped
"""

FACT_EVENT_COLUMNS = [
    "change_code", "change_desc", "event_type", "event_date", "event_year",
    "processed_date", "uninum", "cert", "bank_name", "branch_name", "address",
    "city", "county_fips", "latitude", "longitude", "transaction_number",
]

FACT_BRANCH_EVENT = f"""
-- Dated branch events in the metro counties. The JOIN to event_types keeps
-- only the codes we classify (config.EVENT_TYPES); others stay in bronze.
WITH typed AS (
    SELECT
        ID AS event_id,
        CAST(CHANGECODE AS INTEGER) AS change_code,
        CHANGECODE_DESC AS change_desc,
        CAST(TRY_STRPTIME(EFFDATE, '%Y-%m-%dT%H:%M:%S') AS DATE) AS event_date,
        CAST(TRY_STRPTIME(PROCDATE, '%Y-%m-%dT%H:%M:%S') AS DATE) AS processed_date,
        TRY_CAST(UNINUM AS BIGINT) AS uninum,
        TRY_CAST(CERT AS BIGINT) AS cert,
        INSTNAME AS bank_name,
        OFF_NAME AS branch_name,
        OFF_PADDR AS address,
        OFF_PCITY AS city,
        -- History splits FIPS into state and county numbers; rebuild the
        -- 5-digit code so it joins to branches and SOD.
        LPAD(OFF_PSTNUM, 2, '0') || LPAD(OFF_CNTYNUM, 3, '0') AS county_fips,
        ROUND(NULLIF(TRY_CAST(OFF_LATITUDE AS DOUBLE), 0), 6) AS latitude,
        ROUND(NULLIF(TRY_CAST(OFF_LONGITUDE AS DOUBLE), 0), 6) AS longitude,
        TRANSNUM AS transaction_number
    FROM bronze_history
),
classified AS (
    SELECT
        t.*,
        e.event_type,
        YEAR(t.event_date) AS event_year
    FROM typed AS t
    JOIN event_types AS e
        ON e.change_code = t.change_code
)
SELECT
    event_id,
    {", ".join(FACT_EVENT_COLUMNS)},
    {row_hash(FACT_EVENT_COLUMNS)} AS row_hash
FROM classified AS c
-- Out-of-area towns FDIC filed under a metro county (config.MISCODED_PLACES).
WHERE NOT {is_miscoded("c.county_fips", "c.city")}
QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY processed_date DESC) = 1
"""

FACT_DEPOSIT_COLUMNS = [
    "year", "as_of_date", "cert", "bank_name", "holding_company", "uninum",
    "branch_number", "branch_name", "address", "city", "state", "county_fips",
    "county_name", "service_type_code", "deposits_thousands", "latitude",
    "longitude",
]

SOD_TYPED = f"""
-- Summary of Deposits: each branch's deposits on June 30 of each year,
-- reported by the bank charter that held the branch that day.
WITH typed AS (
    SELECT
        ID AS sod_id,  -- year_cert_branchnumber; UNINUM alone isn't always there
        CAST(YEAR AS INTEGER) AS year,
        MAKE_DATE(CAST(YEAR AS INTEGER), 6, 30) AS as_of_date,
        CAST(CERT AS BIGINT) AS cert,
        NAMEFULL AS bank_name,
        NULLIF(TRIM(NAMEHCR), '') AS holding_company,
        -- NULL for a few hundred 1990s-2010 thrift branches FDIC never
        -- numbered; they still count toward deposits.
        TRY_CAST(UNINUMBR AS BIGINT) AS uninum,
        TRY_CAST(BRNUM AS INTEGER) AS branch_number,
        NAMEBR AS branch_name,
        ADDRESBR AS address,
        CITYBR AS city,
        STALPBR AS state,
        LPAD(STCNTYBR, 5, '0') AS county_fips,
        CNTYNAMB AS county_name,
        TRY_CAST(BRSERTYP AS INTEGER) AS service_type_code,
        CAST(DEPSUMBR AS BIGINT) AS deposits_thousands,
        ROUND(NULLIF(TRY_CAST(SIMS_LATITUDE AS DOUBLE), 0), 6) AS latitude,
        ROUND(NULLIF(TRY_CAST(SIMS_LONGITUDE AS DOUBLE), 0), 6) AS longitude
    FROM bronze_sod
),
flagged AS (
    -- Out-of-area towns FDIC filed under a metro county (config.MISCODED_PLACES).
    -- Aliased: a bare county_fips inside the EXISTS would mean m.county_fips.
    SELECT t.*, {is_miscoded("t.county_fips", "t.city")} AS miscoded
    FROM typed AS t
)
"""

FACT_BRANCH_DEPOSITS_YEAR = f"""{SOD_TYPED}
SELECT
    sod_id,
    {", ".join(FACT_DEPOSIT_COLUMNS)},
    {row_hash(FACT_DEPOSIT_COLUMNS)} AS row_hash
FROM flagged
WHERE NOT miscoded
QUALIFY ROW_NUMBER() OVER (PARTITION BY sod_id ORDER BY sod_id) = 1
"""

# What silver set aside each year. FDIC's control totals still include these
# rows, so the totals check adds them back before comparing.
MISCODED_DEPOSITS_BY_YEAR = f"""{SOD_TYPED}
SELECT
    year,
    COUNT(*) AS branches,
    CAST(SUM(deposits_thousands) AS BIGINT) AS deposits
FROM flagged
WHERE miscoded
GROUP BY year
"""


# --- MERGE patterns ----------------------------------------------------------------

SCD2_COLUMNS = [
    pa.field("valid_from", pa.timestamp("us", tz="UTC")),
    # NULL while the row is current, like Databricks' APPLY CHANGES __END_AT.
    pa.field("valid_to", pa.timestamp("us", tz="UTC")),
    pa.field("is_current", pa.bool_()),
]


def _create_empty(name: str, schema: pa.Schema, run_id: str) -> None:
    """Create the table empty so the first load runs the same MERGE as later ones."""
    lake.write("silver", name, schema.empty_table(), mode="append", run_id=run_id)


def scd2_merge(con, name: str, key: str, snapshot: pa.Table, run_ts: datetime, run_id: str) -> dict:
    """Apply a full snapshot to an SCD2 dimension in one MERGE.

    The staged source has each snapshot row twice when it changed:
      - keyed (merge_key = key): matches the current row and closes it;
      - unkeyed (merge_key = NULL): can't match anything, so it's inserted
        as the new current version.
    Unchanged rows match and do nothing. Keys missing from the snapshot (a
    branch that closed, a charter that merged away) are closed too.
    This is the standard Delta SCD2 MERGE from the Databricks docs.
    """
    if not lake.exists("silver", name):
        _create_empty(name, pa.schema(list(snapshot.schema) + SCD2_COLUMNS), run_id)
    dim = lake.table("silver", name)
    con.register("scd2_snapshot", snapshot)
    con.register("scd2_dim", dim.to_pyarrow_table())  # in memory: see lake.register
    staged = lake.query(
        con,
        f"""
        WITH changed AS (
            SELECT s.*
            FROM scd2_snapshot AS s
            JOIN scd2_dim AS d
                ON d.{key} = s.{key}
               AND d.is_current
            WHERE d.row_hash <> s.row_hash
        )
        SELECT s.{key} AS merge_key, s.*, CAST(? AS TIMESTAMPTZ) AS valid_from
        FROM scd2_snapshot AS s
        UNION ALL
        SELECT NULL AS merge_key, c.*, CAST(? AS TIMESTAMPTZ) AS valid_from
        FROM changed AS c
        """,
        [run_ts, run_ts],
    )
    insert_values = {col: f"s.{col}" for col in snapshot.column_names}
    insert_values |= {"valid_from": "s.valid_from", "valid_to": "NULL", "is_current": "true"}
    return (
        dim.merge(
            source=staged,
            predicate=f"t.{key} = s.merge_key AND t.is_current",
            source_alias="s",
            target_alias="t",
            commit_properties=lake.commit_props(run_id),
        )
        .when_matched_update(
            predicate="t.row_hash <> s.row_hash",
            updates={"valid_to": "s.valid_from", "is_current": "false"},
        )
        .when_not_matched_insert(updates=insert_values)
        .when_not_matched_by_source_update(
            predicate="t.is_current",
            updates={"valid_to": ts_literal(run_ts), "is_current": "false"},
        )
        .execute()
    )


def upsert(name: str, key: str, rows: pa.Table, run_id: str) -> dict:
    """MERGE a full extract into a fact table on its key.

    Changed rows (FDIC corrections) are updated, new rows inserted, and rows
    FDIC no longer sends are deleted, so silver mirrors the source. Fetch
    refuses partial extracts, which is what makes the delete safe.
    """
    if not lake.exists("silver", name):
        _create_empty(name, rows.schema, run_id)
    return (
        lake.table("silver", name)
        .merge(
            source=rows,
            predicate=f"t.{key} = s.{key}",
            source_alias="s",
            target_alias="t",
            commit_properties=lake.commit_props(run_id),
        )
        .when_matched_update_all(predicate="t.row_hash <> s.row_hash")
        .when_not_matched_insert_all()
        .when_not_matched_by_source_delete()
        .execute()
    )


# --- Build -------------------------------------------------------------------------


def metro_county_table(con) -> pa.Table:
    """dim_metro_county: the counties every metro filter and the city picker use."""
    counties = fetch.metro_counties(con)
    in_cbsa = {
        fips
        for (fips,) in con.execute(
            "SELECT DISTINCT LPAD(STCNTY, 5, '0') FROM bronze_locations WHERE CBSA_NO = ?",
            [config.METRO_CBSA],
        ).fetchall()
    }
    states = {"51": "VA", "37": "NC"}
    return pa.table(
        {
            "county_fips": list(counties),
            "locality": list(counties.values()),
            "state": [states.get(fips[:2]) for fips in counties],
            "in_config": [fips in config.METRO_COUNTIES for fips in counties],
            "in_cbsa_today": [fips in in_cbsa for fips in counties],
        }
    )


def register_lookups(con) -> None:
    """The config lookups the silver queries join to."""
    con.register(
        "event_types",
        pa.table({
            "change_code": pa.array(list(config.EVENT_TYPES), pa.int32()),
            "event_type": list(config.EVENT_TYPES.values()),
        }),
    )
    con.register(
        "miscoded_places",
        pa.table({
            "county_fips": [fips for fips, _ in config.MISCODED_PLACES],
            "city": [city for _, city in config.MISCODED_PLACES],
        }),
    )


def build(con, run_ts: datetime, run_id: str, log=print) -> dict:
    """Rebuild every silver table from the latest bronze snapshots."""
    for name in ("institutions", "locations", "history", "sod"):
        if not lake.register_latest_bronze(con, name):
            raise RuntimeError(f"bronze.{name} doesn't exist yet; run without --skip-fetch first")
    register_lookups(con)

    results = {}
    lake.write("silver", "dim_metro_county", metro_county_table(con), mode="overwrite", run_id=run_id)

    results["dim_bank"] = scd2_merge(con, "dim_bank", "cert", lake.query(con, DIM_BANK_SNAPSHOT), run_ts, run_id)
    results["dim_branch"] = scd2_merge(
        con, "dim_branch", "uninum", lake.query(con, DIM_BRANCH_SNAPSHOT), run_ts, run_id
    )
    results["fact_branch_event"] = upsert(
        "fact_branch_event", "event_id", lake.query(con, FACT_BRANCH_EVENT), run_id
    )
    results["fact_branch_deposits_year"] = upsert(
        "fact_branch_deposits_year", "sod_id", lake.query(con, FACT_BRANCH_DEPOSITS_YEAR), run_id
    )
    for name, result in results.items():
        log(
            f"  silver.{name}: +{result['num_target_rows_inserted']:,} inserted, "
            f"{result['num_target_rows_updated']:,} updated, {result['num_target_rows_deleted']:,} deleted"
        )
    return results


def register(con) -> None:
    """Expose silver tables to DuckDB under their own names."""
    for name in ("dim_metro_county", "dim_bank", "dim_branch", "fact_branch_event", "fact_branch_deposits_year"):
        lake.register(con, name, "silver", name)
