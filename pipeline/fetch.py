"""Bronze: copy FDIC BankFind data into append-only Delta tables, as-is.

Bronze keeps every value as the raw text the API sent, plus where and when it
came from. Typing and cleanup wait for silver, so a bad cast never loses the
original value, and a re-run of silver can always rebuild from bronze.

Pages stream straight from the API into one Delta commit per table: rows are
never all in memory at once, and a failure mid-way commits nothing.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from datetime import datetime

import httpx
import pyarrow as pa

from pipeline import config, lake

# Extra columns every bronze table carries.
SOURCE_COLUMNS = ["ID", "_source_index", "_source_created_at"]


class FdicApi:
    """Pages through one BankFind endpoint with offset paging."""

    def __init__(self, client: httpx.Client | None = None):
        # Generic client name on purpose: nothing personal goes to the API.
        self.client = client or httpx.Client(
            timeout=config.HTTP_TIMEOUT_SECONDS,
            headers={"User-Agent": "bank-atlas-pipeline"},
        )

    def _get(self, endpoint: str, params: dict) -> dict:
        url = f"{config.FDIC_API}/{endpoint}"
        for attempt in range(1, config.HTTP_RETRIES + 1):
            last_try = attempt == config.HTTP_RETRIES
            try:
                response = self.client.get(url, params=params)
            except httpx.TransportError:
                if last_try:
                    raise
                time.sleep(2**attempt)  # 2s, 4s: blips usually clear quickly
                continue
            # Busy or broken server: worth a retry. A 400 (bad filter) is not.
            if (response.status_code == 429 or response.status_code >= 500) and not last_try:
                time.sleep(2**attempt)
                continue
            response.raise_for_status()
            return response.json()
        raise AssertionError("unreachable")

    def pages(self, endpoint: str, filters: str | None = None) -> Iterator[tuple[dict, list[dict]]]:
        """Yield (meta, rows) one page at a time until FDIC's total is reached."""
        params = {
            "fields": ",".join(config.FIELDS[endpoint]),
            "limit": config.PAGE_SIZE,
            "offset": 0,
        }
        if filters:
            params["filters"] = filters
        if endpoint in config.SORT_BY:
            params["sort_by"] = config.SORT_BY[endpoint]
            params["sort_order"] = "ASC"
        while True:
            body = self._get(endpoint, params)
            rows = [item["data"] for item in body["data"]]
            yield body["meta"], rows
            params["offset"] += len(rows)
            if not rows or params["offset"] >= body["meta"]["total"]:
                return

    def sod_totals(self, filters: str) -> tuple[dict, list[dict]]:
        """FDIC's own deposit total per SOD year for our counties.

        This is the control total the reconciliation check compares against:
        the API sums the same rows server-side, so any row we drop or double
        shows up as a mismatch.
        """
        body = self._get(
            "sod",
            {
                "filters": filters,
                "agg_by": "YEAR",
                "agg_sum_fields": "DEPSUMBR",
                "agg_limit": 200,
                "limit": 0,
            },
        )
        rows = [
            {"YEAR": r["data"]["YEAR"], "BRANCH_COUNT": r["data"]["count"],
             "DEPSUMBR_SUM": r["data"]["sum_DEPSUMBR"], "ID": str(r["data"]["YEAR"])}
            for r in body["data"]
        ]
        if len(rows) >= 200:
            raise RuntimeError("SOD totals hit agg_limit; raise it so no year is cut off")
        return body["meta"], rows


# --- Writing bronze --------------------------------------------------------------


def bronze_schema(fields: list[str]) -> pa.Schema:
    columns = [pa.field(name, pa.string()) for name in fields + SOURCE_COLUMNS]
    columns += [
        pa.field("_ingest_ts", pa.timestamp("us", tz="UTC")),
        # Partition column: one folder per run day keeps "latest snapshot" reads
        # cheap and lets old snapshots be dropped by folder later.
        pa.field("_ingest_date", pa.string()),
    ]
    return pa.schema(columns)


def _raw(value) -> str | None:
    """Store exactly what FDIC sent, as text."""
    return None if value is None else str(value)


def to_bronze(
    endpoint: str,
    fields: list[str],
    pages: Iterator[tuple[dict, list[dict]]],
    run_ts: datetime,
    run_id: str,
) -> dict:
    """Stream pages into bronze.<endpoint> as one append commit.

    Returns what was loaded: row count and FDIC's index name and build time.
    Raises (and commits nothing) if the rows don't match FDIC's own total or
    any ID repeats, which is how a paging glitch would show up.
    """
    schema = bronze_schema(fields)
    loaded = {"rows": 0, "index": None, "created_at": None}
    seen_ids: set[str] = set()  # IDs only; ~80k short strings at most

    def batches() -> Iterator[pa.RecordBatch]:
        total = None
        for meta, rows in pages:
            total = meta.get("total", len(rows)) if total is None else total
            loaded["index"] = meta.get("index", {}).get("name")
            loaded["created_at"] = meta.get("index", {}).get("createTimestamp")
            columns = {name: [_raw(row.get(name)) for row in rows] for name in fields}
            columns["ID"] = [_raw(row.get("ID")) for row in rows]
            columns["_source_index"] = [loaded["index"]] * len(rows)
            columns["_source_created_at"] = [loaded["created_at"]] * len(rows)
            columns["_ingest_ts"] = [run_ts] * len(rows)
            columns["_ingest_date"] = [run_ts.date().isoformat()] * len(rows)
            seen_ids.update(columns["ID"])
            loaded["rows"] += len(rows)
            yield pa.RecordBatch.from_pydict(columns, schema=schema)
        if total is None:
            problem.append(f"{endpoint}: the API returned no pages")
        elif loaded["rows"] != total or len(seen_ids) != total:
            problem.append(
                f"{endpoint}: got {loaded['rows']} rows ({len(seen_ids)} distinct IDs), "
                f"FDIC says {total}. Not writing a partial snapshot."
            )
        if problem:
            raise RuntimeError(problem[0])  # aborts the write: nothing is committed

    problem: list[str] = []
    reader = pa.RecordBatchReader.from_batches(schema, batches())
    try:
        lake.write("bronze", endpoint, reader, mode="append", run_id=run_id, partition_by=["_ingest_date"])
    except Exception as error:
        # delta-rs wraps our error in a long DeltaError; surface the plain one.
        if problem:
            raise RuntimeError(problem[0]) from error
        raise
    return loaded


# --- The metro's counties ------------------------------------------------------


def metro_counties(con) -> dict[str, str]:
    """County FIPS -> display name for the metro, from the latest locations.

    Today's CBSA 47260 counties come from FDIC; the pinned list in config keeps
    a county even if it loses its last branch. Every year of history and SOD
    is filtered by these counties, not by FDIC's yearly metro code, because
    the metro's definition changed over the decades.
    """
    lake.register_latest_bronze(con, "locations")
    rows = con.execute(
        """
        SELECT DISTINCT
            LPAD(STCNTY, 5, '0') AS county_fips,
            COUNTY AS county_name,
            STALP AS state
        FROM bronze_locations
        WHERE CBSA_NO = ?
        """,
        [config.METRO_CBSA],
    ).fetchall()
    counties = dict(config.METRO_COUNTIES)
    for fips, county_name, state in rows:
        # A county new to the metro gets FDIC's name until it's added to config.
        counties.setdefault(fips, f"{county_name.title()}, {state}")
    return dict(sorted(counties.items()))


def history_filter(counties: dict[str, str]) -> str:
    """Lucene filter for branch events located in our counties.

    History stores the office's state and county as separate numbers
    (OFF_PSTNUM 51, OFF_CNTYNUM 810), so group the counties by state.
    """
    by_state: dict[int, list[int]] = {}
    for fips in counties:
        by_state.setdefault(int(fips[:2]), []).append(int(fips[2:]))
    parts = [
        f"(OFF_PSTNUM:{state} AND OFF_CNTYNUM:({' OR '.join(str(c) for c in sorted(cnty))}))"
        for state, cnty in sorted(by_state.items())
    ]
    return " OR ".join(parts)


def sod_filter(counties: dict[str, str]) -> str:
    return f"STCNTYBR:({' OR '.join(str(int(fips)) for fips in counties)})"


# --- One full fetch ------------------------------------------------------------------


def run(con, source, run_ts: datetime, run_id: str, log=print) -> dict:
    """Fetch every bronze table. Returns per-endpoint load info."""
    loaded = {}
    for endpoint in ("institutions", "locations"):
        started = time.monotonic()
        loaded[endpoint] = to_bronze(endpoint, config.FIELDS[endpoint], source.pages(endpoint), run_ts, run_id)
        log(f"  bronze.{endpoint}: {loaded[endpoint]['rows']:,} rows in {time.monotonic() - started:.1f}s")

    counties = metro_counties(con)
    log(f"  metro counties: {len(counties)}")

    started = time.monotonic()
    loaded["history"] = to_bronze(
        "history", config.FIELDS["history"], source.pages("history", history_filter(counties)), run_ts, run_id
    )
    log(f"  bronze.history: {loaded['history']['rows']:,} rows in {time.monotonic() - started:.1f}s")

    started = time.monotonic()
    loaded["sod"] = to_bronze("sod", config.FIELDS["sod"], source.pages("sod", sod_filter(counties)), run_ts, run_id)
    log(f"  bronze.sod: {loaded['sod']['rows']:,} rows in {time.monotonic() - started:.1f}s")

    meta, rows = source.sod_totals(sod_filter(counties))
    meta = {**meta, "total": len(rows)}  # aggregate rows: one per year
    loaded["sod_fdic_totals"] = to_bronze(
        "sod_fdic_totals", ["YEAR", "BRANCH_COUNT", "DEPSUMBR_SUM"], iter([(meta, rows)]), run_ts, run_id
    )
    log(f"  bronze.sod_fdic_totals: {loaded['sod_fdic_totals']['rows']} years")
    return loaded
