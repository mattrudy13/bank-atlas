"""Run the Bank Atlas pipeline end to end.

    python -m pipeline.run                 fetch -> bronze -> silver -> checks -> gold
    python -m pipeline.run --skip-fetch    rebuild silver and gold from existing bronze
    python -m pipeline.run --housekeeping  OPTIMIZE + VACUUM every table (monthly)

Exits non-zero when any check fails; gold and site-data/ then keep the last
good version. One writer at a time is assumed (no lock service on R2).
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone

import pyarrow as pa

from pipeline import checks, config, fetch, gold, lake, silver

BRONZE_TABLES = ("institutions", "locations", "history", "sod", "sod_fdic_totals")

# Delta's own default. Versions older than this can't be time-travelled to
# after VACUUM, since their data files are gone.
VACUUM_RETENTION_HOURS = 168

RUN_SCHEMA = pa.schema([
    ("run_id", pa.string()),
    ("started_at", pa.timestamp("us", tz="UTC")),
    ("finished_at", pa.timestamp("us", tz="UTC")),
    ("duration_seconds", pa.float64()),
    ("status", pa.string()),
    ("failed_checks", pa.list_(pa.string())),
    ("tables", pa.list_(pa.struct([
        ("layer", pa.string()),
        ("table_name", pa.string()),
        ("row_count", pa.int64()),
        ("total_rows", pa.int64()),
        ("delta_version", pa.int64()),
    ]))),
    ("checks", pa.list_(pa.struct([
        ("name", pa.string()),
        ("passed", pa.bool_()),
        ("detail", pa.string()),
    ]))),
    ("sources", pa.list_(pa.struct([
        ("endpoint", pa.string()),
        ("index_name", pa.string()),
        ("created_at", pa.string()),
        ("rows", pa.int64()),
    ]))),
])


# --- Run bookkeeping ---------------------------------------------------------------


def bronze_sources(con) -> dict:
    """FDIC index name, build time and row count of each latest bronze snapshot."""
    sources = {}
    for name in BRONZE_TABLES:
        index_name, created_at, rows = con.execute(
            f"""
            SELECT
                ANY_VALUE(_source_index),
                ANY_VALUE(_source_created_at),
                COUNT(*)
            FROM bronze_{name}
            """
        ).fetchall()[0]
        sources[name] = {"index_name": index_name, "created_at": created_at, "rows": rows}
    return sources


def table_stats(con) -> list[dict]:
    """Rows and Delta version for every table.

    row_count is what a reader sees: bronze's latest snapshot, a dimension's
    current rows. total_rows includes older snapshots and closed SCD2 rows.
    """
    stats = []
    for layer, name in lake.TABLES:
        if layer == "meta" or not lake.exists(layer, name):
            continue
        dt = lake.table(layer, name)
        # Lazy scan so bronze's full history is counted without loading it.
        # Safe here: only timestamp/boolean filters (see lake.register).
        con.register("stats_table", dt.to_pyarrow_dataset())
        columns = dt.schema().to_arrow().names
        if "_ingest_ts" in columns:
            current = "_ingest_ts = (SELECT MAX(_ingest_ts) FROM stats_table)"
        elif "is_current" in columns:
            current = "is_current"
        else:
            current = "TRUE"
        total, row_count = con.execute(
            f"SELECT COUNT(*), COUNT(*) FILTER (WHERE {current}) FROM stats_table"
        ).fetchall()[0]
        stats.append({
            "layer": layer,
            "table_name": name,
            "row_count": row_count,
            "total_rows": total,
            "delta_version": dt.version(),
        })
    con.unregister("stats_table")
    return stats


def last_passed_counts(con) -> tuple[dict[str, int] | None, str | None]:
    """Row counts from the most recent run whose checks all passed."""
    if not lake.register(con, "pipeline_runs", "meta", "pipeline_runs"):
        return None, None
    rows = con.execute(
        """
        WITH last_pass AS (
            SELECT run_id, tables
            FROM pipeline_runs
            WHERE status = 'passed'
            ORDER BY started_at DESC
            LIMIT 1
        ),
        per_table AS (
            SELECT run_id, UNNEST(tables) AS t
            FROM last_pass
        )
        SELECT run_id, t.layer || '.' || t.table_name, t.row_count
        FROM per_table
        """
    ).fetchall()
    if not rows:
        return None, None
    return {table: count for _, table, count in rows}, rows[0][0]


def record_run(con, run: dict) -> None:
    lake.write("meta", "pipeline_runs", pa.Table.from_pylist([run], schema=RUN_SCHEMA),
               mode="append", run_id=run["run_id"])


def recent_runs(con, limit: int = 10) -> list[dict]:
    lake.register(con, "pipeline_runs", "meta", "pipeline_runs")
    rows = con.execute(
        """
        SELECT
            run_id,
            -- Formatted in SQL: handing TIMESTAMPTZ to Python needs pytz.
            strftime(started_at, '%Y-%m-%dT%H:%M:%SZ') AS started_at,
            duration_seconds,
            status,
            failed_checks
        FROM pipeline_runs
        ORDER BY pipeline_runs.started_at DESC
        LIMIT ?
        """,
        [limit],
    ).fetchall()
    return [
        {"run_id": r, "started_at": s, "duration_seconds": round(d, 1), "status": st, "failed_checks": list(f)}
        for r, s, d, st, f in rows
    ]


def write_status(run: dict, sources: dict, recent: list[dict]) -> None:
    """site-data/pipeline_status.json for the Pipeline page."""
    status = {
        "run_id": run["run_id"],
        "started_at": run["started_at"].isoformat(),
        "finished_at": run["finished_at"].isoformat(),
        "duration_seconds": round(run["duration_seconds"], 1),
        "status": run["status"],
        # Not the lake's path: a local path would leak a home folder name.
        "storage": "Cloudflare R2" if config.lake_uri().startswith("s3://") else "local",
        "metro": {"cbsa": config.METRO_CBSA, "name": config.METRO_NAME},
        "fdic_sources": [{"endpoint": name, **info} for name, info in sources.items()],
        "tables": run["tables"],
        "checks": run["checks"],
        "no_callout_certs": config.NO_CALLOUT_CERTS,
        "recent_runs": recent,
    }
    path = config.SITE_DATA_DIR / "pipeline_status.json"
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(status, indent=2) + "\n")
    tmp.replace(path)


# --- The run -------------------------------------------------------------------------


def run(source=None, run_ts: datetime | None = None, skip_fetch: bool = False, log=print) -> int:
    """Run every layer. Returns the process exit code (0 = gold published)."""
    clock = time.monotonic()
    run_ts = run_ts or datetime.now(timezone.utc)
    run_id = run_ts.strftime("%Y%m%dT%H%M%SZ")
    con = lake.connect()
    log(f"Bank Atlas run {run_id} ({'R2' if config.lake_uri().startswith('s3://') else 'local lake'})")

    if not skip_fetch:
        log("Bronze")
        fetch.run(con, source or fetch.FdicApi(), run_ts, run_id, log)
    for name in BRONZE_TABLES:
        if not lake.register_latest_bronze(con, name):
            raise RuntimeError(f"bronze.{name} doesn't exist yet; run without --skip-fetch first")
    sources = bronze_sources(con)
    previous, previous_run = last_passed_counts(con)

    def finish(results: list[checks.CheckResult]) -> dict:
        failed = [r.name for r in results if not r.passed]
        finished_at = datetime.now(timezone.utc)
        return {
            "run_id": run_id,
            "started_at": run_ts,
            "finished_at": finished_at,
            "duration_seconds": time.monotonic() - clock,
            "status": "failed" if failed else "passed",
            "failed_checks": failed,
            "tables": table_stats(con),
            "checks": [r.as_dict() for r in results],
            "sources": [{"endpoint": n, **s} for n, s in sources.items()],
        }

    def fail(results: list[checks.CheckResult]) -> int:
        record_run(con, finish(results))
        log("Checks FAILED; gold and site-data/ left at the last good run:")
        for r in results:
            if not r.passed:
                log(f"  x {r.name}: {r.detail}")
        return 1

    # Bronze counts are checked before silver on purpose: a half-empty extract
    # would close thousands of SCD2 rows, and that history can't be unwritten.
    bronze_counts = {f"bronze.{n}": s["rows"] for n, s in sources.items()}
    results = [
        checks.row_counts_within_tolerance(
            bronze_counts, previous, previous_run, name="row counts vs last run (bronze)"
        )
    ]
    if not results[0].passed:
        return fail(results)

    log("Silver")
    silver.build(con, run_ts, run_id, log)
    silver.register(con)

    log("Gold")
    tables = gold.build(con, run_ts)
    gold.register(con, tables)

    later_counts = {
        f"{s['layer']}.{s['table_name']}": s["row_count"] for s in table_stats(con) if s["layer"] == "silver"
    } | {f"gold.{name}": data.num_rows for name, data in tables.items()}
    results.append(
        checks.row_counts_within_tolerance(
            later_counts, previous, previous_run, name="row counts vs last run (silver, gold)"
        )
    )
    results += checks.publish_checks(con, run_ts.date())
    for r in results:
        log(f"  {'ok' if r.passed else 'x '} {r.name}: {r.detail}")
    if not all(r.passed for r in results):
        return fail(results)

    gold.publish(con, tables, run_ts, run_id, sources)
    run_record = finish(results)
    record_run(con, run_record)
    write_status(run_record, sources, recent_runs(con))
    for name, data in tables.items():
        log(f"  gold.{name}: {data.num_rows:,} rows -> site-data/{name}.json")
    log(f"Done in {run_record['duration_seconds']:.1f}s")
    return 0


def housekeeping(log=print) -> int:
    """OPTIMIZE (compact small files) then VACUUM (delete unreferenced files).

    Weekly appends leave many small files; compaction keeps reads fast.
    VACUUM then frees the replaced files once they're older than the
    retention window, which also ends time travel past that window.
    """
    run_id = "housekeeping-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    for layer, name in lake.TABLES:
        if not lake.exists(layer, name):
            continue
        dt = lake.table(layer, name)
        compacted = dt.optimize.compact(commit_properties=lake.commit_props(run_id))
        removed = dt.vacuum(retention_hours=VACUUM_RETENTION_HOURS, dry_run=False, enforce_retention_duration=True)
        log(
            f"  {layer}.{name}: {compacted['numFilesRemoved']} files compacted into "
            f"{compacted['numFilesAdded']}, {len(removed)} old files vacuumed"
        )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m pipeline.run", description=__doc__.split("\n\n")[0])
    parser.add_argument("--skip-fetch", action="store_true", help="rebuild silver and gold from existing bronze")
    parser.add_argument("--housekeeping", action="store_true", help="OPTIMIZE + VACUUM every table, then stop")
    args = parser.parse_args(argv)
    if args.housekeeping:
        return housekeeping()
    return run(skip_fetch=args.skip_fetch)


if __name__ == "__main__":
    sys.exit(main())
