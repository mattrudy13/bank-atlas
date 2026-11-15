"""Small helpers around the Delta tables so the layer modules read like SQL.

Tables live at <lake>/<layer>/<name>, e.g. lake/silver/dim_branch. Every
commit carries the run id in its commit metadata, the same way a Databricks
job stamps userMetadata, so DESCRIBE HISTORY-style lookups can tie a version
back to a run.
"""

from __future__ import annotations

import duckdb
import pyarrow as pa
from deltalake import CommitProperties, DeltaTable, write_deltalake

from pipeline import config

# Every table the pipeline owns, in build order. Used by the status page and
# housekeeping, so a new table only needs adding here.
TABLES = [
    ("bronze", "institutions"),
    ("bronze", "locations"),
    ("bronze", "history"),
    ("bronze", "sod"),
    ("bronze", "sod_fdic_totals"),
    ("silver", "dim_metro_county"),
    ("silver", "dim_bank"),
    ("silver", "dim_branch"),
    ("silver", "fact_branch_event"),
    ("silver", "fact_branch_deposits_year"),
    ("gold", "branches_now"),
    ("gold", "branch_events_by_year"),
    ("gold", "branch_events"),
    ("gold", "deposit_share_by_year"),
    ("meta", "pipeline_runs"),
]


def uri(layer: str, name: str) -> str:
    return f"{config.lake_uri()}/{layer}/{name}"


def exists(layer: str, name: str) -> bool:
    return DeltaTable.is_deltatable(uri(layer, name), storage_options=config.storage_options())


def table(layer: str, name: str, version: int | None = None) -> DeltaTable:
    """Open a table, optionally at an older version (time travel)."""
    return DeltaTable(uri(layer, name), version=version, storage_options=config.storage_options())


def version(layer: str, name: str) -> int | None:
    return table(layer, name).version() if exists(layer, name) else None


def write(
    layer: str,
    name: str,
    data,
    *,
    mode: str,
    run_id: str,
    partition_by: list[str] | None = None,
) -> None:
    """Append or overwrite a table, stamping the run id on the commit."""
    write_deltalake(
        uri(layer, name),
        data,
        mode=mode,
        partition_by=partition_by,
        # Bronze widens over time (new FDIC fields); let new columns in.
        schema_mode="merge" if mode == "append" else "overwrite",
        storage_options=config.storage_options(),
        commit_properties=commit_props(run_id),
    )


def commit_props(run_id: str) -> CommitProperties:
    return CommitProperties(custom_metadata={"run_id": run_id})


def connect() -> duckdb.DuckDBPyConnection:
    """A DuckDB connection that keeps timestamps in UTC.

    DuckDB otherwise converts TIMESTAMPTZ to the Mac's local zone on the way
    out to Arrow, and the Delta tables should hold one zone.
    """
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'").fetchall()
    return con


def register(con: duckdb.DuckDBPyConnection, view: str, layer: str, name: str) -> bool:
    """Load a Delta table into memory and expose it to DuckDB under `view`.

    Loaded rather than scanned lazily (to_pyarrow_dataset) on purpose:
    DuckDB 2.0 pushes string filters like event_type = 'opened' into a
    pyarrow dataset as string_view values, which pyarrow 26 can't compare
    ("no kernel matching (string, string_view)"). Silver, gold and meta are
    a few MB, so loading them is cheap. Returns False when the table doesn't
    exist yet.
    """
    if not exists(layer, name):
        return False
    con.register(view, table(layer, name).to_pyarrow_table())
    return True


def register_latest_bronze(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    """View bronze_<name> = only the newest snapshot of an append-only table.

    Bronze keeps every run's full copy; silver only ever wants the latest.
    Only the newest _ingest_date partition is read (delta-rs prunes the rest),
    so memory stays at one snapshot however long bronze grows.
    """
    if not exists("bronze", name):
        return False
    dt = table("bronze", name)
    latest_date = max(p["_ingest_date"] for p in dt.partitions())
    con.register(f"bronze_{name}_latest_day", dt.to_pyarrow_table(filters=[("_ingest_date", "=", latest_date)]))
    con.execute(
        f"""
        CREATE OR REPLACE VIEW bronze_{name} AS
        SELECT *
        FROM bronze_{name}_latest_day
        -- Two runs on one day share a partition; keep the later one.
        WHERE _ingest_ts = (SELECT MAX(_ingest_ts) FROM bronze_{name}_latest_day)
        """
    ).fetchall()  # DuckDB 2.0 keeps a result open until it's read, even for DDL
    return True


def query(con: duckdb.DuckDBPyConnection, sql: str, params=None) -> pa.Table:
    """Run SQL and return an Arrow table (con.sql streams it without a copy)."""
    return con.sql(sql, params=params).to_arrow_table()
