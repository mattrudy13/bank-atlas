"""End-to-end runs on fixture data: bronze -> silver -> checks -> gold."""

from __future__ import annotations

import json
from collections import defaultdict

import pyarrow as pa
import pytest

from pipeline import checks, config, fetch, gold, lake, run
from tests.conftest import RUN_1, RUN_2, FixtureSource


def quiet(*_args):
    pass


def rows(layer: str, name: str, version: int | None = None) -> list[dict]:
    return lake.table(layer, name, version=version).to_pyarrow_table().to_pylist()


def site_json(name: str) -> dict:
    return json.loads((config.SITE_DATA_DIR / f"{name}.json").read_text())


# --- SCD2 ------------------------------------------------------------------------------


def test_scd2_change_closes_old_row_and_opens_new(lake_dir, source, monkeypatch):
    # Fixtures are tiny, so dropping one of four branches is a 25% swing;
    # loosen the row-count check so this test is only about SCD2.
    monkeypatch.setattr(config, "ROW_COUNT_TOLERANCE_PCT", 30)
    assert run.run(source=source, run_ts=RUN_1, log=quiet) == 0
    version_after_run_1 = lake.version("silver", "dim_branch")

    source.edit("locations", "1003", ADDRESS="1600 Laskin Rd")  # moved down the street
    source.drop("locations", "2001")  # Richmond branch closed
    assert run.run(source=source, run_ts=RUN_2, log=quiet) == 0

    dim = rows("silver", "dim_branch")
    old, new = sorted((r for r in dim if r["uninum"] == 1003), key=lambda r: r["valid_from"])
    assert (old["address"], old["is_current"], old["valid_from"], old["valid_to"]) == (
        "1500 Laskin Rd", False, RUN_1, RUN_2,
    )
    assert (new["address"], new["is_current"], new["valid_from"], new["valid_to"]) == (
        "1600 Laskin Rd", True, RUN_2, None,
    )

    # Unchanged branch: still exactly one row, untouched.
    unchanged = [r for r in dim if r["uninum"] == 1001]
    assert len(unchanged) == 1 and unchanged[0]["valid_from"] == RUN_1 and unchanged[0]["is_current"]

    # A branch that vanished from FDIC is closed, not deleted.
    (gone,) = [r for r in dim if r["uninum"] == 2001]
    assert (gone["is_current"], gone["valid_to"]) == (False, RUN_2)

    # Time travel: the version from run 1 still has the old address only.
    before = rows("silver", "dim_branch", version=version_after_run_1)
    assert [r["address"] for r in before if r["uninum"] == 1003] == ["1500 Laskin Rd"]

    # The map shows the new address.
    (branch,) = [r for r in site_json("branches_now")["data"] if r["uninum"] == 1003]
    assert branch["address"] == "1600 Laskin Rd"


def test_rerun_with_same_data_changes_nothing(lake_dir, source):
    assert run.run(source=source, run_ts=RUN_1, log=quiet) == 0
    first = rows("silver", "dim_branch")
    assert run.run(source=source, run_ts=RUN_2, log=quiet) == 0
    assert rows("silver", "dim_branch") == first


# --- Branch events ---------------------------------------------------------------------


def test_merger_713_is_changed_hands_not_open_plus_close(lake_dir, source):
    assert run.run(source=source, run_ts=RUN_1, log=quiet) == 0

    events = {r["event_id"]: r for r in rows("silver", "fact_branch_event")}
    assert events["e-2021-merger"]["event_type"] == "changed_hands"
    assert events["e-2022-sold"]["event_type"] == "changed_hands"
    # 712 is the buyer's side of the same sale as 722: left out, no double count.
    assert "e-2022-purchased" not in events

    by_year = {
        (r["year"], r["area"]): r for r in site_json("branch_events_by_year")["data"]
    }
    counts = lambda r: (r["opened"], r["closed"], r["changed_hands"], r["moved"], r["net_change"])  # noqa: E731

    # 2021: one real closing and one merger. The merger adds no open or close.
    assert counts(by_year[(2021, "Metro")]) == (0, 1, 1, 0, -1)
    assert counts(by_year[(2021, "Norfolk")]) == (0, 1, 1, 0, -1)
    # 2022: one branch sold = one hand-over, not two.
    assert counts(by_year[(2022, "Metro")]) == (0, 0, 1, 0, 0)
    # 2023: the Richmond opening is outside the metro; the move is inside.
    assert counts(by_year[(2023, "Metro")]) == (0, 0, 0, 1, 0)
    # Gold starts in 2000 (FDIC began tracking openings in Aug 1999); the
    # 1997 merger stays in silver but isn't charted.
    assert events["e-1997-merger"]["event_type"] == "changed_hands"
    assert min(year for year, _ in by_year) == config.EVENTS_FIRST_YEAR == 2000
    # Quiet years are present as zeros.
    assert counts(by_year[(2010, "Virginia Beach")]) == (0, 0, 0, 0, 0)


def test_event_list_has_only_openings_and_closings(lake_dir, source):
    """A 713 merger (or 722 sale, or 520 move) never lands in the year list."""
    assert run.run(source=source, run_ts=RUN_1, log=quiet) == 0
    listed = site_json("branch_events")

    assert listed["meta"]["first_year"] == 2000
    assert listed["meta"]["no_callout_certs"] == [35095]
    # Fixtures hold 713s in 1997 and 2021, a 722/712 sale in 2022, a move in
    # 2023 and a Richmond opening; only the two metro openings/closings remain.
    assert listed["data"] == [
        {"date": "2020-03-01", "year": 2020, "event": "opened", "cert": 3511,
         "bank_name": "Wells Fargo Bank, National Association", "branch_name": "HILLTOP BRANCH",
         "city": "Virginia Beach", "locality": "Virginia Beach", "county_fips": "51810"},
        {"date": "2021-05-01", "year": 2021, "event": "closed", "cert": 99999,
         "bank_name": "Old Coast Bank", "branch_name": "OCEAN VIEW BRANCH",
         "city": "Norfolk", "locality": "Norfolk", "county_fips": "51710"},
    ]
    status = site_json("pipeline_status")
    assert any(t["table_name"] == "branch_events" and t["row_count"] == 2 for t in status["tables"])


def test_miscoded_places_are_left_out(lake_dir, source):
    """Rocky Mount VA filed under Franklin city (51620) stays out of events and deposits.

    FDIC's control totals still include its deposits, so the totals check
    has to add them back and pass.
    """
    assert run.run(source=source, run_ts=RUN_1, log=quiet) == 0

    assert "e-2001-rocky-mount-open" not in {r["event_id"] for r in rows("silver", "fact_branch_event")}
    assert "2026_9846_90" not in {r["sod_id"] for r in rows("silver", "fact_branch_deposits_year")}
    by_year = {(r["year"], r["area"]): r for r in rows("gold", "branch_events_by_year")}
    assert by_year[(2001, "Franklin")]["opened"] == by_year[(2001, "Metro")]["opened"] == 0
    franklin = [r for r in rows("gold", "deposit_share_by_year") if r["area"] == "Franklin"]
    assert franklin == []

    status = site_json("pipeline_status")
    totals = next(c for c in status["checks"] if c["name"] == "metro deposits match FDIC totals")
    assert totals["passed"] and "1 mis-coded" in totals["detail"]
    assert "Rocky Mount" in site_json("deposit_share_by_year")["meta"]["notes"]


def test_event_list_check_catches_mismatch():
    con = lake.connect()
    con.register("gold_branch_events", pa.table({"year": [2021], "event": ["closed"]}))
    con.register(
        "gold_branch_events_by_year",
        pa.table({"year": [2021], "area": ["Metro"], "opened": [1], "closed": [1]}),
    )
    result = checks.event_list_matches_yearly_counts(con)
    assert not result.passed and "2021" in result.detail


# --- Checks gate gold -----------------------------------------------------------------


def test_failing_check_blocks_gold(lake_dir, source):
    assert run.run(source=source, run_ts=RUN_1, log=quiet) == 0
    published = {p.name: p.read_text() for p in config.SITE_DATA_DIR.glob("*.json")}
    gold_versions = {name: lake.version("gold", name) for name in gold.GOLD_TABLES}

    # FDIC's control total now disagrees with the rows by $1k a year.
    assert run.run(source=FixtureSource(sod_total_offset=1), run_ts=RUN_2, log=quiet) == 1

    # Last good gold stays: same JSON bytes, same Delta versions.
    assert {p.name: p.read_text() for p in config.SITE_DATA_DIR.glob("*.json")} == published
    assert {name: lake.version("gold", name) for name in gold.GOLD_TABLES} == gold_versions
    runs = sorted(rows("meta", "pipeline_runs"), key=lambda r: r["started_at"])
    assert [r["status"] for r in runs] == ["passed", "failed"]
    assert runs[-1]["failed_checks"] == ["metro deposits match FDIC totals"]


def test_failed_first_run_publishes_nothing(lake_dir):
    assert run.run(source=FixtureSource(sod_total_offset=1), run_ts=RUN_1, log=quiet) == 1
    assert not list(config.SITE_DATA_DIR.glob("*.json"))  # glob of a missing folder is empty
    assert not any(lake.exists("gold", name) for name in gold.GOLD_TABLES)


def test_bronze_drop_stops_before_silver(lake_dir, source):
    """A shrunken extract fails before silver, so SCD2 history isn't damaged."""
    assert run.run(source=source, run_ts=RUN_1, log=quiet) == 0
    dim_version = lake.version("silver", "dim_branch")
    source.drop("locations", "2001")  # 4 -> 3 rows: -25%, over the 10% limit
    assert run.run(source=source, run_ts=RUN_2, log=quiet) == 1
    assert lake.version("silver", "dim_branch") == dim_version


# --- Deposit share ---------------------------------------------------------------------


def test_deposit_shares_sum_to_100_per_year(lake_dir, source):
    assert run.run(source=source, run_ts=RUN_1, log=quiet) == 0
    share = rows("gold", "deposit_share_by_year")

    totals = defaultdict(float)
    for r in share:
        totals[(r["year"], r["area"])] += r["share_pct"]
    assert totals and all(t == pytest.approx(100, abs=1e-9) for t in totals.values())

    metro_2026 = {r["bank_name"]: r for r in share if (r["year"], r["area"]) == (2026, "Metro")}
    assert metro_2026["TowneBank"]["share_pct"] == pytest.approx(50.0)  # 600 of 1,200
    assert metro_2026["TowneBank"]["rank"] == 1
    # The 2025 thrift branch with no UNINUM still counts toward Truist's deposits.
    metro_2025 = {r["bank_name"]: r for r in share if (r["year"], r["area"]) == (2025, "Metro")}
    assert (metro_2025["Truist Bank"]["deposits_thousands"], metro_2025["Truist Bank"]["branches"]) == (400, 2)


def test_share_check_catches_bad_totals():
    con = lake.connect()
    con.register(
        "gold_deposit_share_by_year",
        pa.table({
            "year": [2026, 2026],
            "area": ["Metro", "Metro"],
            "deposits_thousands": [600, 400],
            "share_pct": [60.0, 30.0],  # should be 40
        }),
    )
    result = checks.deposit_shares_sum_to_100(con)
    assert not result.passed and "2026 Metro" in result.detail


# --- Gold JSON shape -------------------------------------------------------------------


def test_gold_json_carries_no_callout_certs_and_metro_only(lake_dir, source):
    assert run.run(source=source, run_ts=RUN_1, log=quiet) == 0
    branches = site_json("branches_now")
    assert branches["meta"]["no_callout_certs"] == config.NO_CALLOUT_CERTS == [35095]
    # Richmond's branch is not on the Hampton Roads map.
    assert sorted(r["uninum"] for r in branches["data"]) == [1001, 1002, 1003]
    status = site_json("pipeline_status")
    assert status["status"] == "passed"
    assert all(c["passed"] for c in status["checks"])
    assert {t["table_name"] for t in status["tables"]} >= set(gold.GOLD_TABLES)
    # Types normalized to one casing per FDIC code.
    assert {r["branch_type"] for r in branches["data"]} == {
        "Full service - brick and mortar",
        "Full service - retail",
    }


# --- Fetch -------------------------------------------------------------------------------


def test_fetch_refuses_partial_snapshot(lake_dir):
    def short_pages():
        yield {"total": 3, "index": {"name": "x", "createTimestamp": "t"}}, [{"ID": "1", "CERT": 1}]

    with pytest.raises(RuntimeError, match="FDIC says 3"):
        fetch.to_bronze("institutions", config.FIELDS["institutions"], short_pages(), RUN_1, "test")
    assert not lake.exists("bronze", "institutions")


def test_history_filter_groups_counties_by_state():
    counties = {"51810": "Virginia Beach", "51073": "Gloucester County", "37053": "Currituck County, NC"}
    assert fetch.history_filter(counties) == (
        "(OFF_PSTNUM:37 AND OFF_CNTYNUM:(53)) OR (OFF_PSTNUM:51 AND OFF_CNTYNUM:(73 OR 810))"
    )
    assert fetch.sod_filter(counties) == "STCNTYBR:(51810 OR 51073 OR 37053)"
