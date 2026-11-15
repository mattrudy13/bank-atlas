"""Data checks. Any failure stops the run before gold is published.

Each check is a SQL query over the DuckDB views run.py registers: silver
tables under their own names, the computed-but-unpublished gold as
gold_<name>, and the latest bronze snapshot as bronze_<name>.

The date and coordinate checks look only at the metro rows the site shows,
so an odd record for a bank in Iowa can't block the Hampton Roads map.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date

from pipeline import config, lake, silver


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str

    def as_dict(self) -> dict:
        return asdict(self)


# (view, key columns, optional filter): each key must appear once.
UNIQUE_KEYS = [
    ("dim_bank", "cert", "is_current"),
    ("dim_bank", "cert, valid_from", None),
    ("dim_branch", "uninum", "is_current"),
    ("dim_branch", "uninum, valid_from", None),
    ("fact_branch_event", "event_id", None),
    ("fact_branch_deposits_year", "sod_id", None),
    ("fact_branch_deposits_year", "year, uninum", "uninum IS NOT NULL"),
    ("dim_metro_county", "county_fips", None),
    ("gold_branches_now", "uninum", None),
    ("gold_branch_events_by_year", "year, area", None),
    ("gold_deposit_share_by_year", "year, area, cert", None),
]


def unique_keys(con) -> CheckResult:
    problems = []
    for view, key, where in UNIQUE_KEYS:
        dupes = con.execute(
            f"""
            SELECT COUNT(*)
            FROM (
                SELECT {key}
                FROM {view}
                WHERE {where or 'TRUE'}
                GROUP BY {key}
                HAVING COUNT(*) > 1
            )
            """
        ).fetchall()[0][0]
        if dupes:
            problems.append(f"{view}({key}): {dupes} repeated")
    return CheckResult(
        "unique keys",
        not problems,
        "; ".join(problems) or f"{len(UNIQUE_KEYS)} keys unique",
    )


def branches_have_coordinates(con) -> CheckResult:
    b = config.METRO_BOUNDS
    missing, outside, examples = con.execute(
        """
        WITH flagged AS (
            SELECT
                uninum,
                latitude IS NULL OR longitude IS NULL AS is_missing,
                latitude NOT BETWEEN $lat_min AND $lat_max
                    OR longitude NOT BETWEEN $lon_min AND $lon_max AS is_outside
            FROM gold_branches_now
        )
        SELECT
            COUNT(*) FILTER (WHERE is_missing),
            COUNT(*) FILTER (WHERE is_outside),
            (LIST(uninum ORDER BY uninum) FILTER (WHERE is_missing OR is_outside))[1:5]
        FROM flagged
        """,
        b,
    ).fetchall()[0]
    passed = missing == 0 and outside == 0
    detail = (
        "every metro branch has coordinates inside the metro"
        if passed
        else f"{missing} missing, {outside} outside the metro box; UNINUMs e.g. {examples}"
    )
    return CheckResult("branches have coordinates", passed, detail)


def no_future_dates(con, today: date) -> CheckResult:
    rows = con.execute(
        """
        SELECT 'fact_branch_event.event_date' AS col, COUNT(*) AS n
        FROM fact_branch_event
        JOIN dim_metro_county USING (county_fips)
        WHERE event_date > $today
        UNION ALL
        SELECT 'fact_branch_deposits_year.as_of_date', COUNT(*)
        FROM fact_branch_deposits_year
        WHERE as_of_date > $today
        UNION ALL
        SELECT 'branches_now.opened_date', COUNT(*)
        FROM gold_branches_now
        WHERE opened_date > $today
        UNION ALL
        SELECT 'branches_now.acquired_date', COUNT(*)
        FROM gold_branches_now
        WHERE acquired_date > $today
        UNION ALL
        SELECT 'dim_branch.valid_from', COUNT(*)
        FROM dim_branch
        WHERE CAST(valid_from AS DATE) > $today
        """,
        {"today": today},
    ).fetchall()
    bad = [f"{col}: {n}" for col, n in rows if n]
    return CheckResult("no future dates", not bad, "; ".join(bad) or f"nothing after {today}")


def row_counts_within_tolerance(
    current: dict[str, int],
    previous: dict[str, int] | None,
    previous_run: str | None,
    name: str = "row counts vs last run",
) -> CheckResult:
    """Each table within ±ROW_COUNT_TOLERANCE_PCT of the last successful run.

    Catches an API that quietly returns half the rows, or a join that fans
    out, before either reaches the site.
    """
    tol = config.ROW_COUNT_TOLERANCE_PCT
    if not previous:
        return CheckResult(name, True, "first run; nothing to compare")
    off = []
    for table, rows in current.items():
        before = previous.get(table)
        if not before:
            continue  # new table this run, or empty last time
        change = 100 * (rows - before) / before
        if abs(change) > tol:
            off.append(f"{table} {before:,} -> {rows:,} ({change:+.1f}%)")
    return CheckResult(
        name,
        not off,
        "; ".join(off) or f"{len(current)} tables within ±{tol}% of run {previous_run}",
    )


def sod_totals_match_fdic(con) -> CheckResult:
    """Each year's metro deposits and branch count equal FDIC's own sums.

    Like a control total at the bank: FDIC adds up the same counties
    server-side, so any row we lose or double counts makes a mismatch.
    FDIC's sums include the mis-coded rows silver sets aside
    (config.MISCODED_PLACES), so those are added back first.
    """
    silver.register_lookups(con)
    con.register("set_aside", lake.query(con, silver.MISCODED_DEPOSITS_BY_YEAR))
    mismatches = con.execute(
        """
        WITH ours AS (
            SELECT
                year,
                SUM(deposits_thousands) AS deposits,
                SUM(branches) AS branches
            FROM gold_deposit_share_by_year
            WHERE area = 'Metro'
            GROUP BY year
        ),
        ours_plus_set_aside AS (
            SELECT
                o.year,
                o.deposits + COALESCE(a.deposits, 0) AS deposits,
                o.branches + COALESCE(a.branches, 0) AS branches
            FROM ours AS o
            LEFT JOIN set_aside AS a
                ON a.year = o.year
        ),
        fdic AS (
            SELECT
                CAST(YEAR AS INTEGER) AS year,
                CAST(CAST(DEPSUMBR_SUM AS DOUBLE) AS BIGINT) AS deposits,
                CAST(BRANCH_COUNT AS BIGINT) AS branches
            FROM bronze_sod_fdic_totals
            WHERE CAST(YEAR AS INTEGER) >= $first_year
        )
        SELECT
            COALESCE(o.year, f.year) AS year,
            o.deposits, f.deposits, o.branches, f.branches
        FROM ours_plus_set_aside AS o
        FULL OUTER JOIN fdic AS f
            ON f.year = o.year
        WHERE o.deposits IS DISTINCT FROM f.deposits
           OR o.branches IS DISTINCT FROM f.branches
        ORDER BY 1
        """,
        {"first_year": config.FIRST_YEAR},
    ).fetchall()
    years = con.execute("SELECT COUNT(*) FROM bronze_sod_fdic_totals").fetchall()[0][0]
    set_aside_rows, set_aside_deposits = con.execute(
        "SELECT COALESCE(SUM(branches), 0), COALESCE(SUM(deposits), 0) FROM set_aside"
    ).fetchall()[0]
    con.unregister("set_aside")
    if mismatches:
        detail = "; ".join(
            f"{y}: ours ${od}k/{ob} branches vs FDIC ${fd}k/{fb}" for y, od, fd, ob, fb in mismatches[:5]
        )
    else:
        detail = f"all {years} years match FDIC's totals"
        if set_aside_rows:
            detail += (
                f" once the {set_aside_rows} mis-coded out-of-area rows "
                f"(${set_aside_deposits:,}k) are added back"
            )
    return CheckResult("metro deposits match FDIC totals", not mismatches, detail)


def branch_count_matches_fdic(con) -> CheckResult:
    ours, fdic = con.execute(
        """
        SELECT
            (SELECT COUNT(*) FROM gold_branches_now),
            (SELECT COUNT(*) FROM bronze_locations WHERE CBSA_NO = $cbsa)
        """,
        {"cbsa": config.METRO_CBSA},
    ).fetchall()[0]
    return CheckResult(
        "branch count matches FDIC's CBSA count",
        ours == fdic,
        f"{ours} branches in gold, FDIC lists {fdic} under CBSA {config.METRO_CBSA}",
    )


def deposit_shares_sum_to_100(con) -> CheckResult:
    bad = con.execute(
        """
        SELECT year, area, SUM(share_pct) AS total_pct
        FROM gold_deposit_share_by_year
        GROUP BY year, area
        HAVING SUM(deposits_thousands) > 0
           AND ABS(SUM(share_pct) - 100) > 0.000001
        ORDER BY year, area
        """
    ).fetchall()
    groups = con.execute("SELECT COUNT(DISTINCT (year, area)) FROM gold_deposit_share_by_year").fetchall()[0][0]
    return CheckResult(
        "deposit shares sum to 100%",
        not bad,
        "; ".join(f"{y} {a}: {t:.4f}%" for y, a, t in bad[:5]) or f"{groups} year-area groups sum to 100%",
    )


def event_list_matches_yearly_counts(con) -> CheckResult:
    """The year strip's counts and the list under it must agree.

    The site reads them from two files (branch_events_by_year and
    branch_events); a mismatch would show "5 openings" above a list of 4.
    """
    mismatches = con.execute(
        """
        WITH listed AS (
            SELECT
                "year",
                COUNT(*) FILTER (WHERE event = 'opened') AS opened,
                COUNT(*) FILTER (WHERE event = 'closed') AS closed
            FROM gold_branch_events
            GROUP BY "year"
        ),
        yearly AS (
            SELECT "year", opened, closed
            FROM gold_branch_events_by_year
            WHERE area = 'Metro'
        )
        SELECT y."year", l.opened, y.opened, l.closed, y.closed
        FROM yearly AS y
        FULL OUTER JOIN listed AS l
            ON l."year" = y."year"
        WHERE COALESCE(l.opened, 0) <> COALESCE(y.opened, -1)
           OR COALESCE(l.closed, 0) <> COALESCE(y.closed, -1)
        ORDER BY 1
        """
    ).fetchall()
    total = con.execute("SELECT COUNT(*) FROM gold_branch_events").fetchall()[0][0]
    return CheckResult(
        "event list matches yearly counts",
        not mismatches,
        "; ".join(
            f"{y}: list {lo} opened/{lc} closed vs counts {yo}/{yc}" for y, lo, yo, lc, yc in mismatches[:5]
        )
        or f"{total} listed openings and closings match the metro's yearly counts",
    )


def publish_checks(con, today: date) -> list[CheckResult]:
    """Everything that must pass before gold is published."""
    return [
        unique_keys(con),
        branches_have_coordinates(con),
        no_future_dates(con, today),
        sod_totals_match_fdic(con),
        branch_count_matches_fdic(con),
        deposit_shares_sum_to_100(con),
        event_list_matches_yearly_counts(con),
    ]
