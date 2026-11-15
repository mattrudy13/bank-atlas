"""Shared test setup: a throwaway lake and an FDIC stand-in that reads fixtures.

Nothing here touches the network or the real ./lake and site-data/.
"""

from __future__ import annotations

import copy
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import pytest

from pipeline import config

FIXTURES = Path(__file__).parent / "fixtures"

# Fixed clock so "no future dates" and year ranges are stable.
RUN_1 = datetime(2026, 11, 9, 12, 0, tzinfo=timezone.utc)
RUN_2 = datetime(2026, 11, 16, 12, 0, tzinfo=timezone.utc)


class FixtureSource:
    """Stands in for fetch.FdicApi: same methods, rows from tests/fixtures.

    Filters are ignored (the fixtures are already the metro, plus one Richmond
    branch and event that silver/gold must leave out on their own).
    """

    def __init__(self, sod_total_offset: int = 0):
        self.rows = {
            name: json.loads((FIXTURES / f"{name}.json").read_text())
            for name in ("institutions", "locations", "history", "sod")
        }
        # Lets a test make FDIC's control total disagree with the rows.
        self.sod_total_offset = sod_total_offset

    def edit(self, endpoint: str, row_id: str, **changes) -> None:
        for row in self.rows[endpoint]:
            if row["ID"] == row_id:
                row.update(changes)
                return
        raise KeyError(row_id)

    def drop(self, endpoint: str, row_id: str) -> None:
        self.rows[endpoint] = [r for r in self.rows[endpoint] if r["ID"] != row_id]

    def pages(self, endpoint: str, filters: str | None = None):
        rows = copy.deepcopy(self.rows[endpoint])
        meta = {
            "total": len(rows),
            "index": {"name": f"{endpoint}_fixture", "createTimestamp": "2026-11-13T00:00:00Z"},
        }
        # Two pages, so the streaming path is exercised.
        half = len(rows) // 2
        yield meta, rows[:half]
        yield meta, rows[half:]

    def sod_totals(self, filters: str):
        totals = defaultdict(lambda: [0, 0])
        for row in self.rows["sod"]:
            totals[row["YEAR"]][0] += 1
            totals[row["YEAR"]][1] += row["DEPSUMBR"]
        rows = [
            {"YEAR": year, "BRANCH_COUNT": n, "DEPSUMBR_SUM": total + self.sod_total_offset, "ID": str(year)}
            for year, (n, total) in sorted(totals.items())
        ]
        meta = {"total": len(rows), "index": {"name": "sod_fixture", "createTimestamp": "2026-09-18T00:00:00Z"}}
        return meta, rows


@pytest.fixture
def lake_dir(tmp_path, monkeypatch):
    """Point the pipeline at a temp lake and a temp site-data folder."""
    monkeypatch.setenv("LAKE_URI", str(tmp_path / "lake"))
    monkeypatch.setattr(config, "SITE_DATA_DIR", tmp_path / "site-data")
    return tmp_path


@pytest.fixture
def source():
    return FixtureSource()
