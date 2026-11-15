# Bank Atlas

Every bank branch in the Virginia Beach–Chesapeake–Norfolk metro (CBSA 47260) from FDIC public data, built as a
bronze/silver/gold pipeline (Python + DuckDB + `deltalake`) that mirrors Rudy's Databricks work. v0.1 = the
Hampton Roads branch map plus a Pipeline run report. Me first, portfolio later.

Scope and decisions: `~/.claude/plans/can-we-go-ahead-shimmering-raccoon.md`. Mockups (looks + detail boards):
https://claude.ai/artifact/M2PPVRL2C4QG7pU3KWDrgp

## Decisions (Rudy, 2026-11-15)

- **Look: "Call Report"** (C of four): Archivo / Archivo Narrow / Red Hat Mono, black ruled boxes, numbered
  PARTs and LINE items, one red accent (#c8102e), blue (#2453c5) for openings. Phones (≤700px) get a lighter
  version: rules instead of boxes, no field codes. 701–1000px: header and totals two per row.
- **Selected branch:** bottom sheet on phones (map scrolls to the top, zooms, dot sits above the sheet); side
  panel ("Selected branch") on desktop.
- **Part II:** then-and-now share table, today's top 8 banks with share in 1994 / 2006 / 2016 / latest, plus
  top-5 share, bank count and total deposits.
- **Part III:** year strip (2000 on; opened above, closed below) plus the chosen year's events by date and bank.
- **Pipeline page:** run report (status, 13+ tables with rows and Delta version, each check, recent runs).
- **TowneBank (CERT 35095) is in `no_callout_certs`.** It shows like any bank on the map, in
  tables and in Part III's event list (Rudy OK'd that: data rows, not callouts), but the automatic "Notes to
  schedule" never name it. If it would top a ranking, that note is skipped, not handed to the runner-up.
- Whole metro (19 counties, VA + NC), city filter by county/independent city ("locality"), not postal city.
- Mergers and sales (713/722) are "changed hands", never a closing plus an opening.
- FDIC banks only, no credit unions. Deposit share by charter as of June 30.

## Data facts that bite

- API moved to `https://api.fdic.gov/banks/<endpoint>`; no key; CORS `*`.
- FDIC openings/closings only exist from 1999-08-02, so Part III starts in 2000 (Rudy, 2026-11-15).
  Mergers go back further; deposits (SOD) go back to 1994.
- `opened_date` for old offices is often the founding bank's date (e.g. 1867); the sheet says so before 1960.
- Branch names are FDIC's all-caps; left as-is (auto-capitalising mangles "JANAF").
- History's `PSTALP` is the bank's HQ state; the branch's place is in the `OFF_*` fields.

## Files

- `index.html`, `app.js`: map page. `pipeline.html`, `pipeline.js`: run report. `common.js`: fetch, DOM
  builder (text only, never innerHTML), formatters. `style.css`: everything, phone rules at the bottom.
- Map: Leaflet 1.9.4 from cdnjs (pinned, SRI). Tiles: OpenStreetMap standard, grayed with a CSS filter
  (CARTO now needs an API key). Bank colors by CERT in `NAMED` (app.js); every other bank is gray.
- `site-data/*.json` (gold, written by the pipeline only when every check passes):
  `branches_now`, `branch_events` (647 rows, 2000 on), `branch_events_by_year`, `deposit_share_by_year`
  (844 KB raw, ~75 KB gzipped by GitHub Pages), `pipeline_status`. Each is `{"meta", "data"}`; meta carries
  `localities`, `fdic_data_as_of`, `no_callout_certs`.
- `pipeline/`: `fetch.py` (paged API), `silver.py` (SCD2 dims, facts), `gold.py`, `checks.py` (9 checks),
  `lake.py` (local folder or R2), `config.py` (metro counties, `NO_CALLOUT_CERTS`), `run.py`.

## Run

    .venv/bin/python -m pipeline.run               # full run (~14 s)
    .venv/bin/python -m pipeline.run --skip-fetch  # rebuild silver/gold from bronze (<1 s)
    .venv/bin/python -m pipeline.run --housekeeping  # OPTIMIZE + VACUUM
    .venv/bin/python -m pytest -q                  # 13 tests, no network

Local preview: the `github-root` server in `~/Desktop/coding/GitHub/.claude/launch.json`, then
http://localhost:8765/bank-atlas/.

The row-count check (±10% vs last run) blocks a deliberate change in a gold table's size once on an existing
lake; delete the local `lake/` and rerun, or expect one failed run.

## Not done yet

- Live since 2026-11-15 at https://mattrudy13.github.io/bank-atlas/ (repo `mattrudy13/bank-atlas`, Pages from
  `main` /, tag v0.1). Site data is the first local run's output until the weekly refresh exists.
- R2: Rudy creates bucket `bank-atlas-lake` + a bucket-only token, saves `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`,
  `R2_SECRET_ACCESS_KEY` as GitHub secrets and in `.env`. The R2 code path is written but untested.
- Weekly GitHub Actions refresh (model: `uva-records/.github/workflows/refresh.yml`).
- Bronze appends a full snapshot every run (~8 MB/week, ~400 MB/year); fine on R2's 10 GB free tier for now.
