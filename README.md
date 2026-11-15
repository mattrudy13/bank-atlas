# Bank Atlas

Every bank branch in the Virginia Beach–Chesapeake–Norfolk metro, from the FDIC's public data:

- **Where branches are now**, on a map, filterable by city or county and by bank.
- **Who opened and closed branches**, year by year since 2000, with each year's openings and closings listed.
- **Who holds local deposits**, today and back to 1994.

The data comes through a small bronze/silver/gold pipeline written in Python with DuckDB and Delta Lake
tables (via `deltalake`), the same layered pattern used on Databricks:

- **Bronze:** raw rows from the FDIC BankFind API, appended every run.
- **Silver:** typed and deduplicated tables. Banks and branches keep their change history (SCD type 2).
- **Gold:** the small tables the site reads.

Nine data checks run before anything is published. Two examples: branch counts must match the FDIC's own
count for the metro, and every year's deposits must equal FDIC's totals. If any check fails, the site keeps
the last good data. The site's Pipeline page shows the latest run.

A branch that changes hands in a merger or sale is counted as changing hands, not as a closing plus an
opening.

## Run it

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python -m pipeline.run
.venv/bin/python -m pytest -q
python3 -m http.server 8000   # then open http://localhost:8000
```

Data: [FDIC BankFind Suite](https://banks.data.fdic.gov/). Map tiles © OpenStreetMap contributors.
