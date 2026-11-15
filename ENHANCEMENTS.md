# Bank Atlas: ideas for later

Roughly in the order picked on 2026-11-15.

1. **v0.2 Bank lookup.** Search any of the ~4,200 active banks: assets and deposits by quarter, branch count by
   year, mergers it took part in, where its branches are. Needs national quarterly financials in bronze
   (1.68M rows); this is where the R2 lake pays off. May need DuckDB-WASM (`sql-drills/engine.js`) or per-bank
   JSON files instead of one big file.
2. **v0.3 Bank failures.** All 4,117 failures since 1934: timeline, map, the 2008–2012 wave, 2023, who took over.
3. **v0.4 Industry trends.** U.S. bank count, mergers per year, branches per person, deposit concentration.
4. **Branch access.** Branches per 10,000 people by ZIP, or neighborhoods far from a branch (needs Census data).
5. **Hub tile** on Rudy's Board: branch count, last pipeline run, this year's openings and closings.
6. **Portfolio card + case study** once polished, with a "How it's built" page (layer diagram, sample rows).
   Check the bank's outside-activity policy first.
7. **Pre-2000 mergers in Part III.** Merger/sale counts exist back to 1994 (still in silver); the site's event
   data starts in 2000 with the opening/closing records.
8. **Bronze retention.** Bronze appends a full snapshot each run (19 MB measured on R2, ~1 GB/year). Keep the
   last N snapshots, or append only changed rows.
9. **Keyboard list of branches.** Dots are focusable, but a plain list under the map (by city) would be easier
   to scan with a screen reader.
10. **Show failed runs on the Pipeline page right away.** A failed check leaves `pipeline_status.json` at the
    last good run, so a failure only appears (in recent runs) after the next run that passes.
11. **Move mis-filed metro events to the right city.** Three events sit in the metro but under the wrong
    locality: Greenbrier's 2003 opening and 2005 closing (Chesapeake, filed under Norfolk) and a 2006
    Forest Park closing (Hampton, filed under Virginia Beach). Metro totals are right; the city filter
    isn't. A small list like `MISCODED_PLACES` would fix it.
