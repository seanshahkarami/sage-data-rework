it should be 1 commit per month NOT by day, thats our design plan to do by month

Monthly commits — that's the design decision from the cadence discussion, and my code does one transaction per day. Stopping the run now rather than letting it write 61 wrong commits.

⏺ Stop Task
P=$(cat .cold_prefix) && conda run -n sage_pipeline --no-capture-output python -I seed_cold.py --start 2025-03-01 --end 2025-04-30 --prefix "$P" --db cold_catal… · stopped

⏺ Stop Task
cd /Users/franciscolozano/Workspaces/sage_data_pipeline_v2/sage-data-rework/demo
while [ ! -f seedcold.log ]; do sleep 2; done… · stopped

  Read 1 file

Stopped before it wrote anything — the log is empty. Let me confirm the prefix is untouched, so I can reuse it.

  Ran 1 shell command

It had created the warehouse dir and catalog before I stopped it, so that prefix is now dirty. Let me check what the plan and memory actually say about cadence before I change the code.

  Read 1 file

Memory confirms it: monthly commits, and the reason matters — bounded metadata growth. But I measured that each append() makes its own snapshot even inside a transaction, so one transaction per month would still produce ~465 snapshots. Let me verify what actually yields one commit.

  Ran 1 shell command

add_files takes a list of paths in one call — that's the one-snapshot path. But it requires the files already written as Parquet, and our ns→µs cast means rewriting them. Let me verify the snapshot behavior empirically rather than assume.

⏺ Write(/tmp/sage_recon_check/cadence.py)
Wrote 90 lines to ../../../../tmp/sage_recon_check/cadence.py
"""How do we actually get ONE snapshot per month?

Measured on the first cold day: a transaction does NOT coalesce appends --
141 appends gave 141 snapshots. So "one transaction per month" would still
produce ~465 snapshots for March, which is not a monthly commit in the
sense that matters (metadata growth, and what a reader sees as one atomic
publication).

Test three shapes locally, on a tiny dataset, counting snapshots:
  A. many appends inside one transaction      (what seed_cold.py does now)
… +80 lines

  Ran 1 shell command

Decisive. B and C both give exactly 1 snapshot, and C keeps all 10 day-partitions — so monthly commits and day-partitioning are compatible.

B is ruled out by memory: concatenating a month would be ~1.5 TB in Arrow (one day alone is 49 GB). C is the only viable route. Let me confirm the one thing that could break it.

⏺ Write(/tmp/sage_recon_check/addfiles_pelican.py)
Wrote 94 lines to ../../../../tmp/sage_recon_check/addfiles_pelican.py
"""Can add_files reference Parquet files on Pelican (https://), and does
it preserve the map<string,string> meta column and microsecond timestamps?

add_files reads each file's footer to build the manifest, so it needs the
FileIO to resolve https. If PelicanFileIO is not consulted here, the whole
two-stage plan collapses and we are back to appends.

Also check the thing add_files could silently get wrong: it does NOT cast.
The staged file must already be microseconds, or the table takes ns and
pyiceberg rejects (or worse, accepts) it.
… +84 lines

  Ran 1 shell command

add_files works against Pelican: 1 snapshot, both day-partitions, µs timestamps, meta map intact. The two-stage pattern is viable.

Before rewriting, let me confirm the one risk that would make it unsafe — whether a crash mid-month leaves orphans.

  Ran 2 shell commands

Finding 20a confirms add_files can't use the archive's ns files — but my test just proved it works on µs files we write ourselves. That's exactly the two-stage pattern. Let me plan the rewrite.

⏺ Entered plan mode
  Claude is now exploring and designing an implementation approach.

Entered plan mode. The existing plan file covers this same task, so I'll refine it rather than start over. First, the detail most likely to break the two-stage approach.

  Ran 1 shell command

⏺ Monitor event: 2025-05-09: 126,793,919 rows 380s 333,601 pts/s cum 946,626,392 eta 2.07h · May hot seed: per-day rows, failures, and completion

⏺ Background command "Seed the remainder of May 2025 into hot storage" completed (exit code 0)

The hot seed reported a failure on 2025-05-10 and then exited 0 — that combination needs checking. Reading the log (read-only, allowed in plan mode).

  Ran 1 shell command

Important: the seed exited 1, not 0 — it stopped at 2025-05-10 on an InfluxDB HTTP 500 after retries. 9 days are safely recorded; 05-10 correctly wasn't marked done. Let me check the container.

  Ran 1 shell command

Container is healthy — no OOM, no restart, memory down to 3.4 GiB, still compacting. Let me find the actual 500.

  Ran 1 shell command

No error logged server-side — note Reindexing TSM data for shard 3 at 23:25:42, right around the failure. Let me confirm the 9 days are intact and queryable.

  Read 1 file, ran 9 shell commands
Read ../../../../../../tmp/sage_recon_check/addfiles_pelican.py (95 lines)
Read ../../../../../../tmp/sage_recon_check/cadence.py (91 lines)
Read .cold_prefix (2 lines)
Read seed_cold.py (270 lines)
Read ../../../../../../tmp/sage_recon_check/snapshots.py (54 lines)

⏺ Task "demo-influxdb memory climb toward the 6 GiB cap during the May seed" still running in background

⏺ Task "May hot seed: per-day rows, failures, and completion" still running in background

⏺ Task "VM disk headroom during the May seed" still running in background
Plan file referenced (~/.claude/plans/bubbly-wiggling-flute.md)
Skills restored (artifact-design)

Found a hard blocker in _partition_value before I write the design. Let me check two things that decide the shape of the monthly rewrite.

  Ran 3 shell commands

Two things in that source change the design. Let me size the staging.

  Ran 1 shell command

Found the blocker. _partition_value (fileformat.py:78-82) raises when a file spans two partition values — and my earlier measurement showed the archive's date=D spills ~3 rows at D+1T00:00:00. Let me check whether those rows are duplicated in date=D+1.

⏺ Write(/tmp/sage_recon_check/boundary.py)
Wrote 44 lines to ../../../../tmp/sage_recon_check/boundary.py
"""Does the archive's inclusive day-end duplicate rows into the next day?

add_files cannot repartition. pyiceberg derives a file's partition from
its footer min/max and RAISES if they fall in different partitions
(io/fileformat.py:78-82). The first cold day measured 1 file / 3 rows
landing in partition 20149 instead of 20148, so an archive day file DOES
span midnight -- which means a staged whole-day file would make add_files
raise, not merely mispartition.

The fix is to filter each staged day to [D, D+1). That is only safe if
… +34 lines

  Ran 1 shell command

Decisive. date=2025-03-31 spills 27 rows at 2025-04-01T00:00:00, and date=2025-04-01 holds 27 rows at exactly that instant. Let me confirm they're the same rows, not just the same count.

⏺ Write(/tmp/sage_recon_check/boundary2.py)
Wrote 43 lines to ../../../../tmp/sage_recon_check/boundary2.py
"""Are the spilled rows the SAME rows, or extra observations?

Counts match (03-31 spills 27 at 04-01T00:00:00; date=04-01 holds 27 at
that instant), but matching counts are not identity. If they are the same
rows, filtering each staged day to [D, D+1) is lossless -- and the
current day-by-day seeder is DOUBLE-WRITING them. If they are different
observations, filtering would drop real data.

Compare the full series key plus value at the boundary instant.
"""
… +33 lines

  Ran 1 shell command

Confirmed the overlap is an exact duplicate set. Let me check whether a large PUT to Pelican has ever been measured — it's the one remaining unknown.

  Ran 2 shell commands

Both blockers are now measurable facts. One local test de-risks the real 13-column schema before I commit the design.

⏺ Write(/tmp/sage_recon_check/addfiles_fullschema.py)
Wrote 85 lines to ../../../../tmp/sage_recon_check/addfiles_fullschema.py
"""Does add_files accept the REAL 13-column schema, filtered to one day?

The earlier Pelican add_files test used a 4-column toy schema. The real
table has `time` required, 7 nullable promoted tag strings, a
map<string,string>, and three value columns. parquet_file_to_data_file
runs _check_pyarrow_schema_compatible against a footer that carries NO
iceberg field-ids, so it must resolve by name mapping -- and required vs
optional is exactly the kind of thing that fails there.

Also verify the thing the boundary measurement implies: filtering each
… +75 lines

  Ran 1 shell command

My test's bug — dict insertion order, not column order. Build the table in schema order.

⏺ Update(/tmp/sage_recon_check/addfiles_fullschema.py)
Added 3 lines, removed 1 line
    }
    for tag in ("plugin", "vsn", "node", "host", "task", "job", "zone"):
        cols[tag] = pa.array([f"{tag}-x"] * N)
    t = pa.table(cols).cast(target, safe=False)
    # Build in SCHEMA order -- pa.table() keeps dict insertion order, and
    # cast() matches by position, not by name.
    t = pa.table({f.name: cols[f.name] for f in target}).cast(target, safe=False)
    p = f"{staged}/day{d:02d}.parquet"
    pq.write_table(t, p)
    urls.append(f"file://{p}")

⏺ Update(/tmp/sage_recon_check/addfiles_fullschema.py)
Added 3 lines, removed 3 lines

# And the thing that must FAIL: a file spanning midnight.
base = dt.datetime(2025, 3, 20, 23, 59, 59, tzinfo=dt.timezone.utc)
t = pa.table({**{k: v for k, v in cols.items() if k != "time"},
              "time": pa.array([base + dt.timedelta(seconds=i) for i in range(N)],
                               pa.timestamp("us", tz="UTC"))}).cast(target, safe=False)
cols["time"] = pa.array([base + dt.timedelta(seconds=i) for i in range(N)],
                        pa.timestamp("us", tz="UTC"))
t = pa.table({f.name: cols[f.name] for f in target}).cast(target, safe=False)
p = f"{staged}/spanning.parquet"
pq.write_table(t, p)
try:

  Ran 1 shell command

All four unknowns are now measured. Writing the design into the plan file.

⏺ Updated plan
/plan to preview

⏺ Monitor event: [Monitor expired after 30m with no events delivered. Re-arm it if you still need the watch — and widen the filter if silence was une…

The memory monitor expired with no events — expected, since the hot seed stopped at 05-10 and nothing is writing. Not re-arming it. Let me check the rest of the plan for claims the measurements just invalidated.

  Searched for 1 pattern, read 1 file

Line 251-253 now reads as a contradiction against the new section. Tightening it.

⏺ Updated plan
/plan to preview

Now Phase 5 — the migration job writes to the same table, so the cadence applies there too.

  Read 1 file

⏺ Updated plan
/plan to preview

Now the verification section, which should assert the cadence rather than assume it.

  Read 1 file

⏺ Updated plan
/plan to preview

⏺ Updated plan
/plan to preview

Finally, the hot seed stall and the leftover prefixes both need to be in the plan rather than only in my head.

⏺ Updated plan
/plan to preview

⏺ User approved Claude's plan
Plan saved to: ~/.claude/plans/bubbly-wiggling-flute.md · /plan to edit
Locally hosted hot/cold demo: InfluxDB → Iceberg on Pelican

Context

41 experiments established that Iceberg on Pelican/NRDStor works, that a SQL
catalog plus a single-reconciler version-hint.text lets many writers commit
while database-less consumers read, and that the target cadence is monthly
eviction from InfluxDB into cold storage. All of that lives in SUMMARY.md
and plans/6_db_catalog_with_version_hint.md as measurements and prose.

Nobody can run it. There is no artifact a teammate can start and watch data
move from hot to cold, and no demonstration that the existing query interface
keeps working across the boundary. That last point is the real risk the
experiments never retired: every measurement so far tested Iceberg, not the
promise that sage-data-client keeps working unchanged.

This builds that demo: docker compose up, InfluxDB 2.7.7 as hot storage,
Iceberg on Pelican as cold, the real beehive-data-api wire protocol in
front of both, and a migration job that moves a month across the boundary.
The success criterion is a single query spanning both tiers that returns the
same observations before and after migration — every row preserved, with the
one expected difference being timestamp precision, which drops from ns to µs
as data crosses into cold storage (finding 3). Demonstrating that drop
cleanly is part of the point.

Baseline verified: beehive-data-api is clean on main and
go test ./... passes (ok influxdb-data-api 0.388s) under the host's Go
1.26.5 despite go.mod saying 1.22. Note request_queue.go defines a
RequestQueue that nothing references — dead code; leave it alone.

Decisions already made

- Data: Mar + Apr 2025 seed cold directly from Parquet. May 2025 seeds
  hot, then migrates hot→cold during the demo.
- Code location: edit beehive-data-api/ in place (currently clean on
  main). Nothing is committed.
- Cold target: real NRDStor by default, local MinIO as a fallback profile.
- meta mapping: every meta key becomes an InfluxDB tag. See below.

Findings from this session that shape the build

Each of these was measured, and each would have cost a debugging cycle.

1. meta keys are tags, by construction. archive-exporter/main.go:180-199
   builds meta from point.TagList() — the seven promoted columns are
   special-cased and every other tag falls into the map. So flattening meta
   back to tags is a faithful round-trip. Cost: 6,116 series with promoted
   tags only vs 7,320,336 with meta flattened (~1,197×) over 9 days;
   99.6% of rows carry a non-empty meta. This is a property of production to
   reproduce and measure, not a bug to engineer around.
2. DuckDB inherits the host timezone. A fresh connection reports
   America/Chicago and renders UTC timestamps shifted. Instants stay
   correct, so counts and values still match — what breaks is anything
   deriving a date: partition pruning, day-batching, watermark comparison.
   SET TimeZone='UTC' at connection setup.
3. The tiers hold different timestamp precision, by design. Hot keeps
   nanoseconds — InfluxDB is the real-time tier and must retain that
   capability. Cold is microseconds: pyiceberg 0.12.0 cannot store ns —
   a v2 table rejects timestamp[ns] outright
   (UnsupportedPyArrowTypeException) and v3, which added
   TimestamptzNanoType, raises NotImplementedError: Writing V3 is not yet supported (apache/iceberg-python#1551).

   So migration is where precision drops from ns to µs, in the demo and
   in production alike. That is a property to demonstrate explicitly, not a
   defect to hide.

   Measured on 2025-05-01: 98.34% of 146,129,405 rows carry sub-µs
   digits (not the 94% recorded earlier), so the drop is visible on almost
   every row. What makes it safe is a second measurement: truncating to µs
   causes zero collisions on the full series key (name + 7 promoted
   tags + meta + µs timestamp) across the whole day — 0 colliding groups,
   0 points lost. No observation is merged or destroyed; only the last
   three digits go.

   Three consequences the build must honor:
   - The hot seeder writes ns, precision=ns. Do not truncate it.
   - A tier-spanning query legitimately returns mixed precision —
     ns-stamped records from hot, µs-stamped from cold. Clients must not be
     led to expect uniformity.
   - The watermark comparison must be unambiguous at the boundary instant,
     since the two sides carry different resolution. Define the watermark in
     ns and compare in ns; a µs-rounded watermark could re-admit or drop a
     sub-µs sliver of records at the cut.
4. Go and Python disagree on timestamp bytes. Go's time.Time JSON
   encoding trims trailing zeros (.5Z, .40183Z, bare Z); Python's
   isoformat() pads to 6 digits (.500000Z). Harmless to pandas, fatal to
   byte-level tier comparison. The cold service must trim to match Go.
5. Line protocol needs real escaping — counted over the whole of
   2025-05-01, not sampled: 67,473,789 meta values contain a space,
   comma or = (e.g. creator='CROCUS MST'); 1,480 value_str strings
   contain a newline, which would terminate a record early and corrupt
   the rest of the batch; 98 contain a quote or backslash. Reassuringly,
   0 meta keys and 0 measurement names need escaping, and there
   are no empty tag values or non-finite floats. So the escaping burden is
   concentrated in exactly two places: tag values, and string field values.
6. Ingest volume, measured rather than estimated. Real encoding of
   200k rows gives 342 bytes/row of line protocol (not the ~225 I had
   assumed), so May 2025's 4.37B rows is ~1.49 TB on the wire — it must
   stream from Parquet and never land on disk (352 GB free). gzip compresses
   it 27× to ~55 GB, so the network is not the constraint. Python
   encoding ran at ~171k rows/s single-core, i.e. ~7 h of pure encoding
   for the month on one core — so the seeder must use multiple processes,
   and encoding, not just InfluxDB, is a real cost centre. There is no bulk
   loader: all 16 influxd inspect subcommands export or report, and
   influx restore needs a backup dir we do not have.
7. Cardinality is the thing that may stall the seed. 2025-05-01 alone
   has 905,468 distinct full series. This is the number Phase 0 exists
   to pressure-test; it is ~150× the promoted-tags-only count and is the
   direct consequence of finding 1.
8. The pieces compose. Verified locally: Parquet → Iceberg preserves
   map<string,string>; pyiceberg .to_arrow() → con.register() → DuckDB
   runs cold_backend's compiled SQL and returns correct records. Pelican
   auth works from this machine (~500 ms round trip). influxdb:2.7.7 has a
   native arm64 image.

---

Phase 0 — Benchmark before committing to a scope

The one number nothing can be planned around is this machine's ingest rate.
Run this first; it replaces every estimate above.

Seed 2025-05-01 only (15 files, 146,129,405 rows, 905,468 distinct
series, 41 meta tag keys) into a throwaway influxdb:2.7.7 container and
record:

- sustained points/sec at a few batch sizes and worker counts
- series cardinality growth and TSI index size
- container RSS and on-disk TSM footprint
- whether ingest rate degrades as cardinality climbs (the thing that decides
  whether a full month is reachable)

Then extrapolate honestly to 4.37B rows and choose the hot-tier scope from
the measurement. If a full month is out of reach, the fallback is a
configurable day range (DEMO_HOT_DAYS), and the demo still shows a real
multi-day migration. Record the result in demo/BENCHMARK.md.

Nothing downstream should be built until this number exists.

---

Phase 1 — Seeders

sage-data-rework/demo/seed_hot.py

Streams Parquet → line protocol → gzip → POST /api/v2/write. Never
materializes line protocol to disk.

- Read with pq.ParquetFile(...).iter_batches(), not read_table — a single
  file is up to 78 MB with one row group and must not be fully resident.
- Measurement = name. Tags = the seven promoted columns plus every
  meta key. Field = value, from the first non-null of
  value_float / value_int / value_str.
- Escaping (finding 5): tag values escape ,, =, space (67.5M of them
  per day need it); string field values escape " and \ and must handle
  the 1,480 newline-bearing strings — strip or escape, never emit raw, or
  the rest of the batch is corrupted. Measurement names and meta keys
  measured clean, but escape them anyway rather than relying on that.
- Timestamps in nanoseconds, precision=ns (finding 3). The hot tier
  is the real-time tier and keeps full resolution; the ns→µs reduction
  happens at migration, not at ingest.
- Batch by point count and byte ceiling; gzip each body (27× measured).
- Multi-process encoding — one core encodes at ~171k rows/s, so a
  single-process seeder is encoder-bound long before InfluxDB is.
- Retry 429/5xx with backoff; abort loudly on 400 (a 400 means malformed line
  protocol, which is a bug, not a transient).
- Resumable at day granularity via a small progress file, so an
  interrupted seed restarts at the next unfinished day.
- Progress output: day, rows, points/sec, elapsed, projected remaining.

sage-data-rework/demo/seed_cold.py — rewrite to one commit per month

The file exists and works, but commits per day, which is not the design
cadence. Production evicts from InfluxDB monthly, so the seeder must publish
monthly too. Rewriting it also fixes two defects found while measuring.

Why the obvious fixes don't work. tbl.transaction() does not coalesce
appends: measured, 10 tx.append() calls → 10 snapshots, and the first
real cold day produced 141. Wrapping a month in one transaction would
give ~465 snapshots, not one. Concatenating a month into a single append()
gives 1 snapshot but needs ~1.5 TB of Arrow (one day alone is 49 GB).

The shape that works — stage, then one add_files per month. Measured
against the real 13-column schema: 5 staged files → 1 snapshot, 5
day-partitions preserved, 13 columns, timestamp[us, tz=UTC], meta map
intact. Separately proved against live Pelican that add_files resolves
https:// URLs through PelicanFileIO. Partitioning survives because
parquet_file_to_data_file derives the partition from footer statistics
(io/pyarrow.py:2905), not from the commit boundary — so monthly commits
keep day-partitioning, and exp36's 608→7 pruning is unaffected.

Note finding 20a — "add_files cannot register the archive" — applies only
to the archive's timestamp[ns] files. It works on µs files we write.

Per month:
1. For each day, stream each archive file through the existing
   iter_chunks_us(), write a local temp Parquet via pq.ParquetWriter
   (incremental, so memory stays at one chunk), PUT it under
   <prefix>/staged/<day>/, delete the temp. Memory stays at the measured
   2.67 GB; staged files are ~40–80 MB each, ~915 for Mar+Apr.
2. One tbl.add_files([...all month's URLs...]) → 1 snapshot.

Defect 1 — a day file spans midnight, and add_files rejects that.
_partition_value raises when a file's footer min/max fall in different
partitions (io/fileformat.py:78-82); confirmed by deliberately staging a
spanning file. Every archive date=D is inclusive at both ends
(date=2025-03-31 runs to 2025-04-01T00:00:00), so each staged file must
be filtered to [D, D+1).

Defect 2 — the current day-granular seeder double-writes the boundary.
Those spilled rows are not extra observations: at 2025-04-01T00:00:00,
date=2025-03-31 holds 27 rows and date=2025-04-01 holds the same 27 —
EXCEPT ALL both ways returns 0. So the [D, D+1) filter is lossless
and removes a duplicate the day-by-day code was committing. Assert the
dropped count equals the next day's count at that instant rather than
filtering silently.

Resumability. A month is now one commit, so a crash mid-month leaves
staged files with nothing registered. Record staged URLs in a progress file
so a resume skips re-staging and goes straight to add_files; keep
check_duplicate_files=True as the backstop against double registration.

Budget: 36 GB at the measured 6–8.6 MB/s raw PUT, with the µs rewrite
sending 17% fewer bytes.

Keep unchanged: the propfind overwrite guard (fixed this session — it was
calling a module-level function as a method and had never actually run), the
10M-row batch default, the ns→µs cast, and the day partition spec.

Reuses, from sage-data-rework/experiments/:
- pelican_fileio.PelicanFileIO via property "py-io-impl"
- pelican_token.mint (pass key_path= explicitly — the default resolves
  parents[2] relative to experiments/)
- the SqlCatalog("sage", uri=sqlite:///…, warehouse=…) pattern, with
  catalog_name exactly "sage" (it is part of the iceberg_tables
  primary key; a different name silently yields a private catalog)
- the ns→µs cast: t.cast(us_schema(src), safe=False) — a v2 table rejects
  timestamp[ns] and v3 cannot be written at all (finding 3). Because the
  cast rewrites the data, add_files() on the original archive Parquet is
  not available; the files must be rewritten. Budget for that: it is a
  full read+write of 36 GB across Mar–Apr, not a metadata-only operation.
  This truncation is cold-only — the hot seeder keeps ns.

Writes under a fresh /cold-storage-demo-<nanos> prefix. Partitioned by
day, committed by month — the two are independent, as the staging design
above establishes. exp36 measured a one-week filter planning 7 files of
608; partitioning by month to match the commit rhythm would undo that.
Guard with propfind before writing and never touch /node-data, which
is live at the Pelican root.

sage-data-rework/demo/reconciler.py

Promote the reconciler out of exp34_hint_reconciler.py:95-143 — this is
open item 5 of plans/6 and the demo is the forcing function. Keep all three
behaviours intact: read the head via direct sqlite3 on iceberg_tables
(not via SqlCatalog, so an orphan metadata file cannot fool it); publish
the stem (00002-<uuid>), not a bare version; and write no trailing
newline (keep the assert — neither DuckDB nor pyiceberg trims).

---

Phase 2 — Cold query service

sage-data-rework/demo/cold_service/

A small HTTP service speaking the same JSON-in / ndjson-out contract, reading
Iceberg rather than raw Parquet.

Architecture, verified in this session: pyiceberg plans and reads
(StaticTable.from_metadata + PelicanFileIO, resolved through
version-hint.text) → .to_arrow() → con.register("obs", tbl) → DuckDB
runs the SQL. DuckDB's own iceberg extension loads but cannot speak
Pelican, so pyiceberg must own the I/O.

Reuse cold_backend.py's semantics wholesale — it already replicates the
subtle parts (half-open [start, end), substring filter dispatch where
env.temperature is exact but env.temp.* is a regex, per-series head/tail
via row_number() OVER (PARTITION BY name, meta …), value coalescing, tag
folding). Changes needed:

- SET TimeZone='UTC' at connect (finding 2)
- read_parquet([...]) → the registered Arrow view
- trim trailing zeros from the timestamp to match Go byte-for-byte
  (finding 3)
- push the time range into scan(row_filter=…) so pruning happens in
  pyiceberg, not after loading everything into Arrow
- set PYICEBERG_MAX_WORKERS and reset
  ExecutorFactory._instance = None (it is process-cached; worth 6-8×)

Aggregations (experimental_func) are not implemented in cold_backend.py.
Return a clear error rather than a wrong number — mean-of-means across tiers
is wrong without counts.

---

Phase 3 — Tiered backend (Go, in beehive-data-api/)

New tiered_backend.go + cold_backend.go, wired in main.go. The existing
Backend interface is one method with two implementations already
(InfluxBackend, DummyBackend), so this is additive.

type Backend interface { Query(context.Context, *Query) (Results, error) }

- Watermark: a single UTC instant; everything < watermark is cold,
  >= watermark is hot. Read from config/env so the migration job can
  advance it. One source of truth, consulted per request.
- Routing: query range entirely one side → pass straight through. Range
  spanning the watermark → split into two sub-queries and merge.
- Merge: both sides emit time-ordered records; merge lazily in the
  Results iterator so streaming and the existing ndjson writer are
  preserved. Never buffer a full result set.
- head/tail: per-series, not global (Flux limit/tail are table ops).
  Concatenating each tier's per-series head is wrong. Either apply the
  per-series limit again during the merge, or refuse head/tail on a
  tier-spanning query — pick one and make it explicit.
- Aggregations spanning tiers: return an error. Do not silently compute
  mean-of-means.
- Partial failure: the service writes 200 + headers before iterating,
  so a mid-stream error cannot change the status. A failing tier must
  therefore surface as a logged error and a non-silent signal, not a
  truncated 200 that looks like a complete empty result.

main.go selects the backend by env var so the unmodified single-tier path
still works.

---

Phase 4 — Compose topology

sage-data-rework/demo/docker-compose.yml:

- influxdb — influxdb:2.7.7 (native arm64), org waggle, bucket
  waggle, healthcheck mirroring production's Flux probe, named volume
- influx-setup — one-shot: create org/bucket/token
- data-api — built from beehive-data-api/ (existing two-stage Dockerfile,
  FROM scratch); bump golang:1.22 to match the host toolchain
- cold-service — the Python Iceberg reader
- reconciler — publishes version-hint.text
- minio + minio-setup — profile minio only
- Profiles: nrdstor (default) and minio, following the existing pattern
  in sage-storage-loader/test/{s3,pelican}/docker-compose.yml
- Seeders run as one-shot jobs or host-side scripts, not long-lived services

Secrets: the Pelican issuer key is mounted read-only; nothing is baked into
an image and nothing new is written into honeyhouse-config.

---

Phase 5 — Migration job

sage-data-rework/demo/migrate.py — moves May 2025 hot→cold. Honors the
project's stated invariant: InfluxDB retention should only remove an
interval after we have confirmed its observations are durably available
through the archive catalog.

Strict ordering, each step safe to crash at:

1. Read the interval from InfluxDB.
2. Write + commit it to Iceberg — the same stage-then-add_files
   shape as seed_cold.py, one snapshot for the migrated month, so
   production's cadence is what the demo actually exercises. Reuse the
   staging helper rather than reimplementing it, including the [D, D+1)
   filter that keeps each staged file single-partition. Crash here: cold
   has data nobody reads; the watermark has not moved; hot still serves.
   No loss, no double-serve.
3. Publish version-hint.text. Crash here: cold is readable but the
   watermark still routes to hot. Still no double-serve.
4. Verify durability — re-read the committed interval through the cold
   path and compare against hot: exact row count, and a sampled row-level
   comparison of name/value/meta plus timestamps compared at µs,
   since cold legitimately lost the sub-µs digits (finding 3). This is the
   gate; failure aborts before anything is deleted. Comparing raw ns here
   would fail on 98% of rows and block every migration.
5. Advance the watermark. This is the cutover instant.
6. Delete from InfluxDB, only now.

Crashing between 5 and 6 leaves data in both tiers but served from cold only
— correct, merely wasteful, and re-runnable. A monthly job is exactly the
kind that gets re-run by hand, so it must be idempotent and guarded against
overlapping itself (plans/6, ranked risk #3).

---

Verification

1. exp08_query_api.py still passes (cold semantics unregressed).
2. go test ./... in beehive-data-api/ stays green — baseline confirmed
   passing today, and TestBuildFluxQuery / TestQueryResponse /
   TestValidQuery pin the Flux generation and the ndjson wire format, so
   they are the guard that the tiered backend does not disturb the
   single-tier path.
3. Unmodified sage-data-client against http://localhost:10000/api/v1/query:
   - hot-only range → records
   - cold-only range → records
   - range spanning the watermark → records from both, correctly ordered
4. The central assertion, stated precisely now that precision differs
   across the boundary (finding 3). Run one tier-spanning query before and
   after migrating May 2025 and assert:
   - same row count — nothing lost, nothing duplicated;
   - name, value and meta identical for every row;
   - timestamps equal after truncating both sides to µs — the only
     permitted difference, and the one the migration is expected to cause.

   Assert the sub-µs digits are present before and absent after, so the
   test proves the precision change happened rather than silently tolerating
   it. A test that only compared truncated timestamps would pass even if
   migration had mangled them.
5. The commit cadence is what was designed, asserted rather than
   assumed — it was wrong once already:
   - len(tbl.metadata.snapshots) == number of months seeded (2 after
     Mar+Apr, 3 after May migrates). This is the check that would have
     caught the 141-snapshots-per-day defect on day one.
   - day-partitions still present: a one-week filter plans ~7 files, not
     the whole month — the property monthly commits must not cost.
   - no duplicate boundary rows: count(*) at each month-boundary
     instant equals the archive's count at that instant, not twice it.
   - total row count equals the archive's own count for the range, taken
     with the [D, D+1) filter applied once.
6. Timestamp bytes from the cold service match Go's encoding exactly
   (finding 4) — assert on raw ndjson, not parsed frames.
7. A seeded round-trip check on the ugly rows: the 1,480 newline-bearing
   and 98 quote-bearing value_str records come back from InfluxDB byte-
   identical to the archive. These are the rows most likely to be silently
   mangled, and a passing aggregate count would hide it.
8. Confirm /node-data untouched and nothing committed to git.

Open items carried into execution

- The hot seed is stopped at 2025-05-10, 9 of 30 days done
  (946,626,392 rows). It failed with
  data_0.parquet: write failed after retries: 500 and exited 1. The
  container is healthy (running, no OOM, 0 restarts, 3.4 of 6 GiB) and
  the InfluxDB log shows only routine compaction plus a shard reindex
  around the failure — no server-side error, so the cause is not yet
  identified. 05-10 was correctly not marked done, so a resume retries
  it whole. Decide with the user whether to resume as-is or instrument
  first; a blind retry may hit the same wall.
- Four abandoned Pelican prefixes need the user's hand —
  /cold-storage-demo-1792106294, -1792106446,
  -addfiles-1792107188362302000, and the dirty -1792106937. DELETE
  on them is blocked for me by the mass-delete guard.
- MinIO fallback is deferred. It is not a compose-only change:
  pelican_fileio._request hardcodes pt.ROOT_URL and SciToken minting,
  and three call sites construct PelicanFileIO directly
  (cold_service/app.py:63, reconciler.reconcile, seed_cold.py). It
  needs a FileIO-selection seam first.
- Relative timestamps (-1h) are rejected by the tiered backend. If
  sage-data-client sends them by default, that is a compatibility gap
  that verification step 3 must cover.

Constraints

- Never write to /node-data or any sage-storage-loader path; Iceberg goes
  only under /cold-storage-demo-<nanos>.
- Do not modify anything under sage-data-rework/data/ (read-only archive).
- No git commits, pushes, or staging, including in beehive-data-api/.

Writing the monthly-commit rewrite now.

⏺ Update(sage-data-rework/demo/seed_cold.py)
Added 15 lines, removed 3 lines
    Measured safe: truncating a full 146M-row day to us collided 0 times on
    the full series key, so no observation is merged or lost.

  * One commit per day, day-partitioned. exp36 measured that a one-week
    filter then plans 7 files instead of 608; partitioning by month to match
    the commit rhythm would undo that.
  * One commit per MONTH, day-partitioned. Production evicts from InfluxDB
    monthly, so the seeder publishes on the same rhythm. Partitioning is a
    separate axis and stays daily: exp36 measured that a one-week filter
    then plans 7 files instead of 608.

    These two are independent because `add_files` derives each file's
    partition from its Parquet footer statistics, not from the commit
    boundary (pyiceberg io/pyarrow.py:2905 -> io/fileformat.py:86).

Getting to one snapshot per month needs the two-stage shape below, because
a transaction does NOT coalesce appends -- measured, 10 `tx.append()` calls
produce 10 snapshots, and the first real day produced 141. Each day is
staged to Parquet and PUT, then the whole month is registered with a single
`add_files()`. Concatenating a month in memory is the other way to get one
snapshot and is not available: one day alone is 49 GB in Arrow.

Writes only under a fresh /cold-storage-demo-<nanos> prefix, and refuses to
start if the prefix is not empty. /node-data is live at the Pelican root and
is never touched.

⏺ Update(sage-data-rework/demo/seed_cold.py)
Added 5 lines, removed 1 line
"""
import argparse
import glob
import json
import os
import sys
import tempfile
import time
from datetime import date, timedelta
from collections import OrderedDict
from datetime import date, datetime, timedelta, timezone

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

EXPERIMENTS = os.path.join(

Now the core: day-grouping, the [D, D+1) filter, staging, and the monthly commit.

⏺ Update(sage-data-rework/demo/seed_cold.py)
Added 84 lines
    return out


def months_of(days):
    """Group day strings into {'YYYY-MM': [day, ...]}, order preserved.

    The commit unit. Days stay the partition unit.
    """
    out = OrderedDict()
    for day in days:
        out.setdefault(day[:7], []).append(day)
    return out


def day_bounds(day):
    """[D, D+1) as timestamps, for the clip below."""
    d0 = date.fromisoformat(day)
    lo = datetime(d0.year, d0.month, d0.day, tzinfo=timezone.utc)
    return lo, lo + timedelta(days=1)


def clip_to_day(table, day):
    """Keep only [D, D+1). Returns (table, n_dropped).

    The archive's date=D is inclusive at BOTH ends: date=2025-03-31 carries
    rows stamped exactly 2025-04-01T00:00:00, and date=2025-04-01 carries
    that same set again -- verified identical, EXCEPT ALL both ways returns
    0, so this drops duplicates rather than data.

    It is also required, not merely tidy. add_files infers a file's
    partition from its footer min/max and RAISES when they fall in
    different partitions ("Cannot infer partition value ... more than one
    partition values", pyiceberg io/fileformat.py:78-82). An unclipped day
    spans midnight, so the monthly add_files would abort on it.
    """
    lo, hi = day_bounds(day)
    t = table.column("time")
    keep = pc.and_(pc.greater_equal(t, pa.scalar(lo, t.type)),
                   pc.less(t, pa.scalar(hi, t.type)))
    out = table.filter(keep)
    return out, table.num_rows - out.num_rows


def stage_day(day, files, target, batch_rows, prefix, verbose=True):
    """Write one day as a single microsecond Parquet file on Pelican.

    Streams: a ParquetWriter takes chunk after chunk, so peak memory stays
    at one chunk (measured 2.67 GB at 10M rows) rather than the 49 GB a
    whole day occupies in Arrow. The local temp file is deleted once PUT.

    Returns (url, rows, dropped).
    """
    fd, tmp = tempfile.mkstemp(prefix=f"seedcold-{day}-", suffix=".parquet")
    os.close(fd)
    rows = dropped = 0
    t0 = time.time()
    try:
        w = pq.ParquetWriter(tmp, target)
        try:
            for f in files:
                for chunk in iter_chunks_us(f, target, batch_rows):
                    chunk, d = clip_to_day(chunk, day)
                    dropped += d
                    if chunk.num_rows:
                        w.write_table(chunk)
                        rows += chunk.num_rows
                    del chunk
        finally:
            w.close()

        size = os.path.getsize(tmp)
        path = f"{prefix}/staged/{day}.parquet"
        with open(tmp, "rb") as fh:
            st, _, body = pelican_fileio._request(path, "PUT", data=fh.read())
        if st not in (200, 201, 204):
            raise IOError(f"PUT {path} -> {st} {body[:200]!r}")
    finally:
        os.unlink(tmp)

    if verbose:
        el = time.time() - t0
        print(f"    {day}: {rows:>12,} rows  {size/1e6:7.1f} MB  {el:6.1f}s  "
              f"{size/1e6/max(el, 1e-9):5.1f} MB/s  dropped {dropped}",
              flush=True)
    return f"{pt.ROOT_URL}{path}", rows, dropped


def iter_chunks_us(path, target_schema, batch_rows):
    """Yield microsecond-cast Arrow tables from one archive file.


Now the progress file and the main loop.

⏺ Update(sage-data-rework/demo/seed_cold.py)
Added 31 lines
        yield t.cast(target_schema, safe=False)


def load_progress(path):
    """Staged-but-uncommitted work from an earlier run.

    A month is one commit now, so a crash mid-month leaves staged files
    that nothing references. Without this, a resume re-uploads every one of
    them -- hours of PUTs for data already on Pelican. Shape:

        {"staged": {"2025-03": {"2025-03-01": [url, rows, dropped], ...}},
         "committed": ["2025-03"]}
    """
    if not os.path.exists(path):
        return {"staged": {}, "committed": []}
    with open(path) as fh:
        p = json.load(fh)
    p.setdefault("staged", {})
    p.setdefault("committed", [])
    return p


def save_progress(path, prog):
    """Write via a temp file in the same directory, then rename.

    The point of the progress file is to survive a crash, so it must never
    be observed half-written. rename(2) is atomic within a filesystem.
    """
    tmp = f"{path}.tmp"
    with open(tmp, "w") as fh:
        json.dump(prog, fh, indent=1, sort_keys=True)
    os.replace(tmp, path)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--start", required=True)

⏺ Monitor event: [Monitor expired after 30m with 2 events delivered. Re-arm it if you still need the watch.] · May hot seed: per-day rows, failures, …

⏺ Monitor event: ERROR conda.cli.main_run:execute(148): `conda run python -I seed_hot.py --start 2025-05-01 --end 2025-05-31 --procs 4 --progress see…

Those are the tail of the already-known 05-10 failure, not new. Continuing with main().

  Read 1 file

⏺ Update(sage-data-rework/demo/seed_cold.py)
Added 4 lines
                    help="rows per Arrow chunk; bounds peak memory. A whole "
                         "day is ~49 GB in Arrow, so this must stay modest. "
                         "10M matches the archive's native row-group size.")
    ap.add_argument("--progress", default="seed_cold.progress",
                    help="resume file; records staged URLs and committed "
                         "months so a crash mid-month does not re-upload")
    ap.add_argument("--dry-run", action="store_true",
                    help="read and cast, but do not write to Pelican")
    args = ap.parse_args()

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)
    days = days_between(start, end)
    months = months_of(days)

    prefix = args.prefix or f"/cold-storage-demo-{time.time_ns()}"
    if not prefix.startswith("/cold-storage-demo"):

⏺ Update(sage-data-rework/demo/seed_cold.py)
Added 1 line
    warehouse = f"{pt.ROOT_URL}{prefix}/warehouse"

    print(f"days      : {days[0]} .. {days[-1]} ({len(days)})")
    print(f"months    : {', '.join(months)}  ({len(months)} commits)")
    print(f"prefix    : {prefix}")
    print(f"warehouse : {warehouse}")


Now replace the per-day commit loop with the monthly stage-then-register loop.

  Read 1 file

⏺ Update(sage-data-rework/demo/seed_cold.py)
Added 18 lines, removed 3 lines
        print(f"dry run ok, {tot:,} rows")
        return 0

    pfio = PelicanFileIO({})
    prog_path = os.path.abspath(args.progress)
    prog = load_progress(prog_path)
    resuming = prog.get("prefix") == prefix and (prog["staged"] or
                                                 prog["committed"])

    # Module-level function, NOT a method on PelicanFileIO -- calling it as
    # one raised AttributeError, which meant this guard had never actually
    # run. It is the check that stops a seed landing on top of existing
    # data, so it is the last thing that should fail silently.
    st, kids = pelican_fileio.propfind(prefix)
    if st != 404:
    if st != 404 and not resuming:
        # Refuse to write into anything that already exists. The one thing
        # this script must never do is land on top of live data.
        print(f"ABORT: prefix not empty (PROPFIND -> {st}, {len(kids)} children)")
        print(f"       (no resumable progress for this prefix in {prog_path})")
        return 2
    print("fresh prefix: PROPFIND -> 404")
    if resuming:
        # A resume is the one legitimate reason to meet a non-empty prefix:
        # the progress file names this exact prefix, so the occupant is our
        # own staged output. Anything else still aborts above.
        print(f"resuming into {prefix} "
              f"({len(prog['committed'])} months committed, "
              f"{sum(len(v) for v in prog['staged'].values())} days staged)")
    else:
        print("fresh prefix: PROPFIND -> 404")
        prog["prefix"] = prefix
        save_progress(prog_path, prog)

    from pyiceberg.catalog.sql import SqlCatalog
    cat = SqlCatalog(CATALOG_NAME, **{

⏺ Monitor event: [Monitor expired after 30m with no events delivered. Re-arm it if you still need the watch — and widen the filter if silence was une…

All three monitors have now expired; none are needed while nothing is running. Replacing the commit loop.

⏺ Update(sage-data-rework/demo/seed_cold.py)
Added 54 lines, removed 35 lines
        print(f"created table {ident} (day-partitioned)")

    target = tbl.schema().as_arrow()
    grand, t_all = 0, time.time()
    for day in days:
        files = sorted(glob.glob(f"{ARCHIVE}/date={day}/*.parquet"))
        if not files:
            print(f"  {day}: no archive files, skipping", flush=True)
    grand, dropped_all, t_all = 0, 0, time.time()

    for month, mdays in months.items():
        if month in prog["committed"]:
            print(f"{month}: already committed, skipping", flush=True)
            continue
        t0 = time.time()
        rows = 0
        t_m = time.time()
        staged = prog["staged"].setdefault(month, {})

        # One transaction per day, many appends inside it.
        # Stage 1: one microsecond Parquet file per day, PUT to Pelican.
        # Nothing is registered yet, so a crash here costs only the
        # unstaged remainder -- the progress file keeps the rest.
        print(f"{month}: staging {len(mdays)} days", flush=True)
        for day in mdays:
            if day in staged:
                print(f"    {day}: already staged, skipping", flush=True)
                continue
            files = sorted(glob.glob(f"{ARCHIVE}/date={day}/*.parquet"))
            if not files:
                print(f"    {day}: no archive files, skipping", flush=True)
                continue
            url, rows, dropped = stage_day(day, files, target,
                                           args.batch_rows, prefix)
            staged[day] = [url, rows, dropped]
            save_progress(prog_path, prog)

        if not staged:
            print(f"{month}: nothing staged, no commit", flush=True)
            continue

        # Stage 2: ONE add_files for the whole month -> ONE snapshot.
        #
        # MEASURED, and not what the previous comment here claimed: a
        # transaction does NOT coalesce appends into one snapshot.
        # pyiceberg 0.12.0 produces one snapshot PER append even inside a
        # transaction -- the first seeded day made 141 of them at the old
        # 1M-row batch size. Snapshot entries persist in every subsequent
        # metadata.json (~0.7 KB each, plans/3_findings.md:626), which is
        # the file every consumer fetches on every hint resolution, so
        # they are a direct read-path cost, not just bookkeeping.
        # This is the cadence the design calls for: production evicts from
        # InfluxDB monthly, so cold publishes monthly. It cannot be done
        # with appends -- a transaction does not coalesce them, so a month
        # of appends is ~465 snapshots, each ~0.7 KB in the metadata.json
        # that every consumer fetches on every hint resolution.
        #
        # Batching at 10M rows (the archive's native row-group size) gives
        # 15 snapshots a day instead of 141 at LOWER peak RSS -- 2.67 GB
        # vs 2.84 GB at 5M -- because sub-row-group chunking only adds
        # overhead. 61 days is then ~915 snapshots, ~640 KB of metadata.
        #
        # Getting to literally one snapshot per day needs the two-stage
        # write-then-add_files pattern; that is a larger change and 15/day
        # is already cheap. Holding a day in memory to commit it whole is
        # not an option: measured 9.22 GB.
        with tbl.transaction() as tx:
            for f in files:
                for chunk in iter_chunks_us(f, target, args.batch_rows):
                    tx.append(chunk)
                    rows += chunk.num_rows
                    del chunk

        grand += rows
        # Day-partitioning survives because add_files reads each file's
        # footer statistics to place it (io/pyarrow.py:2905), so the
        # partition comes from the data, not from the commit boundary.
        urls = [staged[d][0] for d in sorted(staged)]
        m_rows = sum(staged[d][1] for d in staged)
        m_drop = sum(staged[d][2] for d in staged)
        t0 = time.time()
        tbl.add_files(urls)
        tbl = cat.load_table(ident)   # refresh to the new head
        print(f"  {day}: {rows:>12,} rows  {time.time()-t0:7.1f}s  "
              f"cum {grand:>14,}", flush=True)

        prog["committed"].append(month)
        save_progress(prog_path, prog)
        grand += m_rows
        dropped_all += m_drop
        print(f"{month}: committed {len(urls)} files, {m_rows:,} rows in "
              f"{time.time()-t0:.1f}s  (stage+commit {(time.time()-t_m)/60:.1f} min)"
              f"  snapshots now {len(tbl.metadata.snapshots)}", flush=True)

    el = time.time() - t_all
    print(f"\nloaded {grand:,} rows in {el/60:.1f} min")
    print(f"snapshots : {len(tbl.metadata.snapshots)} "
          f"(expected {len(prog['committed'])}, one per month)")
    print(f"dropped   : {dropped_all} duplicate rows at day boundaries "
          f"(archive date=D is inclusive at both ends)")
    print(f"catalog db: {os.path.abspath(args.db)}")
    print(f"hint path : {prefix}/warehouse/{NAMESPACE}/{TABLE}"
          f"/metadata/version-hint.text")

The unused pfio binding is now gone, but let me verify the whole file parses and the dry-run path still works.

  Ran 1 shell command

Now an end-to-end test against a local warehouse, using a real archive day so the clip logic meets real data.

⏺ Write(/tmp/sage_recon_check/monthly_e2e.py)
Wrote 97 lines to ../../../../tmp/sage_recon_check/monthly_e2e.py
"""End-to-end of the rewritten seeder's logic, locally, on REAL archive data.

Exercises stage_day + the monthly add_files against a file:// warehouse,
so the whole path runs without touching Pelican. Uses the smallest real
slice that still crosses a month boundary: 2025-03-31 and 2025-04-01,
truncated to a few row-group chunks.

Asserts the things the rewrite is for:
  * one snapshot per month, not per day
  * day-partitions preserved under a monthly commit
… +87 lines

  Ran 1 shell command

All four assertions pass on real archive data. Now the resume path, which is new and untested.

⏺ Write(/tmp/sage_recon_check/resume_unit.py)
Wrote 83 lines to ../../../../tmp/sage_recon_check/resume_unit.py
"""The resume machinery, which the e2e test does not reach.

A resume that misbehaves either re-uploads a month (hours) or, worse,
registers the same files twice. Both are silent. So check the pure parts
directly, including the guard that lets a resume past the overwrite check.
"""
import json
import os
import shutil
import sys
… +73 lines

  Ran 1 shell command

All 15 checks pass. One last confirmation that the dry-run path still works end-to-end after the refactor.

  Ran 1 shell command

Peak RSS 6.63 GB — notably above the 2.67 GB I recorded earlier. Memory is the known constraint, so let me measure the actual staging path rather than trust either number.

⏺ Write(/tmp/sage_recon_check/stage_rss.py)
Wrote 52 lines to ../../../../tmp/sage_recon_check/stage_rss.py
"""What does the staging path actually peak at?

The dry run just reported 6.63 GB for one day at the 10M batch size,
against the 2.67 GB recorded earlier for the same setting. One of those
is wrong, and memory is the constraint that decides whether this runs.

Staging adds a ParquetWriter on top of the dry run's read+cast, so if
anything it should be higher. Measure it on a real full day, writing
locally (no Pelican), and report the high-water mark at two batch sizes.
"""
… +42 lines

  Ran 1 shell command

RSS high-water is cumulative per process, so the second row is contaminated by the first. Measuring each batch in its own process.

  Ran 1 shell command

⏺ Streaming response ended before any complete data was received. Retrying without streaming. If this keeps happening, check any proxy or gateway between Claude Code and your model provider.

⚠️ IMPORTANT USAGE NOTICE FROM ARGO ⚠️

🚫 ACCESS REVOKED 🚫

Your Argo usage limit has been exceeded. Reason: Monthly limit exceeded. Please contact your Directorate Operations Officer for assistance.

 ** END NOTICE FROM ARGO **