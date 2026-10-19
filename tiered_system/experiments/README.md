# Cold storage experiments

Measurements supporting `plans/3_findings.md`. Findings and the numbers behind
each claim are in `results/SUMMARY.md`.

For sharing outside this directory, `results/iceberg_on_pelican_report.html` is
a standalone summary of §17–§39 — the verdict, the write/read/query numbers, the
Iceberg-versus-custom comparison, the open risks and the actions. Open it in a
browser; it needs no build step and fetches nothing but its fonts. It is written
for a reader who has not followed the experiments, so it labels every projected
figure as extrapolated rather than measured.

## Setup

```sh
conda create -n sage_pipeline python=3.12
conda activate sage_pipeline
pip install duckdb pyarrow pandas numpy pytz psutil
```

Data is a local copy of the sage03 archive, not committed:

```sh
rsync -a --partial sage03:/media/local/archive-export-test/archive/ data/archive/
```

## Method note

Bytes read are measured by serving the Parquet over a local HTTP server that
logs every Range request (`exp03`, `exp05`, `exp06`). That is deliberate: these
files are destined for Pelican, where every byte is an HTTP range request, and
local-disk wall time hides the cost almost entirely. Several conclusions here
reverse depending on which you measure.

The HTTP server stands in for Pelican in `exp01`–`exp16`. `exp17` closes the
gap: it runs against the real NRDStor origin with real SciTokens and confirms
range reads work — while showing that the local server understates the cost,
because Pelican charges ~150 ms per request and localhost charges nothing.

## Scripts

Run from this directory with `python -I <script> ...`.

| script | question |
|---|---|
| `exp01_page_index_pruning.py` | Does pyarrow use page indexes to skip? (no) |
| `exp02_duckdb_pruning.py` | Does DuckDB, by wall time? (looks like yes) |
| `exp03_range_reads.py` | Does it, by bytes over HTTP? (no — the real answer) |
| `exp04_rowgroup_sizing.py` | Rewrite one file at several row group sizes |
| `exp05_rowgroup_pruning.py` | Bytes read per row group size (the key result) |
| `exp06_multiday_queries.py` | Bytes, requests and files touched across days |
| `exp07_reexport_day.py` | Re-export one real day with the fixes |
| `exp08_query_api.py` | Beehive query semantics on Parquet (12 checks) |
| `exp09_manifest.py` | Manifest size, read time, and pruning power |
| `exp10_compaction.py` | The plan's proposed first milestone |
| `exp11_late_data.py` | Export lag, and whether correction files work |
| `exp12_rewrite_sample.py` | Rewrite savings sampled across the whole span |
| `probe_footer_cost.py` | What a planning query actually fetches per file (resolves §5 vs §7) |
| `probe_ns_remainder.py` | How many timestamps lose data in the Iceberg ns -> us cast |
| `exp17_pelican_cold_storage.py` | Pelican/NRDStor for real: ranges, read-after-write, pointer swap |
| `exp18_catalog_on_pelican.py` | The custom catalog with its store swapped for Pelican (12 checks) |
| `exp19_iceberg_on_pelican.py` | Iceberg on Pelican: can it run, does its commit hold, and can readers skip the catalog? (15 checks) |
| `icebergo/` (Go) | Does iceberg-go write format-version 3 with nanosecond timestamps, where pyiceberg cannot? (7 checks) |
| `icebergo/pelican_test.go` (Go) | The same v3 + nanosecond path on the real NRDStor origin (12 checks) |
| `icebergo/evolve_test.go` (Go) | Can a published schema be changed, and can one version be named and cited? (17 checks) |
| `icebergo/reader_test.go` (Go) | The NDP question: what can a read-only third party do without the writer's catalog? (12 checks) |
| `ndp_interop.py` | Can DuckDB/pyiceberg read what the Go writer produces? (v2 vs v3 fixtures) |
| `icebergo/fscatalog_test.go` (Go) | Can Iceberg commit on Pelican with no database? (8 checks — no) |
| `probe_us_collision.py` | Does ns -> us truncation merge distinct readings? (pyarrow) |
| `probe_us_collision_sql.py` | The same question in DuckDB, for speed and as a cross-check |
| `icebergo/portable_test.go` (Go) | What storage can Iceberg address, and can a written table be moved? (15 checks) |
| `icebergo/recover_test.go` (Go) | Can the archive be recovered after the catalog is deleted? (15 checks) |
| `probe_ducklake.py` | Would DuckLake clear the constraints Iceberg was measured against? (11 checks) |
| `probe_ducklake_http.py` | Can a DuckLake be published read-only over HTTP, with no local catalog? (4 checks) |
| `probe_ducklake_pelican.py` | DuckLake on the real NRDStor origin, against Iceberg's §19/§24 results (14/16) |
| `probe_ducklake_concurrency.py` | Where does a DuckLake catalog serialize, and does it lose rows? (5 checks) |
| `icebergo/fscatalog_single_test.go` (Go) | Does the filesystem catalog work on Pelican with ONE writer? (6/6 — yes) |
| `icebergo/fscatalog_race_test.go` (Go) | Is the TOCTOU window real? (3 of 4 writers "win"; 2 commits lost) |
| `icebergo/fscatalog_parallel_test.go` (Go) | Does "one writer" rule out parallel ingestion? (4/4 — no) |
| `exp34_hint_reconciler.py` | plans/6 end to end: many committers, one reconciler, a reader with no DB (12/12) |
| `exp35_sqlite_vs_postgres.py` | Is SQLite actually insufficient as the catalog? (4/4 — no) |
| `exp36_scale.py` | Does plans/6 hold at the archive's real scale? (9/9 — yes, with three corrections) |
| `exp37_pelican_scale.py` | What does that cost over Pelican? (12/12 — writes flat, reads are the problem) |
| `exp38_query_throughput.py` | Is *querying* scalable? (7/7 — only with a concurrent reader) |
| `exp39_iceberg_vs_custom.py` | Does Iceberg add latency vs plain Parquet? (5/5 — no) |
| `exp40_metadata_storage.py` | What storage does Iceberg add? (9/9 — net −35 GB) |
| `exp41_metadata_retention.py` | Can Iceberg bound old `metadata.json` itself? (6/6 — yes, but it can break a hint) |
| `probe_pelican_cas.py` | Does *any* conditional primitive on this origin give compare-and-swap? (7 — none) |
| `probe_pelican_cas2.py` | Controls for the above: are those negatives real, or an origin that acks everything? |

**DuckLake was eliminated on 2026-10-14** (reader independence: it needs a
live SAGE-hosted `meta.db`, while §32 showed Iceberg reads from a warehouse
URL with no database). The four `probe_ducklake*` scripts are kept as the
record of that evaluation — they still run, and several of their results
favour DuckLake — but they are closed, not open questions. The planned
DuckLake catalog-loss test was cancelled with the decision.

`pelican_token.py` mints SciTokens for the `/sage` namespace, mirroring
`waggle-auth-app/downloads/views.py`. `pelican_store.py` is the Pelican
`CatalogStore` port; `pelican_fileio.py` is the pyiceberg FileIO that
pyiceberg does not ship for `https://` — without it Iceberg cannot address
this storage at all, so exp19 supplies it rather than reporting a missing
import as a finding.

`icebergo/` holds the Go experiments (iceberg-go v0.7.0, Go 1.26.5). They
settle whether the nanosecond blocker in §13 is a property of Iceberg or of
pyiceberg — it is pyiceberg's, so the finding was corrected rather than kept.
`main.go` runs on local disk:

```sh
cd icebergo && go run .                  # built-in samples
go run . /path/to/ns_timestamps.json     # real archive timestamps
ICEBERGO_KEEP=1 go run .                 # keep the warehouse to inspect it
```

The JSON is a flat array of int64 nanosecond epochs; extract them with
`pandas.Timestamp(t).value` over a `time` column. The loader **rejects
microsecond-aligned values** — without a non-zero remainder the round-trip
check cannot detect truncation and would pass vacuously.
`testdata/real_ns.json` holds 500 such values from `date=2025-01-01`.

The same directory also carries the one Go experiment that *does* use the
network, behind a build tag so a plain `go test` stays offline:

```sh
go test -tags pelican -run TestPelicanV3Nanoseconds -v ./...
```

It writes under `/sage/cold-storage-test-icebergo/` — its own prefix,
distinct from the Python experiments' — and removes it recursively,
asserting PROPFIND 404 afterwards. `pelican_io.go` is the iceberg-go `IO`
for HTTPS (registered via `io.Register`, no fork) and `token.go` mints
SciTokens natively in Go.

`evolve_test.go` answers the schema-evolution and DOI questions (§22). It
is local-only — those are format questions, not storage questions — and is
behind its own tag so it neither needs credentials nor runs by default:

```sh
go test -tags evolve -v ./...
```

`reader_test.go` answers the NDP question (§23): what a read-only consumer
can do with no catalog. Also local-only, same reasoning:

```sh
go test -tags reader -v ./...
```

The interop half runs from the parent directory and needs fixtures written
by the Go side first, so that both the v3+nanosecond and v2+microsecond
cases are compared — without the v2 control a failure cannot distinguish a
nanosecond gap from a format-version gap:

```sh
cd icebergo && ICEBERGO_FIXTURES=/tmp/ndpfix go test -tags reader \
    -run TestInteropFixtures ./...
cd .. && python -I ndp_interop.py /tmp/ndpfix
```

`fscatalog_test.go` asks whether the database-free filesystem catalog
from §23 also works on Pelican (§24). It does not, so the answer needed
the real origin and it shares the `pelican` tag:

```sh
go test -tags pelican -run TestFilesystemCatalogOnPelican -v ./...
```

Its first three checks need no network — the refusal is in iceberg-go's
constructor — but the ones that matter probe `MOVE` with `Overwrite: F`
and `PUT` with `If-None-Match: *` against NRDStor, so credentials are
required. It writes under `/sage/cold-storage-test-fscat/`, its own
prefix, and removes it recursively.

`probe_pelican_cas.py` and `probe_pelican_cas2.py` (§31) are the follow-up,
and the pair is the point: the first sweeps the seven conditional
primitives `fscatalog_test.go` did not try, the second supplies controls
for its own negatives.

```sh
python -I probe_pelican_cas.py    # 7 primitives: If-Match, If:, LOCK, MKCOL, COPY...
python -I probe_pelican_cas2.py   # controls: does COPY work at all? does LOCK ever refuse?
```

`fscatalog_single_test.go` and `fscatalog_race_test.go` (§32) are the
follow-up that actually builds a table, which §24 and §31 never did:

```sh
go test -tags pelican -run TestFilesystemCatalogSingleWriter -v ./...
go test -tags pelican -run TestRenameNoReplaceIsNotAtomic -v ./...
```

The first implements the eight interfaces `hadoop.HadoopCatalogFS` demands
and gets **6/6** — the filesystem catalog runs on Pelican, and an
independent catalog loads the table from the warehouse URL with no database
anywhere. The second exists because the first was passing while carrying an
*unverified* caveat about concurrency: it drives the adapter's own
`RenameNoReplace` from four goroutines onto one destination and shows three
writers being told they won while two commits vanish. Read them as a pair —
alone, the first reads as "it works" and the second as a known limitation,
and the honest result is both at once.

`fscatalog_parallel_test.go` (§33) answers the question the pair provokes:
does "one writer" mean SAGE must ingest single-threaded?

```sh
go test -tags pelican -run TestParallelProducersSingleCommitter -v ./...
```

No. It separates *producing data files* (parallelizable — nothing about it
touches the catalog) from *committing* (one small metadata operation, the
only part the missing CAS affects), and measures the split: four producers
write Parquet to Pelican concurrently, one committer adds all four in a
single `AddFiles` commit, 1000/1000 rows survive and the pointer advances
once rather than four times. Same storage, adapter and worker count as the
race test — the difference is only where the parallelism sits.

Run all three §32/§33 tests in **one** invocation:

```sh
go test -tags pelican -run 'TestFilesystemCatalog|TestRenameNoReplace|TestParallelProducers' -v ./...
```

That is not a convenience. `iceio.Register` is process-global and
last-call-wins, so three tests each registering an `https` handler meant
only the last one worked, and the others failed **with no output at all**.
They now register once in `init()` and install their adapter per-test;
running them together is what catches a regression of that.

`exp34_hint_reconciler.py` runs the `plans/6` design end to end on the real
origin — the combination every earlier experiment had only tested in
pieces:

```sh
conda run -n sage_pipeline python -I exp34_hint_reconciler.py
```

Writers commit through an Iceberg **SQL catalog** (the database is the CAS
that arbitrates them); a **single reconciler** publishes
`metadata/version-hint.text` from the committed head; a reader with no
database follows the hint. **12/12**: two concurrent committers both land
(one retries on the CAS) where §32 lost 2 of 4; the reader sees every row
with no SAGE endpoint; a stale hint serves an older *valid* snapshot
silently and one reconciler pass repairs it.

The stale-hint case is the one to read. §32's failure mode is silent
**unrecoverable loss**; this design's is silent **recoverable staleness**,
because the database never stops being the head. That asymmetry is why the
design exists, and it is why the reconciler must compare published hint
against `metadata_location` actively — clients never notice a newer
metadata file, so nothing surfaces staleness on its own.

`exp35_sqlite_vs_postgres.py` corrects a recommendation made without
evidence. `exp34` Q1 arbitrated two committers safely on SQLite and the
writeup turned that into "re-run against Postgres, highest-value open
item" — reasoning from reputation, not from a result:

```sh
conda run -n sage_pipeline python -I exp35_sqlite_vs_postgres.py
```

No network and no credentials; this is a question about the catalog
database, so the warehouse is a tmpdir. It re-runs the contention test with
separate **processes** rather than threads: 6 → 60/60 rows, 12 → 120/120,
24 → **240/240**, at most 2 retries throughout. **SQLite is sufficient at
this scale.** The real limit is that
`sqlite://db-host:5432/catalog.db` is rejected — a SQLite URI is a *file
path*, so committers must share a filesystem. Postgres is needed when they
run on separate hosts, which is a deployment question rather than a
reliability one.

`exp36_scale.py` takes the plans/6 design from 100 rows (`exp34`) to the
real archive: 4,564 files, 608 day-partitions, 209 GB.

```sh
conda run -n sage_pipeline python -I exp36_scale.py
```

Local only — tmpdir warehouse, the archive read-only, **nothing uploaded**.
The scale question here is the metadata and commit path; throughput to
Pelican at this file count is still unmeasured. The archive is never
rewritten: Q3 measures per-MB cost on six real files and extrapolates.

Three findings change decisions already written down. **`add_files` cannot
register the archive at all** — it is `timestamp[ns]`, pyiceberg 0.12
refuses the footer, and neither the downcast property (which only governs
`append`) nor format-version 3 (not writable yet) helps; ingest must rewrite
to microseconds, ~26 MB/s, ~2 CPU-hours once. **Snapshot expiry must be on**
even though `delete-after-commit` stays off — they are different mechanisms,
only the former bounds growth, and it does not break a published hint.
**Commit cost rises with accumulated file count**, ~1.3 s per commit at
4,564 files, which floors the reconciler period. Partition pruning and §25's
microsecond decision both hold up.

Q4 needs the `pyiceberg-core` extra for partitioned writes; it **skips
cleanly** without it, so the run is 8/8 in `sage_pipeline` and 9/9 in an
environment that has it.

`exp37_pelican_scale.py` answers what `exp36` left open — the same commit
path, but over the real NRDStor origin, where the unit is **requests at
~190 ms** rather than milliseconds of CPU:

```sh
conda run -n sage_pipeline python -I exp37_pelican_scale.py
conda run -n sage_pipeline python -I exp37_pelican_scale.py --commits=40
```

It writes under a nanosecond-stamped `/sage/cold-storage-test-scale-<ns>/`,
aborts unless `PROPFIND` → 404 first, and removes it after (`--keep` to
inspect). The archive is **not** uploaded; a realistic file *count* is
registered with tiny payloads, which is the right shape because request
volume tracks count, not bytes.

**The write path is flat** — exactly 9 requests per commit whether the
table holds 1 file or 40, so the bill is the number of *commits*: ~10 min
for 608 day-batched commits versus ~1.2 h for 4,564 single-file ones. The
reconciler is O(1) (2 requests to publish, 1 to confirm).

**The read path is the expensive side** and does not flatten: ~5.1 requests
per data file. Iceberg's metadata layer is innocent (4 requests total), but
**a third of the cost is a defect in `pelican_fileio.py`** — it HEADs every
data file for a size the manifest already carries. Q6c measures the fix:
**205 → 122 requests, 40% fewer**. Q7 confirms partition pruning survives
the network (302 → 37 requests, 88% fewer), which is what makes consumers
viable; Q7 skips without `pyiceberg-core`, giving 11/11 in `sage_pipeline`
and 12/12 with it.

`exp38_query_throughput.py` closes the gap `exp37` left: `exp37` measured
query *planning* in requests, but every read it timed moved a few hundred
bytes, so **execution throughput was never measured**:

```sh
conda run -n sage_pipeline python -I exp38_query_throughput.py
conda run -n sage_pipeline python -I exp38_query_throughput.py --files=6
```

Unlike the others this one **does upload real archive parquet** — throughput
cannot be faked with tiny payloads. It takes the smallest file from each of
the first N days (default 4, ~169 MB), casts ns→us the way ingest must
(exp36 Q1), and removes the prefix after; the archive itself is read-only.

**A sequential scan runs at ~5 MB/s and a concurrent one at 30–39 MB/s** —
6.5–8.7× for a configuration change, since pyiceberg already maps scan tasks
onto a thread pool (`PYICEBERG_MAX_WORKERS=8..16`). For a one-week query
that is ~9 min versus ~1 min. Q2 is the counter-intuitive one: selecting 1
of 13 columns moved **94% fewer bytes but took only 43% less time** and
issued *more* requests — projected scans are latency-bound, so size queries
by round trips, not bytes. Q5 found a reprieve: archive files are 1 row
group of 10M rows, but the rewrite ingest must do anyway emitted 2 row
groups per file unasked, making the exporter's `MaxRowsPerRowGroup` change
additive rather than a prerequisite.

`exp39_iceberg_vs_custom.py` answers the obvious follow-up: is Iceberg
*worth* that, compared with dropping it and keeping plain Parquet plus
`catalog.py`?

```sh
conda run -n sage_pipeline python -I exp39_iceberg_vs_custom.py
```

Same files, same origin, same FileIO, same process, interleaved, with
`catalog.py`'s `CatalogStore` ported to Pelican the way its docstring
anticipates. **Latency does not separate the two designs**: write 0.95×,
read-all 0.6×, one-day read 0.8×, all within or near run-to-run noise. The
ns→us rewrite costs no wall time because both paths are network-bound and
the rewrite sends 17% fewer bytes. Iceberg's 12 metadata requests vs 2 is
fixed, not per-file (0.50/file at 24 files, falling). The one real
divergence is Q5: the custom catalog's snapshot JSON embeds every live file
(~1.3 MB at 4,564 files, fetched in full on every read, unprunable), where
Iceberg's equivalent is split across manifests a pruned reader can skip.

Both this and `exp38` take `--files=N` and `--keep`.

`exp40_metadata_storage.py` prices the metadata layer in bytes rather than
in requests — "what does Iceberg cost us in storage?"

```sh
conda run -n sage_pipeline python -I exp40_metadata_storage.py
```

Local only: a tmpdir warehouse and the archive read-only, since metadata
bytes are the same wherever the table lives. The sample files hold real
archive rows, because manifest entries store per-column bounds and the
archive's string columns set their width. **Per-file overhead is
negligible** — ~545 B of manifest per data file, 2.5 MB for the whole
4,564-file archive, 0.0012% of 209 GB. **The net answer is a saving**: the
ns→us rewrite ingest must do anyway stores 83% of on-disk size, so the
archive comes out ~35 GB smaller. What can run away is retention, not file
count — every `metadata.json` embeds the whole snapshot history, so
retaining all of them is quadratic (163 MB at day-batched commits, 34 GB/yr
at hourly). Q4 gives day-batching a third justification: 32× less metadata
for identical data. Q6 re-measures the custom catalog's snapshot at 167
B/file, superseding the 274 B figure in §39.

`exp41_metadata_retention.py` follows up on the one thing `exp40` left
open: whether bounding old `metadata.json` versions is something Sage has
to build.

```sh
conda run -n sage_pipeline python -I exp41_metadata_retention.py
```

Local only, no archive and no network. **It is built in** — two stock table
properties, 41 files/716 KB → 6 files/155 KB, no custom tooling. The two
findings that matter for `plans/6`: the window is **count-based, not
age-based** (20 commits in one second left 6 files; pyiceberg 0.12 has no
age option for metadata files), and it **can delete a file a published hint
still names** — with `previous-versions-max=5` the hint broke after commit
#7 and the consumer got `FileNotFoundError`, which is a hard failure rather
than the stale read the design accepts. The break point is exactly
`previous-versions-max + 2` at every window tested, so the window can be
sized by arithmetic: it must exceed the commits that land in one reconciler
period, plus margin.

Each run uses a nanosecond-stamped prefix
(`/sage/cold-storage-test-hint-<ns>`), checks `PROPFIND` → 404 **before**
writing, and removes it afterwards. Two deployment facts it found, both of
which fail misleadingly: every committing service must open the catalog
under the **same** `catalog_name` (it is part of the primary key, and a
mismatch presents as `NoSuchTableError` on a table that exists), and
pyiceberg lays tables out as `<warehouse>/<ns>/<table>` with no Hive `.db`
suffix.

Three origin behaviours cost a debugging cycle each and are worth knowing
before touching this code: a **collection answers `HEAD` with 200** (so the
status code cannot tell a namespace from a file), the origin **does not
slash-terminate the `PROPFIND` self href** (so the usual trailing-slash test
misclassifies every directory — use `<D:collection/>` or `iscollection`),
and **`MKCOL` is required**, because the catalog creates namespaces as real
directories and then checks `Stat().IsDir()`.

Round 2 exists because two round-1 results were misleading in a way worth
guarding against generally. `COPY Overwrite: F` returned 201 and `LOCK`
returned 200, both of which *look* like "the conditional was ignored" — but
`COPY` to a free destination also returns 201 and copies nothing, and the
nonsense method `FLOOBLE` returns 200 too. **A 2xx from this origin means
the request was not parsed, not that it was evaluated and allowed**, so a
sweep without controls would have reported the right verdict for the wrong
reason. Both scripts read as FAIL-heavy on purpose: a "FAIL" here means the
primitive is absent, which is the information being collected, and the
`verdict` line is what to read.

`exp17`–`exp19`, `exp34`, `exp37`, `icebergo/pelican_test.go`,
`icebergo/fscatalog*_test.go`, `probe_ducklake_pelican.py` and the two
`probe_pelican_cas*` scripts are the only
experiments that touch the network beyond
localhost. They write under `/sage/cold-storage-test/`,
`/sage/cold-storage-test-iceberg/`, `/sage/cold-storage-test-icebergo/`,
`/sage/cold-storage-test-fscat/`, `/sage/cold-storage-test-fscat1/`,
`/sage/cold-storage-test-fscat2/`,
`/sage/cold-storage-test-ducklake/`, `/sage/cold-storage-test-cas*/`,
`/sage/cold-storage-test-hint-<ns>/`,
`/sage/cold-storage-test-scale-<ns>/`
`/sage/cold-storage-test-query-<ns>/`
and `/sage/cold-storage-test-vs-<ns>/`
respectively and delete everything
afterwards. They never touch `/sage/node-data` or the paths
`sage-storage-loader` writes.

The two `probe_us_collision*` scripts back the 2026-10-14 decision to store
microseconds (§25). They ask whether truncation merges *distinct* readings
rather than merely dropping precision, which is the part that would break
de-duplication and correction semantics:

```sh
python -I probe_us_collision_sql.py ../data/archive --n 8
python -I probe_us_collision_sql.py ../data/archive --all   # whole span
```

Both carry the same guard, and it is the point of having two: **DuckDB
cannot be pointed at the Parquet directly for this question.** Its
`TIMESTAMP` is microsecond precision, so `time` is truncated at read and
`epoch_ns(time) % 1000` is always 0 — `CAST(time AS BIGINT)` raises, and
`epoch_ns(time::TIMESTAMP_NS)` is no better. The SQL version therefore has
pyarrow read `time` as int64 and hands DuckDB the Arrow table; it aborts
rather than counting if no non-zero remainder is visible. The pyarrow-only
version is the slow, independent check on the same question.

`portable_test.go` answers the storage-abstraction question (§27): which
backends iceberg-go already ships, and whether a table written on one
storage system survives being copied to another. Local-only and tagged,
like the other format experiments:

```sh
go test -tags portable -run TestPortable -v ./...
```

It is local-only for a specific reason: the move test **deletes the source
directory** before reading the copy, because otherwise the reader follows
the absolute paths back to the original and the test passes vacuously. That
is not something to do to a real origin. Note this test imports the S3, GCS
and Azure backends for their registration side effects, which is what took
`go.mod` from 78 to 133 dependencies — the production exporter should import
only the backend it actually uses.

`recover_test.go` answers the disaster-recovery question (§28): what
happens when the catalog database is lost, given that §24 makes it
mandatory on Pelican. Local-only and tagged:

```sh
go test -tags recover -run TestRecover -v ./...
```

It deletes the catalog outright rather than corrupting it, and checks the
recovered table accepts new commits rather than only reading — a read-only
"recovery" would pass a weaker test while leaving the pipeline down.

The two `probe_ducklake*` scripts answer "DuckLake instead of Iceberg?"
(§29). They need no credentials and no build tag — DuckDB installs the
extension itself on first run:

```sh
python -I probe_ducklake.py
python -I probe_ducklake_http.py
```

They judge DuckLake against the measurements that already decided things
here rather than against a feature list: nanoseconds (§13/§20/§25), what
the catalog costs (§24/§28), relocation (§27 Q2) and catalog-free
publishing (§23). The move test deletes the source directory first, for
the same anti-vacuity reason `portable_test.go` does.

One methodology note worth keeping, because it produced a wrong answer
first: build nanosecond literals with `make_timestamp_ns(<int64>)`, **not**
by adding an `INTERVAL` to a `TIMESTAMP_NS`. Interval arithmetic goes
through microseconds, so the first version of the probe destroyed the
precision itself and reported remainder 0 — which would have been recorded
as a DuckLake limitation.

`probe_ducklake_concurrency.py` (§30) isolates the one question
`probe_ducklake_pelican.py` could not answer. It needs no credentials and
keeps the data on local disk deliberately — the contended resource is the
catalog database, and Pelican never participates in a DuckLake commit, so
putting data on it would add ~150 ms/request of noise to a lock
measurement.

```sh
python -I probe_ducklake_concurrency.py
DUCKLAKE_PG='host=... dbname=... user=...' python -I probe_ducklake_concurrency.py
```

It runs the SQLite case twice, the second time through a filesystem that
sleeps 150 ms per operation (`_SlowFS`) to imitate Pelican's latency (§17).
That row exists because the Pelican probe behaves differently from the
local one and latency was the obvious explanation — it is **not** the
explanation, the slow case passes, and the difference is recorded as
unexplained rather than guessed at. Without the Postgres leg this does not
settle the production question: DuckLake's catalog docs scope SQLite to
"multiple local clients" and recommend Postgres for remote multi-user
writers.

It separates three outcomes that a naive pass/fail collapses into one: the
second writer cannot attach (a backend constraint), a commit is refused
loudly (correct), and rows acknowledged but missing (the §18 silent lost
update — the only real failure). **`database is locked` is not by itself a
transport error**: it is also how DuckLake correctly rejects the losing
writer. The Pelican probe's Q7 originally retried it 25 times and then
reported FAIL, turning right behaviour into a fake failure; its INSERTs are
now deliberately not retried.

§26 has no script. It is a single DuckDB query against one day, recorded as
an observation while answering whether Iceberg could also structure
`/sage/node-data` — 43,353 `name='upload'` rows, 1:1 with distinct blob
URLs. Reproduce it with:

```sh
conda run -n sage_pipeline python -I -c "import duckdb; print(duckdb.connect().execute(\"select count(*), count(distinct value_str) from read_parquet('../data/archive/date=2025-01-01/data_0.parquet') where name='upload'\").fetchone())"
```

It is labelled an observation rather than an experiment because it has no
control and covers one day. The question it raises — whether *every* object
in `/node-data` has such a row — needs a listing of that prefix and is still
open.

`cold_backend.py` is the library the query experiments use: it compiles a
Beehive JSON query into DuckDB SQL and emits the API's record shape. Its
docstring lists the `influx_backend.go` semantics it replicates and why each
one is easy to get wrong.

## Reproducing the headline numbers

```sh
# the pruning failure, and the fix
python -I exp03_range_reads.py <some data_0.parquet>
python -I exp04_rowgroup_sizing.py <same file> ../scratch/rgvariants
python -I exp05_rowgroup_pruning.py ../scratch/rgvariants <same file>

# query semantics
python -I exp08_query_api.py ../data/archive

# the milestone
python -I exp10_compaction.py ../data/archive ../scratch/compaction --days 3
```

A Go A/B harness for the exporter change (`scratch/pqtest/`, gitignored) was
run on sage03 against real archive data; results in `results/SUMMARY.md` §11.
