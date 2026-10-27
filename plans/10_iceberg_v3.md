# Iceberg v3

**Architecture adopted 2026-10-26:** day-partitioned Iceberg v3, chronological
file batches capped at 10,000,000 rows, sorted plugin → vsn → time within
each file, with plugin-pure row groups capped at 262,144 rows. Use
`/cold-storage-demo-v3-bounded-full-1793018629285065000` as the selected
March/April warehouse for subsequent API work. The final decision and measured
tradeoffs are recorded at the end of this plan. Earlier references to opt-in
trials, pending promotion and time-first defaults describe historical stages
and are superseded by this decision.

DuckDB 2.0 reads and writes Iceberg format-version 3. The published demo
warehouse can leave v2. v2 was the ceiling of the old reader and of PyIceberg
0.12.0, which still refuses to serialize v3 metadata
(`apache/iceberg-python#1551`, open as of 2026-07). DuckDB is the writer that
can publish the types we actually want. iceberg-go 0.7.0 can already write v3
`timestamptz_ns`; this pass uses DuckDB for both the write and the
catalog-free read.

v3 is what removes the microsecond timestamp cast and what makes a single
stored `value VARIANT` legal. It also brings binary deletion vectors and row
lineage. Those last two are available, not part of the first schema change.
`GEOGRAPHY` and `UNKNOWN` are not in DuckDB-Iceberg yet.

## Schema direction

Keep the public Arrow and NDJSON schemas. Hot, cold, and tier-spanning
responses still expose `timestamp`, `name`, `value`, and `meta` as they do
now. Arrow `value` and `meta` stay JSON text; NDJSON keeps its native JSON
value and metadata object. Callers do not receive an Arrow VARIANT column.

Stored cold schema:

- `time TIMESTAMPTZ_NS`. The archive's nanosecond remainder is the stored
  value. The v2 table's `timestamptz` cast, which dropped that remainder, goes
  away for data written to v3.
- `value VARIANT`, one column. Float, integer, and string stay distinct,
  including integers above 2^53. This is the exp68 decision: the local cost
  was about 9–17% and is accepted for the simpler type model.
- `meta MAP(VARCHAR, VARCHAR)` and the promoted tag columns stay. The exp67
  JSON fast paths stay. A metadata VARIANT is a separate decision.

Bumping `format-version` on the existing table does not merge
`value_float` / `value_int` / `value_str` and does not restore nanoseconds
that were truncated on the way in. Existing files need a backfill or a
transitional reader. The existing live demo warehouse is left untouched. The updated API is v3-only
and must be retargeted to a newly seeded v3 warehouse before use.

## Reader proof

`TestIcebergV3RoundTrip` in `beehive-data-api/cold_iceberg_v3_test.go` is
opt-in. It attaches a local Iceberg REST catalog, creates `sage.obs` with
`format-version` 3, inserts four rows, and reads them back through
`iceberg_scan` of the metadata JSON on a second connection that has no
catalog. That scan is the API's reader. The linked engine is DuckDB 2.0.0.

The warehouse has to be a directory the catalog container and the host both
see at the same absolute path. Rancher's `/tmp` is not that directory. The
run used `/Users/franciscolozano/tmp/sage-iceberg-v3` and
`http://127.0.0.1:18181`. Nothing was written to Pelican or `/node-data`.

Measured on 2026-10-22, one pass, test PASS in 0.42 s:

| Column | Iceberg type |
|---|---|
| time | `timestamptz_ns` |
| name | `string` |
| value | `variant` |
| meta | `map<string, string>` |

`format-version` is 3. `iceberg_scan` returned, in order:

| epoch ns | name | `to_json(value)` |
|---|---|---|
| 1740787207420753193 | wxt.env.temp | `21.5` |
| 1740787207420753571 | wxt.env.temp | `9007199254740993` |
| 1740787208000000001 | plugin.status | `"123"` |
| 1740787208000000002 | plugin.flag | `true` |

Every timestamp keeps a nonzero nanosecond remainder. The integer above 2^53
is exact, the numeric-looking string stays a string, and the boolean stays
`true`. Metadata came back as JSON. The REST fixture did not write
`version-hint.text`. The current API still resolves the table through that
hint, so a v3 publisher has to emit it the way the reconciler does today.
Pointing `iceberg_scan` at the metadata file is enough for this proof and is
not how the demo process finds the table.

At the time of the original proof, the demo API was not retargeted and still
projected split value columns. The implementation below now removes that v2
reader; do not rebuild the running demo against the old warehouse URL.

```sh
docker run -d --name sage-iceberg-v3 --user root -p 18181:8181 \
  -v /Users/franciscolozano/tmp/sage-iceberg-v3:/Users/franciscolozano/tmp/sage-iceberg-v3 \
  -e CATALOG_WAREHOUSE=/Users/franciscolozano/tmp/sage-iceberg-v3 \
  apache/iceberg-rest-fixture:latest
(cd beehive-data-api && \
  COLD_V3_ENDPOINT=http://127.0.0.1:18181 \
  COLD_V3_ROOT=/Users/franciscolozano/tmp/sage-iceberg-v3 \
  go test -tags=duckdb_arrow -count=1 -run '^TestIcebergV3RoundTrip$' -v)
```

Without those two variables the test skips. `go test` otherwise does not
need the catalog.

## Implemented v3-only path

User decision: no v2 compatibility is required because these upgrades have not
been released. The cold API now reads `time TIMESTAMPTZ_NS`, one `value VARIANT`,
MAP metadata and promoted string tags. Both Arrow and NDJSON return the existing
public fields. NDJSON carries the serialized value as `json.RawMessage`, avoiding
float64 round-trips for integers above 2^53. Query bounds are parsed directly as
TIMESTAMPTZ_NS, fixing the prior loss of sub-microsecond bound digits. Head/tail
still group by the complete merged metadata; tier routing stays half-open at
the exact nanosecond watermark. The exp67 metadata fast paths remain.

`seed_cold.py` now requires DuckDB Python 2.0.x and a local REST catalog sharing
an isolated warehouse directory. It reads the original archive's split values
and converts each CASE branch to VARIANT **at ingestion**, preserves stored
nanoseconds, clips each archive day to [day, next day), and commits per day to a
day-partitioned v3 table. There is no v2 seeder or silent resume. The initial implementation required a
new catalog after failure; explicit empty-interval appending and recorded-head
registration recovery are now available below. These refuse replay into an
occupied interval. It never evicts hot data.

### Publication to Pelican

DuckDB httpfs supports plain HTTPS reads, not writes. A REST writer round-trip
on a local filesystem does not establish Pelican write support. Therefore:

1. DuckDB writes into its local REST-catalog warehouse.
2. Resolve the committed metadata location from REST **LoadTable**, never a
   filesystem filename sort or traversal. The Go round-trip test uses
   `iceberg_load_table_response` for the same authoritative discovery.
3. `v3_publish.py` copies the reachable snapshot graph to a fresh staging
   warehouse. Parquet bytes are unchanged; Avro manifest paths and manifest-list
   lengths are rewritten, original Avro schemas/headers retained, and metadata
   JSON references rebased. All reachable historical snapshots/metadata logs
   are retained. References outside the source table are rejected.
4. Optionally upload this replica under a fresh `/cold-storage-demo-v3-<unique>`
   prefix using the existing Pelican transport. PUTs stream file bodies with
   Content-Length. Existing prefixes are refused. **version-hint.text is the
   final PUT**, contains the committed metadata stem and no newline.

The published warehouse is a **read-only replica** of a committed snapshot.
Its rebased metadata is not a new REST catalog commit. Future ingest commits
continue in the source catalog and require another fresh replica/publication;
this is not a continuous reconciler or an incremental migration/eviction service.
The old v2 SQLite reconciler has been removed from the demo Compose topology
so it cannot publish hints from the wrong catalog. Its historical source and
experiments remain as evidence, not a v2 runtime fallback.

This exporter is intentionally append-only. Delete files, non-append snapshots
and statistics files are rejected pending explicit replication support. Local
seeding plus replica staging needs space for both copies; queries themselves do
not load a day/month into Python memory. Keep the source warehouse until the
replica is verified. No existing March/April objects are rewritten automatically.

### Reproduce in an isolated workspace folder

From the workspace root (choose a new root and Compose project for each seed):

```sh
python -m pip install -r sage-data-rework/tiered_system/demo/tools/requirements-v3.txt
export COLD_V3_ROOT="$PWD/sage-data-rework/scratch/v3-$(date +%s)"
mkdir -p "$COLD_V3_ROOT"
docker compose -p sage-v3 -f sage-data-rework/tiered_system/demo/docker-compose.v3.yml up -d
(cd beehive-data-api && \
  COLD_V3_ENDPOINT=http://127.0.0.1:18181 COLD_V3_ROOT="$COLD_V3_ROOT" \
  go test -tags=duckdb_arrow -count=1 -run '^TestIcebergV3RoundTrip$' -v)
python -I sage-data-rework/tiered_system/demo/tools/seed_cold.py \
  --endpoint http://127.0.0.1:18181 --root "$COLD_V3_ROOT" \
  --start 2025-03-01 --end 2025-03-01 \
  --export "${COLD_V3_ROOT}-replica"
```

The catalog fixture uses `latest` for the same proof setup as before; record
its image digest when comparing runs. The root must have the same absolute
path inside/outside the container. Pin the desired fixture digest for a
reproducible deployment. The REST catalog here is a local unauthenticated
fixture, not a production catalog deployment.

For Pelican publication, add a **new** prefix to the seeder invocation (the
export path must also be fresh). It emits COLD_WAREHOUSE only after all uploads
and the final hint succeed:

```sh
# Add to the seed_cold.py command above:
# --pelican-prefix /cold-storage-demo-v3-<unique>
```

After validating the new warehouse, configure COLD_WAREHOUSE to that emitted
URL and set COLD_WATERMARK to the first hot instant before rebuilding the API.
Do not reuse the v2 URL with this reader. Run the live exp64 queries after the
switch; compare value/metadata semantics and nanosecond timestamps against the
source archive, rather than expecting truncated v2 timestamps to match.

### Validation status

- Selected Go SQL/settings/service/tiered/public-schema/profile tests and raw
  VARIANT JSON tests pass with the race detector using the cached temporary
  Arrow 18.8.0 modfile. This is not a fresh full-package native build.
- Five offline publisher tests pass: committed-head discovery, reference/size
  rewriting, no orphan selection, no hint on failure, hint-last upload ordering,
  unsafe/existing prefix rejection, source clipping and exact value conversion SQL.
- `exp69_v3_validation.py` executes the actual ingestion SELECT on 1,024
  archive-shaped rows and current Go-generated response SQL on stored VARIANT
  Parquet through native DuckDB 2.0. All six cases pass: ordinary reads, head,
  tail, exact lower/upper nanosecond bounds and empty results. Artifact:
  `tiered_system/experiments/results/exp69_v3_validation.json`.
- A real four-row v3 snapshot from the earlier REST proof was copied into a new
  local `/private/tmp/sage-v3-export-*` warehouse. Native DuckDB 2.0 successfully
  reads the relocated metadata/Avro/Parquet graph, retaining the timestamps,
  large integer, numeric string and boolean. Source files were read-only. Exact value strings and timestamp integers match;
  artifact: `tiered_system/experiments/results/exp69_v3_snapshot_copy.json`.
- `TestIcebergV3RoundTrip` now creates a unique namespace per run, gets its
  committed head from the catalog and includes HTTP Arrow/NDJSON cases for
  ordinary reads, head/tail, an exclusive nanosecond bound and tier-spanning
  reads. It uses the production SQL, NDJSON scanner, native Arrow reader and
  HTTP writers, replacing remote authentication/discovery only. The subsequent
  VM acceptance run below passed the expanded test and full native test suite.
- During implementation, Docker access was denied and the pinned Go/native modules were
  not available in the writable cache. No container was started/rebuilt here.
  No Pelican/NRDStor writes, `/node-data` access, commits or pushes were made.
  This describes the implementation session; the subsequent isolated Pelican
  publication is recorded below.

The fresh acceptance run is recorded below. The exp68 percentages remain local
benchmarks, not a prediction for a month query.

#### Measured

Catalog image `apache/iceberg-rest-fixture@sha256:db8de90b5b7693d4ac334c336f91d9bbe320d7b19f4f514d26de84cdfbcbfe8d`.
`TestIcebergV3RoundTrip` passed in 0.51 s, including the HTTP Arrow and
NDJSON cases. `go test -tags=duckdb_arrow ./...` passed in 1.269 s. The five
publisher unit tests passed. exp69's six native SQL cases passed again.

March 1 was seeded from the archive into
`sage-data-rework/scratch/v3-1792707566` in 150.71 s and copied to a local
replica. The committed table is format-version 3, 140,578,674 rows, 697 MB
in the replica. A `wxt.env.temp` / `W0A0` sample is epoch ns
`1740787200026403194` (remainder 194) and VARIANT `11.7`. The hint stem is
`00001-7e585226-f1ac-45a6-8613-b76419d50fa4` with no newline.

That replica was uploaded under `/cold-storage-demo-v3-1792707566`. The demo
API was rebuilt against

`https://nrdstor.nationalresearchplatform.org:8443/sage/cold-storage-demo-v3-1792707566/warehouse`

with watermark `2025-05-01T00:00:00Z`, async=8, automatic read-ahead, and
profiling off. Startup logged DuckDB v2.0.0. One cold minute of
`wxt.env.temp` / `W0A0` returned 698 rows through the published hint. 695 of
those timestamps have a nonzero nanosecond remainder, and the first row is
the same `1740787200026403194` / `11.7` pair. This warehouse is March 1
only, so it does not replace a full-month measurement. The previous v2
objects were not rewritten. The local catalog container was stopped; the
scratch warehouse and replica remain.

This establishes the v3 writer → replica → Pelican hint → API path, including
preserved nanosecond timestamps. It closes the earlier native-build and
REST/HTTP acceptance gaps. The 150.71 s figure is the reported seed duration,
not a query latency measurement or an established complete seed-plus-upload
duration.

The live March 1 baseline is one series, `wxt.env.temp` / `W0A0`, with the
API still on that warehouse, async=8, automatic read-ahead, and profiling
off. Each window is `[start, end)`. Minute and hour had one discarded warmup
per format. The day had none. Medians are of three repetitions. The first
response of each case was compared in full with the March 1 archive through
the current response SQL, after converting the archive's split value fields
the same way the seeder does. Arrow timestamps, names, value JSON, and
metadata strings match exactly as a row multiset, and timestamps are
nondecreasing. This does not check relative order between equal timestamps.
NDJSON matches after canonical
JSON (sorted object keys). Its exact string mismatches are two per row
because Go's JSON encoder sorts metadata keys and DuckDB `to_json` does not;
the values, metadata, and nanosecond timestamps still match.

| window | rows | Arrow total / first batch | NDJSON total / first buffered read |
| --- | ---: | ---: | ---: |
| minute | 698 | 0.215 s / 0.215 s | 0.212 s / 0.212 s |
| hour | 43,509 | 0.619 s / 0.608 s | 0.847 s / 0.631 s |
| day | 1,048,491 | 10.296 s / 10.067 s | 16.128 s / 9.724 s |

The NDJSON fetch loop uses `response.read(1024 * 1024)` and records a first
line only after that read returns. Its first-result measurement is therefore
buffered (or reaches EOF for a small response), not the actual first line's
arrival. Retain these historical values but fix that measurement before
comparing first-result latency between formats. Total times remain the
measured complete-response elapsed times; NDJSON parsing/validation occurs
after that timer, so this is not a Sage client dataframe benchmark.

The day Arrow repetitions were 15.245 s, 10.296 s, and 10.130 s. NDJSON was
21.472 s, 16.128 s, and 15.582 s. Artifact:
`tiered_system/experiments/results/exp70_v3_live_baseline.json`.

The same day Arrow query was then profiled for three repetitions. The first
profiled response still matched the archive exactly (1,048,491 rows). Client
median was 10.146 s total and 9.953 s to the first batch; the cold repetition
was 22.884 s. On the two warm profiles the backend lifetime was 10.031 s and
10.144 s, DuckDB latency about 9.9–10.0 s, and CPU about 19.1–19.4 s. Operator
work, which is not wall time, was about 10.2–10.3 s in the scan, 8.7–8.8 s in
the large projection, and 0.14 s in the sort. The scan visited 531 row groups
and returned the 1,048,491 filtered rows. The cold profile read 695,811,436
bytes, about the whole replica; the warm byte counter was 0 after that read.
Peak buffer was about 1.03 GiB and the temp directory stayed empty. The first
batch arrives only once the ordered scan finishes, so it accounts for nearly
the whole Arrow response. At this size the scan and projection dominate the
sort. Summary: `tiered_system/experiments/results/exp70_v3_day_profiles.json`.
Profiling was turned back off. The API remains on the v3 warehouse.

#### Review of the live baseline (2026-10-23)

The artifacts support retaining Arrow/Zstd: hour total time is 27.0% lower
than NDJSON and day total time is 36.2% lower. The day response is 19,171,032
bytes versus 362,323,649 bytes (18.9x smaller). Minute elapsed time is roughly
equal because response handling saves little against the fixed server work.
The first measured response per case/format was checked against the archive;
subsequent repetitions were timed without repeating the full comparison.

This changes the next optimization priority for the day workload. The two
warm profiles show about 10.2–10.3 s of scan work, 8.7–8.9 s of combined
projection work, 0.14 s of sort work, no spill and about 1.03 GiB peak buffer
memory. Native Arrow conversion is only 0.023–0.028 s. These work figures
overlap across workers; do not add them to the roughly 10 s elapsed time.
The explicit sort still blocks first output, but speeding the sort itself
has little measured upside here. Chronological chunking remains a potential
first-result optimization, not the demonstrated throughput fix for this case.

Read-only inspection of the seeded local Parquet footers confirms two files,
140,578,674 rows and 531 row groups. **All 531 groups have name/vsn min/max
ranges that include `wxt.env.temp` / `W0A0`**; those statistics cannot exclude
any group for this series. The profile indeed scans all 531. Its reported
rows-scanned count of 1,048,491 is the filtered result in this path, not proof
that only those rows' physical storage was touched. The cold engine read
counter is 695,811,436 bytes versus 731,299,180 bytes of Parquet files (about
95%). A zero warm engine byte counter does not establish zero HTTP traffic or
zero decoding/scan work; the warm scan still consumes substantial work.

The next experiment should separate **layout/scan cost** from **response
projection cost** before another server execution rewrite:

- Compare the current time-first layout with a series-oriented candidate such
  as `(name, vsn, time)` in an isolated dataset, preserving VARIANT, nanosecond
  values, metadata, codec and comparable row-group sizes. Measure selected
  groups/bytes, cold and warm latency, and full output equality.
- Include one-name and all-name minute/hour/day queries. Improving series
  pruning can hurt broad time-window reads; choose a layout only after those
  tradeoffs are measured. Do not add one partition per full series by default.
- Separately profile value-only and metadata-only projection ablations on the
  same current files. These diagnostic payloads differ, so their total-time
  differences are not pure serialization CPU measurements.

Neither the 10 s day time nor the earlier v2 month figures establish the
end-to-end cost of VARIANT: the warehouse scale, timestamp precision and file
layout differ. Keep the v3 schema decision and use this measured baseline for
the next controlled comparison. No storage or API execution change was made
as part of this review.

These numbers are the one-series March 1 baseline. They are not a full-fleet
day and not a March/April month. Extend the v3 seed to multiple days before
evaluating chronological chunking, and to all March/April before treating the
May 1 watermark as a complete cold-history boundary. Until then, requests for
other cold days do not test data coverage in this warehouse.


Local native SQL validation can also be rerun with the freshly built test binary:

```sh
(cd beehive-data-api && go test -tags=duckdb_arrow -c -o /tmp/sage-v3.test)
python -I sage-data-rework/tiered_system/experiments/exp69_v3_validation.py \
  --native-probe /tmp/sage-v3.test --sql-probe /tmp/sage-v3.test
python -m unittest discover -s sage-data-rework/tiered_system/demo/tools -p test_v3_publisher.py -v
```

Historical exp63/66/67/68 harnesses describe the former split-column layout;
their saved results are preserved. Use exp69 for the current v3 response SQL.
NDJSON requires valid JSON values; nonfinite numeric variants are rejected by
the scanner rather than silently coercing them to null. Arrow's existing JSON
text behavior for such values is unchanged.


### Historical v2 cost audit before the plugin layout experiment (exp71)
> The full v3 migration baseline is now available. The plugin comparison uses
> an isolated March 1 copy; it does not rewrite the full live warehouse.

The requested comparison is between the previous deployed-demo layout
(Iceberg v2, split values, stored microseconds) and the current v3 VARIANT
layout. Reuse the historical measurements; do not restore a v2 reader into the
API or rewrite the old warehouse. The most recent control is exp67_after_live,
which already used DuckDB 2.0, exp67 metadata fast paths, Zstd, async=8 and the
public four-column response. Earlier DuckDB 1.5 controls introduce another
engine change and should not be the headline comparator.

| Same series/window | Historical v2 Sage-client total | Current v3 response collection total | v2 / v3 first Arrow batch |
|---|---:|---:|---:|
| March 1 minute, 698 rows | 0.315 s | 0.215 s | 0.309 / 0.215 s |
| March 1 hour, 43,509 rows | 0.312 s | 0.619 s | 0.299 / 0.608 s |
| March 1 day, 1,048,491 rows | no matching live historical result | 10.296 s | unavailable / 10.067 s |

**Do not interpret total-time ratios in this table as VARIANT overhead.**
Exp67's timer runs until sage-data-client has reconstructed a dataframe;
exp70's timer ends after collecting the response, before NDJSON parsing or
archive comparison. The first-batch events are similar, but warehouse size,
Parquet layout/row groups, timestamp precision and cache history also differ.
Historical March-month results contain 25,940,207 rows versus the current
1,048,491-row day; dividing or extrapolating those timings is not valid. There
is no matched recent live v2 NDJSON/day control in the saved artifacts.

`exp71_v2_v3_cost.py` records this audit in
`results/exp71_v2_v3_cost.json`. With `--measure-v3`, it reuses the **same
Sage-client timing path** as the historical benchmark, with fresh workers and
an excluded warmup for minute/hour/day. It keeps v3 nanoseconds in the timed
response. Outside the timer it explicitly floors a copy of the timestamps to
microseconds and compares full-row hash sum/XOR, decoded columns and dtypes
against historical minute/hour signatures. This permits a value/metadata
comparison without falsely demanding that restored precision equal truncated
v2 timestamps. It never floors stored data or the API response.

```sh
python -I sage-data-rework/tiered_system/experiments/exp71_v2_v3_cost.py --measure-v3
```

The matching-timer rerun is now complete against the full v3 warehouse; its
measurements are recorded below. It required only the current v3 API and client,
without a v2 deployment. Its ratio is an observed historical **migration**
difference, not an isolated causal VARIANT cost. A fully isolated end-to-end VARIANT measurement would require a matched
split/VARIANT control with the same input precision, layout, engine, client,
codec and cache protocol. The existing exp68 local controlled comparison
remains the available estimate of the value-representation cost (roughly
9–17% in its measured cases), separate from live Iceberg/HTTP effects.

### Plugin row-group layout experiment (exp72)
> The full v3 migration baseline is now available. The plugin comparison uses
> an isolated March 1 copy; it does not rewrite the full live warehouse.

The requested candidate is **plugin → vsn → time**, rather than name → vsn →
time. `exp72_plugin_layout.py` compares the read-only published day's local
Parquet against two new temporary datasets: a time-first repack and a
plugin-grouped repack. Plugin-specific files enforce that no candidate row
group mixes plugins, with vsn/time ordering inside each plugin. This is a
local physical-layout experiment, **not** a change to Iceberg's partition
specification or the running warehouse.

Both repacks use Zstd level 3, a 262,144-row target and explicit DOUBLE
shredding for the single VARIANT value (matching the existing physical
variant typed_value). The original files are a third reference so a benefit
from repacking alone cannot be misattributed to plugin ordering. Explicit
file lists with hive_partitioning=false prevent directory pruning from
substituting for physical column statistics. Plugin grouping changes file
count as well as ordering; record both rather than claiming a pure single-file
row-order effect.

The harness validates a full-day raw-row multiset fingerprint before timing,
then rotates the three layouts across repetitions and checks full public
response signatures plus timestamp order on every run. The query matrix
includes one-series minute/hour/day, plugin+vsn day, all-plugin single-vsn
minute/hour, and fleet-wide minute/hour. Diagnostics report file/group sizes,
min/max pruning eligibility, scan/projection/sort work and spill. All generated
Parquet/IPC/spill stays in temporary local storage; no Pelican writes, hint
changes, server rebuilds, commits or pushes are performed.

```sh
python -I sage-data-rework/tiered_system/experiments/exp72_plugin_layout.py \
  --native-probe /tmp/sage-v3.test --sql-probe /tmp/sage-v3.test --reps 3
```

The binary is built with the current native Go test sources as in exp69.
This measures native Go streaming plus Zstd to a local file, not a live
Iceberg/HTTP latency comparison. Any promising layout still needs an isolated
catalog/publication and live measurement before becoming the seeding default.


### Complete v3 March/April before changing the layout

**Add the remaining data first**, retain the current time-first
layout, and submit the same historical cold queries on this iteration. Hold
plugin layout changes until that full-coverage migration baseline is recorded.
All 61 archive days from March 1 through April 30 are available (879 files).
Reuse the already committed March 1 and append **March 2 through April 30**.

`seed_cold.py --append` now validates the source as v3/timestamptz_ns/VARIANT
and refuses to insert if the requested interval already contains any rows.
It commits daily and records acknowledged source heads in
`seed-v3-progress.json`. Before each INSERT it writes `seed-v3-pending.json`;
that journal is removed only after the commit and progress record succeed.
An unresolved journal blocks further appends/registration recovery, rather
than guessing whether a timed-out INSERT committed. Inspect the authoritative
catalog and journal before explicitly selecting the remaining empty days.

The stopped local REST fixture can restore a missing table registration via
`--restore-publication`. It uses the prior publication's **source_metadata**
and table UUID, preferring a later acknowledged progress head when present.
It never registers the rebased replica as the writable table, replaces an
existing catalog entry, selects a head by filename, or rolls a known newer
head back to March 1. An unresolved INSERT journal requires manual resolution.

Run from the workspace root with DuckDB Python 2.0.x active:

```sh
export COLD_V3_ROOT="$PWD/sage-data-rework/scratch/v3-1792707566"
docker compose -p sage-v3 -f sage-data-rework/tiered_system/demo/docker-compose.v3.yml up -d
export V3_PUBLICATION_ID="$(date +%s)"
python -I sage-data-rework/tiered_system/demo/tools/seed_cold.py \
  --endpoint http://127.0.0.1:18181 --root "$COLD_V3_ROOT" \
  --append --restore-publication "${COLD_V3_ROOT}-replica/publication.json" \
  --start 2025-03-02 --end 2025-04-30 \
  --export "${COLD_V3_ROOT}-full-${V3_PUBLICATION_ID}" \
  --pelican-prefix "/cold-storage-demo-v3-full-${V3_PUBLICATION_ID}"
```

The current March-1-only remote warehouse is unchanged. The complete snapshot
is copied and uploaded under a **new isolated prefix**, with its hint last.
Budget disk for the local source, complete staging replica and sort spill.
Do not delete the source catalog, current replica or old v2 objects. No hot
data is evicted. Before rerunning after interruption, inspect the progress and
pending journal; do not blindly replay this entire date interval.

Only after successful publication, set COLD_WAREHOUSE to the emitted URL,
keep COLD_WATERMARK=2025-05-01T00:00:00Z, async=8, automatic read-ahead, Zstd
and profiling off, and recreate data-api with that environment. Then run:

```sh
python -I sage-data-rework/tiered_system/experiments/exp71_v2_v3_cost.py \
  --measure-v3 --include-month --reps 3 \
  --output sage-data-rework/tiered_system/experiments/results/exp71_v2_v3_full_live.json
```

This repeats the five historical cold cases: one-name minute/hour, all-name
minute/hour for W0A0, and one-name March. March 1 day is an additional baseline.
It reuses the historical Sage-client dataframe timer and fresh worker process;
short cases get an excluded warmup, month cases do not. The reference is
exp67_after_live (DuckDB 2.0 with exp67 metadata optimization). Full-row hash
sum/XOR, decoded columns and dtypes must match after explicitly flooring a
separate copy of current timestamps to microseconds **outside the timer**.
The stored values and public response retain nanoseconds. Record total time,
first batch, body bytes and client CPU for each window.

With matching coverage and timers, these are useful **historical end-to-end
migration** comparisons, including VARIANT and restored precision. They are
not a claim that every difference is caused solely by VARIANT: writer layout
and timestamp entropy also changed. Do not extrapolate a month timing into a
per-day estimate. Collect separate profiles afterward to explain the observed
change before choosing plugin/vsn/time clustering.

Eight offline publisher tests pass, including empty-interval/source validation,
registration recovery, rejecting catalog replacement, preferring acknowledged
progress and refusing uncertain INSERT recovery.

#### Measured

March 2 through April 30 were appended to the existing March 1 source at
`sage-data-rework/scratch/v3-1792707566` and published under
`/cold-storage-demo-v3-full-1792764935`. The seed-and-upload process took
4 h 27 min. The committed table is format-version 3, 61 snapshots,
8,457,186,954 records, 122 data files, and 44,678,623,625 bytes. The hint stem
is `00061-d6d62b83-8bc4-4771-9e67-f0aee3e33bfb`. The March-1-only warehouse and
the old v2 objects were not rewritten. The local catalog was stopped after
publication. The demo API now reads

`https://nrdstor.nationalresearchplatform.org:8443/sage/cold-storage-demo-v3-full-1792764935/warehouse`

with watermark `2025-05-01T00:00:00Z`, async=8, automatic read-ahead, Zstd, and
profiling off. A cold March 1 minute still returns 698 rows, beginning at
`2025-03-01T00:00:00.026403194Z` / `11.7`.

exp71 repeated the historical Sage-client timer against this warehouse.
Decoded columns, dtypes, and full-row hashes match exp67 after flooring a
copy of the timestamps to microseconds outside the timer. Medians of three
repetitions:

| query | rows | v3 total / first batch | body bytes | client CPU | v2 total | change |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| one-name minute | 698 | 0.384 s / 0.381 s | 15,032 | 0.010 s | 0.315 s | +22% |
| one-name hour | 43,509 | 0.761 s / 0.744 s | 780,280 | 0.027 s | 0.312 s | +144% |
| all-name minute | 6,352 | 0.333 s / 0.324 s | 119,040 | 0.016 s | 0.449 s | −26% |
| all-name hour | 395,989 | 1.114 s / 0.982 s | 7,319,728 | 0.168 s | 0.720 s | +55% |
| one-name March | 25,940,207 | 584.861 s / 575.240 s | 474,386,272 | 7.743 s | 29.084 s | +1911% |
| one-name March 1 day | 1,048,491 | 9.792 s / 9.577 s | 19,171,032 | 0.293 s | — | — |

The March repetitions were 584.861 s, 589.866 s, and 581.563 s. Artifact:
`tiered_system/experiments/results/exp71_v2_v3_full_live.json`.

One profiled March Arrow query returned the same 25,940,207 rows in 573.7 s,
with the first batch at 567.0 s. It opened 13,421 row groups, read
20,479,365,337 bytes, and returned only the filtered rows. Operator work was
892.7 s in the scan, 214.1 s in the large projection, and 10.5 s in the sort.
Peak buffer was 2.80 GiB, against the 3 GB engine limit, and the sort spilled
about 2.63 GiB. Summary:
`tiered_system/experiments/results/exp71_v3_march_profile.json`. Profiling is
off again. The month cost follows the day-partitioned scan, not a claim that
VARIANT alone explains the difference from the v2 layout.

The initial exp72 layout attempt time-repacked the local day but exhausted its
3 GB memory limit while concurrently writing 39 plugin partitions, before any
query timing comparison. The retained artifact is
`results/exp72_plugin_layout_initial_attempt.json`; it is a failed attempt, not
a layout performance result. The harness now writes one plugin at a time to
bound active buffers. At the time of the migration baseline, the layout comparison had not run.
The completed local comparison follows below; the production layout remains unchanged.


#### Review of the completed migration comparison (2026-10-23)

The historical comparison is now usable for assessing the migration: same
queries, timer, decoded public columns/dtypes and values/metadata after explicit
out-of-timer timestamp flooring. Independent inspection confirms all 45
current-to-historical signature comparisons pass across the five matched cases
and repetitions. The stored and served data retains nanoseconds.

The March total is **20.11x** the v2 baseline (584.861 / 29.084 s), with three
runs at 581.563–589.866 s. First batch takes 98.36% of the median total time.
Response size grows by about 10.1% (430,985,448 → 474,386,272 bytes), while
median client CPU is 8.297 → 7.743 s. A nine-minute regression is not explained
by that wire-size increase or client decoding. The current v3 schema decision
can be retained while addressing the new execution/storage cost; these results
supersede any assumption that the complete migration costs only the local
exp68 9–17% representation overhead.

Historical exp67's three profiles versus the new single diagnostic show:

| Profile measure | v2 exp67 | v3 March diagnostic |
|---|---:|---:|
| Scan work | 3.67–4.85 s | 892.66 s |
| Large projection work | 13.68–14.33 s | 214.12 s |
| Sort work | 6.92–11.00 s | 10.53 s |
| Native Arrow conversion | 0.56–0.61 s | 0.58 s |
| Reported scanned row groups | 66 | 13,421 |

These are operator work/counter comparisons, not additive wall-time phases or
pure CPU measurements. Engine byte counters include cache/spill effects and
are not a direct Pelican traffic measurement. Nevertheless, the evidence
prioritizes scan/pruning and response projection, not IPC conversion or a
faster sort. More than 25 million correct result rows still emerge from both
layouts; the new reader does substantially more upstream work before they
can be served.

The full source now contains 122 data files. Exp72 was updated to select
**March 1 by exact Parquet timestamp statistics**: only its two complete files
and 140,578,674 rows are copied. It rejects files that straddle the selected
day instead of accidentally rewriting both months or reporting whole-file
counts for a clipped query. Its boundary-selection regression test passes.
The plugin/vsn/time comparison remains a local physical-layout test with
unchanged VARIANT/metadata, followed by a live isolated publication only if
the local result warrants it.


### Measured plugin/vsn/time layout comparison (exp72, 2026-10-23)

The requested benchmark is now complete on **March 1 only**, selected from the
full source warehouse by timestamp statistics. It copied two read-only source
files (140,578,674 rows) into temporary local datasets. No live warehouse,
Iceberg partition specification, API process or storage hint was modified.

Three layouts were measured on native DuckDB 2.0.0 with actual current Go
response SQL and the production public Arrow schema/Zstd writer: original
published time-first files, a time-first repack control, and plugin-grouped
files ordered by vsn/time inside each plugin. Both repacks use Zstd level 3,
a 262,144-row group target and explicit DOUBLE VARIANT shredding. Columnar
plugin values remain stored; hive_partitioning=false and explicit file lists
prevent directory pruning from substituting for file/row-group statistics.
A separate writer process per plugin avoids the initial concurrent-partition
OOM. Timings exclude rewrite, native-engine initialization and checksum work;
OS caches were warmed by rewriting and full-day validation. This is **local
Parquet + Go streaming**, not a live Iceberg/HTTP measurement.

All 140,578,674 rows match by a whole-day raw-row multiset fingerprint across
all columns, including stored VARIANT types, metadata and nanosecond times.
All **72 unprofiled runs** (eight cases × three layouts × three repetitions)
and **12 separate profile runs** match full public row signatures/schema and
nondecreasing timestamps. Layout order rotates between repetitions. Medians:

| Query | Rows | Original time | Repacked time | Plugin/vsn/time |
|---|---:|---:|---:|---:|
| One series, minute | 698 | 0.0279 s | 0.0234 s | 0.0179 s |
| One series, hour | 43,509 | 0.4585 s | 0.4490 s | 0.0698 s |
| One series, day | 1,048,491 | 9.9320 s | 9.9510 s | 1.1273 s |
| One plugin + vsn, day | 9,436,419 | 15.8968 s | 15.4193 s | 6.3330 s |
| All plugins, W0A0, minute | 6,352 | 0.0339 s | 0.0285 s | 0.0269 s |
| All plugins, W0A0, hour | 395,989 | 0.6450 s | 0.6478 s | 0.2586 s |
| Full fleet, minute | 94,654 | 0.1148 s | 0.1103 s | 0.1519 s |
| Full fleet, hour | 6,011,615 | 3.9932 s | 3.9225 s | 4.0585 s |

The grouped layout is **8.81x faster** for the series day than the published
time-first files, and **8.83x** versus the repacked time control. Its hour
query is about 6.6x faster and its plugin/vsn day query about 2.5x faster.
The time-only repack does not recover the day regression, which supports a
benefit from the grouping/clustering rather than simply rewriting files.
VARIANT, exact values and metadata serialization remain unchanged.

The tradeoff is broad short windows: full-fleet minute is **32% slower**
(+0.037 s locally); fleet hour is within about 2% of the original. Do not
assume the minute's absolute overhead stays this small remotely: it touches
many more files/groups. Min/max statistics leave 55 groups eligible versus
one in the time-first layout. Its diagnostic engine read count rises from
2.76 MB to 43.98 MB. Fleet-hour counters likewise show more candidate groups
(84 versus 26); three local runs do not establish a meaningful hour speedup.

For the one-series day, eligible groups fall from 531 to 109, and the profile
reports the same scanned-group counts. Engine bytes read fall from
693,884,964 to 90,418,659. Operator work changes from scan 9.35 → 0.62 s,
projection 9.97 → 1.32 s and sort 0.112 → 0.057 s. Neither spills in this
case. These are overlapping operator-work values, not wall-time components.
The 9.4-million-row plugin/vsn query spills with all layouts; its speedup does
not establish a spill reduction (diagnostic peak temp sizes are approximately
407 MB original, 538 MB time repack and 560 MB grouped).

| Physical measure | Original | Time repack | Plugin/vsn/time |
|---|---:|---:|---:|
| Files | 2 | 178 | 112 |
| Row groups | 531 | 625 | 605 |
| Groups mixing plugins | 531 | 623 | 0 |
| Parquet bytes | 731,299,180 | 767,341,762 | 482,107,000 |
| Rewriting wall time | — | 54.9 s | 145.0 s |

Grouping saves about **34.1%** of Parquet bytes versus the original, but this
bounded-memory sequential rewrite takes about **2.64x** the repacked-time
rewrite duration. File count changes alongside row order; this is a physical
layout comparison, not an isolated single-file sort-key experiment. Rewrite
times include the particular harness's repeated plugin passes and native
process startup; they are not an established ingestion cost for a future
integrated writer. All generated datasets/IPC/spill were removed by their
temporary-directory cleanup after validation; source files were read-only.

Artifact: `tiered_system/experiments/results/exp72_plugin_layout.json`.
The earlier OOM artifact remains separately labelled as a failed attempt.
The current source SQL was unchanged since the exporter binary was built;
probe hashes, engine version/settings and full results are retained.

**Conclusion:** keep VARIANT and prioritize an isolated live trial of this
plugin/vsn/time layout for the common filtered workloads. The 20x historical
month regression cannot be assigned to VARIANT alone: physical grouping
recovers almost 9x on the same v3 day with unchanged values and response code.
Do not extrapolate that factor to March or replace the live seeding layout
yet. The next acceptance step is a correctly registered Iceberg v3 candidate
and isolated publication, followed by live series/plugin/node/fleet queries
and a month rerun when candidate coverage matches. Preserve the current full
v3 warehouse as the baseline throughout that trial.


### Opt-in registered plugin-layout live trial (exp73)

**Historical experiment:** the exactly-one-group-per-plugin rule below was
superseded by the bounded plugin-group policy in exp77–78. Its measurements
and published warehouses remain controls; do not reinterpret them as capped
row-group results.

The trial tooling now implements the agreed **batch first, then group** layout:

1. Keep `day(time)` as the only Iceberg partition transform.
2. Assign input rows to fixed batches of **10,000,000 rows**, with a smaller
   final batch at the end of each day. Batch membership is fixed before sorting
   or grouping, following the archive exporter's row limit.
3. Sort each batch by **plugin → vsn → time**, ascending, NULLS LAST.
4. Write one Parquet file per batch, with **exactly one row group per distinct
   plugin in that file**. Keep every row for that plugin in its one group,
   regardless of group size. Vsn/time ordering remains intact inside the group.
   Several small plugins share a file. A plugin can recur in multiple files,
   but never in multiple row groups within one file.
5. Preserve the existing schema, including `time TIMESTAMPTZ_NS`, one logical
   `value VARIANT` with Iceberg field ID **11**, and metadata map child IDs.

Parquet supports multiple row groups in one file; its writer chooses the
boundaries. Sorting alone does not guarantee plugin boundaries or one group
per plugin. See https://parquet.apache.org/docs/concepts/.

**Baseline:** `/cold-storage-demo-v3-full-1792764935` remains the time-first
warehouse. Its committed source inventory confirms all **122** files already
have outer Parquet `value (VARIANT(1))`, field ID **11**: 8,457,186,954 rows,
32,062 mixed-plugin row groups. All 122 have a DOUBLE `typed_value` child, a
VARIANT shredding detail. Artifact: `results/exp73_time_baseline_inventory.json`.
Nothing in this trial rewrites that baseline or its API.

#### Writer and publication path

`seed_cold.py --layout plugin-vsn-time --layout-helper <binary>` is opt-in;
`time` remains the default. The new table property
`sage.layout.version=batch-plugin-v1` distinguishes this writer from the old
per-plugin-file experiment. Append refuses an old layout/version or an occupied
interval. The API still explicitly sorts public query results by timestamp.

The measured archive-order writer uses single-thread row-number windows over
the clipped day's v3 projection, sorting each fixed batch independently. It
uses a 6 GB DuckDB limit during this step and restores the previous setting.
It does not materialize the whole day as an in-memory DuckDB table: that path
failed on the VM. Each intermediate Parquet file has explicit Iceberg field
IDs and is not published. The chronological-window alternative below first
sorts directly to a local Parquet file on disk, then uses its physical row
numbers to fix batches before plugin/vsn/time sorting.

The Go helper at `experiments/icebergo/cmd/layoutpack` uses the existing pinned
Arrow Go 18.8.0 and iceberg-go 0.7.0 dependencies. It reads the temporary file
in 65,536-row batches, preserves the VARIANT extension and field IDs, and
writes a buffered row group until the plugin changes. Input read-batch and
intermediate row-group boundaries do not split a plugin's final row group.
The helper checks plugin/vsn/time ordering, UTC nanoseconds and VARIANT ID 11,
and refuses to overwrite an output file. Each final file is Zstd compressed.

Before registration, Python audits each final file and compares its complete
raw-row count/hash sum/XOR with its original fixed batch, including VARIANT,
metadata and nanoseconds. It saves batch offsets, counts and fingerprints in a
local `layout-batches-*.json` provenance record. All final files reside inside
the fresh source table location. Then a single iceberg-go `AddFiles` transaction
registers the day's files through the isolated local REST catalog. The library
infers daily partitions and writes manifests, v3 row lineage and table metadata;
we do not manually fabricate a committed snapshot. The helper checks the
expected catalog head before attempting the commit.

The seeder requires one additional daily snapshot and checks the committed
day's full raw-row fingerprint against its archive projection. An unresolved
packing, registration or verification failure leaves the pending journal for
inspection and never reaches Pelican publication. Completed files can remain
as local orphans after a failure; they are not permission to replay the day.
The default time-first INSERT path is unchanged.

Before publication, the committed-file audit requires:

- Manifest/footer row-count agreement, UTC nanoseconds and the VARIANT ID-11
  logical schema and physical Parquet annotation.
- At most 10 million rows per file, and timestamps within one UTC day.
- Exactly one row group per plugin per file, with no mixed-plugin groups,
  no repeated plugin groups, and plugin groups ordered ascending, NULLS LAST.
- Vsn/time ordering inside each group, including read-batch seams.

The resulting `layout-audit.json` includes `one_group_per_plugin=true` and
`file_row_limit=10000000`. The live harness requires these markers as well as
March-1-only extent. The old per-plugin-file experiment is a different layout;
its exp72 results must not be reported as measurements of this batch layout.
Existing export/hint-last publication guards and `/node-data` exclusions remain.

**Resource implication:** the Go writer buffers the current plugin row group;
its memory is not capped by DuckDB's 3 GB memory limit. One group can contain
all 10 million rows of a file. This is the requested layout, not an implicit
smaller-group fallback. Measure writer peak RSS, seed duration, file/group
sizes and live scan behavior on the VM. Larger groups can reduce pruning by
vsn/time and within-file scan parallelism compared with the old exp72 layout.
Do not change the one-group-per-plugin rule without revisiting the design.

#### VM trial: fresh March 1 candidate, baseline stays online

Run from the workspace root with Python DuckDB **2.0.x** and the v3 requirements
installed. Build the helper on the VM; a macOS binary is not portable to Linux.
Use a new shared root, project and catalog port, with the pinned REST fixture.

```sh
export V3_PYTHON=python
"$V3_PYTHON" -c "import duckdb; assert duckdb.__version__.startswith('2.0.'), duckdb.__version__"
export COLD_V3_LAYOUT_HELPER="$PWD/sage-data-rework/scratch/bin/layoutpack"
mkdir -p "$(dirname "$COLD_V3_LAYOUT_HELPER")"
(cd sage-data-rework/tiered_system/experiments/icebergo && go build -o "$COLD_V3_LAYOUT_HELPER" ./cmd/layoutpack)
export PLUGIN_TRIAL_ID="$(date +%s)"
export COLD_V3_ROOT="$PWD/sage-data-rework/scratch/v3-plugin-trial-${PLUGIN_TRIAL_ID}"
export COLD_V3_PORT=18182
export COLD_V3_IMAGE=apache/iceberg-rest-fixture@sha256:db8de90b5b7693d4ac334c336f91d9bbe320d7b19f4f514d26de84cdfbcbfe8d
mkdir -p "$COLD_V3_ROOT"
docker compose -p sage-v3-plugin-trial -f sage-data-rework/tiered_system/demo/docker-compose.v3.yml up -d
COLD_V3_LAYOUT_ENDPOINT=http://127.0.0.1:18182 COLD_V3_LAYOUT_ROOT="$COLD_V3_ROOT" \
  "$V3_PYTHON" -m unittest discover -s sage-data-rework/tiered_system/demo/tools -p test_v3_layout.py -v
```

**Require the REST round-trip test to PASS, not skip, before seeding.** It
creates a unique test namespace, leaving `sage.obs` for the candidate. It uses
small test batches to verify shared files, exact row-group counts, values,
nanoseconds, native DuckDB reads and one daily AddFiles snapshot. The production
CLI always uses 10-million-row batches; smaller batches are a test seam only.

```sh
export PLUGIN_TRIAL_PREFIX="/cold-storage-demo-v3-plugin-trial-${PLUGIN_TRIAL_ID}"
"$V3_PYTHON" -I sage-data-rework/tiered_system/demo/tools/seed_cold.py \
  --endpoint http://127.0.0.1:18182 --root "$COLD_V3_ROOT" \
  --layout plugin-vsn-time --layout-helper "$COLD_V3_LAYOUT_HELPER" \
  --start 2025-03-01 --end 2025-03-01 \
  --export "${COLD_V3_ROOT}-replica" --pelican-prefix "$PLUGIN_TRIAL_PREFIX"
```

Only after successful publication, start the separate candidate API on port
10001 using the exact running baseline image. The baseline stays on port 10000.

```sh
export LAYOUT_API_IMAGE="$(docker inspect --format '{{.Image}}' demo-data-api)"
export LAYOUT_COLD_WAREHOUSE="https://nrdstor.nationalresearchplatform.org:8443/sage${PLUGIN_TRIAL_PREFIX}/warehouse"
export LAYOUT_PROFILE_HOST="${COLD_V3_ROOT}/profiles"
export LAYOUT_PROFILE_DIR=""
mkdir -p "$LAYOUT_PROFILE_HOST"
docker compose -f sage-data-rework/tiered_system/demo/docker-compose.layout-trial.yml up -d --no-deps candidate-api
"$V3_PYTHON" -I sage-data-rework/tiered_system/experiments/exp73_plugin_live_compare.py \
  --candidate-audit "$COLD_V3_ROOT/layout-audit.json" --api-image "$LAYOUT_API_IMAGE" \
  --reps 3 --output sage-data-rework/tiered_system/experiments/results/exp73_plugin_live_compare.json
```

The harness runs eight exp72 query shapes in Arrow and NDJSON, with excluded
warmups and rotated sequential request order. It checks canonical row multiset
digests across layouts/formats, preserving nanoseconds, large integers, value
types and metadata, plus timestamp ordering. The first measured body per
layout/format/case is fully checked by default; `--validate all` checks every
body. All Arrow responses must retain the public four-field schema and Zstd.
NDJSON measures the first complete line before buffered bulk reads.

Responses spool to temporary files with bounded-batch validation outside the
timer. The timer includes HTTP receipt, Arrow IPC iteration and file spooling;
it excludes Sage-client dataframe decoding and is not comparable to exp71's
absolute totals. Compare time versus plugin within each format. The baseline
has 61 snapshots and March/April coverage; the initial candidate has one day.
Record this metadata-footprint difference and do not issue month queries to
the one-day candidate.

After primary timings, optionally profile only the sidecar:

```sh
export LAYOUT_PROFILE_DIR=/profiles
docker compose -f sage-data-rework/tiered_system/demo/docker-compose.layout-trial.yml up -d --no-deps candidate-api
"$V3_PYTHON" -I sage-data-rework/tiered_system/experiments/exp73_plugin_live_compare.py \
  --candidate-audit "$COLD_V3_ROOT/layout-audit.json" --api-image "$LAYOUT_API_IMAGE" \
  --match series_day --reps 1 \
  --output sage-data-rework/tiered_system/experiments/results/exp73_plugin_profiled_day.json
"$V3_PYTHON" -I sage-data-rework/tiered_system/experiments/exp65_cold_query_profile.py \
  "$LAYOUT_PROFILE_HOST" --output sage-data-rework/tiered_system/experiments/results/exp73_plugin_profiles.json
export LAYOUT_PROFILE_DIR=""
docker compose -f sage-data-rework/tiered_system/demo/docker-compose.layout-trial.yml up -d --no-deps candidate-api
```

Keep profiled timings separate. Inspect eligible complete profiles for scan,
row-group, read-byte, projection, sort and spill costs. Do not promote this
layout or reseed both months before accepting filtered and fleet results live.

#### Implementation validation status

- Offline publisher and batch writer checks pass, including fixed batch
  selection before sorting, one daily commit command, and no commit on a pack
  failure. The prior publication failure guards remain covered.
- A native DuckDB 2.0 → Arrow Go test packs **70,005 rows** into exactly three
  groups of **70,000 / 2 / 3** rows, sharing one file across two named plugins
  and NULL. This crosses both intermediate row groups and the 65,536-row read
  boundary. Complete raw-row fingerprints match before/after, including mixed
  VARIANT values, >2^53 integers, NULLs, metadata and nanoseconds. Wrong ID,
  non-VARIANT storage, repeated plugin groups and bad ordering are rejected.
- A real local SQLite-catalog iceberg-go **v3 AddFiles** test registered a
  packed file under `day(time)` with the DuckDB-compatible field IDs. Native
  DuckDB 2.0 `iceberg_scan` read all four mixed VARIANT values, metadata and
  exact timestamps back. The empty test fixture was adjusted to DuckDB's field
  ID allocation before committing; no live catalog was altered. Artifact:
  `results/exp73_batch_layout_native_validation.json` (temporary local paths).
- The isolated REST round trip passed against catalog port 18182, including
  one daily AddFiles commit. March 1 was then packed and published at
  `/cold-storage-demo-v3-plugin-trial-1792792556`. The time-first warehouse
  `/cold-storage-demo-v3-full-1792764935` and the API on port 10000 were not
  changed. The candidate API uses that same image on port 10001, with profiling
  off. The local trial catalog was stopped after publication.

  A single in-memory table for this day is about 54 GB at the measured
  762 MB per 2 million rows, and DuckDB did not spill it under the 3 GB limit.
  The writer now sorts each fixed 10-million-row window on its own, with the
  DuckDB limit raised to 6 GB for that step only. Timestamp statistics use the
  Parquet physical nanosecond values. The committed day took 687.93 s before
  audit and upload. The audit records 15 files, 140,578,674 rows, 730,100,005
  bytes, 63 plugin row groups, and no mixed groups. One group contains all
  10 million rows of its file. `value` remains VARIANT field ID 11. The largest
  writer RSS was not sampled.

  exp73 checked the eight March 1 shapes in Arrow and NDJSON. Canonical
  multisets match across both layouts and both formats, including nanoseconds.
  Arrow medians of three repetitions, receive plus IPC iteration, not the
  exp71 dataframe timer:

  | case | rows | time-first | plugin/vsn/time |
  | --- | ---: | ---: | ---: |
  | one series, minute | 698 | 0.386 s | 0.310 s |
  | one series, hour | 43,509 | 0.759 s | 0.367 s |
  | one series, day | 1,048,491 | 9.859 s | 1.388 s |
  | one plugin and vsn, day | 9,436,419 | 28.190 s | 11.988 s |
  | all plugins, one vsn, minute | 6,352 | 0.355 s | 0.849 s |
  | all plugins, one vsn, hour | 395,989 | 0.967 s | 0.991 s |
  | fleet minute | 94,654 | 0.459 s | 1.377 s |
  | fleet hour | 6,011,615 | 5.458 s | 9.313 s |

  The baseline still has 61 snapshots and both months; the candidate is March 1
  only. No month query was sent to the candidate. Artifact:
  `results/exp73_plugin_live_compare.json`.

  A separate profile of the candidate's one-series day opened 4 row groups and,
  on the first profiled read, 199,742,586 bytes. The warm backend finished in
  1.351 s. Operator work was 0.225 s in the scan, 0.675 s in the large
  projection, and 0.080 s in the sort. Peak buffer was 591 MB and the sort did
  not spill. Summary: `results/exp73_plugin_profiles.json`. These are not a
  promotion of the layout and not a reason to reseed March and April.

### Time pruning follow-up: paired profiles and chronological batches (exp74–76)

The existing candidate is retained; it is not promoted or overwritten. Both
published warehouses and the APIs on ports 10000/10001 remain unchanged.

#### Local paired diagnosis on the actual March 1 Parquet files

`exp74_layout_scan_profiles.py` selects only complete March 1 files using
physical nanosecond footer statistics. It uses the current Go SQL exporter and
native DuckDB 2.0 streaming probe, 3 GB engine memory, async=8 and Zstd. Runs
rotate layouts; primary timings are unprofiled and profiles are separate.
Every response must match public schema and row signatures. This deliberately
excludes Iceberg metadata planning, Pelican and HTTP. It compares equal day
coverage but cannot establish end-to-end VM latency.

The initial two-layout local test completed (three unprofiled repetitions per
case/layout, plus separate profiles):

| case | time-first median | archive-order plugin median | footer-retained rows: time → plugin |
| --- | ---: | ---: | ---: |
| series day | 9.904 s | 1.028 s | 140,578,674 → 39,873,056 |
| all plugins / one vsn / minute | 0.038 s | 0.467 s | 282,667 → 133,009,301 |
| fleet minute | 0.126 s | 1.140 s | 282,667 → 139,973,377 |
| fleet hour | 3.950 s | 5.221 s | 6,949,400 → 139,988,640 |

The profiles independently confirm scan amplification. Fleet minute opens
1 versus 56 row groups, with engine-reported reads about 2.76 MB versus
727.45 MB and scan operator work 0.016 s versus 2.060 s. Fleet hour opens
26 versus 60 groups and scan work grows from 0.442 s to 2.202 s; projection
and sort work remain similar. Operator work includes waits and thread work,
not additive elapsed time. Footer-retained rows/bytes are eligibility upper
bounds, not actual examined rows or network bytes. Engine byte counters also
must not be reported as Pelican HTTP traffic.

This reproduces the narrow-time regression without network or unequal Iceberg
snapshot coverage. It supports wide time ranges in plugin groups as a cause;
it does not quantify the additional HTTP/Pelican overhead.
Artifact: `results/exp74_layout_scan_profiles.json`.

#### New opt-in file-batch policy

`seed_cold.py --layout plugin-vsn-time --batch-order time` adds a second
candidate, preserving the agreed **10-million-row maximum**, daily Iceberg
partitions, **exactly one row group per plugin per file**, vsn/time ordering
inside groups, nanoseconds and VARIANT ID 11. `--batch-order archive` remains
the default and reproduces the exp73 candidate.

The time policy externally sorts the full day **directly to temporary
Parquet on disk**. It never stores the whole day in a DuckDB table. It checks
that staging preserves the complete raw-row fingerprint. Physical
`file_row_number` windows then select exact 10-million-row batches before
plugin/vsn/time sorting and packing. The final partial batch contains the
remaining rows of that day. The staging file is removed after the daily commit.
This adds a global external sort and temporary disk I/O; it is an experiment,
not an assumed ingestion improvement.

`sage.batch.order=time` records provenance. Append refuses a different batch
policy; older exp73 tables without that property mean archive order. Batch
provenance includes per-file time ranges. The audit checks that chronological
file ranges do not overlap out of order within a day (equal timestamps may
straddle a boundary), in addition to the existing plugin/row-limit/schema
checks. The live comparison requires this proof for a time-batch candidate.
The seeder records separate parent/child RSS high-water marks; these are not
simultaneous combined memory. The Go row-group buffer is outside DuckDB's limit.

`exp76_time_batch_layout.py` can build and retain the physical candidate
locally using the same staging/window SQL and the existing Go helper, without
catalog commits or remote writes. It validates every batch and the complete
day's raw fingerprint and samples native/helper RSS. Its output can be added
as a third `--dataset` in exp74. Local results remain separate from live API
measurements and are recorded below once complete.

#### Matched one-day control and VM execution

To remove the exp73 metadata-coverage imbalance, `seed_time_control.py` creates
an isolated March 1 control with **one snapshot**, copying the original
time-first Parquet files **byte for byte**. It uses SHA-256 verification and a
committed raw-row fingerprint check. It never re-encodes or edits the original
files. Files crossing the day boundary are rejected. Registration uses the
same isolated REST/Go AddFiles path; only a new prefix is published.

Run the following from the workspace root on the VM, with the v3 Python
requirements and compiled helper available. All roots/prefixes must be fresh.

```sh
export V3_PYTHON=python
export COLD_V3_LAYOUT_HELPER="$PWD/sage-data-rework/scratch/bin/layoutpack"
export FOLLOWUP_ID="$(date +%s)"
export CONTROL_ROOT="$PWD/sage-data-rework/scratch/v3-time-control-${FOLLOWUP_ID}"
export TIME_BATCH_ROOT="$PWD/sage-data-rework/scratch/v3-time-batches-${FOLLOWUP_ID}"
export CONTROL_PREFIX="/cold-storage-demo-v3-time-control-${FOLLOWUP_ID}"
export TIME_BATCH_PREFIX="/cold-storage-demo-v3-plugin-time-batches-${FOLLOWUP_ID}"
export REST_FIXTURE_IMAGE=apache/iceberg-rest-fixture@sha256:db8de90b5b7693d4ac334c336f91d9bbe320d7b19f4f514d26de84cdfbcbfe8d
export SOURCE_HEAD="$("$V3_PYTHON" -c 'import json,sys; print(json.load(open(sys.argv[1]))["source_metadata"])' sage-data-rework/scratch/v3-1792707566/seed-v3-progress.json)"
mkdir -p "$CONTROL_ROOT" "$TIME_BATCH_ROOT"
COLD_V3_ROOT="$CONTROL_ROOT" COLD_V3_PORT=18183 COLD_V3_IMAGE="$REST_FIXTURE_IMAGE" \
  docker compose -p sage-v3-time-control -f sage-data-rework/tiered_system/demo/docker-compose.v3.yml up -d
COLD_V3_ROOT="$TIME_BATCH_ROOT" COLD_V3_PORT=18184 COLD_V3_IMAGE="$REST_FIXTURE_IMAGE" \
  docker compose -p sage-v3-time-batches -f sage-data-rework/tiered_system/demo/docker-compose.v3.yml up -d
COLD_V3_LAYOUT_ENDPOINT=http://127.0.0.1:18184 COLD_V3_LAYOUT_ROOT="$TIME_BATCH_ROOT" \
  "$V3_PYTHON" -m unittest discover -s sage-data-rework/tiered_system/demo/tools -p test_v3_layout.py -v
```

Require **both REST round trips to pass**, including the new time-batch case,
not skip. Test namespaces are unique and preserve `sage.obs` for the candidate.
The control catalog must still have an empty root before its seeder starts.

```sh
"$V3_PYTHON" -I sage-data-rework/tiered_system/demo/tools/seed_time_control.py \
  --source-head "$SOURCE_HEAD" --root "$CONTROL_ROOT" --endpoint http://127.0.0.1:18183 \
  --layout-helper "$COLD_V3_LAYOUT_HELPER" --day 2025-03-01 \
  --export "${CONTROL_ROOT}-replica" --pelican-prefix "$CONTROL_PREFIX"
"$V3_PYTHON" -I sage-data-rework/tiered_system/demo/tools/seed_cold.py \
  --endpoint http://127.0.0.1:18184 --root "$TIME_BATCH_ROOT" \
  --layout plugin-vsn-time --batch-order time --layout-helper "$COLD_V3_LAYOUT_HELPER" \
  --start 2025-03-01 --end 2025-03-01 \
  --export "${TIME_BATCH_ROOT}-replica" --pelican-prefix "$TIME_BATCH_PREFIX"
```

Only after successful publication, start dedicated comparison sidecars with
the exact running baseline API image. The function below affects only its
named project; it does not recreate the original baseline or exp73 candidate.

```sh
export COMPARE_API_IMAGE="$(docker inspect --format '{{.Image}}' demo-data-api)"
export CONTROL_URL="https://nrdstor.nationalresearchplatform.org:8443/sage${CONTROL_PREFIX}/warehouse"
export ARCHIVE_PLUGIN_URL="https://nrdstor.nationalresearchplatform.org:8443/sage/cold-storage-demo-v3-plugin-trial-1792792556/warehouse"
export TIME_PLUGIN_URL="https://nrdstor.nationalresearchplatform.org:8443/sage${TIME_BATCH_PREFIX}/warehouse"
export ARCHIVE_AUDIT="$PWD/sage-data-rework/scratch/v3-plugin-trial-1792792556/layout-audit.json"
start_layout_api() {
  mkdir -p "$4"
  LAYOUT_API_IMAGE="$COMPARE_API_IMAGE" LAYOUT_API_PORT="$2" LAYOUT_COLD_WAREHOUSE="$3" \
    LAYOUT_PROFILE_HOST="$4" LAYOUT_PROFILE_DIR="$5" \
    docker compose -p "$1" -f sage-data-rework/tiered_system/demo/docker-compose.layout-trial.yml up -d --no-deps candidate-api
}
start_layout_api sage-layout-control 10002 "$CONTROL_URL" "$CONTROL_ROOT/unprofiled-logs" ""
start_layout_api sage-layout-time-batches 10003 "$TIME_PLUGIN_URL" "$TIME_BATCH_ROOT/unprofiled-logs" ""
"$V3_PYTHON" -I sage-data-rework/tiered_system/experiments/exp73_plugin_live_compare.py \
  --baseline http://127.0.0.1:10002/api/v1/query --candidate http://127.0.0.1:10001/api/v1/query \
  --baseline-audit "$CONTROL_ROOT/layout-audit.json" --candidate-audit "$ARCHIVE_AUDIT" \
  --require-matched-coverage --api-image "$COMPARE_API_IMAGE" --reps 3 \
  --output sage-data-rework/tiered_system/experiments/results/exp73_matched_archive_batches.json
"$V3_PYTHON" -I sage-data-rework/tiered_system/experiments/exp73_plugin_live_compare.py \
  --baseline http://127.0.0.1:10002/api/v1/query --candidate http://127.0.0.1:10003/api/v1/query \
  --baseline-audit "$CONTROL_ROOT/layout-audit.json" --candidate-audit "$TIME_BATCH_ROOT/layout-audit.json" \
  --require-matched-coverage --api-image "$COMPARE_API_IMAGE" --reps 3 \
  --output sage-data-rework/tiered_system/experiments/results/exp73_matched_time_batches.json
```

These are the primary unprofiled Arrow/NDJSON comparisons. The coverage gate
requires equal row counts, timestamp extent and snapshot counts. All eight
query shapes must still match content. The exp73 harness also accepts repeated
`--case` and `--format` flags for targeted runs.

For paired server profiles, use separate profiling sidecars and fresh mounts.
`exp75_paired_live_profiles.py` pairs each Arrow request with exactly one new,
complete backend timing/native profile pair from that sidecar. It refuses
missing/incomplete pairs, concurrent extra profiles or unequal coverage. It
rotates requests and excludes one warmup per layout/case. Profiles are
associated with the exact query shape; old log files are ignored. Full content
is checked on the first measured body per layout/case; counts/schema are
checked on every response. These timings are **profiling-on diagnostics**.

```sh
start_layout_api sage-profile-control 10004 "$CONTROL_URL" "$CONTROL_ROOT/paired-profiles" /profiles
start_layout_api sage-profile-candidate 10005 "$ARCHIVE_PLUGIN_URL" "$TIME_BATCH_ROOT/archive-paired-profiles" /profiles
"$V3_PYTHON" -I sage-data-rework/tiered_system/experiments/exp75_paired_live_profiles.py \
  --baseline http://127.0.0.1:10004/api/v1/query --candidate http://127.0.0.1:10005/api/v1/query \
  --baseline-profiles "$CONTROL_ROOT/paired-profiles" --candidate-profiles "$TIME_BATCH_ROOT/archive-paired-profiles" \
  --baseline-audit "$CONTROL_ROOT/layout-audit.json" --candidate-audit "$ARCHIVE_AUDIT" \
  --api-image "$COMPARE_API_IMAGE" --reps 3 \
  --output sage-data-rework/tiered_system/experiments/results/exp75_archive_batches_profiles.json
start_layout_api sage-profile-candidate 10005 "$TIME_PLUGIN_URL" "$TIME_BATCH_ROOT/time-paired-profiles" /profiles
"$V3_PYTHON" -I sage-data-rework/tiered_system/experiments/exp75_paired_live_profiles.py \
  --baseline http://127.0.0.1:10004/api/v1/query --candidate http://127.0.0.1:10005/api/v1/query \
  --baseline-profiles "$CONTROL_ROOT/paired-profiles" --candidate-profiles "$TIME_BATCH_ROOT/time-paired-profiles" \
  --baseline-audit "$CONTROL_ROOT/layout-audit.json" --candidate-audit "$TIME_BATCH_ROOT/layout-audit.json" \
  --api-image "$COMPARE_API_IMAGE" --reps 3 \
  --output sage-data-rework/tiered_system/experiments/results/exp75_time_batches_profiles.json
```

The default paired cases are fleet minute/hour and all-plugins/vsn minute.
Use `--case series_day` for the filtered control. Compare scan/row-group/read
metrics, preparation/hint time, projection, sort, spill and memory separately.
Do not add parallel operator work to wall phases. Record writer peak RSS and
seed/audit/publication durations; the original 687.93 s was not a complete
upload-inclusive seed time.

This session cannot access Docker, and localhost port 10001 is unreachable.
The new REST policy test, control publication, chronological candidate
publication and paired live HTTP runs therefore remain **pending on the VM**.
Local native results below are not claims about published candidates or
end-to-end performance. No remote objects, existing catalogs or APIs were
modified while preparing this follow-up.

#### Completed local chronological-batch experiment (2026-10-23)

The complete March 1 physical candidate was built in the fresh local directory
`scratch/exp76-time-batches-20261023/data`. This is **local Parquet only**, not
a published Iceberg warehouse. Source and staged-day fingerprints matched;
every fixed batch matched before/after packing; the final complete raw-row
fingerprint matched the original day. Nanoseconds, VARIANT and metadata survive.

- **140,578,674 rows**, **15 files**, **565 plugin row groups**, no mixed groups.
- Fourteen files contain exactly 10 million rows; the last contains 578,674.
- **456,518,307 bytes**, versus 730,100,005 for archive-order plugin batching
  and 731,299,180 for the original time-first day.
- File time ranges are ordered and non-overlapping, with equal timestamps
  allowed at boundaries. Plugin/vsn/time order and VARIANT field ID 11 pass.
- Build plus validation took **383.54 s locally**. Sampled native subprocess
  RSS peaked at **6,324,535,296 bytes**; Go helper RSS at **373,899,264 bytes**.
  The native process used a 6 GB DuckDB limit; that limit is not total RSS.
  These local physical-build measurements are not directly comparable with
  the previous VM seeding time and exclude catalog commits/upload.
- Build artifact: `results/exp76_time_batch_layout.json`.

A fresh three-way comparison then completed **36 unprofiled runs and 12
separate profiles**. Every public schema/row signature matched across layouts,
including the profiled runs. Three-repetition medians, with the same local
native Go/DuckDB 2.0 path and 3 GB query limit:

| case | original time-first | archive-order plugin batches | chronological plugin batches |
| --- | ---: | ---: | ---: |
| series day | 9.890 s | 1.010 s | 1.762 s |
| all plugins / one vsn / minute | 0.034 s | 0.433 s | 0.124 s |
| fleet minute | 0.115 s | 1.111 s | 0.265 s |
| fleet hour | 3.883 s | 4.720 s | 6.074 s |
| plugin only / all nodes / hour | 3.821 s | 4.508 s | 6.276 s |
| plugin + W0A0 / day | 15.205 s | 5.884 s | 6.883 s |

Chronological batches improve minute queries **3.5–4.2x relative to the
archive-order candidate**. They retain a **5.6x** series-day advantage over
time-first, but lose part of archive-order's filtered-day gain. Fleet hour
gets **29% slower than archive-order** despite much less scanning. The layout
is therefore **not a universal improvement and remains opt-in**.

The paired profiles distinguish these effects:

- Fleet minute footer eligibility drops from about **140 million** to
  **10 million** rows. Engine reads drop from about **727 MB to 36 MB**,
  and scan work from **2.031 s to 0.181 s**.
- Fleet hour similarly drops from about **140 million** to **10 million**
  eligible rows; engine reads from **728 MB to 38 MB**, scan work from
  **2.166 s to 0.348 s**, and sort work from **0.575 s to 0.342 s**. Projection
  work remains about **4.2–4.4 s**, yet elapsed time rises. This is consistent
  with lower useful parallelism: the first chronological file contains a
  **9,590,724-row group for the dominant wxt plugin**, concentrating most of
  the hour's result work in one group. DuckDB parallelizes Parquet work at
  row-group boundaries. This is a supported explanation, not a per-thread
  attribution measurement. See [DuckDB 2.0 row-group guidance](https://duckdb.org/docs/2.0/guides/performance/file_formats#the-effect-of-row-group-sizes).
- Series-day footer eligibility grows from **39.9 million** to **134.7 million**
  rows. The archive stream's batches also had narrower metric-name ranges;
  chronological batches mix those metric names into every time window. The
  name filter consequently prunes fewer groups: **4 → 15**. This explains why
  improving time pruning can weaken metric-name pruning under the same plugin
  grouping contract.

Artifact: `results/exp74_time_batches_scan_profiles.json`. These results support
keeping the chronological policy available for the VM experiment, **not
changing the default, the one-group-per-plugin contract or the 10-million-row
file limit**. Paired live profiles with matched one-day catalogs remain the
next external validation; no existing warehouse/API was changed. Any later
proposal to split a dominant plugin into smaller row groups or change the file
limit needs a separate design decision.

Offline validation passed: batch-policy sequencing and failure guards,
chronological-range rejection, control copy/day-boundary checks, profile pairing
and coverage gates, physical VARIANT/order tests and existing publisher tests.
Both REST integration cases are skipped here because no local REST fixture is
reachable; the earlier archive-order case already passed on the VM, while the
new chronological case still needs its VM run.


#### Plugin query extension to the local table (exp74)

Added a targeted run of the same three read-only datasets and the same native
DuckDB 2.0 / Go SQL exporter binaries (SHA-256 identities match the preceding
four-case comparison). Three unprofiled repetitions rotate layout order, with
one separate profile per case/layout. The two new rows above come from this
follow-up run; the original four rows have not been rerun or overwritten.

Both queries use plugin
`registry.sagecontinuum.org/jrobrien/waggle-wxt536:0.24.11.14`:

- **Plugin only / all nodes / hour:** March 1 00:00–01:00 UTC, filtering only
  `plugin`, with no name or vsn filter. Returns **5,767,699 rows**, about 96% of
  the fleet-hour result. This dominant plugin therefore has low selectivity
  for a fleet query. Its medians are **3.821 / 4.508 / 6.276 s** for original
  time-first / archive-order plugin / chronological plugin layouts.
- **Plugin + W0A0 / day:** March 1 00:00 through March 2 00:00 UTC, filtering
  `plugin` and `vsn=W0A0`, with no name filter. Returns **9,436,419 rows**.
  Medians are **15.205 / 5.884 / 6.883 s**. Both grouped layouts benefit this
  more selective query, by about **2.6x / 2.2x** versus time-first.

All **18 unprofiled responses and six profiled responses** match the public
schema and row signatures across layouts. The plugin-only hour profile has no
sort spill in any layout. Actual scanned row-group counts are **26 / 14 / 1**
for time-first / archive-order plugin / chronological plugin. The last layout
therefore supplies only one row group for the dominant query work.
Chronological batching reduces its scan work from
2.117 s to 0.356 s relative to archive-order batching, while projection work
stays around 4.1–4.3 s and elapsed time grows. This reinforces the parallelism
concern rather than proving a resource-exhaustion problem. The plugin/vsn day
query spills in all three layouts under the fixed 3 GB budget; its engine byte
counters include local spill traffic and are not network measurements.

The docs' aggregation microbenchmark tests 960–1,966,080 rows per group. It
separately warns that one large group limits parallelism. Our largest groups
are approximately 10 million rows, with strong plugin skew, and our layouts
also change file membership and metric clustering. These results therefore do
not contradict that microbenchmark or isolate row-group size as the sole
cause. [DuckDB 2.0 documentation](https://duckdb.org/docs/2.0/guides/performance/file_formats#the-effect-of-row-group-sizes).

Tooling: exp74 now accepts `--case plugin_hour --case plugin_vsn_day` and an
optional `--plugin` override; the default is the recorded exp73 plugin. Exact
query ranges and filters are included in the JSON report. Artifact:
`results/exp74_plugin_queries_scan_profiles.json`. These remain local native
Arrow/Zstd measurements, excluding Iceberg metadata, HTTP and Pelican. No
warehouse files, catalogs, APIs, git commits or pushes were changed by this
measurement.

### Bounded plugin-pure row groups (exp77–78)

The agreed policy now **drops exactly one group per plugin per file**. A group
flushes when it reaches its configured cap **or when the plugin changes**.
Large plugins occupy multiple consecutive groups; small plugins continue to
share files through separate groups. These are maximum group sizes, not a
minimum: small plugins and the final group for a plugin/file can have fewer
than 100,000 rows. No group mixes plugins.

Unchanged: `day(time)` Iceberg partitions, a 10-million-row maximum per file,
plugin → vsn → time order inside each file, nanosecond timestamps, the public
response schema, logical `value VARIANT` and Iceberg field ID 11. Vsn/time order
continues across adjacent groups belonging to the same plugin. A file's input
batch is fixed before sorting/grouping.

#### Tooling

- Go `layoutpack --row-group-rows N` accepts caps **100,000–1,000,000**. Arrow's
  buffered writer splits at the cap, while plugin changes explicitly flush.
  The initial default for new opt-in plugin trials is 262,144. This does not
  change the overall seeder's default time-first layout or any existing data.
- `seed_cold.py --layout plugin-vsn-time --row-group-rows N` records
  `sage.layout.version=bounded-plugin-v1`, `sage.row-group.rows=N` and the
  chosen `sage.batch.order`. Append refuses an uncapped table, a different
  cap, or a different batch policy; use a fresh root/prefix for each trial.
- Audits enforce plugin purity, the cap, consecutive plugin ordering and
  vsn/time order **across group boundaries**. A nonfinal group for a plugin
  must be full; a short group can end that plugin or file. Existing field-ID,
  VARIANT, timestamp, file-limit and chronological-file-range checks remain.
  Reports include `bounded_plugin_groups`, `plugin_pure_groups`,
  `row_group_row_limit` and observed `max_row_group_rows`.
- Live comparison accepts capped audited candidates. Historical uncapped
  audit files remain usable as read-only comparison controls, not as append
  targets for the new writer. Rebuild the Go helper before a new seed.
- The native physical test splits a 200,005-row plugin into **100,000 /
  100,000 / 5**, followed by two small-plugin groups. Mixed VARIANT values,
  >2^53 integers, metadata, NULLs and nanoseconds round-trip with unchanged
  raw-row fingerprints. Small plugin groups remain separate and share a file.

#### Controlled local experiment

`exp77_bounded_plugin_groups.py` repacks both existing 15-file plugin layouts
at **100,000, 262,144, 524,288 and 1,000,000** rows per group. It preserves the
exact file membership and plugin/vsn/time ordering for each policy, so it
isolates the cap from rebatching. The input files are immutable; each output
is in a fresh local directory. Every file's full raw-row fingerprint must
match its source, and the complete output receives the new bounded audit.
The result is eight candidates, each covering the same 140,578,674-row day.
This is a physical Parquet experiment, not a new published Iceberg table.

`exp78_bounded_layout_benchmark.py` requires all builds to finish before
running exp74. It compares the eight candidates with the original time-first
March 1 files, using the same native DuckDB 2.0 / current Go SQL exporter path,
3 GB query budget, two workers, async=8 and Zstd IPC. Repetitions rotate layout
order. Primary timings exclude profiling, with separate diagnostic profiles.
Every response's public schema and row signature must match across layouts.

The nine query shapes retain all eight original exp73 shapes plus plugin-only
hour: series minute/hour/day, plugin+vsn day, all-plugins/vsn minute/hour,
fleet minute/hour and plugin-only hour. Reports show per-query ratios to
time-first and worst-case ratio rather than hiding regressions in an average.
A median win from three repetitions is an observation, not a significance
claim or evidence about end-to-end Pelican/HTTP performance.

```sh
export V3_PYTHON=python
export BOUNDED_ID="$(date +%s)"
export COLD_V3_LAYOUT_HELPER="$PWD/sage-data-rework/scratch/bin/layoutpack-bounded"
mkdir -p "$(dirname "$COLD_V3_LAYOUT_HELPER")"
(cd sage-data-rework/tiered_system/experiments/icebergo && go build -o "$COLD_V3_LAYOUT_HELPER" ./cmd/layoutpack)
"$V3_PYTHON" -I sage-data-rework/tiered_system/experiments/exp77_bounded_plugin_groups.py \
  --dataset archive=sage-data-rework/scratch/v3-plugin-trial-1792792556/sage/obs/data \
  --dataset time=sage-data-rework/scratch/exp76-time-batches-20261023/data \
  --native-probe /absolute/path/to/duckdb200.test --layout-helper "$COLD_V3_LAYOUT_HELPER" \
  --work-dir "sage-data-rework/scratch/exp77-bounded-${BOUNDED_ID}" \
  --output sage-data-rework/tiered_system/experiments/results/exp77_bounded_plugin_groups.json
"$V3_PYTHON" -I sage-data-rework/tiered_system/experiments/exp78_bounded_layout_benchmark.py \
  --build-report sage-data-rework/tiered_system/experiments/results/exp77_bounded_plugin_groups.json \
  --baseline sage-data-rework/scratch/v3-1792707566/sage/obs/data \
  --native-probe /absolute/path/to/duckdb200.test --sql-probe /absolute/path/to/v3-sql.test \
  --reps 3 --output sage-data-rework/tiered_system/experiments/results/exp78_bounded_layout_benchmark.json
```

These local binaries are the existing Go evaluation and SQL-exporter probes,
not DuckDB CLI executables. The SQL exporter must match the API source. Locally,
the retained chronological source already exists; on a different VM, first
build it using exp76 (which now also accepts a cap), or prepare a chronological
trial source. Exp77 records source inventories and helper/native binary hashes.
Use distinct result filenames if retaining previous runs.

Before live promotion, seed the chosen cap/batch policy through a **fresh**
REST catalog/root and isolated Pelican prefix, passing `--row-group-rows N`
and `--batch-order archive|time`. Run the REST round-trip tests using the newly
built helper, then use the byte-identical one-day time-first control and
paired sidecars described above. No current warehouse is rewritten, and no
existing API is repointed by these tools. The VM/HTTP phase remains separate
from the local native measurements below.

#### Completed physical builds

All eight full-day builds completed in `scratch/exp77-bounded-groups-20261023`.
Each retains 15 files and **140,578,674 rows**. All 120 output-file fingerprints
match their respective inputs. Aggregating those fingerprints also matches
the original time-first complete-day fingerprint for every candidate. Each
passes the group-cap/purity/order/VARIANT/ns audit.

| batch policy | cap | row groups | Parquet bytes |
| --- | ---: | ---: | ---: |
| archive | 100,000 | 1,449 | 1,023,457,798 |
| archive | 262,144 | 584 | 885,503,238 |
| archive | 524,288 | 316 | 805,926,039 |
| archive | 1,000,000 | 186 | 764,970,406 |
| chronological | 100,000 | 1,907 | 455,992,582 |
| chronological | 262,144 | 1,071 | 465,676,466 |
| chronological | 524,288 | 818 | 471,346,809 |
| chronological | 1,000,000 | 691 | 477,592,158 |

The original time-first day is 731,299,180 bytes in two files and 531 groups.
Thus the cap has a storage tradeoff as well as an execution effect. Do not
assume the smallest cap is automatically best.

The committed local SQLite-catalog v3 round trip also passed with a
200,010-row mixed-VARIANT fixture and five groups, including a large plugin
split at 100,000. Native DuckDB 2.0 `iceberg_scan` reproduces the complete
raw-row fingerprint. Artifact: `results/exp77_v3_bounded_native_validation.json`.
This is a real local v3 registration test, not a REST/HTTP/Pelican result.

For nine layouts and three timing repetitions, exp74 now spreads rotations
by three positions: each layout gets an early, middle and late position.
The two-/three-layout historical schedules are unchanged. OS caches remain
uncontrolled, and profiles are separate from primary timings.

#### Completed bounded-group query sweep (exp78, 2026-10-23)

All **243 unprofiled runs and 81 separate profiles** completed: nine layouts, nine query shapes, three timing repetitions. Every response passed public-schema, nanosecond ordering and full row-signature validation across layouts. The same source day, native DuckDB 2.0/Go SQL binaries, 3 GB query limit, two workers, async=8 and Zstd were used. No builder or heavy validation job ran concurrently with the native timed queries.

**Recommended next live candidate: chronological file batches + plugin/vsn/time sorting + a 262,144-row cap.** It was the only candidate whose observed median was faster than time-first for all nine shapes. The fleet-hour and plugin-only-hour differences are only 2–4%; treat those as approximately tied at three repetitions, not statistically established wins.

| query | time-first | chronological / 262,144 | observed speedup |
| --- | ---: | ---: | ---: |
| series / minute | 0.0283 s | 0.0200 s | 1.42x |
| series / hour | 0.4530 s | 0.0623 s | 7.27x |
| series / day | 9.9130 s | 1.0577 s | 9.37x |
| plugin + vsn / day | 15.6594 s | 6.1790 s | 2.53x |
| all plugins + vsn / minute | 0.0339 s | 0.0249 s | 1.36x |
| all plugins + vsn / hour | 0.6431 s | 0.2911 s | 2.21x |
| fleet / minute | 0.1160 s | 0.1081 s | 1.07x |
| fleet / hour | 3.9313 s | 3.8523 s | 1.02x |
| plugin only / hour | 3.8252 s | 3.6734 s | 1.04x |

The chosen physical candidate has **15 files, 1,071 plugin-pure row groups and 465,676,466 bytes**—about **36.3% smaller** than the original time-first day. The observed maximum group size is exactly 262,144. Its helper peak RSS was sampled at 351,125,504 bytes during repacking; this is not a combined seeder/engine memory bound.

Unlike the previous single-group policy, the dominant plugin now spans bounded groups. This restores useful execution parallelism while keeping chronological file pruning. The archive-order variants remain fastest for some series queries, but none handles this mixed workload as well: their fleet-minute medians are 2.1–7.2x slower than time-first.

##### Full cap matrix: archive-order file batches

All values below are unprofiled seconds from this sweep, not earlier exp73/74 runs.

| query | time-first | 100,000 | 262,144 | 524,288 | 1,000,000 |
| --- | ---: | ---: | ---: | ---: | ---: |
| series / minute | 0.0283 | 0.0256 | 0.0190 | 0.0192 | 0.0260 |
| series / hour | 0.4530 | 0.0643 | 0.0462 | 0.0459 | 0.0629 |
| series / day | 9.9130 | 0.5620 | 0.5477 | 0.5407 | 0.7402 |
| plugin + vsn / day | 15.6594 | 5.3239 | 5.3534 | 5.4320 | 5.6253 |
| all plugins + vsn / minute | 0.0339 | 0.0383 | 0.0496 | 0.0695 | 0.1152 |
| all plugins + vsn / hour | 0.6431 | 0.2523 | 0.2564 | 0.2711 | 0.3263 |
| fleet / minute | 0.1160 | 0.2410 | 0.3498 | 0.5016 | 0.8322 |
| fleet / hour | 3.9313 | 3.9485 | 4.1002 | 4.1989 | 4.4668 |
| plugin only / hour | 3.8252 | 3.7081 | 3.8139 | 4.1970 | 4.5598 |

##### Full cap matrix: chronological file batches

| query | time-first | 100,000 | 262,144 | 524,288 | 1,000,000 |
| --- | ---: | ---: | ---: | ---: | ---: |
| series / minute | 0.0283 | 0.0305 | 0.0200 | 0.0176 | 0.0188 |
| series / hour | 0.4530 | 0.0726 | 0.0623 | 0.0741 | 0.0944 |
| series / day | 9.9130 | 1.0690 | 1.0577 | 1.0599 | 1.1076 |
| plugin + vsn / day | 15.6594 | 6.1747 | 6.1790 | 6.3218 | 6.2228 |
| all plugins + vsn / minute | 0.0339 | 0.0342 | 0.0249 | 0.0230 | 0.0237 |
| all plugins + vsn / hour | 0.6431 | 0.2602 | 0.2911 | 0.3332 | 0.4055 |
| fleet / minute | 0.1160 | 0.1090 | 0.1081 | 0.1156 | 0.1329 |
| fleet / hour | 3.9313 | 3.9272 | 3.8523 | 3.9511 | 3.9220 |
| plugin only / hour | 3.8252 | 3.7319 | 3.6734 | 3.8195 | 3.6842 |

Artifact: `results/exp78_bounded_layout_benchmark.json`. The JSON includes all repetitions, separate profiles, per-query ratios and worst-case ratios; ratios below 1 are faster. No workload weighting is assumed.

##### Next VM validation

The local result is strong enough to choose the next candidate, but does not measure Iceberg planning, Pelican/HTTP latency or Sage-client dataframe decoding. The published baseline and existing APIs stay unchanged. Use a fresh REST root/catalog and prefix; pass these explicit flags to the existing seeding/sidecar procedure:

```sh
--layout plugin-vsn-time --batch-order time --row-group-rows 262144
```

Rebuild `icebergo/cmd/layoutpack` for the VM and run both isolated REST round trips first. The new table must report `sage.layout.version=bounded-plugin-v1`, `sage.batch.order=time`, `sage.row-group.rows=262144`, and pass the bounded audit. Compare it to the byte-identical one-day time-first control using `exp73 --require-matched-coverage`, then collect dedicated paired live profiles with exp75. Do not append the new policy to an old uncapped table or repoint the full March/April baseline. The overall seeder layout default remains time-first; this candidate is opt-in.

Offline bounded writer/auditor, profile gate and rotation checks passed. A real local v3 registration/read round trip also passed. The two REST integration cases are skipped in this session because Docker is unavailable; VM publication and end-to-end measurements remain pending. No remote objects, existing warehouse files, catalogs or APIs were modified; no commits or pushes were made.

#### Completed VM validation (2026-10-24)

Both isolated REST round trips passed against a fresh catalog on port 18184, archive batch order and time batch order. The helper was rebuilt as `scratch/bin/layoutpack-bounded`. The full March/April baseline and the uncapped plugin trial were not appended or repointed.

The byte-identical time-first control is one snapshot, 2 files, 531 groups, 140,578,674 rows, 731,299,180 bytes, at `/cold-storage-demo-v3-time-control-1792855974`. The candidate committed in 583.44 s and was published at `/cold-storage-demo-v3-plugin-time-batches-1792855974`. Its table properties are `sage.layout.version=bounded-plugin-v1`, `sage.batch.order=time`, and `sage.row-group.rows=262144`. The bounded audit passed: 15 files, 1,071 plugin-pure groups, max group 262,144, 458,718,589 bytes, same row count and March 1 extent, `time_batch_ranges_checked=true`.

Unprofiled Arrow medians, matched coverage, three repetitions. `first` is time to the first record. The same API image served both sides. Content matched across layouts.

| query | time-first | chronological / 262,144 |
| --- | ---: | ---: |
| series / minute | 0.230 s | 0.201 s |
| series / hour | 0.613 s | 0.242 s |
| series / day | 9.506 s | 1.211 s |
| plugin + vsn / day | 25.667 s | 8.376 s |
| all plugins + vsn / minute | 0.222 s | 0.231 s |
| all plugins + vsn / hour | 0.767 s | 0.437 s |
| fleet / minute | 0.276 s | 0.283 s |
| fleet / hour | 3.972 s | 4.115 s |

The long series reads are the live wins: a day of one series is about 7.9x, and the plugin-plus-node day is about 3.1x. The one-minute fleet and one-minute all-plugins reads are tied with time-first. Fleet hour is a few percent slower. That is a smaller sweep than the local result, where this candidate was ahead on every shape; HTTP and Iceberg planning take the short queries.

While the previous uncapped API was still running, the 14 GB candidate sidecar was OOM-killed (exit 137) on a plugin-plus-node day request. After that other API was stopped, the same query completed. The sidecar was restarted afterward.

Paired profiles, profiling on, separate from the table above. Medians of three measured repetitions. `groups` is `row_groups_scanned`.

| query | time-first groups | candidate groups | time-first | candidate |
| --- | ---: | ---: | ---: | ---: |
| series / day | 531 | 50 | 9.751 s | 1.285 s |
| all plugins + vsn / minute | 1 | 14 | 0.225 s | 0.208 s |
| fleet / minute | 1 | 46 | 0.331 s | 0.308 s |
| fleet / hour | 26 | 70 | 5.431 s | 4.850 s |

A one-series day opens 50 groups instead of all 531. A one-minute fleet read still opens 46 candidate groups against 1 time-first group, and the wall times meet. Artifacts: `results/exp73_matched_archive_batches.json`, `results/exp73_matched_time_batches_plugin_vsn_day.json`, `results/exp73_matched_time_batches_rest.json`, `results/exp75_time_batches_profiles.json`. The seeder default remains time-first.

### Retained layout: full March/April validation (2026-10-25)

Retain **chronological file batches, plugin → vsn → time sorting, and a
262,144-row plugin-pure cap**. Keep day partitions and the 10-million-row file
limit. This is the selected candidate policy; the exporter default and current
full time-first warehouse remain unchanged until full validation succeeds.
The VM's candidate day was 37.3% smaller. Its total Arrow response improvements
were 6.88x for series day and 2.56x for plugin/node day; the preceding table's
7.9x/3.1x numbers describe first-record latency, not complete response time.

The full candidate must contain all **61 days, 8,457,186,954 rows**, with one
daily snapshot per day, matching the full baseline. Build in a fresh catalog
and workspace root, then publish under a new prefix. Do not append to the
March-1-only trial or modify `/cold-storage-demo-v3-full-1792764935`. No
`/node-data` access is part of this experiment.

The commands below are prepared, **not executed in this session**: Docker's
socket is inaccessible here. Full validation results are pending VM access.
Run from the workspace root on the VM with DuckDB Python 2.0.x and the v3
requirements installed. Preserve these variables for the whole run:

```sh
export V3_PYTHON=python
export FULL_LAYOUT_ID="$(date +%s)"
export FULL_LAYOUT_ROOT="$PWD/sage-data-rework/scratch/v3-bounded-full-${FULL_LAYOUT_ID}"
export FULL_LAYOUT_PREFIX="/cold-storage-demo-v3-bounded-full-${FULL_LAYOUT_ID}"
export FULL_LAYOUT_REPORTS="$PWD/sage-data-rework/tiered_system/experiments/results/full-layout-${FULL_LAYOUT_ID}"
export COLD_V3_LAYOUT_HELPER="$PWD/sage-data-rework/scratch/bin/layoutpack-bounded-full-${FULL_LAYOUT_ID}"
export REST_FIXTURE_IMAGE=apache/iceberg-rest-fixture@sha256:db8de90b5b7693d4ac334c336f91d9bbe320d7b19f4f514d26de84cdfbcbfe8d
mkdir -p "$FULL_LAYOUT_ROOT" "$(dirname "$COLD_V3_LAYOUT_HELPER")"
(cd sage-data-rework/tiered_system/experiments/icebergo && go build -o "$COLD_V3_LAYOUT_HELPER" ./cmd/layoutpack)
COLD_V3_ROOT="$FULL_LAYOUT_ROOT" COLD_V3_PORT=18185 COLD_V3_IMAGE="$REST_FIXTURE_IMAGE" \
  docker compose -p sage-v3-bounded-full -f sage-data-rework/tiered_system/demo/docker-compose.v3.yml up -d
COLD_V3_LAYOUT_ENDPOINT=http://127.0.0.1:18185 COLD_V3_LAYOUT_ROOT="$FULL_LAYOUT_ROOT" \
  "$V3_PYTHON" -m unittest discover -s sage-data-rework/tiered_system/demo/tools -p test_v3_layout.py -v
```

Both REST round trips must pass, not skip. Check free disk before seeding:
budget candidate plus replica plus one day's external-sort spill, in addition
to existing source data. The 61-day run can take many hours; retain its log.
Do not run seed/audit jobs concurrently with timed queries.

```sh
"$V3_PYTHON" -I sage-data-rework/tiered_system/demo/tools/seed_cold.py \
  --endpoint http://127.0.0.1:18185 --root "$FULL_LAYOUT_ROOT" \
  --layout plugin-vsn-time --batch-order time --row-group-rows 262144 \
  --layout-helper "$COLD_V3_LAYOUT_HELPER" \
  --start 2025-03-01 --end 2025-04-30 \
  --export "${FULL_LAYOUT_ROOT}-replica" --pelican-prefix "$FULL_LAYOUT_PREFIX" \
  > "${FULL_LAYOUT_ROOT}/seed.log" 2>&1
```

The existing seeder checks every committed day's full raw-row fingerprint
against the archive and audits the complete layout before publication. After
interruption, inspect `seed-v3-pending.json` and the authoritative catalog;
never blindly rerun or delete the journal. The existing explicit append and
registration-recovery procedure applies only to confirmed remaining empty
days with exactly the same policy. A failed publication uses a fresh export
directory/prefix after inspecting the recorded source head.

Independently compare **all stored rows** with the original time-first table,
including raw VARIANT values, metadata, promoted tags and nanosecond times:

```sh
export FULL_BASELINE_HEAD="$("$V3_PYTHON" -c 'import json,sys; print(json.load(open(sys.argv[1]))["source_metadata"])' sage-data-rework/scratch/v3-1792707566/seed-v3-progress.json)"
export FULL_CANDIDATE_HEAD="$("$V3_PYTHON" -c 'import json,sys; print(json.load(open(sys.argv[1]))["source_metadata"])' "$FULL_LAYOUT_ROOT/seed-v3-progress.json")"
"$V3_PYTHON" -I sage-data-rework/tiered_system/experiments/exp79_full_warehouse_validation.py \
  --baseline-head "$FULL_BASELINE_HEAD" --candidate-head "$FULL_CANDIDATE_HEAD" \
  --output-dir "$FULL_LAYOUT_REPORTS"
```

Exp79 reads local committed heads only and writes fresh local reports. It
checks both inventories, the retained policy, matching counts for every UTC
day, candidate group purity/order/cap, and daily count/hash-sum/hash-XOR over
every stored row. It uses DuckDB 2.0, UTC, two threads and a 3 GB query budget.
It emits usable `baseline-audit.json` and `candidate-audit.json` only after all
61 days match; `validation.json` records completed days and failures. These
fingerprints are correctness checks, not timed query results.

Only after successful publication and exp79, start a dedicated candidate API
with the baseline's exact image. Keep the existing API at port 10000 unchanged.
Check available VM memory and existing experiment sidecars before starting:
the earlier OOM means container limits alone do not establish safe aggregate
memory use. Do not change unrelated services to make room.

```sh
export COMPARE_API_IMAGE="$(docker inspect --format '{{.Image}}' demo-data-api)"
mkdir -p "$FULL_LAYOUT_ROOT/unprofiled-logs"
LAYOUT_API_IMAGE="$COMPARE_API_IMAGE" LAYOUT_API_PORT=10006 \
  LAYOUT_COLD_WAREHOUSE="https://nrdstor.nationalresearchplatform.org:8443/sage${FULL_LAYOUT_PREFIX}/warehouse" \
  LAYOUT_PROFILE_HOST="$FULL_LAYOUT_ROOT/unprofiled-logs" LAYOUT_PROFILE_DIR="" \
  docker compose -p sage-layout-bounded-full -f sage-data-rework/tiered_system/demo/docker-compose.layout-trial.yml up -d --no-deps candidate-api
"$V3_PYTHON" -I sage-data-rework/tiered_system/experiments/exp73_plugin_live_compare.py \
  --full-warehouse --baseline http://127.0.0.1:10000/api/v1/query \
  --candidate http://127.0.0.1:10006/api/v1/query \
  --baseline-audit "$FULL_LAYOUT_REPORTS/baseline-audit.json" \
  --candidate-audit "$FULL_LAYOUT_REPORTS/candidate-audit.json" \
  --api-image "$COMPARE_API_IMAGE" --format arrow --reps 3 --timeout 7200 \
  --output "$FULL_LAYOUT_REPORTS/http-arrow.json"
```

Before benchmarking, confirm the baseline API's actual COLD_WAREHOUSE is the
recorded full time-first URL, both APIs have the same image/watermark/async/
read-ahead/Zstd settings, and profiling is off. Audits describe storage; they
cannot prove which URL a separately configured endpoint is serving.

The full mode requires equal snapshot counts and per-day inventory counts for
all 61 days, not just equal endpoints of the timestamp range. Its 39 queries
cover the original eight shapes plus plugin-only hour on March 1, March 31,
April 1 and April 30, a series month for each month, and a fleet hour crossing
March/April. `--case` selects explicit names from that suite for staged runs;
retain distinct output filenames. Arrow is the first sweep; repeat selected
cases with `--format ndjson` for direct-HTTP schema/content validation. Full
body signatures are checked outside the timer for the first response per
layout/format/case; counts and schema are checked on every response. Bodies
spool to disk, including the month responses, rather than building a pandas
month dataframe.

Report both **total response** and **first-record** medians, individual repeats,
row counts, body bytes and CPU. Record VM/container memory peaks, OOM counters,
restart counts and seed/audit/publication duration separately. Three repeats
are descriptive; differences of a few percent are near ties. Full validation
passes only with matching content, no unexplained failures/OOM, retained large
filtered-query gains and acceptable fleet-query behavior across both months.
Default promotion and further scan/setup optimization follow the measured
result; no full-warehouse win is claimed yet.

#### Completed full March/April Arrow sweep (2026-10-26)

The 61-day candidate was already committed locally before this sweep. Its audit is 8,457,186,954 rows, 879 files, 61 snapshots, chronological batches, and a 262,144-row cap. Exp79 matched every day's count, hash-sum, and hash-XOR against the time-first warehouse, including VARIANT values, metadata, tags, and nanoseconds, in 22.2 minutes. The published warehouse used here is `/cold-storage-demo-v3-bounded-full-1793018629285065000`. A second identical upload also finished at `/cold-storage-demo-v3-bounded-full-1793018629285077000`. The baseline API stayed on `/cold-storage-demo-v3-full-1792764935`. Both APIs used image `sha256:c1c465a46a16432040956221f36373ff905e482c5eb4a389d51230ef3b97c243`, watermark `2025-05-01T00:00:00Z`, 8 async threads, read-ahead -1, Zstd, and profiling off. Neither container was OOM-killed or restarted. The leftover one-day APIs on ports 10001, 10004, and 10005 were stopped before the sweep so the 16 GB VM had about 8.5 GB available; after the sweep about 5.5 GB was available. No peak-memory sample was taken during the month queries.

Arrow, three repetitions, 39 queries, 234 responses. Every response passed schema and row-count checks; full-body value signatures matched across layouts on the first measured repetition per query/layout (78 validated bodies). Exp79 independently matched the full stored dataset. Day rows below are the median of March 1, March 31, April 1, and April 30. `first` is time to the first record; `total` is the full response.

| query | time-first first | candidate first | time-first total | candidate total |
| --- | ---: | ---: | ---: | ---: |
| series / minute | 0.354 s | 0.392 s | 0.355 s | 0.394 s |
| series / hour | 0.775 s | 0.459 s | 0.784 s | 0.470 s |
| series / day | 9.951 s | 1.444 s | 10.120 s | 1.630 s |
| plugin + vsn / day | 25.061 s | 7.629 s | 27.758 s | 10.169 s |
| all plugins + vsn / minute | 0.343 s | 0.389 s | 0.345 s | 0.390 s |
| all plugins + vsn / hour | 0.895 s | 0.619 s | 0.984 s | 0.707 s |
| fleet / minute | 0.380 s | 0.454 s | 0.407 s | 0.480 s |
| fleet / hour | 3.359 s | 3.327 s | 4.923 s | 4.859 s |
| plugin / hour | 3.230 s | 3.412 s | 4.666 s | 4.849 s |
| March series month | 585.530 s | 70.546 s | 594.017 s | 77.411 s |
| April series month | 609.983 s | 72.941 s | 620.465 s | 82.139 s |
| March/April fleet hour | 3.941 s | 4.490 s | 5.958 s | 6.813 s |

The month reads are the large win: March returned 25,940,207 rows in 594 s versus 77 s, and April returned 31,189,045 rows in 620 s versus 82 s. A series day is about 6.2x on total response. A plugin-plus-node day is about 2.7x. Fleet hour on the four sampled days is a tie. The hour that crosses March 31 into April 1 is about 14% slower on the candidate. One-minute fleet and one-minute all-plugins reads are slower by roughly a tenth of a second. The sweep itself ran 2 hours 10 minutes. Artifact: `results/full-layout-1792962354/http-arrow.json`.

NDJSON checks that finished matched the other layout: March 1 series day (1,048,491 rows), March 1 plugin-plus-node day (9,436,419 rows), March 1 fleet minute (94,654 rows), and April 30 fleet hour (5,803,431 rows). The next request, the March series month as NDJSON, OOM-killed the baseline API at its 14 GB limit (exit 137) after the request was accepted and before a response. The candidate never received that request. The baseline was restarted on the same time-first warehouse. The month NDJSON comparison was not retried. The month-boundary fleet hour then matched in NDJSON: both layouts returned 5,744,480 rows, time-first in 43.7 s and the candidate in 40.8 s. Artifact: `results/full-layout-1792962354/http-ndjson-boundary.json`. Default promotion remains open, and the seeder default remains time-first.

#### Architecture decision and handoff to beehive-data-api (2026-10-26)

**Promote the bounded chronological plugin layout as the Parquet and warehouse
architecture going forward.** The warehouse/layout investigation is complete.
Subsequent implementation and performance work returns to `beehive-data-api`
using the selected full warehouse:

```text
COLD_WAREHOUSE=https://nrdstor.nationalresearchplatform.org:8443/sage/cold-storage-demo-v3-bounded-full-1793018629285065000/warehouse
COLD_WATERMARK=2025-05-01T00:00:00Z
```

The second identical upload is a duplicate replica, not a second active
warehouse. The old time-first warehouse remains a retained comparison artifact.
This decision records the selected architecture and endpoint; it does not
claim that a running API was reconfigured by editing this plan.

The physical contract for future exports is:

- Iceberg format v3, partitioned by `day(time)`.
- Form chronological file batches of at most **10,000,000 rows**, then sort
  each file by **plugin → vsn → time**.
- Keep groups plugin-pure and flush at **262,144 rows or a plugin change**.
  Large plugins occupy multiple groups; small plugins share files through
  separate groups. Final groups can be smaller than 100,000 rows.
- Preserve `time TIMESTAMPTZ_NS`, the single `value VARIANT` at field ID 11,
  promoted tags and `meta MAP(VARCHAR,VARCHAR)`.
- Keep the existing four-field public Arrow/NDJSON schema across hot and cold
  storage. Clients do not need to reconstruct split value fields.

Use these explicit seeder flags for future builds; older tool defaults and
historical time-first commands in this plan are not the adopted policy:

```sh
--layout plugin-vsn-time --batch-order time --row-group-rows 262144
```

The decision is supported by complete stored-row validation across all
**8,457,186,954 rows and 61 days**, plus the full-coverage HTTP Arrow sweep.
Total response times improved **7.67x for March series month** (594.017 →
77.411 s), **7.55x for April series month** (620.465 → 82.139 s), about
**6.2x for series day**, and **2.7x for plugin-plus-node day**. Parquet storage
fell from **44,678,623,625 to 27,458,938,029 bytes**, a **38.5% reduction**.

Accepted tradeoffs: minute queries generally lose tens of milliseconds;
sampled fleet-hour medians are broadly tied; the month-boundary fleet hour is
**14% slower**. This is a workload tradeoff rather than a win on every query.
The candidate has **879 files versus 122** and **63,552 groups versus 32,062**;
file/metadata overhead is a plausible contributor to investigate, not a
measured attribution. Three timing repetitions are descriptive evidence.

The NDJSON month failure belongs to the time-first baseline; the selected
warehouse never received that request. Selected-warehouse NDJSON month
behavior and concurrent-query memory capacity remain unmeasured. No peak
memory sample was taken during the successful Arrow month requests. These
are API validation/performance follow-ups, not a reason to reopen the stored
schema or layout decision.

Resume work in **beehive-data-api** against this warehouse. Priorities are
short-query setup and metadata/preparation overhead, the month-boundary scan
and sort regression, and controlled memory measurements for large NDJSON and
concurrent queries. Preserve the public schema and the adopted warehouse
contract while addressing those costs. No additional layout sweep is planned.
