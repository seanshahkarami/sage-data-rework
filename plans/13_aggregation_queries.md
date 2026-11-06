# Aggregation queries on the tiered backend

Recorded 2026-11-04; implementation started 2026-11-05. Historical findings and
intermediate proposals below are retained as evidence. The current implementation
policy is recorded here and in the final implementation section; production
deployment and the live three-placement runtime gate remain separate steps.

**Selected scope (2026-11-05): no shard-boundary discovery or coordination.**
Use ordinary Flux raw reads for hot staging, retain hot-only Flux and ordinary
extract execution, and document the mixed-kind source-read limitation. Do not
add an API-to-loader metadata endpoint. Implement marked unsigned preservation
through archival proofs and cold/spanning aggregation with the existing wire
schema and read-lease lifecycle. Earlier shard-metadata integration proposals
are superseded by this decision.

## Goal and scope

Support existing aggregation requests after measurements move from Influx to the
Iceberg v3/Pelican warehouse. A caller should keep sending `experimental_func`
and optional `experimental_window` to `POST /api/v1/query`, receiving the existing
`timestamp`, `name`, `value`, `meta` schema in NDJSON or Arrow. The Python client
already forwards both request fields; no new client-side rebuilding is required.

The initial function set is `mean`, `min`, `max`, `sum`, and `count`. Support
cold-only and border-spanning queries. Keep hot-only queries on Flux. Do not
rename the public fields or change the Iceberg schema, VARIANT value column,
metadata MAP, day partitioning, or bounded plugin/vsn/time Parquet layout.

Correctness and compatibility come before partial-aggregate pushdown. The first
executor combines filtered raw input and performs one logical aggregate over the
whole requested range. Optimized per-tier sufficient statistics are a later
candidate measured against this reference, not a prerequisite for restoring the
feature.

A representative request keeps the existing public fields:

```json
{
  "start": "2025-03-01T00:00:00Z",
  "end": "2025-03-02T00:00:00Z",
  "filter": {"name": "wxt.env.temp", "vsn": "W0A0"},
  "experimental_func": "mean",
  "experimental_window": "5m"
}
```

Omit `experimental_window` for one aggregate per series over the entire range.
Arrow selection remains the `Accept: application/vnd.apache.arrow.stream` header;
NDJSON remains available through the existing HTTP negotiation.

## Current behavior verified in this workspace

| Range | Current behavior | Target behavior |
| --- | --- | --- |
| Hot-only deployment | Flux aggregation | Preserve existing Flux path |
| Tiered query starting at/after the border | Flux aggregation | Preserve existing Flux path |
| Query ending at/before the border | Cold backend rejects aggregation | DuckDB aggregates the pinned cold scan |
| Query spanning the border | Router rejects aggregation before querying either tier | DuckDB aggregates cold rows plus staged raw hot rows |

Relevant source paths, relative to `beehive-data-api/`:

| Location | Current responsibility / required change |
| --- | --- |
| `internal/query/types.go` | Public request/record and backend/result contracts |
| `internal/httpapi/query.go` | Request parsing and the two response formats; retain wire contract |
| `internal/backend/hot/query.go` | Flux construction; filters, head/tail, then aggregation |
| `internal/backend/hot/backend.go` | Flux records; unwindowed aggregates expose `_start` as timestamp |
| `internal/backend/tiered/backend.go` | Relative-bound resolution, single-tier routing, spanning rejection |
| `internal/backend/tiered/duration.go` | Existing Flux-style relative duration support |
| `internal/backend/tiered/coordination.go`, `lease.go` | Pinned metadata/frontier/coverage and renewable read grant |
| `internal/backend/cold/backend.go`, `sql.go` | Reject aggregation today; resolve metadata and execute cold SQL |
| `internal/backend/cold/arrow.go`, `arrow_stream.go` | Native Arrow streaming with backend-owned DuckDB and serialized resources |
| `internal/backend/cold/record.go` | NDJSON value JSON preserves large integer precision |
| `internal/arrowio/response.go` | Existing public Arrow schema/encoding |

`start` is required. Relative bounds are resolved from one API clock instant
before tier routing. `end` is exclusive. `end == border` is cold-only and
`start == border` is hot-only. A dynamic query owns one grant, coverage start,
border, generation, and exact cold metadata location; aggregation must use those
same mechanisms rather than refreshing a version hint halfway through execution.

The hot builder validates the five unwindowed functions. Its windowed branch
currently forwards function text to Flux without the same allowlist. Do not
assume the two branches have identical validation. Inventory actual usage of
other Flux window functions before tightening hot-only validation; this plan does
not silently remove existing hot-only calls that Flux currently accepts.

## Corrections to the original sketch

- `count` counts records, including strings, booleans, and nulls. It is not a
  count of successful numeric casts. Use row count over the eligible input.
- Do not use `TRY_CAST(value AS DOUBLE)` as a type policy. It can hide invalid
  input and conversion failures, and converting all integers to doubles loses
  precision. Explicitly inspect native value kinds and dispatch by function.
- DuckDB's default `time_bucket` grid is not universally the Flux grid. Weekly
  alignment differs; unusual fixed intervals can differ too. Set the origin
  explicitly or use exact epoch-based arithmetic.
- Window output must reproduce Flux's query-range clipping, not blindly use
  `bucket_start + width` beyond the requested end. Verify the behavior on the
  actual Influx/Flux version, including the final partial window.
- Empty-window behavior differs between aggregates and selectors. It is part of
  compatibility, not an optional response-size optimization.
- Series identity is measurement plus complete metadata, independent of map key
  insertion order. Promoted columns alone are insufficient.
- Head/tail are applied before aggregation on hot queries. Applying them to
  each tier separately, or to the aggregate output, changes the answer.

Sources: [Flux count](https://docs.influxdata.com/flux/v0/stdlib/universe/count/),
[aggregateWindow](https://docs.influxdata.com/influxdb/v2/reference/flux/stdlib/built-in/transformations/aggregates/aggregatewindow/),
[DuckDB timestamp functions](https://duckdb.org/docs/2.0/sql/functions/timestamp),
and [VARIANT type inspection](https://duckdb.org/docs/2.0/sql/data_types/variant).
The current [Flux implementation](https://github.com/influxdata/flux/blob/master/stdlib/universe/aggregate_window.go)
also intersects generated bounds with the query bounds. This source review is
not a claim that parity was tested on the VM.

## Compatibility contract

### Filtering, series identity, and ordering of operations

Use this order on the combined input:

1. Restrict the measurement range and apply the existing request filters.
2. Reassemble full public metadata from promoted tags and the remaining MAP.
3. Canonicalize metadata key ordering for grouping; preserve keys and values.
4. If requested, select global per-series head/tail rows over the whole range.
5. Apply the function, with optional windowing, to those rows.
6. Project the existing response schema and order aggregate output by timestamp.

Reuse existing hot and cold filter implementations. Missing tags must not match
an equality/regex filter, and custom tags such as `sensor` remain part of series
identity. Preserve promoted-tag collision precedence from the existing cold
projection. Do not group by raw JSON text or an order-sensitive map encoding.
Distinct series must remain distinct even if they share plugin and vsn.

Group by `name` and complete canonical metadata. Influx table keys also include
internal columns such as `_field`; the managed data contract is one `value`
field. Include a fixture detecting unexpected multiple fields rather than merging
hidden field identities accidentally. Do not introduce a silent `_field` filter
change to existing hot-only requests as part of this work.

For aggregate requests that touch cold storage, reject a bucket override that
differs from the configured warehouse's source bucket, including static mode.
Combining another hot bucket with this cold table is not a valid aggregate.
Dynamic mode already enforces the configured bucket. Hot-only bucket selection
keeps its existing behavior. Ordinary static extracts are outside this change.

Aggregation plus head/tail must use raw rows from both halves and one global
per-series limit before aggregating. Standalone spanning head/tail remains a
separate feature and may retain its current rejection. Reject simultaneous head
and tail and negative limits. Zero input limits must preserve observed series/table
membership for aggregates such as mean, while never inventing a series absent
from the original filtered input (confirmed by experiment 86).

### Functions and value kinds

| Function | Required behavior |
| --- | --- |
| `count` | Count every input record; signed integer result; no numeric coercion |
| `mean` | Numeric input, floating result; ignore nulls according to Flux, not incompatible kinds |
| `sum` | Numeric input; retain integer/unsigned semantics where applicable |
| `min`, `max` | Numeric selectors for the stored scalar kinds supported by the pinned Flux engine; retain the selected value kind |

Strings that look numeric remain strings. Booleans do not become 0/1. A request
against unsupported nonnumeric input must reproduce the existing failure class
rather than silently drop rows and return a plausible partial aggregate.
`count` must still work for those records.

Use `variant_typeof` or an equivalent checked native-kind dispatch. Keep signed
and unsigned integers exact through staging, grouping, selection, summation, JSON,
and Arrow. Do not serialize an internal partial-state object into public `value`.

Mixed numeric types across time/shards, integer overflow, null-only input,
nonfinite floats, and tie selection require an Influx oracle fixture. The default
policy is to match the pinned engine, including errors. Do not automatically
promote incompatible types to DOUBLE or split one series into public groups by
kind just to make DuckDB accept the query. Wider internal accumulators are fine
only if final values/errors retain the agreed semantics. Floating means/sums may
vary in the last bits with execution order; specify tolerances for those tests.

The current source review indicates numeric-only mean/sum and numeric min/max
selectors; verify against the deployed engine rather than assuming behavior from
DuckDB's generic VARIANT comparison order. Sources:
[mean](https://docs.influxdata.com/flux/v0/stdlib/universe/mean/),
[sum](https://docs.influxdata.com/flux/v0/stdlib/universe/sum/),
[min](https://docs.influxdata.com/flux/v0/stdlib/universe/min/), and
[selector implementation](https://github.com/influxdata/flux/blob/master/stdlib/universe/min.go).

### Bounds and windows

Without a window, return one result per observed series with the original
resolved request start as timestamp. This includes `min`/`max`: the API's hot
adapter intentionally exposes `_start`, not the selected observation's time.
For spanning queries it must not become the hot half's clipped start.

For aggregations, freeze an omitted end to the request's single `now` before
running either input. Use that same end for filtering, empty-window generation,
and timestamp clipping. Explicit future bounds retain their explicit meaning.
Do not separately evaluate Flux `now()` and cold `time.Now()` for one aggregate.

Parse `experimental_window` into a validated duration structure, never interpolate
raw caller text into SQL. Reuse the existing duration parser's checked arithmetic
where appropriate, but do not assume its permissive grammar exactly matches Flux
window syntax. Reject zero, negative, malformed, overflowed, and unrepresentable
windows with clear validation errors before scanning either tier.

Target Flux units `ns`, `us`/`µs`, `ms`, `s`, `m`, `h`, `d`, `w`, `mo`, and `y`,
including accepted compound literals. Days/weeks are fixed UTC durations; calendar
months/years are calendar operations, not 30/365-day approximations. Mixed
calendar/fixed durations need the same boundary algorithm as Flux. They are an
explicit parity gate, not something to reinterpret as a fixed interval.

For fixed durations, use exact epoch nanoseconds and floor division relative to
1970-01-01 UTC. Negative epoch values need mathematical floor, not truncation
toward zero. Use checked wider arithmetic internally. Do not let DuckDB `/`
introduce floating division or let an INTERVAL/TIMESTAMP conversion discard
submicrosecond digits. Reconstruct public timestamps at nanosecond precision.
For calendar windows, use an explicit epoch origin and prove alignment against
Flux, including leap years, month-end, and compound boundaries.

The output timestamp comes from the effective window stop under the original
query bounds. A window crossing the hot/cold border is one window, and the border
must not clip or relabel it. For example, the final `5m` window of a request
ending at `00:12:00.000000194Z` must follow the oracle's partial-window timestamp,
not automatically become `00:15:00Z` or the border instant.

[Flux window documentation](https://docs.influxdata.com/influxdb/v2/reference/flux/stdlib/built-in/transformations/aggregates/aggregatewindow/)
defines calendar units, default timestamp source, empty-table behavior, and
Thursday weekly alignment. [Duration literals](https://docs.influxdata.com/flux/v0/spec/lexical-elements/#duration-literals)
define accepted lexical units. These are supplemented by actual engine fixtures.

### Empty input and empty windows

Preserve the current default `aggregateWindow(createEmpty: true)` behavior.
Do not switch hot queries to `createEmpty: false` for an implementation shortcut.

- For an observed numeric series, materialize the same empty-window results as
  the pinned engine. The recorded Influx 2.7.7 behavior is null for empty
  means/sums and
  windowed min/max, and zero for empty counts. The min/max result differs from
  the generic selector documentation; use the observed storage-backed behavior
  and test any non-pushdown path separately.
- Construct series/window expansion after combining the input. A series seen
  only in cold still needs eligible empty windows on the hot side, and vice versa.
- A wholly unobserved series is not created from a filter value. A query with no
  matching input must follow Flux's no-series result, not one fabricated count row.
- Use actual row count, not `count(value)`, where SQL null skipping would differ.

Empty windows can dominate output for broad ranges or tiny intervals. Count the
prospective series/window expansion with checked arithmetic and enforce a
resource budget; fail explicitly rather than silently dropping windows.

## Execution design

### Cold-only

Extend the cold query compiler to produce a filtered raw source, canonical full
metadata, optional input head/tail, then aggregation/window expansion and the
existing NDJSON/Arrow projection. The cold-only path must not query Influx or
materialize the entire cold dataset into Go or a local duplicate first.

Use the pinned metadata from the coordinator. In static mode, resolve the hint
once using the existing metadata URL validation and scoped Pelican secret.
Retain filter/time pushdown on the Iceberg scan and avoid the extract path's raw
row sort: aggregate output is sorted after reduction.

### Border-spanning

Introduce an explicit aggregation executor capability wired by `main.go` into
the tiered backend. Keep interfaces in `internal/query`, for example a method
accepting the original resolved request, cold end/border, and a raw hot Results
stream. Implement native execution/staging in `internal/backend/cold` so it reuses
the existing engine/authentication/projection machinery. The router must not
import a concrete cold implementation or reconstruct snapshot discovery itself.

The intended flow inside the pinned read-grant scope is:

1. Resolve request time bounds once and retain the original aggregation spec.
2. Build a raw hot query for `[border, end)` with identical filters, clearing
   function/window and clearing head/tail so limits are applied globally later.
3. Consume it into a private per-request Parquet staging file in bounded batches.
   Check iterator errors and successfully close the hot result before proceeding.
4. Read cold rows directly from the pinned Iceberg scan over `[start, border)`.
5. `UNION ALL` the two compatible raw relations, then run the single logical
   aggregation pipeline. Do not concatenate independently aggregated API rows.
6. Return the existing native Arrow or NDJSON results with a cleanup owner for
   query-local staging and any resources not yet released.

Internal staging can use primitive fields `time_ns`, `name`, canonical tags,
`kind`, and value text. Decode hot scalars without converting int/uint through
float64, then reconstruct typed VARIANT values in SQL from the explicit kind.
This avoids needing a new public multi-value schema or a full in-memory hot
map. No generated SQL contains unchecked user function names, interval strings,
paths, or metadata keys.

Cold rows need not be globally copied or sorted to stage hot rows. The two inputs
have disjoint half-open ranges; do not deduplicate them or use `UNION DISTINCT`.
For example, cold values 10/20/30 and hot value 100 yield mean 40, not the naive
mean-of-means 60. The same rule applies within a window straddling the border.

Do not stage while holding a native cold connection unnecessarily, or recursively
call `Cold.Query` while holding the backend mutex. Both native Arrow and
`database/sql` must see the same staging file and logical aggregate statement.
There is no new DuckDB-per-request engine or repeated extension-install path.

### Resource ownership and failure behavior

Reuse the cold engine's two CPU threads, 3GB native memory budget, and existing
async I/O/read-ahead policy initially. Memory limits do not bound Go buffers,
Influx execution, all native allocations, or staging disk; measure those scopes.

Use a fresh private temporary directory and bounded Parquet writer batches.
Introduce explicit staging/spill and expanded-window budgets with checked free
space during writes. Choose and record production defaults from the fixture and
representative VM measurements before enabling the feature; no unmeasured laptop
limit becomes the service policy. A budget failure is a query error, never an
implicitly truncated successful aggregate. Reuse existing serialized cold
execution ownership so requests cannot each assume the whole native budget.

The read lease covers hot staging, cold execution, and response streaming.
Cancellation or lease loss stops hot iteration, staging, and native execution.
All errors close partially opened results and remove only this request's scratch.
The result wrapper removes staging after the native reader/connection has closed,
including early client disconnects. A cleanup failure must not be hidden to
release a grant whose relevant resources have uncertain closure. Existing lease
expiry remains the final recovery mechanism.

Do not scan or delete other request directories on failure. Crash leftovers may
be diagnosed separately; this change must not add broad temp-directory cleanup.
No persistent aggregate table, archive publication, hot deletion, or remote
warehouse mutation occurs during a query.

## Implementation steps

| Step | Deliverable | Gate |
| --- | --- | --- |
| 1. Capture the existing contract | Isolated Influx fixtures through the current HTTP API in both response formats; record engine/Flux versions and cases below | Expected values, types, timestamps, empty behavior, and errors are recorded |
| 2. Shared aggregation specification | Safe function dispatch and validated window representation under `internal/query`; reuse duration logic without import cycles | Grammar, overflow, ordering, and injection cases pass; hot-only behavior preserved |
| 3. Cold-only implementation | Raw scan -> canonical grouping -> optional limit -> aggregate/empty windows -> existing projection | All five functions match the oracle for cold-only requests |
| 4. Spanning executor | Bounded hot staging, interface wiring, one combined aggregate, and lifecycle cleanup | Border-straddling windows and unequal-count means match whole-range hot reference |
| 5. HTTP/client and lease integration | Arrow/NDJSON/client parity; static and dynamic frontiers; cancellation/renewal during staging and execution | No partial-success responses, leaks, or missing grant coverage |
| 6. Representative evaluation and docs | Profile minute/hour/day and wider ranges, decide budgets, update API support matrix and examples | Correctness gates pass and stage/resource costs are published |

The steps are development milestones, not separate feature toggles. Do not
advertise full tier-transparent aggregation until calendar/type/empty-window
compatibility cases are resolved. If a construct cannot be matched initially,
document and validate the exact unsupported case instead of silently changing
its answer or claiming all previous aggregate requests are supported.

## Validation matrix

Use a fresh isolated Influx bucket and Iceberg v3 fixture with the managed schema
and adopted physical layout. Keep the same complete input in the reference hot
bucket while independently placing disjoint halves into test hot/cold storage.
Compare every result against one unsplit Flux request over that complete input.
No reference deletion, production query load, `/node-data` access, or writes to
published warehouses are needed. A later Pelican read-only sample is optional;
mutable fixtures use a newly created isolated prefix.

| Area | Required cases |
| --- | --- |
| Functions | All five, with and without windows; count on numeric/string/bool input |
| Routing | Hot-only, cold-only, spanning, and exact-border endpoints |
| Arithmetic | Unequal side counts; negatives; zero; >2^53 integers; uint64 boundaries; integer overflow; mixed types across shards; unsupported types |
| Windows | 1ns/submicrosecond, 5m, non-divisor fixed widths, 1w Thursday alignment, 1mo, 1y, compound intervals, pre-epoch floor, leap years/month ends |
| Query bounds | Relative bounds, omitted end frozen once, timezone offsets, mid-window start/end, end with nanosecond remainder, explicit future end |
| Sparse input | Series present in one tier only; leading/interior/trailing gaps; all-null fixture; wholly empty range; empty mean/count/sum/selector behavior |
| Series identity | Custom sensor tags, missing/promoted tags, identical metadata in different key order, multiple names, unexpected multiple Influx fields |
| Filters | Exact and anchored regex, missing tag, combined custom/promoted filters |
| Head/tail with aggregation | Global per-series selection before aggregation; unequal tier contributions; zero/negative/conflicting limits |
| Bucket scope | Correct source bucket; rejected mismatched aggregate override when cold is involved; preserved hot-only overrides |
| Formats | NDJSON and Arrow logical equality; exact integer JSON; nulls; nanosecond timestamps; sage-data-client decoding |
| Dynamic coordination | One pinned view during border advancement/late snapshot replacement; renewal in staging; cancellation at lease deadline; cleanup/release errors |
| Failures | Hot setup/iteration/close errors, disk budget, canceled staging, malformed values, native execution/fetch errors, client disconnect, concurrent request serialization |
| Validation/security | Missing function with window, empty/unknown function, malformed/zero/negative/overflowed intervals, function/interval injection attempts |

Unit tests should prove arithmetic, routing, grouping, and resource ownership.
Native DuckDB and real Influx oracle tests establish cross-engine semantics;
SQL-string assertions alone do not. Use exact record multisets after canonical
metadata normalization, allowing only specified floating-point tolerances.
Ordering among tied timestamps remains unspecified as in the existing API.

The current API module pins DuckDB 2.0 bindings and Arrow dependencies. Run with
`-tags=duckdb_arrow` and the actual module pins. If the local cache lacks those
artifacts, report the limitation and use the VM; do not substitute dependencies
and report that as a production build result.

## Evaluation and observability

Benchmark the same filtered inputs in these placements: complete hot reference,
complete cold, and a split through a populated window. Test minute/hour/day plus
representative March/April ranges; do not interpret tiny output as cheap input.
Separate cold cache/warm cache and preserve the accepted warehouse layout.

Report total latency, first-byte latency, source rows and output rows, hot fetch
and staging time/bytes, cold scan and aggregate time, empty-window expansion,
output sort, encoding, process/child RSS where applicable, spill/staging peak,
and cancellation/cleanup outcome. Keep NDJSON and Arrow measurements separate.
Aggregation can reduce response bytes substantially while still scanning all
matching points and spending significant time staging the hot half.

Retain existing metric names. Extend bounded observability with aggregation
function (`none` or the five names), execution route (`hot`, `cold`, `mixed`),
and phases such as hot staging and aggregation, without adding labels to legacy
families. Do not label timestamps, window strings, metadata, SQL, paths, or
series IDs. Reuse JSON profile substeps for detailed native evidence and add
Grafana views only for metrics actually emitted by the implementation.

After this reference implementation is correct, evaluate typed per-tier partial
states. Means require both sums and counts, empty windows require whole-range
series membership, and partial-window timestamps must use the original bounds.
Head/tail and mixed-type failure semantics need separate handling. That
optimization must match the raw-union oracle before replacing any execution path.

## Decisions and validation still required before release

The architecture, request/response schema, five-function target, raw-union
reference, hot-only routing, and pinned-read ownership above are the proposed
implementation policy. No runtime benchmark or Influx parity fixture was run as
part of this planning update.

The first implementation task is the oracle fixture, specifically resolving
empty sums, null-only/no-series behavior, mixed types, overflow, and accepted
compound calendar windows on the actual deployed Influx version. InfluxDB
2.7.7's [dependency manifest](https://github.com/influxdata/influxdb/blob/v2.7.7/go.mod)
pins Flux v0.195.1; record the actual deployed version rather than assume that all
current documentation or master source applies unchanged. If the fixture and
source review differ, the observed existing API contract drives the decision,
and this plan must record it explicitly.

Resource budgets are the other release decision: select defaults from real
staging/expansion measurements, document the rejection behavior, and keep them
operational limits rather than user-visible experimental query switches.

## Experiment 86: first compatibility screen (2026-11-04)

The [experiment guide](../tiered_system/experiments/exp86_aggregation_semantics.md)
and `exp86_aggregation_semantics.py` now provide a dedicated small-fixture runner.
The full run requires DuckDB 2.0.x before creating an isolated
`agg-exp86-<UUID>` bucket. It reads the fixture back, captures the Influx engine
version, and compares 57 cases in two placements (local cold relation and raw hot
Parquet plus cold relation), with three additional oracle-only observations.
Every statement, response, exact result, classified error, and report is retained.
It never deletes a bucket or accesses Pelican, the warehouse, or `/node-data`.

The test covers all five functions, sparse/empty windows, clipped window labels,
2ns windows, epoch/weekly/calendar alignment, integers above 2^53 and unsigned
boundaries, overflow, mixed kinds across shards, and head/tail before aggregation.
It also includes series present in only one tier and intentionally reverses hot
metadata insertion order before canonicalizing it for grouping. It does not
implement standalone spanning head/tail or exercise HTTP response formats.

Local candidate-only preview completed on **DuckDB 1.5.6**, because this session
cannot access the Docker socket or local Influx HTTP endpoints. The tool shell
initially resolved the Homebrew DuckDB ahead of the separately installed 2.0 CLI;
the earlier conclusion that no native 2.0 CLI was available was incorrect. Whole/split candidates agreed across 57 cases with
zero self-consistency failures; eight manual expected-value controls also passed.
The offline harness has 13 passing tests. The retained local report is
[here](../tiered_system/experiments/results/exp86_local_smoke_metadata_20261104/report.json).
These are **not Flux parity results, DuckDB 2.0 validation, API integration
results, or performance measurements**.

Local diagnostic observations, scoped to that actual engine version:

| Probe | Observed |
| --- | --- |
| Default vs explicit epoch `1w` bucket for March 1 | February 24 vs February 27 |
| `TRY_CAST` of a numeric-string VARIANT to DOUBLE | `123.0` |
| `TRY_CAST` of a boolean VARIANT to DOUBLE | `1.0` |
| Mean of cold 10/20/30 plus hot 100 | `40.0` in the split candidate |
| Selected unsigned maximum | Exact `18446744073709551613` |

Those previews support testing explicit origins and explicit value kinds rather
than trusting default bucketing or successful casts. They do not settle the
pinned Flux contract. The next required run is the guide's full comparison command against
an isolated bucket on Influx 2.x and a native DuckDB 2.0.x binary. Influx remains
unreachable from this sandbox, so that comparison can run on the VM.
Capture the full `report.json`, including mismatches. Empty sum filling, mixed
numeric types, overflow, compound calendar windows, and null-only behavior remain
hypotheses until that run; do not promote this candidate into API production
based on candidate self-consistency.

### Corrected local DuckDB 2.0 run

Re-ran the candidate screen with the explicit native 2.0 path: **57 cases and
eight manual controls passed, zero self-consistency failures**.
[DuckDB 2.0 report](../tiered_system/experiments/results/exp86_local_duckdb200_20261104/report.json).
The weekly-origin and string/boolean cast observations above also hold in 2.0.
Native VARIANT JSON serialization differs: the 2.0 diagnostic emits numeric JSON
`1`, while the 1.5.6 preview emitted a quoted string. This further supports
checking the actual target engine rather than treating 1.5 results as 2.0 proof.

This is now target-version candidate evidence, but still **not Flux parity,
production API/Arrow integration, Iceberg/Pelican validation, or performance
measurement**. The full Influx comparison remains pending because local service
network access is blocked in the tool sandbox.

## Experiment 86 Flux comparison (2026-11-04)

Port 18086 had no Influx process. The comparison used a new isolated
InfluxDB 2.7.7 container, `exp86-influx-1793828580`, published only on
`127.0.0.1:18096`, organization `waggle`, with data under
`tiered_system/experiments/results/exp86-vm-1793828580-influx`. Setup created
an infinite-retention bucket named `exp86-init`. No bucket was deleted.
DuckDB `v2.0.0` is the existing Linux ARM binary at
`beehive-hot-to-cold-loader/bin/duckdb`, run in `python:3.12-slim-bookworm`
with the workspace mounted at the same path. The offline harness still has
13 passing tests.

The first attempt, `exp86-vm-1793828580`, created bucket
`agg-exp86-0f4af7affc1040039f5c965d4ecb24ca` (`fdbd52d54e5fd133`) and stopped
during raw readback. Influx omitted `#datatype` until the query body requested
the annotated CSV dialect. `Influx.query` now sends that dialect. That run is
retained and was not resumed.

The completed run is
[exp86-vm-1793828688/report.json](../tiered_system/experiments/results/exp86-vm-1793828688/report.json).
Bucket `agg-exp86-c54816fcda9945169bc06e11585e9770` (`97e6637edbe5cb24`).
Raw fixture readback matched. Influx health reports `v2.7.7` commit
`e9e0f744fa`. The process exited 1 because comparisons mismatched. Of 114
candidate comparisons (57 cases, cold-only and split): **66 PASS, 8
BOTH_ERROR, 40 FAIL**. Cold-only and split had the same verdict on every
case. Three oracle-only cases were not scored.

What matched: whole-range `mean`, `min`, `max`, `sum`, and `count` on the
float series, including the weighted mean of cold 10/20/30 plus hot 100
equaling 40; 5-minute `mean`, `sum`, and `count`, including clipped
nanosecond stops; signed integer aggregates; counts of strings and booleans;
explicit epoch week, 7-minute, compound fixed, and pre-epoch windows; head
and tail of at least one row before a whole or windowed mean; and a filter
that matches no series.

Observed Flux behavior on this Influx 2.7.7, which the candidate does not
yet follow:

| Case | Flux | Candidate |
| --- | --- | --- |
| Windowed `min` and `max`, including 2ns windows | Null row for an empty window. 12 rows versus the candidate's 6 on the 5-minute cases | Non-null windows only. Present values matched |
| `head: 0` and `tail: 0` before `mean` | One null row per series that existed in range | No rows |
| Unsigned `mean` of the two values near 2^64 | `9.223372036854776e18` | `1.8446744073709552e19` |
| Unsigned `sum` | Wrapped uint `18446744073709551609` | DuckDB overflow; cast to `UINT64` refused |
| Signed overflow `sum` of `9223372036854775807 + 1` | Wrapped int `-9223372036854775808` | DuckDB overflow error |
| String or boolean `mean` and `sum` | HTTP 400, unsupported input type | Candidate input-type error. `BOTH_ERROR` |
| String or boolean `min` and `max` | HTTP 500 panic, `unsupported for aggregate min/max` | Candidate input-type error. `FAIL` because the Flux error is unclassified |
| Mixed int and float in one series | `count` is 1, `mean` is 2.0, `sum` is 2 | `count` is 2; `mean` and `sum` error as mixed input |
| `aggregateWindow(every: 1mo)` and `1y` | December 31 stays in its own month; the year mean keeps that point separate from January | The implicit timestamp conversion rounds December 31's final nanosecond into January; corrected in the replay below |
| `1mo1d` | HTTP 400: a duration interval cannot mix month and nanosecond units | Oracle only, not implemented |
| All-null float series | `count` is 2; `mean` is null | Oracle only |

Empty-window nulls are confirmed for Flux `min` and `max`. `mean`, `sum`, and
`count` agreed on the empty windows in this fixture. The mixed-kind queries returned only the integer observation in this fixture;
the cause requires the raw/shard diagnostics described below. Integer `sum`
wraps. The calendar failure was subsequently traced to implicit timestamp
rounding in the candidate, rather than a different month origin. This is not an API,
Iceberg, or performance result. Do not promote the candidate into the API on
this run.

## Review and recorded-oracle replay (2026-11-04)

The original run has 33 passing, four classified-error, and 20 mismatching
**distinct cases**, each tested in two placements. Cold-only and split verdicts
agree throughout, supporting the raw-union architecture. These failures do not
show that crossing the border itself changes the calculation.

### Confirmed candidate corrections

1. **Calendar assignment rounded across midnight.** A local DuckDB 2.0 probe
   shows `2024-12-31 23:59:59.999999999` converting implicitly to microsecond
   TIMESTAMP as `2025-01-01 00:00:00`. The candidate's `time_bucket` overload
   therefore assigned that row to January. Explicitly flooring epoch nanoseconds
   to microseconds for this calendar-only operation gives December's bucket.
   Public timestamps and fixed-width arithmetic remain nanosecond precision;
   do not globally downgrade time storage or response precision. Include a
   negative-epoch floor correction too.
2. **Empty windowed min/max output.** Retain the null rows observed in this
   storage-backed Flux path. Removing them was an incorrect assumption from
   generic selector semantics. Actual non-null selected values already match.
3. **Zero head/tail before mean.** Discover series from filtered input before
   removing rows with the limit. Preserve a null mean for each previously
   observed series. A wholly unmatched filter still returns no series.

These changes are confined to the experimental compiler. Replayed the unchanged
VM Flux expectations and saved fixture through the native local DuckDB 2.0.0:

| Comparison | PASS | BOTH_ERROR | FAIL |
| --- | ---: | ---: | ---: |
| Original live Flux vs candidate | 66 | 8 | 40 |
| New candidate vs recorded Flux oracle | 86 | 8 | 20 |

[Replay report](../tiered_system/experiments/results/exp86_replay_safe_fixes_20261104/report.json).
It records the source report/fixture hashes, current script hash, generated SQL,
and exact results. No old reports were changed and no VM/Influx/Pelican access
was used for the replay. Its exit code remains 1 because mismatches remain.
The offline harness now has 16 tests, including these regressions.

### Remaining differences need policy and targeted evidence

| Remaining distinct cases | Paired comparisons | Interpretation / next check |
| --- | ---: | --- |
| String/bool min/max | 8 | Both paths reject the input, but the direct Flux endpoint panics with HTTP 500. Keep normal validation errors; do not reproduce a panic. Check behavior through the real API before calling this a user-visible feature incompatibility. |
| Unsigned mean | 2 | Recorded mean equals `(sum(values) mod 2^64) / count` for this fixture. That suggests unsigned accumulator overflow, but is not proof of the implementation. Compare storage-backed, windowed, and pure Flux-array paths before choosing a policy. |
| Signed/unsigned overflowing sums | 4 | Flux wraps while DuckDB's narrowing cast rejects overflow. Explicit modulo arithmetic can emulate wrapping if that is the chosen compatibility contract; do not narrow through float64. |
| Mixed int/float across shards | 6 | Whole-range Flux count is **1**, although the candidate fixture contains **2** observations. Mean 2 and sum 2 match only the integer observation, not a legitimate combined result. Do not describe this as general mixed-numeric support or implement arbitrary data omission to match it. |

The mixed-type series was deliberately excluded from raw fixture preflight, so
the existing evidence does not prove both versions are query-visible across the
whole range. Query each shard/range independently and capture a whole-range raw
read before deciding whether ingestion, storage cursors, table schema collisions,
or aggregate pushdown is responsible. Also test reversed type chronology and a
pure Flux `array.from`/union reference. This is the next important diagnostic.

For unsigned mean, the mathematical mean is about `1.8446744073709552e19`, while
the observed storage result is about `9.223372036854776e18`. Preserve ordinary
supported aggregation behavior without silently promising mathematical accuracy
and bug-for-bug legacy arithmetic simultaneously. Record the selected policy
explicitly after the targeted probes. Existing hot-only execution remains on
Flux regardless, so any intentional difference must be documented.

A direct Flux HTTP 400/500 is not automatically the public data API's status:
`httpapi.Service` currently maps backend Query errors to HTTP 500. Test the API
response/error path itself before designing error-code compatibility or changing
classification merely to improve the pass count. `BOTH_ERROR` is not a valid
numeric result and does not assert identical messages/statuses.

### Next experiment scope

Use the existing isolated bucket read-only where sufficient, or a new tiny bucket
for chronology variants. Capture mixed-series raw and aggregate results by
shard/range and after explicit conversion, and compare integer overflow/means in
storage pushdown versus pure Flux arrays, both windowed and unwindowed. Keep this
separate from warehouse/API changes. Once those decisions are recorded and the
supported success cases pass, validate a small real Iceberg v3 fixture and both
HTTP formats before performance work on historical warehouse data.

## Experiment 87: targeted type/arithmetic diagnostic (2026-11-04)

[Runner and VM instructions](../tiered_system/experiments/exp87_aggregation_type_probes.md)
are ready in `exp87_aggregation_type_probes.py`. The original exp86 bucket is
accessed through a read-only client. The default live run creates a separate
`agg-exp87-<UUID>` bucket containing 16 points; it never deletes or modifies the
old fixture. It does not access the API, warehouse, Pelican, or `/node-data`.

The default run captures 331 tiny Flux probes across these comparisons:

- Original mixed series: whole-range raw input versus January-only/March-only
  raw input and count/mean/sum, before and after float conversion.
- New mixed series: int-then-float versus float-then-int chronology, plus the same
  temporal values with reversed insertion order. Whole and per-period reads
  distinguish stored visibility from aggregate behavior.
- Pure Flux unions of separately typed arrays, before/after grouping and float
  conversion, with union order reversed.
- Signed/unsigned arithmetic using native arrays, float arrays, native storage,
  mapped storage, and float-converted storage; functions run unwindowed and with
  5m/15m windows to isolate separate versus combined points.

An identity map is only a diagnostic transformation; do not claim it disables
pushdown without physical-plan evidence. Pure arrays supply the no-storage
reference. `--read-only` skips new fixture creation and records 127 probes.
Reports retain results/errors as observations rather than declaring every
successful HTTP request a parity pass. Transport/authentication errors abort.

### Local work completed

Native DuckDB **2.0.0** and Python exact-integer controls passed for five pairs:
unsigned-large, unsigned-small, signed overflow, signed underflow, and
signed-small. [Local arithmetic report](../tiered_system/experiments/results/exp87_local_arithmetic_checked_20261104/report.json).

| Probe | Exact sum | Explicit 64-bit wrapped sum |
| --- | ---: | ---: |
| Unsigned-large | 36893488147419103225 | 18446744073709551609 |
| Signed overflow | 9223372036854775808 | -9223372036854775808 |
| Signed underflow | -9223372036854775809 | 9223372036854775807 |

Unsigned-large's mathematical floating mean is `1.8446744073709552e19`.
Dividing its explicit wrapped sum by two gives `9.223372036854776e18`, matching
the previous storage-backed Flux observation. This confirms that wrapping can
reproduce that fixture, **not** that all Flux paths implement that algorithm or
that production should adopt it. No compatibility policy changed.

There are 25 passing offline tests across exp86/87. The live Flux portion was
not executed in this session: Docker socket access and connection to the
isolated Influx endpoint remain sandbox-blocked. Run the guide's command using
the existing dedicated fixture container/credential, retain `report.json`, and
compare raw visibility plus array/storage results before resolving the policy.
No performance conclusion or production implementation is claimed here.

## Experiment 87 live Flux run (2026-11-04)

The full probe set ran against the existing `exp86-influx-1793828580` fixture
on Influx **2.7.7** (`e9e0f744fa`) and DuckDB **v2.0.0**. The exp86 bucket
`agg-exp86-c54816fcda9945169bc06e11585e9770` was read only. The run created
`agg-exp87-31fdeaaa9e8e41f7a49749d91906eb5c` (`814e0a833c17e623`) and did not
delete either bucket. All 331 probes were captured: **317 success, 14 error**.
`completed: true` means the planned probes returned, not that they match exact
arithmetic. Report:
[exp87-vm-1793830028/report.json](../tiered_system/experiments/results/exp87-vm-1793830028/report.json).
The five DuckDB/Python arithmetic controls passed again inside this run.

### Mixed field types

Both points are stored. A range that covers only January returns that point,
and a range that covers only March returns the other. A range that covers both
returns one row, count 1, and the type of the earlier point. `toFloat()` and
an explicit float map convert that surviving value. They do not bring the
omitted point back. A one-year window has the same single surviving value.

| Series | January | March | Whole range |
| --- | --- | --- | --- |
| Existing exp86 mixed, int then float | int `2`, count 1 | float `3.5`, count 1 | int `2`, count 1, mean `2.0`, sum `2` |
| New int-then-float | int `2` | float `3.5` | int `2` |
| New float-then-int | float `2.5` | int `3` | float `2.5`, count 1, mean `2.5`, sum `2.5` |
| Same times as int-then-float, insertion reversed | int `2` | float `3.5` | int `2` |

The surviving type follows chronological order, not write order. A pure Flux
array cannot hold both kinds: all 14 array probes failed at compile time with
`expected int but found float` or `expected float but found int`, including the
probes that tried to convert before aggregating. The omission is a storage-read
behavior. It is not a behavior a typed Flux array can express, and it is not a
candidate rule to copy.

### Integer arithmetic

Native array input, native storage, and storage followed by an identity map
agreed on every arithmetic pair. The identity map did not change a result in
this fixture, so it is not evidence about pushdown. `toFloat()` before the
aggregate agreed between array and storage and used float64 arithmetic.
Separate 5-minute windows kept each point and added one null empty window.
A 15-minute window matched the unwindowed combined result.

| Input | Combined native `sum` | Combined native `mean` | After `toFloat()` |
| --- | --- | --- | --- |
| Unsigned-large | wrapped `18446744073709551609` | `9.223372036854776e18` | sum `3.6893488147419103e19`, mean `1.8446744073709552e19` |
| Unsigned-small | `8` | `4.0` | same values as floats |
| Signed overflow | wrapped `-9223372036854775808` | `-4.611686018427388e18` | sum `9.223372036854776e18`, mean `4.611686018427388e18` |
| Signed underflow | wrapped `9223372036854775807` | `4.611686018427388e18` | sum `-9.223372036854776e18`, mean `-4.611686018427388e18` |
| Signed-small | `6` | `3.0` | same values as floats |

The exp86 unsigned mean is the native combined path: Flux wraps the uint64 sum,
then divides. Each point's own 5-minute mean is the mathematical float
`1.8446744073709552e19`. Float conversion before the aggregate also produces
that mathematical mean. Small signed and unsigned values do not wrap.
No production policy is selected here.

## Experiment 87 review and recommended implementation policy (2026-11-04)

Reviewed the underlying live report, not just its success count. All 331 probes
were captured. The 14 errors are compile-time failures in the mixed-type array
union expressions; the other 317 probes succeeded. There were no unrecorded
transport failures. The arithmetic comparisons agree for all tested pairs:

| Independent comparison | Matching function/window pairs |
| --- | ---: |
| Native arrays vs native storage | 45 / 45 |
| Native arrays vs storage followed by identity map | 45 / 45 |
| Float arrays vs float-converted storage | 45 / 45 |

Each comparison includes five value pairs, three functions, and unwindowed/5m/15m
execution. Identity-map equality alone still does not establish an optimizer
plan; pure-array agreement provides the independent storage-free evidence.

### Arithmetic: sufficient evidence for a compatibility recommendation

For the tested homogeneous int64/uint64 series, the native Flux contract uses
64-bit wrapping for sums, and means agree with wrapped native sums divided by
the value count. The array and windowed results support preserving that behavior
instead of treating the earlier unsigned-mean result as an isolated storage bug.

**Recommendation, not yet a selected production policy:** preserve native Flux
integer behavior in the cold/spanning implementation so moving storage tiers
does not change existing answers. Use exact wider accumulation and explicit
modulo 2^64 reduction; interpret the wrapped result as signed or unsigned as
appropriate. For means, convert that wrapped accumulator to double and divide
by the non-null numeric count, with a null result when there are no eligible
values. Count itself counts rows, including nulls, and must remain a separate
operation. Float inputs retain their existing floating arithmetic.

This deliberately preserves surprising overflow answers; do not describe those
answers as mathematically accurate outside the native 64-bit range. Do not
implicitly `toFloat()` all input or use a wider public integer result, since
both would change the established native contract. Test a Flux-compatible SQL
candidate against the saved arithmetic report before integrating it. No new
user-facing arithmetic switch is proposed.

### Mixed types: a separate source-completeness limitation

The per-period probes prove that both observations are stored. Whole-range raw
queries return only the chronologically earlier kind, regardless of insertion
order; conversion after that read cannot restore the missing observation. This
is upstream of aggregation, not a defect the DuckDB GROUP BY can repair.

Consequently, the proposed hot-staging source can already be incomplete for a
series whose field kind changes across hot shards. A correct cold aggregate over
all stored rows cannot reproduce that old answer without intentionally dropping
data. A function-specific SQL cast or CASE expression does not resolve this.

**Recommendation:** support homogeneous scalar series first, reject detected
mixed-kind aggregation explicitly, and never emulate chronological row omission.
Detection in the staged rows is not proof that hot input was complete: an omitted
kind is no longer visible there. Keep this limitation explicit in the contract
and do not promise full tier transparency for mixed-kind series. A complete
solution requires a separate, verified hot read strategy, such as actual shard-
aligned reads with known boundaries or an enforced stable-type ingestion
contract; neither is implemented or validated by these experiments. Do not add
an arbitrary day-sized query split and assume it coincides with every shard.
Existing hot-only Flux behavior remains unchanged under the current scope.

The loader's integer-to-float coercion helps ordinary numeric ingestion, but
cannot establish that every historical/direct writer or string-to-number series
has a stable kind. That requires evidence rather than an assumption.

The 14 array errors should also be interpreted narrowly: these specific union
expressions fail type unification at compile time. `toFloat()` applied after the
union cannot repair that compile error. This does not prove that normalized
branches cannot be unioned; a separate test would convert each typed branch
before union. It does not invalidate the storage omission evidence either.

### Unsupported functions/types and remaining integration work

Reject unsupported string/bool min/max input normally. Reproducing an Influx
panic is not feature compatibility, and direct Flux HTTP statuses still need to
be distinguished from the public API's error mapping. Retain the experiment's
mismatch classifications until the public API error contract is tested; do not
improve a score merely by broadening error matching.

The next implementation-independent step is a Flux-compatible integer SQL
candidate and replay of the captured array/storage results, including exact
signed/unsigned bounds and empty windows. Then exercise actual Iceberg v3 scans,
the pinned read grant, and Arrow/NDJSON responses with a small fixture. Performance
work on the historical warehouse follows those correctness gates. Standalone
spanning head/tail remains outside the selected scope.

This review changes the plan and recommendations only. No production arithmetic
policy, hot read strategy, API implementation, or warehouse mutation was applied.

## Experiments 88–89: native integer candidate and actual Iceberg fixture (2026-11-04)

[Commands, scope, and VM HTTP gate](../tiered_system/experiments/exp88_89_aggregation.md).
No production aggregation path is enabled. The API repository adds only an
opt-in `_test.go` adapter; all compiler/preparation changes remain in research.

### Native integer SQL replay

The candidate now has an explicit research-only `flux64` policy. It accumulates
integers exactly in a wider internal type, applies modulo 2^64, interprets the
result as int64/uint64, then converts the wrapped accumulator to double only for
mean division. The old strict candidate remains the default comparison control;
no service flag or API request field was introduced.

Replayed the immutable exp87 native storage oracle: five arithmetic pairs,
three functions, three window forms, and both cold-only and split-through-
Parquet placements. **90/90 comparisons passed on native DuckDB 2.0.0.**
[Exp88 report](../tiered_system/experiments/results/exp88_integer_replay_20261104/report.json).
Integer values and nanoseconds were checked exactly, with the existing tolerance
only for floating results. This validates the homogeneous integer arithmetic
candidate, not unsigned persistence or production routing.

### Unsigned VARIANT persistence blocks a full-domain claim

Moving from in-memory VARIANT into actual Parquet exposed a separate storage
boundary. The default all-types preparation failed; no existing warehouse was
involved. A retained native DuckDB 2.0 diagnostic confirms:

| Input / encoding | Stored result |
| --- | --- |
| UBIGINT 6 as native VARIANT | Value survives, subtype becomes INT64 |
| UBIGINT 18446744073709551612 as native VARIANT | Parquet writer rejects conversion to INT64 |
| Both values as DECIMAL(20,0) VARIANT | Exact numeric JSON survives and subtype reads as DECIMAL(20, 0) |

[Unsigned storage report](../tiered_system/experiments/results/exp89_unsigned_variant_check_20261104/report.json).
Parquet [VARIANT primitive encoding](https://github.com/apache/parquet-format/blob/master/VariantEncoding.md)
uses signed integer and decimal representations. Do not infer that an in-memory
DuckDB UINT64 VARIANT subtype survives the published representation. Without a
preserved source-kind convention, a small unsigned number becomes indistinguishable
from signed input, which can change wrapping semantics in aggregates too.

Decimal is an **unselected encoding candidate**, not an automatic fallback.
Adopting it would require explicit exporter, comparison-proof, and query-reader
rules to reserve/recognize the unsigned subtype, and a full Iceberg-go/DuckDB/API
round trip. It must not narrow via float64 or relabel arbitrary signed input as
unsigned. The warehouse schema, source data, exporter, and client decoder have
not been changed to adopt it. Full uint64 storage/aggregation compatibility
remains a blocker until this is resolved or the support domain is explicitly
restricted; the 90 in-memory replay passes do not remove that blocker.

### Supported-domain actual Iceberg scans

Prepared a **fresh local Iceberg v3 SQL catalog** using iceberg-go 0.7.0 and
DuckDB-written Parquet, without REST, Influx, Pelican, or remote publication.
It preserves timestamptz_ns, promoted tags, MAP metadata, native VARIANT value
field ID 11, day partitioning, plugin/vsn/time ordering, and plugin-pure bounded
row groups. Tiny plugin/day files validate semantics, not large-file packing
performance. The Go fixture writer was built and vetted locally with existing
module dependencies.

The preparation uses `--exclude-unsigned` explicitly, records all **23 excluded
input cases**, and never coerces them into another type to claim a pass.
Thirty remaining raw records round-tripped with exact values, full tags, kinds,
and nanosecond instants. **136/136 cold-only/split aggregate comparisons passed**
against saved Flux expectations, spanning all five functions, signed overflow,
floats, counts of strings/booleans, empty windows, calendar/submicrosecond bounds,
and head/tail combinations.
[Exp89 report](../tiered_system/experiments/results/exp89_supported_fixture_20261104/report.json).
An [independent actual JSON scalar-kind check](../tiered_system/experiments/results/exp89_supported_fixture_20261104/public-value-types.json)
verified all 136 outputs without borrowing a type from the expected answer.

An intermediate preparation found an experiment-classifier issue:
`variant_typeof(true)` is BOOL_TRUE and `variant_typeof(false)` is BOOL_FALSE.
Both are now classified as boolean. The false value had been stored correctly;
this was not another storage-loss defect. Failed preparations remain intact.

### HTTP/read-grant gate prepared, not yet run

`beehive-data-api/internal/backend/cold/aggregation_probe_test.go` adds the opt-in
`TestAggregationCandidateHTTPProbe`. The generated manifest supplies precompiled
experimental SQL and captured expectations. It reads the actual fixture through
existing native Arrow / database/sql paths and uses the production HTTP writers.
It is designed for 272 successful response cases (136 x two formats) plus six
renewal/expiry/changed-snapshot scenarios. Expected integer JSON, nulls, metadata,
public Arrow schema, and nanosecond timestamps are checked; native resources must
close before grant release. Responses may be exported to a new private directory.

The adapter reuses the production coordinator using a separate internal raw
admission query, because the production router still rejects aggregations. The
original HTTP aggregation request is matched against the compiled manifest, and
a fake pinned URL maps to the private Iceberg snapshot. This validates component
integration at a test seam, not a deployed aggregation router, real hot fetch,
real coordinator transport, or Pelican authentication. Native cancellation under
a large active aggregate and performance remain separate gates.

**The Go HTTP probe has not been compiled or passed locally.** Actual API-pinned
Arrow 18.5.1, DuckDB Go bindings/driver, JWT, protobuf and sys artifacts are not
available in the sandbox's module cache. No dependency substitutions were made.
Run the guide's command with actual API pins on the VM. The preparation contains
absolute file URLs, so prepare a new fixture on the same host/filesystem where
that Go test will execute; do not copy the macOS catalog and call it portable.

There are **32 passing offline Python tests** across experiments 86–89. Local
native validation covers the integer replay and supported Iceberg scan layers
only. No historical-warehouse speed claim, production rollout, or unsigned
encoding decision follows from these results.

### Host HTTP and read-grant probe (2026-11-04)

The same-filesystem preparation was used, so no second catalog was built. Go
1.26.5 darwin/arm64 compiled `TestAggregationCandidateHTTPProbe` with
`CGO_ENABLED=1 -tags=duckdb_arrow` and `GOFLAGS=-mod=readonly` against the API
module pins. `LOAD iceberg` succeeded inside the test; DuckDB reported `v2.0.`.

The probe passed in 5.435s: **272/272** NDJSON and Arrow response cases and all
six lease scenarios (renewal during delayed setup, cancellation at an unrenewed
deadline, and rejection of a changed snapshot, each in both formats). Grant
release follows native result closure in the test. Responses are in
[http-responses](../tiered_system/experiments/results/exp89_supported_fixture_20261104/http-responses)
and the log is
[http-probe.log](../tiered_system/experiments/results/exp89_supported_fixture_20261104/http-probe.log).
This still does not enable a production aggregation router, live hot extraction,
a real coordinator, or Pelican authentication. The 23 unsigned cases stay
excluded.

## Host probe review and experiment 90 client decoding (2026-11-04)

Verified the actual Go log: **278 passing subtests**, comprising 272 NDJSON/native
Arrow response cases plus six lease scenarios. All 272 expected body files are
present. This closes the opt-in component-level HTTP/read-grant gate for the
supported fixture domain with actual API pins. The adapter still does not enable
production aggregation routing or prove real hot extraction/Pelican transport.
Unsigned-input cases remain excluded, and this is not a speed benchmark.

### Additional current-client replay

To validate the user-visible DataFrame layer too,
`exp90_aggregation_client.py` reads the immutable HTTP manifest and body files
through the current workspace Sage client's actual `_arrow._load_arrow` and
`_ndjson.load`. It performs no network, catalog, or warehouse operations and
changes no client code. It hashes the decoder sources and bodies, and compares
exact integer values without numpy's potentially coercing scalar equality.

[Client replay report](../tiered_system/experiments/results/exp90_client_checked_20261104/report.json):
pandas **3.0.6**, pyarrow **25.0.1**, **268/272 pass, four precision failures**.
All failures are the signed-overflow series' 5m sum in both cold-only/split and
both NDJSON/Arrow. The first populated window contains INT64_MAX; later windows
include a small integer and an empty/null result.

| Layer | Value |
| --- | --- |
| Native aggregate and HTTP body | Exact integer `9223372036854775807` |
| Client DataFrame value dtype | `float64` |
| Decoded first-window value | Float representing `9223372036854775808` |

The wire encoder and read grant did not corrupt this value. The Arrow decoder's
`Series.infer_objects()` and NDJSON DataFrame construction both infer float64 for
an integer/null column. The result silently loses one at this boundary. This is
a client precision issue exposed by aggregation's empty windows, not evidence
that cold/split SQL differs. Small exact int-to-float conversions are recorded
as dtype changes but are not counted as precision failures.

**Next corrective step:** preserve exact nullable integer values during both
client decoding paths, using an appropriate nullable integer dtype or object
representation without losing unsigned range or mixed scalar values. Keep the
common float path fast, avoid reconstructing all rows solely for Arrow, and add
regressions for large ints plus nulls and large ints mixed with floats. Replay
all 272 unchanged bodies after that fix. Four offline checker tests already
cover precision comparisons and distinguish them from harmless dtype changes.
No fix has been applied to the client as part of this review.

### Current readiness and remaining work

| Layer / contract | Evidence / status |
| --- | --- |
| Homogeneous native integer compatibility SQL | 90/90 replay comparisons pass |
| Supported-domain actual Iceberg scans | 136/136 pass; unsigned inputs explicitly excluded |
| Native HTTP encoders and pinned-grant test seam | 272/272 response cases and six lease scenarios pass |
| Current Sage client DataFrames | Four nullable-integer precision failures; fix required |
| Full uint64 persistence/kind preservation | Native VARIANT blocker remains; decimal convention unselected |
| Mixed-kind hot read completeness | Source omission remains; no stable complete-read strategy selected |
| Production aggregation router / real hot staging | Not implemented; fixture adapter only |
| Historical/concurrent performance | Not measured for the new aggregate path |

Fixing client precision can proceed independently of the unsigned storage policy.
The unsigned convention and mixed-kind scope still need explicit resolution
before claiming full-domain tier transparency. No production API/client changes,
remote mutations, or performance conclusions are made by this review.

## Completed client precision correction (2026-11-04)

Both Sage client decoders now share precision-preserving value inference.
NDJSON separates values before DataFrame construction can coerce them; Arrow
keeps its bounded decoding and infers values after all batches. Integer/null
columns use nullable `Int64` or `UInt64` when representable, with `pd.NA` for
missing entries. Wider integers remain objects. Mixed integer/float columns
also remain objects so large integers cannot round through a common float type.
Float-only columns retain float64 using a direct conversion path.

The [validated replay report](../tiered_system/experiments/results/exp90_client_fixed_20261104/report_validated.json)
passes **272/272 unchanged saved HTTP bodies**, with **zero precision failures**,
using pandas 3.0.6 and pyarrow 25.0.1. It hashes both decoder modules and their
new shared inference helper. The previous 268/272 report remains as failure
evidence. The client suite ran 35 tests: 32 passed and three opt-in integration
tests skipped. Four offline replay-checker tests also passed. New transport
regressions cover signed/unsigned boundaries, nulls, wider Python integers,
mixed floats, Arrow batch boundaries, and unchanged float dtype.

A local decode-only smoke comparison against the original Git HEAD modules
used 30,000 float rows, repeated metadata, Arrow batches of 8,192, one warmup
and five alternating measured trials. Median Arrow decoding was 1.453 ms before
and 1.476 ms after; NDJSON was 393.529 ms before and 393.174 ms after. Frames
were equal. These small local measurements do not establish end-to-end API
performance. The final cleanup removed a deprecated pandas `copy` argument;
the final suite and fixture replay passed again afterward.

This closes the client precision gate for the supported fixture domain. It
does not resolve the uint64 VARIANT persistence blocker or mixed-kind hot read
completeness, and does not enable production aggregation routing. Those remain
the next design gates before full-domain implementation/performance claims.

## Experiments 91–92: resolving type boundaries (2026-11-04)

### Unsigned storage: validated candidate, integration still pending

The opt-in fixture now supports table-marked unsigned encoding without changing
the public schema or adding value columns. Source uint64 values are written as
`DECIMAL(20,0)::VARIANT`, under
`sage.value.encoding=variant-uint64-decimal-v1`. Only this reserved representation
in an explicitly marked table is restored to uint64 for arithmetic. Other source
kinds remain unchanged. This relies on the existing five-kind source contract,
which has no decimal input type. A future native decimal type needs a different
or extended convention.

The first actual Iceberg read returned `DECIMAL(38,0)` rather than precision 20;
the experimental reader therefore recognizes both 128-bit zero-scale forms,
checks `0..18446744073709551615`, and fails on invalid reserved values. It does
not reinterpret unmarked decimals. This cannot recover unsigned kind already
lost in older native Parquet files.

[Boundary-inclusive report](../tiered_system/experiments/results/exp91_unsigned_boundaries_20261104/report.json):
DuckDB 2.0.0, **51 exact raw rows verified** through actual Iceberg v3,
**182/182 cold-only/split comparisons passed**, **zero unsigned cases excluded**.
Raw controls include both signed/unsigned equal values, 2^53+1, INT64_MAX,
INT64_MIN, 2^63, UINT64_MAX, floats, strings and both booleans. Negative and
overflow reserved-decimal controls reject. Typed SQL verifies logical kinds
separately; public JSON verifies exact integer tokens, which do not carry
signedness. Earlier failed preparations remain as evidence rather than being
overwritten.

The existing opt-in HTTP/read-grant test can now cover **364 responses** plus
six lease cases. This new HTTP run and the 364-body client replay are pending:
the sandbox's offline cache lacks the API's pinned native DuckDB library. No
module versions were substituted. The published warehouse and production
exporter/API remain unchanged. Before adoption, the exporter staging,
reconciliation and deletion proofs must consistently use the logical-kind
convention, with explicit marker compatibility rules.

### Mixed-type hot reads: metadata-driven candidate ready for live validation

`exp92_complete_hot_reads.py` reads only the retained exp87 chronology bucket.
Instead of assuming hour/day splits, it obtains the actual shard-group bounds,
clips half-open queries, respects deleted/truncated groups, and rejects
overlapping active groups. All six raw observations, their kinds, tags and
nanoseconds must match the original fixture. Whole-range reads and type-predicate
queries are retained as controls, without assuming either recovers missing data.

Influx's metadata-backup response includes the group manifests. The experiment
discards KV/SQL and unrelated bucket metadata without persisting the raw backup;
only the selected fixture's allowlisted bounds are saved. It supports a separate
metadata credential or an already-filtered manifest. This is a diagnostic
mechanism, not a proposal to give the production API backup privileges or fetch
full server metadata for every query.

The local **synthetic complete-input** SQL contract passed **15/15**:
[report](../tiered_system/experiments/results/exp92_local_mixed_contract_20261104/report.json).
Each recovered two-point series counts as two; mixed-kind arithmetic rejects
explicitly. This is the homogeneous-first candidate from the earlier review,
not a newly selected float-coercion policy. The local run does not read Influx
or prove that actual shard-scoped reads are complete. Live validation is still
required and expects 18 gates (three raw-series checks plus 15 aggregate checks).
Eight offline tests cover marker, planner, metadata and mutation guards; the
32 earlier experiment tests still pass.

If the live test passes, production still needs a filtered metadata provider,
refresh/concurrency rules, overlap handling and the API's raw hot staging path.
No full-domain aggregation readiness claim follows from the local checks alone.

Commands, expected counts and host/VM instructions are in the
[experiments 91–92 guide](../tiered_system/experiments/exp91_92_type_boundaries.md).

### Live shard-scoped reads (2026-11-04)

The first attempt, `exp92-host-1793847673`, stopped before any shard query.
Influx 2.7.7 returns `/api/v2/backup/metadata` as `multipart/mixed`: a binary
`sql` part and a JSON `buckets` array. Treating that body as one JSON document
fails UTF-8 decoding. That directory is retained. The reader now keeps only the
`buckets` part in memory, then writes the allowlisted fixture manifest. A new
offline test covers a binary SQL part that must not survive selection. Nine
offline tests pass.

The rerun is
[exp92-host-1793847958](../tiered_system/experiments/results/exp92-host-1793847958/report.json).
DuckDB 2.0.0, Influx 2.7.7 commit e9e0f744fa, `completed` true,
`live_flux_executed` true, **18/18 gates passed**, **6 raw rows verified**.
The fixture bucket has two non-overlapping week groups. Clipped to the query
window they are `[2025-01-01, 2025-01-06)` and `[2025-02-24, 2025-03-02)`.

Shard-scoped reads recovered both points of each series: float 2.5 then int 3,
int 2 then float 3.5, and the reversed-insert series int 2 then float 3.5, with
the original tags and nanoseconds. Each series counts as 2. mean, min, max, and
sum reject with `unsupported or mixed aggregate input`. Whole-range reads and
type-predicate controls each returned only the January point and are recorded
as differences, not as gates.

This validates complete reads for this fixture. Production still needs a
filtered metadata provider, refresh and concurrency rules, overlap handling,
and the API's raw hot staging path. No aggregation router was enabled.

### Independent review of live evidence (2026-11-05)

Reviewed the saved exp92 report, shard manifest, generated Flux and actual CSV
bodies. The recorded script and source-report hashes match the current files.
Reparsed both shard responses for each series and compared them independently
with the original exp87 fixture: all six observations match exactly in value,
kind, tags and nanosecond timestamp. Every generated shard query matches the
reported clipped bounds. All 18 recorded gates pass. The current offline
experiment suite also passes **75/75 tests**, including the multipart regression.

This establishes a working extraction strategy for this mixed-type fixture:
query actual non-overlapping shard-group ranges separately and combine the
typed rows outside Flux. Whole-range reads and type predicates remain incomplete
in all three chronology controls. A value cast or type filter after that read
is therefore not an adequate repair.

The 18 passing gates comprise three complete raw reads, three exact counts and
12 expected arithmetic rejections. They do not demonstrate mixed-type numeric
aggregation: `mean`, `sum`, `min` and `max` deliberately reject mixed input under
the current homogeneous-first candidate. They also do not establish a concurrent
cross-shard snapshot or production performance.

**Next validation:** complete exp91's native HTTP/read-grant test and current
client replay for all 364 responses; no new exp91 HTTP log or client report is
present in the workspace. The unsigned Iceberg SQL gate remains 182/182, while
the new unsigned HTTP/client gate remains pending.

After that gate, implement the filtered shard-metadata provider in the
hot-to-cold loader and consume it in API hot staging, with explicit refresh,
concurrency and overlap rules. Adopt the unsigned convention consistently in
exporter staging, reconciliation, deletion proofs and API readers. Those are
production integration tasks; this successful fixture run did not enable them.

### Unsigned HTTP/client gate prepared and attempted (2026-11-05)

Added `exp91_http_client.py` to run both remaining checks as one command. It
verifies all 364 response subtests and six lease subtests, rejects skipped or
incomplete runs, checks the exact exported body set, and then replays all bodies
through the current client. A new output directory retains Go JSON events,
stderr, module/test source hashes, and a stage report. Module pins are read-only.

The [sandbox attempt](../tiered_system/experiments/results/exp91_http_attempt_20261105/report.json)
stopped before HTTP execution: the writable offline cache still lacks
`go.duckdb.org/libduckdb/darwin-arm64 v0.20000.0`. No new HTTP/client pass is
claimed. Run the one-command gate from the normal laptop terminal against the
existing local fixture, or prepare a new fixture on the VM first. Commands are
in the [91–92 guide](../tiered_system/experiments/exp91_92_type_boundaries.md#one-command-httpclient-gate-on-an-already-prepared-host).

### Unsigned HTTP/client gate passed (2026-11-05)

`exp91-http-1793886680` compiled and passed all 364 response subtests and six
lease subtests, then stopped before the client replay. The two no-series NDJSON
bodies are legitimately empty: the manifest expects zero rows, and the Arrow
bodies for those cases are nonempty schema frames. The export check now allows
an empty NDJSON file only when that case's expected rows are empty. Any other
empty file still fails. That directory is retained.

The completed run is
[exp91-http-1793886760](../tiered_system/experiments/results/exp91-http-1793886760/report.json).
Go 1.26.5 darwin/arm64, `GOFLAGS=-mod=readonly`, `CGO_ENABLED=1`,
`-tags=duckdb_arrow`, and the API module pins. `completed` is true.
Native coverage is **364 responses and 6 leases**. The current client replay,
pandas 3.0.6 and pyarrow 25.0.1, passed **364/364** with **zero** integer
precision failures.
[Client report](../tiered_system/experiments/results/exp91-http-1793886760/client-report.json).
Production aggregation routing stays disabled.

### Independent review of unsigned HTTP/client evidence (2026-11-05)

Verified the saved Go JSON events directly: all **364 response subtests and six
lease subtests pass**, with no failed or skipped events. The runner, manifest,
API module pins and probe source hashes match the current workspace. All 364
expected body files are present, and every body hash matches the client report.
Of these responses, **92 exercise unsigned-input aggregate cases**.

Replayed the immutable bodies again with the current client:
[independent client review](../tiered_system/experiments/results/exp91-http-1793886760/client-review_20261105.json).
Result: **364/364 pass, zero integer precision failures**. The current offline
experiment suite also passes **77/77 tests**. Accepting empty NDJSON is correct
only for a manifest case expecting no rows; Arrow still requires its schema
frame, and unexpected empty responses remain failures.

This closes the new unsigned component HTTP/client gate. Together with exp92's
18/18 live checks, both previously identified type-boundary issues now have
validated candidate solutions: marked decimal encoding for uint64, and actual
shard-scoped extraction for complete mixed-kind fixture input. The selected
candidate still rejects mixed-kind arithmetic explicitly; it does not establish
general mixed-numeric arithmetic compatibility.

**Next step: production integration**, beginning with the unsigned convention
through loader staging, canonical comparison/reconciliation and deletion proofs,
then the filtered shard-metadata provider and API hot staging, followed by the
production cold-only/spanning aggregation router. Preserve the current extract
path, public four-column schema, and existing hot-only behavior. Validate the
assembled path end to end before historical/concurrent performance measurement
or rollout. These fixture results do not establish production performance,
concurrent snapshot semantics, or historical unsigned-type recovery.

### Production endpoint range-completeness check prepared (2026-11-05)

Attempted the supplied production request for plugin
`registry.sagecontinuum.org/jrobrien/waggle-wxt536:1.00` with `start=-30m`.
The sandbox's curl failed DNS resolution (`HTTP 000`, zero response bytes);
the web tool could not access the endpoint either. Therefore there is **no
production result or production omission claim** from this attempt.

`exp94_production_range_parity.py` is a read-only public API check, ready for the
normal terminal. It freezes absolute UTC bounds for a 30-minute extract,
compares the multiset of full records against six adjacent five-minute reads,
and repeats the whole-range read to detect data changes. Values and nanosecond
timestamps are compared without float conversion. Requests and response bodies
are retained in a new directory, with a 32 MiB response cap and bounded timeouts.
It never writes to production or accesses Influx metadata, Pelican or `/node-data`.

Optional `--history-days 14` selects one recent measurement name/vsn (preferring
`wxt.env.temp`) and compares a fixed historical whole-range count with summed
daily counts for that same series, including a second whole-range control.
This avoids downloading two weeks of raw measurements or issuing historical
queries for every plugin series. Queries run sequentially. The historical end
excludes the most recent full UTC day to reduce ingestion races, although late
backfill remains possible. No series is selected if the recent extract is empty.

```sh
python sage-data-rework/tiered_system/experiments/exp94_production_range_parity.py \
  --history-days 14 \
  --run-dir "sage-data-rework/tiered_system/experiments/results/exp94-prod-$(date +%s)"
```

Outcomes distinguish no observed omission, a range-dependent difference,
changing-data/inconclusive, and no-data/inconclusive. A stable difference would
establish public API range-dependent behavior needing investigation; identifying
the responsible Influx component still needs direct source evidence. Agreement
does **not** establish that the selected data spans a native type change or that
daily splits coincide with actual shards. JSON numeric spelling also cannot
reliably identify the original stored Influx kind. This is a production
diagnostic, not a replacement for the controlled native-type oracle.

Three offline comparator tests pass. The
[saved attempt](../tiered_system/experiments/results/exp94_prod_attempt_20261105/report.json)
is incomplete due to DNS failure; no production response body was obtained.

### Production range check (2026-11-05)

[Report](../tiered_system/experiments/results/exp94-prod-1793888273/report.json):
`completed` true, 24 sequential public reads, no writes. The frozen window is
`2026-11-05T13:47:54Z` to `2026-11-05T14:17:54Z` for plugin
`registry.sagecontinuum.org/jrobrien/waggle-wxt536:1.00`.

The 30-minute extract has **312** records across **52** name/vsn series. Each
adjacent five-minute read has 52 records. The repeated whole-range read matches
the first, and the six parts match the whole multiset: **no omission observed**
(`full` 312, `split` 312, no missing or extra identities).

The historical count selected `wxt.env.temp` / `W08D`, which had 6 points in
that recent window. Both 14-day counts and all 14 daily counts for
`2026-10-21T00:00:00Z` to `2026-11-04T00:00:00Z` returned empty bodies. The
outcome is **inconclusive, no data**, not a range-dependent difference. This
does not identify an Influx component, a native type change, or shard alignment.

### Independent production-result review (2026-11-05)

Reparsed the saved exp94 NDJSON bodies and independently reconstructed the
whole/split record multisets. The script hash matches the recorded report.
Verified **312 records across 52 name/vsn series**, six slices of 52 records,
identical whole-range before/after reads, and zero missing or extra identities.
This confirms **no omission observed for the supplied plugin's tested 30-minute
window**. The comparison includes full metadata, exact numeric values and
nanosecond timestamps, rather than just total row counts.

All 16 historical count bodies are genuinely empty (two whole-range controls
plus 14 daily queries). They provide no populated historical comparison, so the
cross-shard check remains inconclusive. No raw historical existence query was
performed; empty count bodies alone do not establish why historical input was
unavailable. The next useful diagnostic would first establish a populated
historical interval for a specific name/vsn, using a bounded raw existence read,
before repeating count comparisons. Do not infer native Influx kinds or actual
shard boundaries from JSON numeric spelling or five-minute/day slicing.

There is currently **no reproduction of the omission on production**. The
controlled Influx 2.7.7 fixture still demonstrates the conditional failure when
the stored kind changes across shards; the production result neither contradicts
that fixture nor establishes that production has the same condition. Production
shard-aware extraction remains a robustness proposal with integration costs,
not a repair justified by a demonstrated loss in this production sample.

### Targeting the retained March/April production data (2026-11-05)

The earlier historical diagnostic used October/November 2026 and plugin version
`1.00`, not the benchmark data. The known production baseline uses
`name=wxt.env.temp`, `vsn=W0A0`, March/April **2025**, without a plugin filter.
The archive's temperature plugin is
`registry.sagecontinuum.org/jrobrien/waggle-wxt536:0.24.11.14`. Reusing the recent
plugin version would incorrectly exclude this historical data.

Performed a read-only native DuckDB inventory of the retained March/April files
for W0A0:
[summary](../tiered_system/experiments/results/exp94_archive_full_inventory_20261105/summary.json),
[per-day/name/plugin counts](../tiered_system/experiments/results/exp94_archive_full_inventory_20261105/inventory.json).
All 61 day directories were inspected; W0A0 has rows on 56 days. The inventory
contains **519,906,203 records**: 519,654,554 in `value_float`, zero in
`value_int`, and 251,649 in `value_str`. No measurement name switches between
these stored representations in this inventory, including across plugin tags.
These are the pre-VARIANT archive's representations, **not independent proof of
the original native Influx field kinds**.

The temperature totals are **25,940,207 for March** and **31,189,045 for April**.
They exactly match the whole-month production raw-download counts already
captured in exp85 on October 27. This is existing evidence of count agreement
for these historical whole-month extracts; it is not a new production query or
a complete value/hash comparison against the archive.

Added `--benchmark-archive` to exp94. It first reads a bounded minute of raw data
at each month's start and calibrates the public count result against those exact
records. It then compares whole-month counts before/after with each day's
counts, compares the combined March/April count with the sum of the months,
and checks exact raw records across the two-minute March/April calendar seam.
The latter is not asserted to be an actual shard boundary. It uses the exact
benchmark name/vsn filters and intentionally omits the newer plugin version.
If all checks have data, the run makes 75 sequential read requests, returning
only small raw samples and aggregate counts. Empty samples or a count function
that fails calibration remain inconclusive and skip the expensive month scans.

```sh
python sage-data-rework/tiered_system/experiments/exp94_production_range_parity.py \
  --benchmark-archive \
  --run-dir "sage-data-rework/tiered_system/experiments/results/exp94-archive-prod-$(date +%s)"
```

The [new sandbox attempt](../tiered_system/experiments/results/exp94_benchmark_attempt_20261105/report.json)
again failed production DNS before receiving data. The normal-terminal run is
still required for a fresh production conclusion. All **84 offline experiment
tests pass**, including count calibration, missing-data handling, the 61-day
schedule and preservation of the previous recent-window mode. There remains no
production reproduction of the controlled native-type-change omission.

### March/April production range check (2026-11-05)

[Report](../tiered_system/experiments/results/exp94-archive-prod-1793889142/report.json):
`completed` true, **75** sequential public reads. Filters were `name=wxt.env.temp`
and `vsn=W0A0`, with no plugin filter. Both minute samples and both month
counts use one metadata group, plugin
`registry.sagecontinuum.org/jrobrien/waggle-wxt536:0.24.11.14`.

Minute counts matched the raw samples (698 in the first March minute, 721 in
the first April minute). Repeated whole-month counts matched the sum of the
daily counts and the exp85 downloads:

| Window | Whole count | Daily sum | Repeated read |
| --- | ---: | ---: | --- |
| March 2025 | 25,940,207 | 25,940,207 | same |
| April 2025 | 31,189,045 | 31,189,045 | same |
| March + April | 57,129,252 | 57,129,252 | same |

March 13–17 returned empty daily counts. Those zeros are included in the
matching sum and line up with the archive inventory's 56 days that contain
W0A0 rows. The two-minute calendar seam has **1,444** raw records, equal to
723 on the March side plus 721 on the April side, and the repeated read
matched. Every checked outcome is **no omission observed**.

These are count and small-sample agreements on one float temperature series.
They do not reproduce the controlled native-type omission, prove shard bounds,
or compare every archived value.

### Independent March/April production review (2026-11-05)

Reparsed all saved count bodies and the raw minute/seam bodies from
`exp94-archive-prod-1793889142`. The runner and reference-report hashes match;
all 75 request artifacts are present. Independent reconstruction confirms:

| Comparison | Whole | Partitioned | Stable before/after |
| --- | ---: | ---: | --- |
| March vs daily counts | 25,940,207 | 25,940,207 | Yes |
| April vs daily counts | 31,189,045 | 31,189,045 | Yes |
| March–April vs monthly counts | 57,129,252 | 57,129,252 | Yes |
| Two-minute seam, exact raw records | 1,444 | 1,444 | Yes |

The calibration counts match 698/721 raw records. March 13–17 are the only
empty daily count bodies; those dates are consistent with the retained archive's
W0A0 inventory. No range-dependent omission was reproduced for this historical
temperature series, and its month counts also match the previous production
raw downloads and retained archive.

This evidence does not justify calling shard-aware extraction a repair for an
observed production loss in these data. It remains a candidate safeguard for
the separately demonstrated native mixed-type condition. It does not establish
that every production series has stable types, or that all 57 million individual
values match the archive. Likewise, count agreement does not establish parity
for mean/sum/min/max.

The next aggregation validation should assemble the live hot/cold/spanning API
paths and compare their five functions on identical data. Before rollout, make
an explicit scope decision for mixed-kind series: require and enforce a stable
source-kind contract, or integrate a verified complete-read strategy with the
necessary metadata/concurrency handling. A whole-range read cannot reliably
detect a type it has already omitted. Do not silently drop that limitation or
make a loader metadata endpoint mandatory solely because of this production
check; its ownership and integration remain separate architecture decisions.

## Production implementation, no shard coordination (2026-11-05)

Implemented the selected policy in the service repositories; no deployment,
warehouse mutation, commit or push was performed. This supersedes the earlier
proposal to add a filtered shard-metadata provider. Ordinary Flux reads are used
for hot input; the documented native mixed-kind source omission remains a
limitation rather than introducing shard-boundary coordination.

### Service changes

- **Archival unsigned preservation:** `beehive-hot-to-cold-loader` materializes
  source uint64 as reserved decimal VARIANT and creates the encoding property.
  Reconciliation, dictionary/packing comparisons, remote verification and the
  pre-delete exact proof normalize declared decimal representations to logical
  `UINT64`, checking range. Equal signed/unsigned numbers remain different proof
  records. Startup and commits refuse a destination with a missing/wrong marker.
  Unmarked bootstrap decimals are rejected rather than relabeled; unknown
  bootstrap markers fail. No published warehouse was automatically marked or
  rewritten. Use an audited migration into a new managed prefix/state directory
  for an existing unmarked destination; lost historical unsigned kind cannot be
  reconstructed from numeric values.
- **Cold and spanning execution:** the actual Go compiler supports all five
  functions, positive fixed/compound or month/year windows, exact nanosecond
  bounds and clipped labels, canonical full metadata, global pre-aggregate
  head/tail and observed-series empty windows. Integer sums and mean accumulators
  use exact accumulation plus tested Flux 64-bit reduction. Count includes every
  input record; detected mixed/unsupported arithmetic and nonfinite results fail.
- **Raw hot staging:** the optional aggregation capability on the cold backend
  reads one ordinary raw Flux request with function/window/limits cleared,
  validates the single value-field contract, and streams explicit scalar kinds
  and exact textual values into private NDJSON, then Parquet. The cold scan
  remains native and unsorted until aggregation, using the existing pinned
  snapshot. Only the aggregate result is finally sorted and projected into the
  existing four-column Arrow/NDJSON schema. Whole-tier aggregates are never
  concatenated or averaged together.
- **Ownership and limits:** the existing renewable read lease covers staging,
  setup and streaming. Hot results must close before native scanning; uncertain
  source closure leaves the grant to expire rather than releasing it early.
  Scratch closes after the native result, with composite Arrow access preserved.
  Omitted aggregate ends touching cold freeze at the original routing clock;
  hot-only requests retain their original behavior. Standalone crossing head/tail
  stays rejected. Cold-touching bucket overrides must match the configured source.
  Output series/window expansion is limited to 1,000,000 rows; raw hot staging
  defaults to a configurable 16 GiB per request. These are explicit errors,
  not truncation. Ordinary extract SQL and hot-only execution are unchanged.

New API configuration: `COLD_AGGREGATE_STAGING_DIR` and
`COLD_AGGREGATE_MAX_STAGING_BYTES` (default `17179869184`), with matching
`-cold.aggregate-*` flags. Source/warehouse schema, day partitioning, file size,
plugin/vsn/time sorting and plugin-pure bounded row groups are unchanged.

### Validation performed here

[Validation summary and source hashes](../tiered_system/experiments/results/aggregation-implementation-20261105/summary.json).

| Check | Actual result |
| --- | --- |
| Actual Go production compiler against captured hot oracle via DuckDB 2.0 CLI and actual Iceberg | **182/182 pass**; also parser/budget, timezone/calendar and JSON-to-Parquet staging-codec regressions |
| Loader unit suite | All packages pass; vet passes |
| Native unsigned encoding/Parquet/plugin-packing/exact proof | Pass; UINT64_MAX preserved and signedness loss rejected |
| Native canonical and reconciliation dictionary gates | Pass |
| API hot, tiered, HTTP and Arrow utility packages | Pass; tiered race suite and focused vet pass |
| Offline research/seed tests | **87 pass**, including rejection of a native unsigned-to-signed seed conversion and nonlocal/credential URLs |
| Full pinned API native build/test | **Blocked**: offline sandbox lacks `go.duckdb.org/libduckdb/darwin-arm64 v0.20000.0`; no module pins substituted |
| Updated native loader lifecycle with UINT64_MAX | **Blocked before setup**: sandbox refuses the local TLS listener (`bind: operation not permitted`) |
| New production-path HTTP and live three-placement runtime checks | Implemented, **not yet executed successfully here** |

The native compiler gate found and fixed two issues before integration: hot
metadata key ordering must be normalized at the raw union, and calendar bounds
must use UTC instants even when callers supply offset timestamps. Both have
regression coverage. The CLI gate validates the actual production SQL source,
not the earlier Python candidate. It does not stand in for the native Go
runtime/HTTP gate.

### Required host/VM gates before rollout

The [API aggregation validation guide](../../beehive-data-api/docs/aggregation-validation.md)
contains exact commands. Reuse the prepared local exp91 fixture on the same
host/absolute paths, or prepare it afresh on the VM.

1. Run the full API tagged suite/vet with its pinned native library.
2. Run `TestAggregateCompilerOracle` and `TestAggregationProductionHTTP`.
   The latter uses the actual production router/compiler, annotated CSV decoder,
   private staging, native query, encoders and read-grant ownership checks:
   **364 response checks**, with optional actual-body export for client replay.
   It uses a synthetic hot HTTP transport, not live Influx.
3. Use `seed_aggregation_parity.py` against the dedicated **loopback** Influx
   fixture. It creates and retains a fresh UUID bucket from the same queried
   raw records as the cold table (36 for the current fixture), verifies every
   native kind/value/tag/nanosecond by querying them back, and records the source
   manifest hash. Unqueried mixed-kind encoding controls stay cold-only.
4. Run `TestAggregationLiveTierParity` using that verified seed report and token.
   Expected coverage is **546 responses**: 91 cases × hot-only/cold-only/split ×
   both formats. Hot-only aggregates in actual Flux, cold-only scans Iceberg,
   and split uses live raw Flux plus production staging. Export actual bodies
   with their generated client manifest and replay with exp90. Do not call the
   old precompiled-SQL probe a pass of this new gate.
5. Run the updated native archival lifecycle and unsigned proof tests on the host.
   The lifecycle now includes UINT64_MAX through verification and deletion.

A passing default/unit suite alone does not establish the live three-placement
result, runtime throughput, or concurrent cross-database snapshot isolation.
The current warehouse has not been changed and deployment remains pending these
runtime gates.

### Host gate run (2026-11-05)

Go 1.26.5, `GOFLAGS=-mod=readonly`, `CGO_ENABLED=1`, and DuckDB 2.0.0. Logs are
under
[aggregation-host-gates-1793895000](../tiered_system/experiments/results/aggregation-host-gates-1793895000).

| Gate | Result |
| --- | --- |
| `go test -tags=duckdb_arrow ./...` and vet | Pass. Opt-in fixture tests skip without their environment |
| `go test -race ./internal/backend/tiered` | Pass |
| Compiler oracle and `TestAggregationProductionHTTP` | Pass in 18.2s. Client replay of the synthetic-hot bodies is **364/364**, zero integer precision failures. [Bodies](../tiered_system/experiments/results/parity-http-1793895399) |
| Loopback seed | **36/36** raw rows verified in retained bucket `agg-exp86-f40598c008d943e89c702249207612a3`. [Report](../tiered_system/experiments/results/parity-seed-1793895351/report.json) |
| Native unsigned proof, canonical dictionary, reconciliation dictionary, lifecycle | Pass, including `TestNativeLifecycle` in 2.66s on private fixture storage |
| `TestAggregationLiveTierParity` | Fail |

The live failure is only `baseline-pre-epoch-split`, in both NDJSON and Arrow:
HTTP 500, `hot aggregate input outside requested nanosecond range`. The hot
slice starts at the border `1970-01-01T00:00:00Z`. A direct Flux read of that
same slice still returns the cold-side point
`1969-12-31T23:55:00.000000194Z` (`exp86.negative`, value 4) together with the
in-range `1970-01-01T00:01:00.000000195Z` point. Staging rejects the early
point rather than taking it into the hot half. The synthetic transport does
not reproduce this, so the 364-body gate stayed green. The partial live
directory is
[parity-live-http-1793895447](../tiered_system/experiments/results/parity-live-http-1793895447);
its client replay stopped because those two bodies were not written. No
warehouse was changed and nothing was deployed.

### Pre-epoch hot slice and live rerun (2026-11-05)

Hot staging now leaves a row strictly before the requested slice out of the
hot half. A row at or after the exclusive end, or one that does not round-trip
through nanoseconds, still fails the request. `TestHotAggregateStageOmitsRowsBeforeSlice`
covers the epoch case: the `1969-12-31T23:55:00.000000194Z` point is omitted
and the in-range point is kept.

The same seed was reused. `TestAggregationLiveTierParity` passed in 5.871s.
The current client replay of the exported bodies is **546/546**, pandas 3.0.6
and pyarrow 25.0.1, with zero integer precision failures.
[Bodies](../tiered_system/experiments/results/parity-live-http-1793895866).
No warehouse was changed and nothing was deployed.

### Independent results review (2026-11-05)

Replayed all saved live HTTP bodies through the current checkout's Python
decoders again. The [independent report](../tiered_system/experiments/results/parity-live-http-1793895866/client-independent-review-20261105.json)
matches the original report's manifest hash, decoder source hashes, response
hashes, and per-case results: **546/546 pass**, zero integer precision failures.
There are 273 responses per format and 182 per placement: 91 cases × two
formats for each of hot-only, cold-only, and split execution.

The retained host logs also confirm the full API suite and the native loader
unsigned-value, canonical dictionary, reconciliation dictionary, and lifecycle
gates passed. These host results supersede the earlier sandbox-blocked runtime
status above; the independent replay checks saved responses without rerunning
the live service or native lifecycle.

The pre-epoch fix enforces the lower bound of the requested hot slice. In a
split query, the earlier point belongs to the cold half, so excluding it from
hot staging prevents double counting. The regression retains the in-range
nanosecond point and still rejects a point at the exclusive end. This is not a
workaround for missing mixed-type observations or shard-boundary discovery.

The planned correctness gate is now satisfied for the controlled supported
fixture: actual Flux hot-only aggregates, actual Iceberg cold-only aggregates,
and the production raw-union split path agree with the captured expectations
in both response formats. This does not establish large-query performance,
mixed-kind Flux completeness, or concurrent cross-store snapshot isolation.
The next validation should measure representative historical aggregation
workloads, including split-query staging cost, before rollout. No deployment
or published warehouse changes were made during this review.

### Historical performance benchmark prepared (2026-11-05)

Added [exp95 and its host/VM runbook](../tiered_system/experiments/exp95_aggregation_performance.md)
to measure the production implementation on the adopted immutable March/April
warehouse. It compares cold-only routing with a static noon-March-1 split over
the same data. The historical Influx fixture must already contain the hot half
of March 1; the 36-row correctness seed is not used for performance testing.
The benchmark performs read-only HTTP queries and Docker inspection, without
seeding, deleting, publishing, or restarting services.

The workload covers all five functions on the daily temperature series, hourly
windows, fleet counts, all-node temperature means, and a cold-only monthly
series. The primary sweep measures Arrow and NDJSON with profiling disabled;
the diagnostic Arrow sweep requires a matching completed unprofiled control.
All responses are retained and hashed, with timestamp/metadata/integer parity
and tolerance-based floating parity checked against the cold reference.
Configuration/image/resource mismatches, restarts, OOM events, empty results,
duplicate identities and content mismatches fail the run.

Extended existing opt-in cold Arrow profiling rather than adding another
profiling mechanism. A split aggregate now creates its profile before opening
the hot source and reuses it through native execution. Added hot open/read-stage
time, staging queue/COPY time, eligible row count and NDJSON/Parquet sizes.
Existing backend first-batch/EOF/lifetime markers consequently include staging
for split aggregates. The final native profile still covers the aggregate SQL,
not the staging COPY; NDJSON and ordinary extracts keep their execution paths.
Full HTTP timing remains separate from backend lifetime and final scratch
cleanup. `AGGREGATION_PARITY_PROFILE=1` enables native HTTP fixture assertions
for a single complete shared profile with all split-stage diagnostics.

Local validation: five offline benchmark tests pass (cross-border workload,
runtime/OOM rejection, exact unsigned values/schema, Arrow/NDJSON nanosecond
and UINT64_MAX fidelity, and retained failure
evidence on parity mismatch); standalone Go profile tests pass. The native
tagged staging/profile suite cannot run here because the offline module cache
lacks the pinned `go.duckdb.org/libduckdb/darwin-arm64 v0.20000.0`. Docker
inspection is also blocked by this session's socket permissions. Therefore
**no historical timing conclusion is available yet**. Run the native profiling
gate, unprofiled sweep and diagnostic sweep from the runbook on the host/VM,
then add their report paths and results here. No commits, deployment or
published warehouse modifications were made.

### Profiling gate and sweep availability (2026-11-05)

The first profiling run,
[exp95-profile-gate-1793896832](../tiered_system/experiments/results/exp95-profile-gate-1793896832/profile-gate.log),
failed every Arrow case with profile status `cancelled`. Those responses had
already recorded end-of-stream; the request context was done by the time the
summary was written, and cancellation was preferred over completion. A finished
stream is now `complete`. Cancellation remains the status only when the context
ends before end-of-stream. The existing profile unit test cancels after EOF for
the complete case.

The rerun passed: profiling HTTP gate in 4.098s, tagged `./...`, and vet.
[exp95-profile-gate-1793896914](../tiered_system/experiments/results/exp95-profile-gate-1793896914).

The unprofiled and diagnostic sweeps were not started. The only running Influx
is the synthetic exp86 fixture on port 18096. It does not contain the March 1
noon-to-midnight historical hot slice, and the demo Influx is not running.
Pointing the split API at that fixture would not measure the adopted warehouse.
No benchmark containers were created, and nothing was seeded or deployed.

### Unprofiled and diagnostic sweeps (2026-11-05)

The published warehouse stayed read-only. The split half used a new isolated
Influx 2.7.7 bucket, `exp95-hot`, loaded with 832,654 archive rows covering
only the windows a split query reads: W0A0 `wxt.env.temp` from noon through
midnight, all-node `wxt.env.temp` from noon to 12:30, and every series in the
first 30 seconds after noon. The 36-row correctness bucket was not used.
Four dedicated APIs from one image shared that bucket and the adopted
warehouse. Reports:
[control/report.json](../tiered_system/experiments/results/exp95-bench-1793897282/control/report.json)
(136 responses, status complete) and
[profile/report.json](../tiered_system/experiments/results/exp95-bench-1793897282/profile/report.json)
(68 Arrow responses, status complete). Every split body matched the cold
reference. Arrow and NDJSON unprofiled medians agree within a few tenths of a
second.

Unprofiled medians, cold then split:

| Case | Rows | Cold | Split |
| --- | ---: | ---: | ---: |
| series day, each of count/mean/sum/min/max | 288 | 4.3–4.8s | 6.0–6.6s |
| series hour mean | 60 | 0.64–0.66s | 0.78–0.79s |
| fleet minute count | 1,995 | 0.79–0.82s | 1.08–1.10s |
| temperature fleet hour mean | 192 | 3.0s | 3.8–3.9s |
| series month mean | 744 | 109.2–109.9s | cold only |

The diagnostic Arrow profiles attribute the time as follows. Delivery of the
aggregate rows is negligible on every case, and sort is negligible. The daily
split stages 521,840 hot rows (about 180 MB of NDJSON, 2.4 MB of Parquet):
hot read and NDJSON encoding is about 2.1s and COPY is 0.6–1.3s, which is the
extra time over the cold-only run. The remaining roughly 4s is the native
scan and value projection of the cold morning plus the staged afternoon;
projection is the largest operator class, ahead of the window aggregate.
The hour and one-minute fleet cases stay under 1.2s, with hot staging,
native execution, and fixed prepare time each a fraction of a second. The
all-node temperature hour stages 289,674 hot rows; native scan and projection
(about 1.8s) slightly exceed hot read/encoding plus COPY (about 1.6s). The
monthly series is entirely cold: 109s inside chunk fetch, led by projection,
then the window aggregate, then the scan.

These are sequential single-request times on this host. They do not measure
dynamic read-grant overhead, concurrent throughput, or mixed-kind
completeness. The benchmark containers were left running for inspection.

### Independent exp95 review and next target (2026-11-05)

Independently decoded and compared **all 204 retained responses**: 136 control
and 68 diagnostic Arrow responses, including warmups. Every body hash, decoded
row count and cold/split/reference comparison passes. All 68 timing files match
their embedded phase data and have complete status. Recorded before/after
container identities, settings, restarts and OOM events pass the benchmark's
runtime checks. See the [independent review report](../tiered_system/experiments/results/exp95-bench-1793897282/independent-review-20261105.json).
This was a replay of retained evidence, not another live measurement.

The native profiling fix is consistent with the successful stream lifecycle:
an explicit failure still wins, EOF establishes completion, and cancellation
without EOF remains cancelled. The saved rerun confirms the HTTP profiling
gate, full tagged API suite and vet passed. Preserve that fix.

The historical measurements support retaining the current aggregation path.
Arrow reduces response size substantially (monthly mean: about 19 KB versus
258 KB NDJSON), but execution dominates: only 744 monthly output rows are
returned after about 109 seconds. Across all measured unprofiled responses,
the time from first complete record/batch to completion is at most 15 ms.
Changing response encoding or final sorting is therefore a low-priority target
for this workload. Queue waits are negligible in this sequential test; this
does not establish behavior under concurrent load.

The next promising target is **metadata reconstruction and canonicalization
before aggregation**, tested in isolation first. The retained monthly native
profile reads 25,940,207 observations. Its two largest projections process all
those rows and record about 122 s and 46 s of operator work in one measured
run. The compiled SQL places those projections around the merged metadata
MAP and its canonical key ordering: `map_concat(...)`, followed by
`map_from_entries(list_sort(map_entries(meta)))`. These values are operator
work, not additive wall seconds. They implicate per-row metadata preparation;
they do not yet isolate its cost from the other expressions in those nodes.
The monthly median peak engine buffer memory is about 1.31 GB and reported
temporary-directory usage is zero, so these profiles do not point to disk
spill as the cause of the 109-second query.

Recommended experiment: compare the existing metadata path with a candidate
that builds canonical series metadata once per distinct tag combination and
uses a compact series identity through reduction. Restore the original
metadata at output. Preserve promoted-tag collision/null semantics, full
series identity, window/empty-series behavior and head/tail partitioning; do
not substitute the request filter for actual metadata or drop metadata keys.
Use the existing oracle and HTTP parity gates before timing the same exp95
workload. This is a proposed optimization, not an implemented fast path.

Hot staging is a separate secondary target. For daily mean, the diagnostic
median is about 3.07 s through staging and 2.99 s in final chunk fetch; the
cold-only fetch is about 4.26 s. Thus the net split penalty is **not** the full
staging cost: the split simultaneously replaces half the cold input with local
Parquet. The 180 MB NDJSON stage shrinking to 2.4 MB Parquet suggests direct
typed staging could be worth testing later, but the monthly cold bottleneck
and shared metadata projections offer broader benefit first. These results
do not justify changing the adopted warehouse layout or adding shard-boundary
coordination. No service, published warehouse or Git history was changed by
this review.

### Aggregation metadata dictionary candidate implemented (2026-11-05)

Implemented the aggregation-specific optimization in
`beehive-data-api/internal/backend/cold/aggregate_sql.go`. Cold scans retain raw
metadata and promoted columns until distinct tag tuples have been collected.
The dictionary merges promoted tags and canonicalizes metadata once per tuple,
then assigns a numeric ID to each distinct public `(name, meta)` identity.
Different raw tuples can resolve to one ID; observations are not deduplicated.
Null-safe lookup preserves null/empty raw metadata and key-order differences.
Already-merged hot metadata supplies null promoted columns, preserving its
existing tags. Head/tail selection, window reduction and aggregate joins use
series IDs, and the output restores complete metadata. Series membership still
comes from all observed rows, including head-zero queries. Expansion limits,
integer/VARIANT semantics, public response schema and ordinary extract/hot-only
paths are retained. There is one production aggregation SQL path, with no new
runtime tuning flag. The pre-change source is kept only as experiment evidence.

Validation and evidence are in [exp96](../tiered_system/experiments/exp96_aggregation_metadata.md)
and its [source-hashed summary](../tiered_system/experiments/results/exp96-metadata-20261105/summary.json):

| Gate | Result |
| --- | --- |
| Final production compiler against actual local Iceberg/captured oracle | **182/182 cold/split cases pass** on DuckDB 2.0.0 |
| Focused metadata dictionary regressions | **20/20 pass**: five functions × all/head0/head1/tail1; reordered keys, promoted collisions, hot/cold identity unification, null/empty metadata, repeated tuples, distinct extra tags and empty windows |
| Isolated final-source 1M-row Parquet mean | All eight warmup/measured executions agree; three measured runs per implementation, alternating order; median **1.490 s → 0.220 s (6.8×)** |
| Existing exp95 offline tests | Five pass |
| New historical comparison tool | Self-check of the saved baseline passes; verifies both sets of body hashes and values; **not new historical timing evidence** |
| Native Go API/HTTP gate | Blocked here by missing offline pinned `darwin-arm64 v0.20000.0` library |
| Historical Iceberg/HTTP performance | Pending host/VM rerun |

The local smoke uses 16 series, two threads and a 3 GB engine limit, includes
CLI startup, and excludes one warmup per implementation. It does not predict
the improvement for the 109-second monthly historical query. Raw input is
materialized once and dictionary construction/joins still compare raw tuples;
high metadata cardinality can reduce the benefit or increase memory pressure.
One intermediate local run overlapped correctness tests and is explicitly
excluded; the cited final-source measurements ran sequentially afterward.

Next: reuse the existing verified parity bucket and historical `exp95-hot`
bucket, run the new native HTTP/profile and 546-response live parity gates,
then build fresh dedicated APIs and repeat exp95 control/diagnostic sweeps.
Compare the **new unprofiled** report with
`exp95-bench-1793897282/control/report.json` using
`exp96_aggregation_metadata_compare.py`. The separate diagnostic sweep must
use its own new-image control. The comparison permits a changed API image,
requires matching workload/settings/resources, and checks every saved body
before reporting speedups. Inspect memory/spill and small-query overhead as
well as daily/monthly gains before retaining the candidate. Exact commands
are in the exp96 runbook. No published warehouse changes, commits or pushes
were made.

### Metadata dictionary host gates and historical rerun (2026-11-05)

Reused the exp91 fixture, the verified 36-row bucket
`agg-exp86-f40598c008d943e89c702249207612a3`, and the retained `exp95-hot`
bucket. Nothing was reseeded, and the published warehouse was not changed.
Logs are under
[exp96-host-1793902421](../tiered_system/experiments/results/exp96-host-1793902421).

| Gate | Result |
| --- | --- |
| Profiled compiler, metadata dictionary, production HTTP, hot staging, cold profile | Pass in 18.974s. Arrow profiles saved `complete` |
| `go test -tags=duckdb_arrow ./...` and vet | Pass |
| `TestAggregationLiveTierParity` | Pass in 5.874s. Client replay **546/546**, pandas 3.0.6, pyarrow 25.0.1, zero integer precision failures |

Fresh image `beehive-data-api:aggregation-meta-1793902541` served the same
warehouse, Influx bucket, static borders, execution settings and unlimited
container resources. The old image and containers were left in place and
received no requests. Reports:
[control](../tiered_system/experiments/results/exp96-bench-1793902541/control/report.json),
[diagnostic](../tiered_system/experiments/results/exp96-bench-1793902541/profile/report.json),
and
[comparison](../tiered_system/experiments/results/exp96-bench-1793902541/metadata-comparison.json).
The comparison checked 272 baseline and candidate bodies, including hashes and
decoded values, before reporting speedups. Every case improved.

Unprofiled medians, baseline to candidate:

| Case | Cold | Split |
| --- | --- | --- |
| Day, each function | 4.3–4.8s → 1.1–1.3s (3.5–4.3×) | 6.0–6.6s → 3.8–4.2s (1.45–1.75×) |
| Hour mean | 0.64–0.66s → 0.45–0.46s | 0.78–0.79s → 0.58–0.61s |
| Fleet one-minute count | 0.79–0.82s → 0.49–0.52s | 1.08–1.10s → 0.88–1.03s |
| All-node temperature hour | 3.0s → 0.84–0.87s | 3.8–3.9s → 2.3s |
| Month mean | 109.2–109.9s → 76.1–77.2s (1.42–1.44×) | cold only |

The first result arrives with the finished aggregate, so first-batch time is
the request time. Small queries did not regress. The daily split is still
about 2.8s slower than cold-only because hot read/encoding (about 2.3s) and
COPY (about 0.7s) remain; native execution of that split fell to about 0.6s.

Diagnostic plans build the dictionary with a hash group over distinct tag
tuples, then join the ID onto every observation. A one-series day groups to
one tuple before joining 1,048,491 rows; the one-minute fleet groups to 1,995
tuples, matching the series count rather than its 98,211 observations. Value
projection fell from about 180s to 3.9s on the month and from about 7.2s to
0.13s on the cold day. The month now scans and re-reads the materialized input:
scan work rose from 16s to 65s. It also reached the 3 GB engine cap, spilled
about 4.0 GB, and blocked threads for about 27s. The baseline month did not
spill. Daily and smaller queries used more buffer memory (the cold day rose
from about 90 MB to 526 MB) without spilling.

Native correctness and the historical comparison support keeping this SQL.
The monthly gain is smaller than the local one-million-row smoke because the
scan and the new spill now dominate projection. No commits or deployment were
made.

### Independent exp96 review and recommendation (2026-11-05)

Independently repeated the saved baseline/candidate comparison: **272 body
checks pass**, including hashes, decoded values, workload/settings/resource
matching and recorded runtime stability. All 34 case/placement/format medians
improved. See the [independent comparison](../tiered_system/experiments/results/exp96-bench-1793902541/metadata-independent-review-20261105.json).
Also replayed the new live parity bodies through the current Sage client:
**546/546 pass**, zero integer precision failures. The retained native focused
gate, full tagged API suite and vet logs confirm success. These host results
supersede the earlier sandbox-blocked native status for this implementation.

The [independent diagnostic review](../tiered_system/experiments/results/exp96-bench-1793902541/profile-independent-review-20261105.json)
checks all 68 diagnostic body hashes and their values against the new control,
all timing/native profile pairs and recorded runtime checks. Every check passes.
These reviews replay existing evidence; they do not add live timing trials.

**Recommendation: retain the metadata dictionary.** The daily cold-only mean
falls from 4.70 s to 1.21 s in Arrow (3.89×); split mean falls from 6.45 s to
4.00 s (1.61×). Monthly Arrow mean falls from 109.18 s to 76.05 s (1.44×).
NDJSON corroborates the gains, and none of the measured small workloads
regressed. The improvement preserves response parity and resolves the dominant
per-observation metadata projection cost without changing the warehouse.

The memory tradeoff is material: daily cold mean's diagnostic median engine
peak rises from about 90 MB to 526 MB, with no spill; monthly peak rises from
about 1.31 GB to 3.00 GB and reported peak temporary-directory usage reaches
4.03 GB. Monthly blocked-thread time is about 26.7 s. These are diagnostic
engine metrics, not process RSS or additive elapsed phases. Unlimited container
resources do not remove the API's separately fixed **3 GB DuckDB memory limit**.

The next isolated target is the width/lifetime of raw-input materialization.
In one retained measured monthly plan, the scan class includes about 42.3 s
of `ICEBERG_SCAN` operator work and 21.8 s of `CTE_SCAN` work. It is not evidence
that the current query scans Pelican twice: the raw CTE is reused, including a
materialized consumer. Do not interpret their sum or the 26.7 s blocked metric
as independently additive request latency, or attribute all scan work to the
network. The new bottleneck is consistent with retaining and rereading wide
raw rows under the engine cap; exact attribution needs another comparison.

Recommended next experiment: keep this result as the baseline and compare
ways to reduce retained raw data or dictionary-build buffering, measuring
memory, spill, bytes read and end-to-end HTTP latency together. A separate
larger-engine-memory control would distinguish spill cost from unavoidable
scan/join work; it would not by itself prove a better SQL design. Removing
materialization blindly can introduce a second remote scan and should be
measured explicitly. For split daily queries, hot read/encoding and COPY now
dominate over the much smaller final native aggregate; direct typed staging
remains a separate promising target. No additional implementation, service or
published storage changes were made during this review, and nothing was
committed or pushed.

### Metadata dictionary adopted as the sole implementation (2026-11-05)

Accepted following the passing host correctness gates and historical results.
Audited `beehive-data-api`: both cold-only and split aggregates already call
the dictionary-based `buildColdAggregateSQL` → `buildAggregateSQL` path. The
earlier per-observation aggregation builder was replaced during implementation;
there is no alternate builder, feature flag, environment variable or fallback
branch to remove. Updated the API documentation to identify the dictionary as
the sole implementation, rather than a candidate. Historical source snapshots
and benchmark results remain outside the service as experiment evidence; they
are not linked into the API. No runtime code change was needed for this
adoption, and nothing was committed or pushed.

### Raw-input materialization/spill experiment prepared (2026-11-05)

Targeted the retained wide raw CTE identified by exp96, without changing the
adopted production aggregation path or introducing a fallback/configuration
option. Added [exp97 and its host runbook](../tiered_system/experiments/exp97_aggregation_materialization.md).
The test-only candidate changes `raw AS MATERIALIZED` to `raw AS NOT
MATERIALIZED`. It can build the tag dictionary from a metadata-focused source
scan and stream a second source scan through the dictionary, trading remote
scan work for much less retained raw data. This must be measured on Iceberg
before adoption. Removing spill alone does not establish faster HTTP queries.

Both the adopted SQL and the inlined variant pass **182 captured-oracle
cold/split cases plus 20 metadata/head-tail cases** using the actual compiler,
native DuckDB 2.0.0 CLI and local Iceberg fixture. Four offline harness tests
pass, including rejection of changed snapshots and distinguishing source scans
from CTE rereads. See the [source-hashed summary](../tiered_system/experiments/results/exp97-materialization-final-20261105/summary.json).

The final local one-million-row Parquet comparison, two threads and three
measured repetitions after an excluded warmup, found:

| Budget | Materialized median | Inlined median | Peak engine buffers | Peak temp usage | Source scans |
| --- | ---: | ---: | --- | --- | --- |
| 128 MB | 0.327 s | 0.271 s | 130 MB → 31 MB | 146 MB → zero | 1 → 2 |
| 3 GB | 0.241 s | 0.242 s | 372 MB → 39 MB | zero for both | 1 → 2 |

These include CLI process startup and local output capture, not HTTP or remote
storage. The initial repeat showed a larger no-spill regression (0.220 s →
0.434 s); it did not repeat at that magnitude. Timings are noisy, while lower
buffers and the extra source scan are consistent. Retained final SQL, bodies,
profiles and trials are [here](../tiered_system/experiments/results/exp97-materialization-final-20261105/report.json).
The production SQL source hash still matches the adopted exp96 implementation.

Also tested packing observations into per-tag lists to retain metadata only
once while scanning once. It failed at 128 MB with a native allocation OOM,
where the baseline completed. Rejected that prototype and removed it from the
maintained harness. Its [failed evidence](../tiered_system/experiments/results/exp97-packed-20261105/report.json)
and generated SQL remain outside the service as experiment artifacts.

Prepared `TestAggregationMaterializationProbe` and
`exp97_iceberg_materialization.py` for sequential, read-only native Arrow
queries against the adopted immutable warehouse. The matrix is materialized
3 GB, inlined 3 GB, and materialized 6 GB (experimental memory control only).
Default cases are daily and monthly mean, with fleet count available. Each
configuration gets an excluded warmup and three measured trials; a separate
profiled sweep requires its completed unprofiled control and the same binary.
Every output must match the retained exp96 historical body; binary/reference
hashes, actual engine settings, pinned metadata, source scan projections,
buffers, spill and native phases are retained. A changed snapshot fails the run.
The probe excludes initialization/auth/hint setup and writes IPC locally, so
its timings are **native diagnostics, not end-to-end HTTP measurements**.

The sandbox still cannot execute the pinned Go native probe: the cached pinned
native library is missing. A secondary type-check attempt reached linking and
failed for missing `libduckdb`; it is not a native runtime pass and no module
versions were changed. Host commands are in the runbook. No Influx seeding is
needed, no published storage was changed, and no commits or pushes were made.
After the host comparison, choose an implementation only if its scan/spill
tradeoff is supported, then validate the production HTTP/live path and repeat
the HTTP benchmark before adoption.

### Materialization host comparison (2026-11-05)

The inlined raw CTE stays a test-only candidate. Production aggregation remains
the materialized dictionary SQL at the 3 GB engine budget. No HTTP/live parity
rerun and no exp95 sweep were started, because this comparison does not support
the change.

Inline compiler and metadata checks passed in 14.417s on DuckDB 2.0.0. The
tagged suite and vet passed. The probe binary is
[exp97-host-1793905323/cold.test](../tiered_system/experiments/results/exp97-host-1793905323/cold.test).
The first trial recorded garbage engine settings: `ValueStr` aliased the Arrow
buffer, and releasing the reader freed it. The probe now copies those three
strings before release. That failed trial is retained under
`control-buffer-lifetime`. The rerun recorded engine `v2.0.0`, two threads, and
DuckDB's displayed limit `2.7 GiB` for the requested 3 GB setting.

Unprofiled native medians, three measured trials, every body matching the
exp96 cold Arrow reference
([report](../tiered_system/experiments/results/exp97-host-1793905323/control/report.json)):

| Case | Materialized 3 GB | Inlined 3 GB | Materialized 6 GB |
| --- | ---: | ---: | ---: |
| Daily mean | 2.755s | 2.183s | 2.785s |
| Monthly mean | 69.740s | 68.560s | 63.037s |

The separate one-repetition diagnostic
([report](../tiered_system/experiments/results/exp97-host-1793905323/profile/report.json))
shows one Iceberg scan in every configuration. Its projection is the full raw
row (`time`, `name`, `value`, `meta`, and the promoted tags), not a narrow
metadata pass followed by a second value scan. `CTE_SCAN` rereads that result
inside the engine. The monthly scan is about 39s of the roughly 64–66s native
time. First result and completion are the same instant.

Monthly spill did not go away. At 3 GB both SQL forms sat on the engine cap
(about 3.00 GB of buffer), wrote a temporary directory of about 6.5 GB, and
blocked threads for about 30s. The 6 GB control still spilled about 3.6 GB
while using a 6.00 GB buffer, and its monthly median was 63.0s. Daily queries
did not spill under any configuration; their sub-second differences changed
sign between the unprofiled and profiled runs.

Inlining therefore does not buy a second, narrower remote scan or a
no-spill month on this warehouse. The production path and its 3 GB budget stay
as they were. No warehouse writes, commits, or deployment were made.

### Independent exp97 review and next direction (2026-11-05)

Independently verified **36/36 retained native Arrow bodies**, including body
hashes and parity with the exp96 historical reference, and **12/12 diagnostic
timing/native profile pairs**. The retained test binary matches both reports'
hash, all trials pin the same metadata URL, the SQL files match their intended
materialized/inlined variants, and recomputed unprofiled medians match the
reports. See the [independent review](../tiered_system/experiments/results/exp97-host-1793905323/independent-review-20261105.json).
This is an offline evidence review, not another live timing run. Preserve the
probe's `strings.Clone` fix: engine-setting strings must outlive the native
reader that owns their original buffers.

The monthly inlined median is only **1.7% lower** than materialized 3 GB
(69.740 s → 68.560 s), while the measured inlined range is 68.221–72.782 s.
The physical plans do not deliver the intended structural change: every
configuration has one full-row Iceberg scan, and the 3 GB variants retain
roughly the same buffer cap, ~6.5 GB peak temporary-directory usage and
~30 s blocked-thread metric. The local Parquet result of two source scans
with zero spill therefore does not transfer to this actual Iceberg plan.
The unprofiled daily improvement reverses in the single profiled repetition;
that is insufficient evidence for adopting the hint as an optimization.

The 6 GB materialized control lowers monthly native median by **9.6%** to
63.037 s, but doubles the allowed engine budget and still reaches ~3.61 GB
peak temp usage. It is a useful control, not evidence for a spill-free plan or
a reason to silently raise the service default. Reported peak temporary size
is not cumulative spill bytes written; operator work and blocked time are not
additive wall-clock phases. These native timers exclude engine/auth/hint setup
and do not replace the earlier HTTP benchmark.

**Decision: retain the adopted materialized dictionary SQL and 3 GB production
budget.** The unsuccessful inlining candidate remains test-only; no production
fallback or option was added. No new HTTP rollout validation is needed for an
unchanged implementation. Keep the failed/successful experiment artifacts as
evidence rather than promoting this candidate.

Further work on this target should reduce the amount of observation data that
must be retained, rather than only changing the CTE directive. A promising
next isolated experiment is partial reduction before buffering: compact
per-tag/window aggregate state instead of complete raw observations, followed
by canonical series merging. This is a proposal, not implemented behavior.
It must preserve native-kind rejection, exact integer wrap/unsigned semantics,
mean sum/count behavior, floating tolerances, metadata collision equivalence,
empty windows and limits applied before aggregation. Head/tail requires special
care because arbitrary reduction before selecting observations is incorrect.
Do not replace the proven path until those semantics and historical performance
are validated. No runtime, warehouse or Git-history changes were made by this
review.

### Materialization experiment code removed (2026-11-05)

Retained the adopted materialized metadata dictionary and fixed 3 GB budget.
Removed the exp97 native probe, SQL-rewrite helper and environment hooks from
the compiler tests, both Python experiment drivers and their driver tests.
The compiler and metadata tests now exercise only the production SQL again.
Updated the API validation guide and experiment index, and replaced the exp97
execution runbook with a retired experiment summary. Historical result files,
profiles, SQL/source snapshots and binaries remain preserved under `results/`;
they are not part of the service or its active tooling.

Validation: the production aggregation SQL hash still matches the adopted
exp96 source; no experiment hooks remain in active Go/Python sources. The
native CLI compiler oracle and metadata regressions pass again (**182 + 20
cases**). No new runtime implementation or option was introduced, published
storage was unchanged, and nothing was committed or pushed.

### Pre-buffer aggregation experiment prepared (2026-11-05)

Added [exp98](../tiered_system/experiments/exp98_partial_aggregation.md) as an
isolated experiment, leaving the adopted production dictionary SQL and 3 GB
engine budget unchanged. No API fallback or tuning option was introduced.

The initial design grouped raw tag/window tuples and merged numeric states
after metadata canonicalization. It passes the ordinary no-limit oracle, but
floating-point reassociation is unsafe for preserving answers. A native
[counterexample](../tiered_system/experiments/results/exp98-partial-20261105/float-reassociation-counterexample.json)
returns 0 for a single sum over `1e16, 1, -1e16`, versus 1 after grouping the
outer values into one subtotal. That design is rejected and retained only as
historical experiment evidence. Early development failures and the rejected
prototype's timing are not final-candidate validation.

The revised candidate derives an equivalent grouping tuple per observation:
name, sorted non-promoted metadata entries, and each promoted tag's effective
value plus a presence bit. This preserves override precedence, map ordering,
and null-versus-absent keys without constructing the full public MAP per row.
Each canonical series/window then uses the **unchanged production numeric
expression in one native reduction**. Only reduced rows are materialized;
public metadata is reconstructed after reduction. It does not average averages
or merge separately computed numeric subtotals from equivalent raw tuples.
Residual-key normalization still costs CPU per observation. Limits are excluded
from this first experiment because head/tail must select original observations
before aggregation. No limit request is silently sent through a partial path.

Final [local evidence and source hashes](../tiered_system/experiments/results/exp98-partial-20261105/summary.json):

| Check | Result |
| --- | --- |
| Existing production oracle and metadata/limit suite | 182 + 20 cases pass |
| Candidate captured oracle, actual local Iceberg | **170 pass**; 12 head/tail cases explicitly skipped |
| Candidate metadata equivalence | Five functions pass; 15 limit variants skipped |
| Numeric/type/alias regressions | **40 pass**, including cancellation, weighted means, signed/unsigned wrap, nullable/all-null values and mixed/unsupported input rejection |
| Additional checks | Null-versus-absent promoted keys stay distinct; head/tail candidate rejection passes |
| Offline tooling | Four tests pass, including snapshot-change failure retention and exact timestamp/unsigned decoding |
| Native Go historical probe | Pending host; type-check reaches linker but local native library is unavailable |

One-million-row local Parquet mean, two threads, one excluded warmup and three
measurements per variant, alternating sequential order:

| Budget | Current dictionary | Revised candidate | Peak engine buffers | Peak temporary size |
| --- | ---: | ---: | --- | --- |
| 128 MB | 0.304 s | 0.324 s | 130 MB → 33 MB | 151 MB → zero |
| 3 GB | 0.221 s | 0.371 s | 372 MB → 41 MB | zero for both |

All 16 outputs match. The local plan reduces one million observations to 272
observed series/window groups before returning 320 padded window rows. Lower
buffers and zero spill are promising, but CPU/latency is worse on this local
case. High metadata cardinality or tiny windows can reduce the benefit. Native
profiles must confirm the actual buffering behavior on Iceberg; SQL shape
alone does not prove it. No historical speedup is claimed.

Prepared `TestPartialAggregateIcebergProbe` and
`exp98_partial_iceberg.py` to compare current dictionary and revised reduction
at the same 3 GB budget on the adopted immutable warehouse. Default cases are
daily/monthly mean and fleet minute count. Every saved Arrow response must
match the retained exp96 historical reference; binary/reference hashes,
metadata pinning, actual engine settings, group cardinalities, source scan
projections, memory/spill and timing are retained. The probe clones native
engine-setting strings before releasing their reader. A separate diagnostic
sweep requires the matching unprofiled control. Native timers exclude
engine/auth/hint startup and are not HTTP latencies. No Influx seeding is
needed. Run the commands in the exp98 guide on the host/VM; integration and
HTTP/live parity remain subsequent gates only if the tradeoff is worthwhile.
No published storage changes, commits or pushes were made.

### Pre-buffer reduction host comparison (2026-11-05)

The revised candidate stays a test-only probe. Production aggregation remains
the materialized dictionary SQL at 3 GB. The unprofiled historical medians do
not show a latency win, so production integration and the HTTP/live gates were
not started.

Correctness passed in one cold-package run of 30.530s: production oracle and
metadata/limit suite, candidate oracle, state merges, metadata equivalence,
null-versus-absent keys, and head/tail exclusion. The tagged suite and vet
passed. The probe binary is
[exp98-host-1793912997/cold.test](../tiered_system/experiments/results/exp98-host-1793912997/cold.test).
Engine settings were `v2.0.0`, two threads, and DuckDB's displayed `2.7 GiB`
for the requested 3 GB budget. Every Arrow body matched the exp96 cold
reference.

Unprofiled medians, three measured trials
([report](../tiered_system/experiments/results/exp98-host-1793912997/control/report.json)):

| Case | Dictionary | Pre-buffer reduction |
| --- | ---: | ---: |
| Daily mean | 2.186s | 3.636s |
| Monthly mean | 66.496s | 67.192s |
| Fleet minute count | 2.635s | 2.711s |

The one-repetition diagnostic
([report](../tiered_system/experiments/results/exp98-host-1793912997/profile/report.json))
confirms the intended plan shape. Both variants make one Iceberg scan of the
full raw projection. The candidate's aggregate hash group emits the reduced
series/window rows directly: 288 for the day, 597 for the month, and 1,995 for
the fleet count, using the production numeric expressions in that one
reduction. The dictionary plan still builds a one-row tag dictionary before
the window reduction. Monthly peak buffer fell from 3.00 GB to 1.22 GB, and
temporary storage fell from 6.84 GB to zero. Daily peak buffer fell from 519 MB
to 80 MB, and the fleet count from 91 MB to 44 MB. Neither of those smaller
queries spilled under the dictionary either. Blocked thread time on the month
stayed about 31s, so the remote scan, not the spill, still accounts for that
wait. That single profiled month was 63.8s against 71.3s, which is not the
three-trial median.

The buffer and spill improvement is real. It does not outweigh the daily
regression or the flat monthly and fleet medians, so spill elimination alone
does not justify a production change. No warehouse writes, commits, or
deployment were made.

### Independent exp98 review and recommendation (2026-11-05)

Independently verified **36/36 saved native Arrow bodies** against the retained
exp96 reference, including hashes, bounds, identities, row counts and numeric
parity. All **12 diagnostic timing/native profile pairs** match their embedded
data and plan summaries. The retained binary and reference hashes match both
reports, metadata pinning is consistent, and recomputed medians match. The
retained focused correctness, tagged suite and vet logs confirm success. See
the [independent review](../tiered_system/experiments/results/exp98-host-1793912997/independent-review-20261105.json).
This review replayed existing evidence; it did not execute new live queries.

The memory result is a genuine resource improvement, but the measured speed
result does not support adoption:

| Unprofiled native workload | Dictionary | Candidate | Change |
| --- | ---: | ---: | ---: |
| Daily mean | 2.186 s | 3.636 s | about 66% slower |
| Monthly mean | 66.496 s | 67.192 s | 1.0% slower; broadly flat |
| Fleet minute count | 2.635 s | 2.711 s | 2.9% slower; small difference |

The daily measured ranges do not overlap: dictionary 2.153–3.051 s versus
candidate 3.623–4.351 s. Monthly ranges overlap substantially, and the single
profiled candidate month is faster than its profiled baseline; that diagnostic
sample must not replace the three-trial unprofiled result. These numbers are
native probe timings, not end-to-end API latency.

The intended structural change occurred. Monthly diagnostic engine peak drops
from about 3.00 GB to 1.22 GB, and peak temporary-directory usage drops from
6.84 GB to zero. Both variants still scan 25,940,207 observations once and
report similar bytes read. The candidate emits 597 observed monthly groups
before padding to 744 output windows. It trades raw buffering for per-row
canonical-key work; daily diagnostic CPU work rises from about 2.00 s to
2.74 s even though peak buffers fall from 519 MB to 80 MB.

Blocked-thread time remaining near 31 s after eliminating spill shows that
spill cannot explain all the waits. It does **not**, by itself, attribute every
blocked second to the network or prove remote scanning is the only bottleneck.
Operator work, blocked time and elapsed time overlap. The lower memory might
help concurrent/resource-constrained workloads, but that has not been measured.

**Recommendation: retain the existing dictionary implementation and do not
integrate exp98 as a latency optimization.** Production was never changed;
head/tail still uses its proven behavior, and no candidate fallback or option
exists in the API. The experiment remains test-only pending a separate cleanup
decision. Do not broaden it to head/tail or run HTTP rollout gates for an
implementation we are not adopting.

The next clearer latency target is **direct typed hot staging** for split
aggregates. Exp96 already measured roughly 2.3 s reading/encoding the hot day
slice plus 0.7 s converting NDJSON to Parquet, versus roughly 0.6 s in the
final native aggregate. Benchmark replacing the intermediate NDJSON/COPY step
with direct typed Parquet staging, while retaining source-kind validation,
exact uint64/nanoseconds, staging budgets, cleanup and read-grant ownership.
That proposal changes private staging only, not the public response schema or
warehouse layout. It has not been implemented by this review. No service,
published storage, commits or pushes were changed.

### Bounded concurrency comparison prepared (2026-11-05)

Added [exp99 and its host runbook](../tiered_system/experiments/exp99_aggregation_concurrency.md)
to evaluate exp98's memory/spill benefit under queued callers and concurrent
engine pressure, rather than discarding a resource improvement solely because
single-query medians did not improve. Production SQL, its 3 GB budget and
scheduling remain unchanged.

Scheduling is important to this comparison: the API holds its shared cold
backend mutex through native result closure, for both Arrow and NDJSON. Several
cold callers to one instance therefore queue; they do not run several active
queries in one engine. The earlier capacity suggestion needs this qualification.
The probe has two explicit scopes:

- **Shared backend:** one real backend/Arrow engine, with 1 or 2 callers by
  default (4 opt-in), preserving its actual mutex and queueing behavior.
- **Independent engines:** one backend per caller, 2 by default (4 opt-in),
  in a single probe process. This is a replica/resource-pressure proxy, not
  concurrency enabled within one deployed API or a multi-container load test.

All active Arrow engines retain two CPU threads, eight async I/O threads,
read-ahead -1 and 3 GB. Engine count is one in shared scope and N in independent
scope; normal per-backend idle SQL/runtime overhead also exists. Shared N=1
serves as the single-engine baseline; independent N=1 is redundant and omitted.
Engines are fresh per wave and reused by queued requests within that wave.
The probe is not a steady-state HTTP/replica benchmark and does not impose a
node-wide memory cap. Use and record the same host/resource limits.

Each wave initializes engines/auth and pins metadata, signals readiness, then
waits for the driver to sample its idle process and open a start barrier. Query
latency starts from common wave admission, including shared mutex waiting,
native execution and Zstd IPC to private output files. The process stays alive
after wave completion for final CPU/RSS/scratch sampling, then closes engines.
The driver records throughput and all request latencies, sampled process RSS,
CPU total/per-query, Linux storage-I/O deltas when available, simultaneous
aggregate allocated/logical scratch peaks and post-close scratch. It does not
sum per-query memory/temp maxima. Linux lifetime RSS high-water includes setup
and is labelled separately; sampled peaks may miss short transients. Kernel
page cache is not included in RSS and process storage I/O is not network bytes.

Every output must match the retained exp96 historical reference, with exact
integers/timestamps/metadata and unchanged float tolerance. The driver rejects
missing/failed queries, wrong backend assignments, changed snapshots, settings
or reference bodies. Binary/reference hashes and raw resource samples are
retained. Native timers exclude engine/auth/hint setup and are not HTTP/SLO
latencies. Descriptive p95 over a few requests is not a service-tail estimate.

Defaults are monthly mean, concurrency 1/2, one excluded warmup plus three
measured waves per mode/scope/concurrency cell, alternating dictionary/candidate
order: **24 waves / 40 monthly executions**, including warmups. Concurrency is
hard-bounded at 4; daily mean is available for a shorter smoke/control. A
separate optional profiled phase requires the matching unprofiled control and
same binary/cases/scenarios. It exposes actual `arrow_queue_wait_seconds`;
unprofiled queue-and-setup timing is combined and must not be labelled pure
queue time. No Influx service or reseeding is needed.

Local validation: **five offline tests pass** for process units, global peaks,
scratch isolation, shared/independent orchestration, parity, and snapshot-change
failure/process cleanup. These mock the native process and do not establish
concurrency performance. The Go probe type-check reaches the linker, then
fails because `libduckdb` is unavailable; the pinned native runtime remains
unavailable in this sandbox. Real process sampling is also blocked here by
the `ps` sandbox restriction. The [source-hashed summary](../tiered_system/experiments/results/exp99-concurrency-20261105/summary.json)
records these limits. Run the host build/preflight and comparison commands in
the runbook. Do not claim a capacity benefit until the real measurements exist.
No published storage, deployment, commits or pushes were changed.

### Bounded concurrency host comparison (2026-11-05)

Production SQL, the 3 GB budget, and the backend mutex stay as they are. The
candidate remains test-only. The host is Darwin arm64 with 18 logical CPUs and
24 GiB of physical memory. The probe had no process memory cap. Sampling used
`ps`; macOS has no Linux storage-I/O counters. Existing API containers were
left idle. Suite, vet, and the probe build passed. Process sampling returned
RSS and CPU. Every Arrow body matched the exp96 cold monthly reference.
Reports:
[control](../tiered_system/experiments/results/exp99-host-1793916413/control/report.json)
(24 waves, status complete) and
[profile](../tiered_system/experiments/results/exp99-host-1793916413/profile/report.json)
(one measured wave per cell). Engine settings were `v2.0.0`, two threads, and
DuckDB's displayed `2.7 GiB`. Scratch was gone after every engine close.

Unprofiled medians, three measured waves, monthly mean:

| Scope | Callers | Mode | Wave | Queries/s | Median request | Peak RSS | Scratch |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: |
| Shared | 1 | Dictionary | 70.96s | 0.0141 | 70.96s | 2.74 GB | 6.54 GB |
| Shared | 1 | Candidate | 66.93s | 0.0149 | 66.93s | 1.41 GB | 0 |
| Shared | 2 | Dictionary | 131.43s | 0.0152 | 102.13s | 3.36 GB | 6.60 GB |
| Shared | 2 | Candidate | 91.64s | 0.0218 | 79.78s | 1.19 GB | 0 |
| Independent | 2 | Dictionary | 75.86s | 0.0264 | 74.53s | 4.23 GB | 12.41 GB |
| Independent | 2 | Candidate | 70.66s | 0.0283 | 68.61s | 1.78 GB | 0 |

CPU per query was higher for the candidate: 48.4s versus 45.8s at one shared
caller, 46.5s versus 41.7s at two shared callers, and 50.6s versus 43.6s with
two independent engines. The descriptive p95 values use three or six requests
and are not a service-tail estimate.

The profiled wave separates mutex wait from native execution. Independent
callers and the first shared caller wait about 0s. The second shared caller
waits for the first query to finish, 63.6s for the candidate and 67.4s for the
dictionary, matching that query's lifetime. After the wait, the candidate's
follow-up fetch is 24.2s; the dictionary follow-up fetch stays 62.8s. The same
split appears in all three unprofiled shared pairs: the second candidate query
runs about 24–25s, and the second dictionary query runs about 62–65s. One
locked backend still runs one query at a time. The shorter wave is a faster
second query on the reused engine, not concurrent execution inside one API.

Two independent engines do overlap. Their makespan stays near one query, and
the candidate's throughput gain is about 7%. Each dictionary engine still uses
a 3.00 GB buffer and about 6.6 GB of temporary space, so the pair spills about
12 GB and peaks near 4.2 GB RSS. The candidate pair uses about 1.22 GB of
buffer per engine, no temporary space, and 1.78 GB sampled RSS.

The memory and scratch reduction is repeatable. It is a replica-density
observation, not a faster single-query implementation. The shared-queue
completion gain is real and comes from the warm follow-up query. Independent
throughput barely moves, and CPU per query is higher. Head/tail remains
outside this candidate, so it is not the production path. No warehouse writes,
commits, or deployment were made.

### Independent exp99 review: resource benefit merits further validation (2026-11-05)

Independently verified **60/60 saved responses** across 36 control/diagnostic
waves, **20/20 diagnostic timing/native profile pairs**, the retained binary
and reference hashes, metadata pinning, and engine assignments. Recomputed
resource peaks and CPU deltas from raw samples and recomputed the reported
medians/throughput. All checks pass; scratch logical bytes after engine close
are zero. The retained tagged suite and vet logs confirm success. See the
[independent review](../tiered_system/experiments/results/exp99-host-1793916413/independent-review-20261105.json).
This is offline evidence replay, not a new live run.

**This changes the recommendation from rejecting the candidate as a latency
optimization to retaining it for production-shaped integration validation.**
The earlier daily regression still matters, but the lower working set now has
a repeatable queued-workload benefit as well as substantial resource savings.

| Measured monthly workload | Dictionary | Candidate | Interpretation |
| --- | --- | --- | --- |
| Two callers sharing one backend: wave completion | 131.43 s | 91.64 s | About 30% sooner; throughput about 43% higher |
| Two callers sharing one backend: sampled peak RSS | 3.36 GB | 1.19 GB | About 65% lower |
| Two independent engines: wave completion | 75.86 s | 70.66 s | About 7% higher throughput |
| Two independent engines: sampled peak RSS | 4.23 GB | 1.78 GB | About 58% lower |
| Two independent engines: peak allocated scratch | 12.41 GB | Zero to reported GB precision | Spill pressure removed |

The shared backend remains serialized. In every measured unprofiled pair,
the candidate's first completion is about 66–69 s and the follow-up takes only
24–25 s; the dictionary follow-up takes 62–65 s. Actual profiled mutex waits
confirm the second query waits for the first to close. Thus the gain is a
faster follow-up query on the reused engine, not two queries running at once.

The diagnostic candidate follow-up still processes 25,940,207 observations,
but engine-reported read bytes fall from roughly 1.18 GB to 20.6 MB and blocked
time from about 29 s to 0.33 s. This is consistent with better reuse of cached
input under the smaller working set. It does not establish a specific cache
mechanism or quantify total Pelican traffic, and it is not a cached-response
claim. The workload repeats the identical monthly query; different or partly
overlapping ranges need validation before generalizing the warm benefit.

CPU per query increases about 12% in the shared two-caller case and 16% with
two independent engines. Independent throughput gains are much smaller than
RSS/scratch savings. The 24 GiB host had no process memory cap, and independent
engines share one process; these results demonstrate resource savings and a
capacity opportunity, not maximum replica density, multi-container throughput
or behavior under a particular production cgroup limit. Sampled RSS excludes
kernel page cache; sampled peaks and the few-request p95 retain their previous
limitations.

Recommended next gate: exercise the candidate through the actual HTTP path for
unrestricted aggregates, including cold-only and split queries, Arrow/NDJSON
and the existing live parity oracle. Preserve correct observation selection
for head/tail; this experiment has not implemented it. Then compare reused
engines with repeated and mixed/overlapping ranges, and include short daily
queries so the CPU/latency cost is visible. This is worth investigating as a
resource-efficiency improvement rather than claiming every query is faster.
Do not introduce a public fallback/tuning flag or silently replace all query
classes based on this native-only result. Production remains unchanged by
this review; no warehouse writes, commits or pushes were made.

### Pre-buffer reduction integrated in API source; HTTP gates prepared (2026-11-05)

Integrated the exp98 canonical-key reduction into the real
`beehive-data-api` cold/split SQL compiler. Aggregates **without head/tail**
now use `aggregate_reduce.go` to group equivalent series/window tuples before
buffering and reconstruct public metadata afterward. Aggregates **with
head/tail** use dictionary-based observation selection before reduction,
preserving global limits, head-zero membership and empty windows. This is
dispatch by query semantics, not a memory heuristic or operator fallback.
No new API/configuration option was added. The warehouse, staging, cold mutex,
3 GB engine budget, public schema, hot-only Flux and ordinary extracts are
unchanged. Deployment has not been changed.

Extracted the existing numeric expression into shared `aggregateValueSQL`:
both executors retain the same native-kind validation, integer wrapping,
unsigned handling, mean and finite-result rules. The production reduction
builder receives its source directly; it no longer parses SQL from a probe.
Historical exp98/99 test adapters explicitly generate their named dictionary
and reduced baselines after recompilation. Numeric edge tests still compare
against an independent dictionary query. Removed redundant experimental oracle
wrappers: the main compiler/metadata tests now verify actual dispatch for all
queries, including head/tail.

[Source-hashed local validation](../tiered_system/experiments/results/exp100-integration-20261105/summary.json):

| Gate | Result |
| --- | --- |
| Integrated production compiler / actual local Iceberg / captured oracle | **182/182 pass**, including head/tail |
| Metadata/limits/empty-window equivalence | **20/20 pass** through actual dispatch |
| Numeric/type/alias/cancellation edges | **40/40 pass** versus dictionary baseline |
| Null-versus-absent key and direct-limit guard | Pass |
| Paired HTTP benchmark tooling | Four offline tests pass: repeat/overlap/limit sequence, retained-image/settings checks, reused-engine order, and retained parity-failure evidence |
| Native Go API/HTTP/live gate | Pending host; secondary type-check reaches linker then fails for unavailable local `libduckdb`; no module pins substituted |

Added [exp100 and its exact host/VM runbook](../tiered_system/experiments/exp100_aggregation_http.md).
First run actual production HTTP with profiling enabled, the full tagged
suite/vet, tiered race checks, and the **546-response live three-placement
client replay** using the existing verified seed. No reseeding is needed.

The new paired HTTP tool compares the retained exp96 dictionary image with a
new image from the integrated source, using dedicated APIs and read-only
inspection. It requires matching runtime settings/resources, validates the
baseline image digest, reuses the same warehouse/historical hot bucket, and
checks every body semantically across images/formats. Cold mixed sequences
include immediate monthly repeats, March 8–29 overlap, all five daily functions,
hour/fleet/all-node short queries, head/tail, then return to the original month.
Split sequences use only the ten verified March 1 short/day/limit windows;
a month-long split would exceed the retained hot fixture's coverage and is not
pretended here. All split queries cross the noon border.

Each whole sequence runs on a reused API before switching images; image order
alternates by repetition. Both Arrow and NDJSON use their existing distinct
native engines. One whole-sequence warmup is excluded; caches are not reset
and no cold-cache claim is made. Defaults yield 224 cold and 160 split HTTP
responses including warmups. Shorter smoke/one-format runs are available with
explicit scope. Primary timing excludes semantic validation and container
inspection. Optional Arrow diagnostics require a matching unprofiled control.
OOM/restart/config drift or parity failures stop the run and retain evidence.

Use two dedicated containers at a time, cold phase then split phase, to avoid
unnecessary simultaneous engine caches. The guide stops only those newly
created dedicated containers between phases and preserves their mounted
outputs/logs. No tool here deployed or restarted services, wrote published
storage, committed or pushed. HTTP runtime correctness, client replay and
reused-engine performance conclusions remain pending the host results.

### Integrated reduction host gates (2026-11-05)

Correctness, the tagged suite, vet, the tiered race check, and the 546-response
live replay passed before the paired HTTP comparison.
[Correctness](../tiered_system/experiments/results/exp100-host-1793922689/correctness.log)
was `ok` in 21.567s. The
[suite](../tiered_system/experiments/results/exp100-host-1793922689/suite.log)
and [vet](../tiered_system/experiments/results/exp100-host-1793922689/vet.log)
exited 0. The
[race check](../tiered_system/experiments/results/exp100-host-1793922689/race.log)
was `ok` for the tiered package in 1.987s. The live cold package was `ok` in
6.397s, and the
[client replay](../tiered_system/experiments/results/exp100-host-1793922689/parity-live-http/client-report.json)
was 546/546 with zero integer-precision failures (pandas 3.0.6, pyarrow 25.0.1).
The verified hot bucket was reused. Nothing was reseeded.

The first cold pair then failed and was retained at
[cold-control](../tiered_system/experiments/results/exp100-host-1793922689/cold-control/report.json).
Eight idle experiment APIs from the exp95 and exp96 runs were still resident
and held most of the VM's anonymous memory, so the dictionary month scan ran
for 20 minutes and the 20-minute SciToken expired. Pelican returned HTTP 403
for one parquet object and the API closed the connection. Those eight
containers were stopped and left on disk. Influx and kube-system were left
running. The comparison was repeated with fresh containers and was not resumed
in the failed directory.

The retained dictionary image is
`beehive-data-api:aggregation-meta-1793902541`. The candidate image is
`beehive-data-api:aggregation-reduce-1793922815`. Both used the adopted
read-only warehouse, `exp95-hot`, two DuckDB threads, a 3 GB engine budget,
and no extra container memory or CPU cap. Cold border
`2025-05-01T00:00:00Z`. Split border `2025-03-01T12:00:00Z`.

[Cold control](../tiered_system/experiments/results/exp100-host-1793922689/cold-control-r2/report.json)
completed 224/224 responses, all parity checks passed, and neither container
restarted or was OOM-killed. Medians exclude the one whole-sequence warmup.
They are reused-engine times, not a cold-cache measurement.

| Cold step | Rows | Dictionary Arrow | Reduction Arrow | Dictionary NDJSON | Reduction NDJSON |
| --- | ---: | ---: | ---: | ---: | ---: |
| Full March, first measured | 744 | 72.85s | 20.23s | 81.39s | 20.05s |
| Immediate March repeat | 744 | 68.63s | 19.23s | 75.31s | 19.85s |
| March 8–29 overlap | 504 | 44.02s | 11.51s | 46.64s | 11.81s |
| Return to full March | 744 | 68.34s | 19.02s | 69.34s | 19.42s |
| Day count | 288 | 3.11s | 0.94s | 3.15s | 0.90s |
| Day mean | 288 | 1.07s | 1.19s | 1.06s | 1.15s |
| Day sum | 288 | 0.98s | 1.15s | 1.48s | 1.13s |
| Day min | 288 | 0.95s | 1.53s | 1.02s | 1.13s |
| Day max | 288 | 1.00s | 1.13s | 1.02s | 1.52s |
| Hour mean | 60 | 0.59s | 0.43s | 0.42s | 0.45s |
| Fleet minute count | 1995 | 2.00s | 0.44s | 1.97s | 0.45s |
| Temperature fleet hour | 192 | 1.16s | 0.84s | 1.10s | 0.85s |
| Day head mean | 288 | 1.05s | 1.07s | 0.95s | 1.05s |
| Day tail mean | 288 | 0.94s | 1.09s | 1.18s | 1.11s |

The repeated and overlapping cold months are about 3.6–4.1× faster on the
reduction image, in both formats. Day count and the warm fleet minute count
are also faster. Day mean, min, max, and sum move by less than about 0.6s and
are not consistently faster. Head and tail match the dictionary bodies; a
1,000-row limit does not shorten this 288-row day. The excluded warmup still
matters: the first dictionary March query took 134s and the first reduction
March query took 65s. The ~20s figures are later queries on the same engine.

[Split control](../tiered_system/experiments/results/exp100-host-1793922689/split-control/report.json)
completed 160/160 responses, including head/tail, with parity passed. Every
query crosses noon. No month-long split was run. Split medians stay within
about 13% of the dictionary image either way; most daily queries are within a
few percent. The noon-crossing day remains about 3.6–4.3s for both images.
Head and tail again return all 288 windows.

The separate
[cold Arrow diagnostic](../tiered_system/experiments/results/exp100-host-1793922689/cold-profile/report.json)
used the same image digests and settings, fresh containers,
`COLD_PROFILE_DIR=/profiles`, and one measured repetition after one warmup.
All 56 responses matched the unprofiled cold control. These times are
explanatory, not a second latency result. Both plans scan the same 25,940,207
rows and 1,357 row groups for a full March query.

| Measured cold query | Image | Request | Buffer peak | Temp peak | Blocked | Bytes read | CPU |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| March repeat | Dictionary | 71.5s | 3.00 GB | 3.92 GB | 27.8s | 1.19 GB | 91.6s |
| March repeat | Reduction | 19.9s | 1.33 GB | 0 | 0.04s | 8 MB | 46.4s |
| March 8–29 | Dictionary | 42.3s | 3.00 GB | 2.30 GB | 17.0s | 742 MB | — |
| March 8–29 | Reduction | 11.7s | 1.33 GB | 0 | 0.0s | 3 MB | — |
| Return to March | Dictionary | 67.0s | 3.00 GB | 4.11 GB | 24.3s | 1.19 GB | — |
| Return to March | Reduction | 18.9s | 1.33 GB | 0 | 0.0s | 3 MB | — |
| Day mean | Dictionary | 1.09s | 0.53 GB | 0 | 0.02s | ~0 | 1.70s |
| Day mean | Reduction | 1.54s | 1.33 GB | 0 | 0.02s | ~0 | 1.81s |
| Day head | Dictionary | 1.01s | 0.56 GB | 0 | 0.01s | ~0 | 1.66s |
| Day head | Reduction | 1.05s | 1.76 GB | 0 | 0.01s | ~0 | 1.61s |

The dictionary month still joins one grouped series tuple back onto the full
observation stream, sits on the 3 GB buffer cap, and spills about 4 GB. The
reduction groups those observations to 597 window rows directly, keeps the
buffer near 1.3 GB, and spills nothing. Its first March query still reads
about 1.17 GB and takes 63s; the repeated, overlapping, and returning queries
report only a few megabytes read and about 20s, with blocked time near zero.
That is consistent with reuse of cached input under the smaller working set.
It does not measure Pelican traffic and is not a cached-response claim. The
warm fleet minute count follows the same shape: the first reduction query was
2.26s against a 1.91s dictionary query, then the measured one was 0.44s
against 1.97s. Head/tail stays on dictionary observation selection, matches
the old bodies, and does not spill; its buffer peak on the reduction engine
was 1.76 GB versus 0.56 GB on the dictionary image.

Every aggregate scratch mount was empty after its run. Staging was cleaned
up. Containers were stopped between phases and after the diagnostic; they
were not removed. No warehouse writes, commits, pushes, or deployment were
made.

The HTTP gates support leaving the integrated reduction in the API source.
Repeated and overlapping cold months, day count, and the warm fleet count are
faster, and the monthly spill is gone. Daily mean/min/max/sum and the
noon-crossing split queries are not faster. Head/tail parity holds. This is
local source, not a deployed change, and it is not a claim that every query
got faster.

### Independent exp100 review: integrated reduction validated (2026-11-05)

Independently verified **440/440 retained HTTP responses**: 224 cold-control
rerun, 160 split-control and 56 cold diagnostics. Every body hash, decoded
row count, bound/identity/value comparison, runtime setting/image check and
reported median passes. All **56 timing/native profile pairs** match their
embedded data; their bodies also match the separate unprofiled baseline.
See the [independent review](../tiered_system/experiments/results/exp100-host-1793922689/independent-review-20261105.json).
Replayed the saved live HTTP bodies through the current client again:
**546/546 pass**, zero integer precision failures. See the
[client review](../tiered_system/experiments/results/exp100-host-1793922689/parity-live-http/client-independent-review-20261105.json).
The retained native correctness, full tagged suite, vet and tiered race logs
confirm success. This was saved-evidence replay, not another live benchmark.

**Recommendation: retain the integrated reduction as the normal unrestricted
aggregate path.** The warm benefit survives HTTP delivery, both formats,
overlapping ranges and intervening short queries; it is not limited to two
identical native probe requests. Representative unprofiled reused-engine
results are:

| Cold workload | Dictionary Arrow → reduction | Dictionary NDJSON → reduction |
| --- | --- | --- |
| Full March at sequence start | 72.85 s → 20.23 s | 81.39 s → 20.05 s |
| Immediate repeat | 68.63 s → 19.23 s | 75.31 s → 19.85 s |
| March 8–29 overlap | 44.02 s → 11.51 s | 46.64 s → 11.81 s |
| Return after mixed queries | 68.34 s → 19.02 s | 69.34 s → 19.42 s |

The monthly/overlap gains are approximately **3.6–4.1×**. The diagnostic repeat
processes the same 25,940,207 observations and 1,357 row groups, while peak
engine buffers fall from 3.00 GB to about 1.33 GB and monthly temporary usage
falls to zero. Lower engine read counters and near-zero blocked time on later
requests are consistent with cached-input reuse; they do not measure every
Pelican byte or prove a cached response. The excluded first-ever reduction
query still took about 65 s. Do not describe the ~20 s result as cold-cache
performance or a gain for every query.

Daily arithmetic queries are mixed, with some regressions, and the verified
noon-crossing split queries remain roughly 3.6–4.3 s. Their hot staging cost
was not changed. Head/tail retains dictionary observation selection and body
parity. Returning 288 rows after `head=1000` or `tail=1000` does **not** mean
the limit had no effect: fixed-window expansion retains the day's 288 labels.
Independent inspection finds exactly one non-null window for each—00:05 UTC
for head and the following midnight for tail—in cold and split responses.
The other 287 windows are padded null means. The 1,000 limit applies to input
observations before aggregation, not to output windows.

Per-request diagnostic engine peaks include the reused engine's retained
cache/working set. For example, the larger candidate head/tail peak after a
monthly scan is not an isolated measurement of that unchanged dictionary
selection algorithm. Do not infer a head/tail algorithm change from that peak
alone. Whole-process/cgroup demand remains a separate capacity consideration.

The initial failed cold comparison is correctly excluded from successful
medians and retained as operational evidence. The reported resident-container
memory pressure drove the old scan past the existing fixed 20-minute Pelican
read-token lifetime, consistent with the observed 403. The clean rerun proves
the comparison under its recorded host conditions; it does not eliminate the
general long-running-query credential/resource limitation. No token policy
change was made during this review.

The code-level integration and host correctness/performance gates for this
change are now complete. Deployment remains a separate action and was not
performed. The next independent latency target can be direct typed hot
staging for split queries; it need not delay retaining the validated cold
reduction. No runtime code, warehouse, commits or pushes were changed by this
review; API validation documentation now records the completed host gates.

### Integrated reduction retained; superseded code removed (2026-11-05)

Retained the integrated unrestricted reduction and the required head/tail
selection plan. Removed the exp98 native probe/local SQL exporter, exp99 native
concurrency probe and barrier/resource helpers, their SQL-parsing adapters,
Python drivers and driver tests. Removed the older precompiled-SQL HTTP adapter
and its obsolete exp91 runner/tests. The fixture contracts, fake transport,
decoder and comparator actually used by production HTTP/live tests remain in
`aggregate_http_fixture_test.go`, with names reflecting that role. Fixture SQL
paths are no longer part of those production HTTP test cases. Existing
production HTTP, client, cleanup/read-grant and tiered lease tests remain.

Renamed the numeric/metadata tests to `aggregate_reduce_test.go` and made them
call production builders directly. Their independent comparison selects every
fixture observation through the live head path. `buildLimitedAggregateSQL`
now explicitly requires head/tail and no longer supports an unrestricted
dictionary mode; the no-limit legacy branch was removed. No runtime fallback,
experimental mode, SQL rewrite or private probe environment hook remains.
The unrestricted `aggregate_reduce.go` source hash is unchanged from the
validated exp100 integration.

Kept the maintained exp100 paired HTTP regression benchmark, current-client
replay, fixture/seed preparation and all historical datasets/results. Exp98/99
guides now summarize completed evidence rather than advertising removed
commands; exp88/89 and exp91 fixture guides link to the actual production HTTP
validation workflow. Reports, source snapshots, profiles, SQL, binaries and
previous failed runs under `results/` were not deleted or rewritten.

[Cleanup validation](../tiered_system/experiments/results/aggregation-cleanup-20261105/summary.json):
**182 compiler + 20 metadata/limit + 40 numeric cases pass**, with null-key and
direct-limit guards also passing. The tiered race suite passes. **64 maintained
Python tests pass**, including fixture/replay, source-kind and HTTP benchmark
checks. The full cold package type-check reaches the linker; native runtime
execution remains unavailable in this sandbox due to missing `libduckdb`.
The earlier successful host exp100 HTTP/live gates remain the runtime evidence;
this cleanup does not claim a new host execution. No deployment, published
storage change, commit or push was made.

### Direct hot Parquet staging implemented; host validation pending (2026-11-05)

The next candidate removes the split-query path
`Influx → Go records → temporary NDJSON → DuckDB COPY → Parquet` and writes
`Influx → Go records → Parquet` directly. Final combined aggregation still
uses the retained integrated reduction, or the required head/tail dictionary
selection. No unrestricted dictionary fallback or materialization experiment
was reintroduced. Cold-only and hot-only queries do not use this staging path.

A request-private Arrow Go Parquet writer retains the existing staging schema:
`time BIGINT` (epoch nanoseconds), `name VARCHAR`, `meta MAP(VARCHAR,VARCHAR)`,
`kind VARCHAR`, `value VARCHAR`. Scalar text preserves exact signed/unsigned
integers; this is private input for aggregation, not a new public schema or
warehouse layout. Metadata keys are sorted, nil metadata becomes an empty map,
and output uses Zstd. Batches flush at 65,536 rows or an 8 MiB uncompressed
input target. A single larger observation may occupy its own batch if it fits
the request budget. Malformed UTF-8 is rejected rather than writing an invalid
Parquet UTF8 column. Go staging allocations remain outside the DuckDB budget.

The configured staging-byte limit retains its prior uncompressed
JSON-equivalent charge, calculated without encoding JSON. It does not become
a compressed-file limit. There is no intermediate NDJSON file, staging COPY,
or staging native-engine lock. Per-request source iteration/writing remains
concurrent; final native execution retains its existing backend mutex. Source
closure, cancellation, nanosecond bounds, grant ownership and private scratch
lifetime remain in the existing lifecycle.

Split Arrow sidecars now expose `aggregate_input_bytes` instead of
`aggregate_ndjson_bytes`. `aggregate_hot_read_stage_seconds` includes direct
Parquet output and all staging/source closure. The removed
`aggregate_stage_queue_seconds` and `aggregate_stage_copy_seconds` are not
reported for the candidate. Compare the shared staging-complete marker and
full HTTP latency against the retained exp100 image, not isolated timings or
the pre-reduction exp96 image.

The [exp101 runbook](../tiered_system/experiments/exp101_direct_hot_staging.md)
contains writer/codec, full API, hot/cold/split parity, client replay and paired
historical split HTTP gates. The maintained HTTP driver now optionally pins
the next baseline to a previous completed candidate report; its original
reference mode remains for reproducing earlier comparisons. Historical
artifacts were preserved. Host correctness/performance results are pending;
no speedup is claimed from implementation alone.

Local validation: the uncompressed budget counter passes against Go JSON
encoding, including deterministic randomized byte strings. **46 Python tests
pass**, including the new retained-candidate baseline checks; the **tiered
race suite passes (cached)**. [Validation logs](../tiered_system/experiments/results/aggregation-direct-stage-20261105/summary.json)
record that Parquet writer and full API execution are blocked by uncached
pinned module dependencies and the missing DuckDB 2.0 native library. No
versions were substituted. Independent Parquet consumption, full HTTP/live
parity and the paired performance comparison still require the host gate.

### Direct hot Parquet staging host gates (2026-11-06)

The pinned Arrow module was downloaded at the existing `v18.5.1` sum. No
dependency version was changed. DuckDB CLI `v2.0.0` consumed the writer
output. The writer package passed, including the independent Parquet round
trip, empty and boundary batches, the uncompressed budget, and failed
writes. The direct-write microbenchmark is diagnostic only: about 290 MB/s
with no tags and 211 MB/s with eight tags on this host. It is not an
end-to-end comparison.

[Correctness](../tiered_system/experiments/results/exp101-host-1793968135/correctness.log)
passed in 21.672s with no skipped fixture cases. That includes the hot
staging lifecycle, native codec, compiler oracle, metadata and numeric
edges, production HTTP, and profile status. The tagged
[suite](../tiered_system/experiments/results/exp101-host-1793968135/suite.log)
and [vet](../tiered_system/experiments/results/exp101-host-1793968135/vet.log)
exited 0. The tiered
[race check](../tiered_system/experiments/results/exp101-host-1793968135/race.log)
passed in 1.957s. The live package passed in 6.488s, and the
[client replay](../tiered_system/experiments/results/exp101-host-1793968135/parity-live-http/client-report.json)
was 546/546 with zero integer-precision failures (pandas 3.0.6, pyarrow
25.0.1). The retained 36-row seed was reused.

The split comparison used the retained reduction image
`beehive-data-api:aggregation-reduce-1793922815`
(`sha256:c31360fa869622d3729d9d303399d918856115aca7d011ee70788c2bd8bf2546`)
as the baseline and a new image,
`beehive-data-api:aggregation-direct-1793968296`
(`sha256:4f166601cd0a3611ae36bb7d711cd3c3a8dd2497bb9de2d1594c5c5aec3adfe5`).
Both used the adopted warehouse, `exp95-hot`, the noon split border, two
DuckDB threads, and a 3 GB engine budget, with no extra container cap.
[Split control](../tiered_system/experiments/results/exp101-host-1793968135/split-control/report.json)
completed 160/160 responses. Every body matched, including head and tail.
Neither container restarted or was OOM-killed. Medians exclude one warmup.
First-result time tracks total time, so the difference is query completion,
not client parsing after the first byte.

| Split step | Rows | Reduction Arrow | Direct Arrow | Reduction NDJSON | Direct NDJSON |
| --- | ---: | ---: | ---: | ---: | ---: |
| Day count | 288 | 4.02s | 4.38s | 4.22s | 4.22s |
| Day mean | 288 | 3.88s | 4.20s | 4.24s | 4.42s |
| Day sum | 288 | 3.85s | 4.42s | 3.88s | 4.41s |
| Day min | 288 | 3.80s | 4.39s | 3.96s | 4.40s |
| Day max | 288 | 3.91s | 4.38s | 3.92s | 4.34s |
| Hour mean | 60 | 0.67s | 0.65s | 0.65s | 0.67s |
| Fleet minute count | 1995 | 0.88s | 0.87s | 0.92s | 0.86s |
| Temperature fleet hour | 192 | 2.55s | 2.78s | 2.60s | 2.63s |
| Day head mean | 288 | 3.97s | 4.49s | 4.51s | 4.55s |
| Day tail mean | 288 | 4.70s | 5.28s | 4.07s | 4.60s |

The noon-crossing day is about 0.3–0.6s slower with direct Parquet on Arrow,
and NDJSON is flat to about 0.5s slower. The short hour and fleet minute
count stay within a few hundredths. This is not a faster split query.

The separate
[Arrow diagnostic](../tiered_system/experiments/results/exp101-host-1793968135/split-profile/report.json)
matched the unprofiled bodies on all 40 responses. These timings explain the
difference; they are not a second latency result. For the measured day mean,
both sides stage 521,840 hot rows and charge the same 179,920,552
uncompressed input bytes. The reduction image spends 2.21s reading and
writing NDJSON plus 0.66s in COPY, and reaches the shared staging-complete
marker at 2.89s. The direct writer has no queue or COPY phase; its read and
Parquet write take 3.30s and the marker is at 3.31s. Final fetch stays about
0.6–0.7s. Neither side spills. The baseline queue time is already about
zero with one client, so removing the staging lock is not visible here.
DuckDB's COPY file is 2.42 MB; the direct file is 4.02 MB. The same byte
equality holds for every measured split query.

Sampled container RSS peaked near 1.74 GB for the reduction image and 1.90
GB for the direct image during the unprofiled run, and near 1.86 GB and
1.30 GB during the profiled run. That sample is not a continuous cgroup
peak and does not establish a resident-memory change. The scratch mounts
did: the day NDJSON reached about 188 MB, while the direct Parquet scratch
peaked near 4 MB. All four scratch mounts were empty after the runs.
Containers were stopped and left on disk. Influx and kube-system were left
running. No warehouse writes, commits, pushes, or deployment were made.

Correctness, the unchanged byte budget, and scratch cleanup pass, so the
direct writer remains the source path. The measured split HTTP time does
not. Direct staging removes the NDJSON file and COPY, and it lowers the
request scratch high-water mark, without making these split queries faster.

### Review of direct hot staging results (2026-11-06)

Independently checked the retained reports and logs: the focused correctness
run has no skipped fixture cases; the full tagged suite, vet and tiered race
check pass. All 160 control and 40 diagnostic responses pass body parity, and
all 546 client replays pass with zero integer-to-float rows. Every paired
diagnostic response has equal hot row counts and equal uncompressed input
charges between the previous NDJSON counter and the direct input counter.
The budget and correctness evidence therefore support the new path.

This is a scratch-space improvement with a latency tradeoff, not a successful
latency optimization. Day-mean Arrow is 3.881s → 4.197s (**8.1% slower**);
day-min is about **15.3% slower**, and day-sum about **14.7% slower**. Diagnostic
staging completion is 2.886s → 3.308s, while final fetch is slightly lower
(0.682s → 0.622s) and neither engine spills. The extra time is in the shared
hot-read/stage phase, not the final aggregate or response decoder. The direct
file's larger size (4.02 MB versus 2.42 MB) does not by itself establish why
staging takes longer. Sampled RSS does not establish a memory improvement;
request scratch high-water does improve from about 188 MB to 4 MB.

Retain direct staging for the recorded scratch-space benefit, with its measured
latency cost explicit. Do not restore the removed path or add another runtime
switch in this review. The next isolated investigation should capture **Go CPU
and allocation profiles of the real split hot-staging path**, plus time spent
in source iteration/CSV decoding versus writer append, batch flush and footer
closure. The current phase combines all these operations; it cannot attribute
the 0.42s staging regression solely to Parquet encoding. Potential costs visible
in the source include repeated string scans, per-row metadata sorting and byte
allocations, batch-buffer allocation, and compression. These are hypotheses to
measure before changing buffer reuse, row-group sizes or compression. Bound
any buffer reuse so a single large observation cannot permanently inflate each
request's working set. Any subsequent candidate must preserve the input charge,
exact values, cancellation and cleanup, and rerun paired HTTP measurements.

Updated API validation documentation to reflect the completed host gates and
the measured tradeoff. No runtime implementation, published data, deployment,
commit or push was changed in this review.

### Direct staging retained as the sole API path (2026-11-06)

Retain direct Parquet staging despite the documented latency
tradeoff, for its request scratch-space benefit. Verified the API already routes
split aggregates directly through `writeHotStage` and `hotstage.Writer` into
`hot.parquet`, then the existing combined executor. The old NDJSON staging
writer, byte-limiting output wrapper, DuckDB staging COPY builder and staging
queue/COPY profile fields were removed during integration; no runtime fallback
or experimental selection flag remains. No second implementation needed to be
promoted or deleted in this finalization. Updated API validation, CLI byte-limit
help and the experiment's retained outcome. Kept correctness/codec tests,
JSON-equivalent input budget accounting, and historical comparison artifacts.
No deployed service, warehouse, commit or push was changed.

## Standalone head/tail follow-up

Standalone head/tail integration, validation and historical benchmark results
now live in [plan 14](14_head_tail_queries.md). Its historical split-query stall
is not a completed performance comparison; aggregation's retained exp100/101
measurements remain separate evidence.

## Resources

- [Current API contract](../../beehive-data-api/docs/api.md)
- [Arrow implementation and performance history](8_arrow_query_response.md)
- [Iceberg v3 and adopted warehouse layout](10_iceberg_v3.md)
- [Dynamic archival and read-grant lifecycle](12_dynamic_archival_service.md)
- [Flux aggregateWindow](https://docs.influxdata.com/influxdb/v2/reference/flux/stdlib/built-in/transformations/aggregates/aggregatewindow/)
- [Flux count](https://docs.influxdata.com/flux/v0/stdlib/universe/count/), [mean](https://docs.influxdata.com/flux/v0/stdlib/universe/mean/), [sum](https://docs.influxdata.com/flux/v0/stdlib/universe/sum/), [min](https://docs.influxdata.com/flux/v0/stdlib/universe/min/)
- [DuckDB 2.0 timestamp functions](https://duckdb.org/docs/2.0/sql/functions/timestamp)
- [DuckDB 2.0 VARIANT](https://duckdb.org/docs/2.0/sql/data_types/variant)


### Backend ownership after the tiered refactor (2026-11-06)

Cross-border aggregate orchestration now lives in `beehive-data-api/internal/backend/tiered/execute.go`,
sharing its hot staging and scratch ownership with cross-border head/tail.
Cold-only queries remain in `cold.Query`; hot-only queries remain in
`hot.Query`. Both cold and tiered aggregation use the shared pure compiler in
`internal/query/aggregate`. The cold backend exposes its filtered raw scan
and native engine through `query.PlannedSourceBackend`, with no dependency on hot
storage or staged-input context. Numeric policies, SQL plans, wire schemas,
read leases and settings are preserved. See plan 14's cross-border ownership
section and `beehive-data-api/docs/aggregation-validation.md` for validation
and the pending VM HTTP/live regression commands. No performance optimization
or deployment is part of this package refactor.
