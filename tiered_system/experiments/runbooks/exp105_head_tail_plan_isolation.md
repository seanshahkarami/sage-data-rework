# Separate the remote scan, union and selection plan

Saved runs cited below as `results/...` are paths inside the split archive in the parent directory (`../results.z01`, `../results.z02`, `../results.z03`, and `../results.zip`). See [recorded results](../README.md#recorded-results). A command that creates a new run directory still writes local untracked output.

> Archived comparison after raw-candidate adoption (2026-11-06). The live API
> has removed the superseded standalone selectors and diagnostic probes.
> Reproducing this experiment requires its retained pre-adoption source
> snapshot. Current regression commands are in
> [API validation](../../../beehive-data-api/docs/aggregation-validation.md#standalone-headtail-across-the-border).


Exp104 localized the wait to the first native fetch: staging, hint resolution,
prepare and pending setup returned. Native local range/Parquet cases passed.
This probe changes neither the API nor its live SQL. It runs diagnostic SQL in
fresh native Arrow engines over **one pinned remote snapshot** and a private
copy of the retained 21,912-row hot hour file. No Influx reads or reseeding are
needed. See [plan 14](../../plans/14_head_tail_queries.md).

## Cases

| Case | Operation | Purpose |
| --- | --- | --- |
| `cold_scan` | Read every matching observation strictly before noon | Establish that the remote cold half completes without union/selection |
| `hot_read` | Read the retained local hot slice | Establish that local input decodes independently |
| `union_scan` | Raw cold/hot union, canonical metadata and timestamp order | Isolate combining the two scans without a dictionary/window |
| `direct_window` | Per-row canonical metadata and one per-series window | Test selection without the dictionary/CTE graph |
| `dictionary` | Current production selection SQL | Reproduce the historical blocker |
| `dictionary_no_ahead` | Identical current SQL, read-ahead depth 0 | Test the existing I/O setting independently of a SQL rewrite |

All cases keep two CPU threads, 3 GB, eight async I/O threads and the same
snapshot and copied hot data. Only the last case changes read-ahead; no API
flag/default is changed. Each creates a fresh production backend (its SQL and
Arrow engines) and executes through `queryArrow` with a **45-second query
deadline**. Backend/extension startup precedes that deadline; the whole test
binary has an eight-minute timeout.
The first two successful reads form an independent Go first-observation oracle
per series; the union and selection cases are checked against it. The oracle
is diagnostic and capped at 100,000 rows, for this fixed hour/sensor only; it is
not a proposed Go-side production merge. Native profiles include successful or
failed phases. Failed/partial rows are retained as evidence.

These are native scan/plan diagnostics, not HTTP latency measurements. Metadata
is resolved once outside per-case timing; no hot extraction/staging is timed.
There is no claim of production correctness/performance for a diagnostic
alternative. Any candidate still needs the exp102 correctness gates and the
exp103 full two-format control before adoption.

## Run in the failing image's container environment

Use the retained image and bind mounts/network to minimize host-versus-container
differences. Compile a test binary with the repository's **existing builder**;
then execute it inside the retained failing runtime image. This adds no service,
port, backend selector or deployment.

From the workspace root, reuse `BENCH_NETWORK` and the absolute
`PELICAN_BENCH_KEY` from the earlier runs. Select the **hour** `hot.parquet`
retained under exp104's `profile-split-scratch`, not an exp103 day file. The
source containers must remain stopped; neither the original file nor its
parent directory is modified. Do not select a path under `/node-data`.

```sh
export EXP105_SUFFIX="$(date +%s)"
export EXP105_ROOT="$PWD/sage-data-rework/tiered_system/experiments/results/exp105-host-$EXP105_SUFFIX"
export EXP105_BUILDER="beehive-data-api:head-tail-isolation-builder-$EXP105_SUFFIX"
export EXP105_RUNTIME='beehive-data-api:head-tail-bench-1793972898'
export EXP105_PLATFORM="$(docker image inspect --format '{{.Os}}/{{.Architecture}}' "$EXP105_RUNTIME")"
mkdir -m 700 "$EXP105_ROOT"
```

Set `EXP105_STAGED` to the exact absolute hour file found with this scoped,
read-only search, and confirm its size is **197,586 bytes** (the probe also
requires 21,912 rows within the half-hour hot slice):

```sh
find "$PWD/sage-data-rework/tiered_system/experiments/results/exp104-host-1793974848/remote-probe/profile-split-scratch" \
  -type f -name hot.parquet -print
export EXP105_STAGED='/absolute/path/printed/above/hot.parquet'
```

Build using unchanged module pins and the existing glibc-compatible builder.
The diagnostic test does not require a DuckDB CLI inside the runtime image.

```sh
docker build --platform "$EXP105_PLATFORM" --target builder -t "$EXP105_BUILDER" beehive-data-api \
  > "$EXP105_ROOT/build.log" 2>&1
docker run --platform "$EXP105_PLATFORM" --name "headtail105-compile-$EXP105_SUFFIX" \
  --mount "type=bind,src=$EXP105_ROOT,dst=/diagnostics" \
  "$EXP105_BUILDER" \
  go test -tags=duckdb_arrow -c -o /diagnostics/selection-isolation.test ./internal/backend/cold \
  > "$EXP105_ROOT/compile.log" 2>&1
```

The test binary uses the production compiler and native reader plus test-only
projections/plans. It pins the adopted warehouse internally, reads metadata
with the existing key/scoped token, and writes only inside the new output mount
and engine-owned temporary directories. Tokens and secret setup SQL are not
saved. Saved query SQL contains operational paths/URLs; output directories
are private.

```sh
if docker run --platform "$EXP105_PLATFORM" --name "headtail105-probe-$EXP105_SUFFIX" --network "$BENCH_NETWORK" \
  --entrypoint /diagnostics/selection-isolation.test \
  -e HEAD_TAIL_ISOLATION_OUTPUT=/diagnostics/native \
  -e HEAD_TAIL_ISOLATION_STAGED=/retained/hot.parquet \
  -e HEAD_TAIL_ISOLATION_KEY=/run/secrets/pelican.pem \
  --mount "type=bind,src=$EXP105_ROOT,dst=/diagnostics" \
  --mount "type=bind,src=$EXP105_STAGED,dst=/retained/hot.parquet,readonly" \
  --mount "type=bind,src=$PELICAN_BENCH_KEY,dst=/run/secrets/pelican.pem,readonly" \
  "$EXP105_RUNTIME" \
  -test.v -test.run '^TestHeadTailRemoteIsolation$' -test.timeout=8m \
  > "$EXP105_ROOT/probe.log" 2>&1; then
  printf '%s\n' 0 > "$EXP105_ROOT/probe.exit"
else
  printf '%s\n' "$?" > "$EXP105_ROOT/probe.exit"
fi
```

An expected stall makes the test exit nonzero but does not stop later cases;
the test retains every case's status/error/profile and incremental report.
Input/setup errors or a process-level timeout can stop collection early.
`native/report.json` status `diagnostics_collected` means all cases were
attempted, **not** that they all passed. A case may complete with parity
`unavailable` if either source-control read failed. Use only `complete` plus
`parity=pass` as evidence that a combined candidate returned the expected rows.
Source controls have no combined parity assertion, but validate their slices.

Each case directory retains `query.sql`, `rows.json` (including partial rows),
a timing sidecar, and any native profile that completed. The report records the
pinned metadata URL, copied hot-file SHA-256, timing paths, row counts and
redacted errors. No stopped container, previous scratch, old report or warehouse
object is removed. The new compile/probe containers exit and remain for review.

## Interpret the results

- `cold_scan` fails: a remote scan/setup problem remains independently of the
  combined selection. Inspect reached phases and error/profile, rather than
  promoting a new window plan.
- Cold/hot reads pass but `union_scan` fails: combining remote and local inputs
  is sufficient to trigger the wait; dictionary selection is not required.
- Union and direct window pass but current dictionary stalls: the dictionary/
  CTE selection graph is implicated in this environment. A direct plan is a
  candidate only after parity and full feature/performance gates.
- Only `dictionary_no_ahead` completes correctly: read-ahead interacts with the
  failing plan; do not change the global setting without broader validation.
- Everything completes: the stall has not reproduced in this diagnostic. Keep
  the observation and compare the compiled/native extension environment with
  the original API; do not call it fixed or manufacture placement medians.

Run order warms external caches. These single, profiled cases establish
completion/failure and parity, not relative latency or throughput. Empty native
profiles on interrupted fetches remain valid failure evidence. Add results to
plan 14 before any runtime SQL or policy change.

## Local checks

The test-only direct-window plan is compared with current production selection
on a mixed-value/promoted-collision fixture in the CLI, including head and tail.
To run this compiler-only check without linking native Go DuckDB:

```sh
export COLD_AGGREGATION_DUCKDB2="$(command -v duckdb)"
(
  cd beehive-data-api
  go test -count=1 -v internal/backend/cold/sql.go \
    internal/backend/cold/aggregate_sql.go internal/backend/cold/aggregate_reduce.go \
    internal/backend/cold/selection_sql.go internal/backend/cold/selection_scale_test.go \
    internal/backend/cold/selection_isolation_test.go
)
```

The remote probe is opt-in and skipped unless `HEAD_TAIL_ISOLATION_OUTPUT` is
set. Native execution cannot be validated in the local sandbox with missing
pinned modules/library and denied Docker socket access. No runtime fix is
claimed from CLI-only plan checks.

## Outcome and current source status

[Exp105 results](../../plans/14_head_tail_queries.md#scan-and-plan-isolation-2026-11-06)
show the source scans, raw union and direct window completed with parity; both
dictionary cases stalled, including read-ahead 0. The direct window has now
replaced standalone selection in the API source. The `dictionary` labels in
this diagnostic remain frozen **pre-fix** SQL for reproduction, not current
API behavior or an available fallback. `direct_window` calls the production
builder. Use [exp106](exp106_head_tail_direct_integration.md) to validate the
replacement through both formats and the full historical control. Exp105's
single-case times are not adoption/performance medians.
