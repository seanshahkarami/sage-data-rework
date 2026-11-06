# Standalone head/tail across the border

The API now stages raw hot input in the retained direct Parquet format, unions
it with cold rows strictly before the pinned border, and selects head/tail once
per canonical measurement-and-metadata series. Output is ordinary measurement
rows. There is no aggregate/window expansion and no numeric-kind restriction
on a series. Existing aggregate arithmetic and unrestricted reduction remain.

There is no Go-side unbounded series map. DuckDB performs the dictionary join,
window ranking and final ordering under its existing memory/spill settings.
All hot input is staged before results can stream; this change establishes
functionality, not a measured performance improvement. The existing staging
byte/directory settings apply. Both head and tail, negative limits and standalone
windows are invalid; zero selects no rows. A missing end freezes request time.

## Local compiler and routing checks

The compiler gate can run without linking the API's native Go DuckDB library.
From the workspace root, set the actual DuckDB 2.0 CLI path:

```sh
export COLD_AGGREGATION_DUCKDB2="$(command -v duckdb)"
(
  cd beehive-data-api
  go test -count=1 -v internal/backend/cold/sql.go \
    internal/backend/cold/aggregate_sql.go internal/backend/cold/aggregate_reduce.go \
    internal/backend/cold/selection_sql.go internal/backend/cold/selection_sql_test.go
)
go -C beehive-data-api test -race -count=1 ./internal/backend/tiered
```

These check mixed values, exact large integers, nanosecond instants, promoted
collisions, map key order, empty/null metadata, cold-only/hot-only series within
the union, zero/small/oversized limits and the exclusive cold border. A private
Parquet union fixture includes cold overlap beyond the border and an observation
exactly at it; only the hot copy can appear. Routing verifies one executor,
relative/omitted bounds and no mutation of caller queries. This is not the
native Go HTTP gate.

## VM production HTTP and live parity gate

Reuse the [exp101](exp101_direct_hot_staging.md) fixture environment,
`COLD_AGGREGATION_MANIFEST`, `COLD_AGGREGATION_DUCKDB2`, the verified retained
36-row `AGGREGATION_PARITY_SEED_REPORT` and its `INFLUX_TOKEN`. Do not reseed or
write to a published warehouse. Use fresh isolated output paths, never
`/node-data`. Ensure the manifest paths resolve on the VM as in exp101.

```sh
export EXP102_ROOT="$PWD/sage-data-rework/tiered_system/experiments/results/exp102-host-$(date +%s)"
mkdir -m 700 "$EXP102_ROOT"
AGGREGATION_PARITY_PROFILE=1 \
  AGGREGATION_HTTP_OUTPUT_DIR="$EXP102_ROOT/http" \
  go -C beehive-data-api test -tags=duckdb_arrow -count=1 -v \
  ./internal/backend/cold -run '^TestHeadTailProductionHTTP$' \
  > "$EXP102_ROOT/http.log" 2>&1
AGGREGATION_PARITY_PROFILE=1 \
  AGGREGATION_HTTP_OUTPUT_DIR="$EXP102_ROOT/live-http" \
  go -C beehive-data-api test -tags=duckdb_arrow -count=1 -v \
  ./internal/backend/cold -run '^TestHeadTailLiveTierParity$' \
  > "$EXP102_ROOT/live.log" 2>&1
go -C beehive-data-api test -tags=duckdb_arrow -count=1 ./... \
  > "$EXP102_ROOT/suite.log" 2>&1
go -C beehive-data-api vet -tags=duckdb_arrow ./... \
  > "$EXP102_ROOT/vet.log" 2>&1
go -C beehive-data-api test -race -count=1 ./internal/backend/tiered \
  > "$EXP102_ROOT/race.log" 2>&1
```

Require no skipped fixture/live gates and successful command exits. These
requests go through the real router, source validation, direct writer, native
query and HTTP encoders. Head/tail expected rows are selected independently in
Go from original fixture observations, not aggregate outputs. Cases cover
limits 0, 1, 2 and 1000, plus all original fixture names/ranges, nanosecond and
uint64 boundaries, and booleans/strings. Each dynamic request must release its
read grant after native closure and scratch removal; split Arrow profiles use
the existing `aggregate_*` staging counters to retain observability continuity.
With the retained manifest, the synthetic gate produces 512 wire responses
and the live gate 768 (eight limit/direction combinations per unique original
range/placement, two formats). The live gate additionally compares hot-only,
cold-only and split results to
the same observation oracle. The exported client manifests/bodies retain the
existing fixture contract; numeric cases can use the current client replay,
but the old aggregation-only numeric comparator does not establish string or
boolean client parity. The HTTP gate validates those JSON scalars directly.

Record the completed evidence and any failures in plan 13. Do not deploy or
claim an HTTP performance gain based on the compiler-only checks.

## Completed host outcome (2026-11-06)

[The host results and review](../../plans/14_head_tail_queries.md#standalone-crossing-headtail-host-gates-2026-11-06)
confirm 512 synthetic HTTP and 768 live parity cases pass without skips. The
full tagged suite, vet and tiered race checks pass. Independent JSON/PyArrow
replay verifies all 1,280 saved bodies, including exact integers, nanosecond
identities, strings/booleans, limits and cold/split timestamp ordering. Native
HTTP validation is complete. These commands remain as regression gates;
performance gains and production deployment are not established by this run.
