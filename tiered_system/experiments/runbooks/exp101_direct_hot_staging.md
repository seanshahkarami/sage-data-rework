# Direct hot Parquet staging

Saved runs cited below as `results/...` are paths inside the split archive in the parent directory (`../results.z01`, `../results.z02`, `../results.z03`, and `../results.zip`). See [recorded results](../README.md#recorded-results). A command that creates a new run directory still writes local untracked output.

This isolates split-aggregate staging from the retained unrestricted reduction.
The candidate writes validated Go records directly to request-private Zstd
Parquet. It removes hot NDJSON encoding, DuckDB JSON decoding/COPY and the
staging mutex wait. It leaves final aggregation, head/tail selection and public
responses intact. There is no runtime fallback.

## Correctness first

From the workspace root, on the VM with the pinned Go dependencies and DuckDB
2.0 library available, reuse the exp100 fixture/seed variables and token. Do not
reseed or modify the published warehouse. Use a fresh isolated output directory:

```sh
export EXP101_ROOT="$PWD/sage-data-rework/tiered_system/experiments/results/exp101-host-$(date +%s)"
mkdir -m 700 "$EXP101_ROOT"
export COLD_AGGREGATION_DUCKDB2="$(command -v duckdb)"
go -C beehive-data-api test -count=1 -v ./internal/backend/cold/hotstage \
  > "$EXP101_ROOT/writer.log" 2>&1
go -C beehive-data-api test -run '^$' -bench BenchmarkDirectStage -benchmem \
  ./internal/backend/cold/hotstage > "$EXP101_ROOT/writer-bench.log" 2>&1
AGGREGATION_PARITY_PROFILE=1 CGO_ENABLED=1 \
  go -C beehive-data-api test -tags=duckdb_arrow -count=1 -v ./internal/backend/cold \
  -run '^(TestHotAggregate|TestNativeHotStageCodec|TestAggregateCompilerOracle|TestAggregateMetadataDictionary|TestAggregateNumericAndTypeSemantics|TestAggregateNullKeyPresence|TestReducedAggregateRejectsObservationLimits|TestAggregationProductionHTTP|TestColdQueryProfile)' \
  > "$EXP101_ROOT/correctness.log" 2>&1
CGO_ENABLED=1 go -C beehive-data-api test -tags=duckdb_arrow ./...
CGO_ENABLED=1 go -C beehive-data-api vet -tags=duckdb_arrow ./...
go -C beehive-data-api test -race ./internal/backend/tiered
```

Reuse the [exp100 live parity and client replay instructions](exp100_aggregation_http.md#host-correctness-gate),
with output under `$EXP101_ROOT`, to exercise all hot/cold/split responses,
including exact uint64 and timestamp cases. The live gate requires the retained
36-row seed report and its `INFLUX_TOKEN`. Do not treat a skipped fixture/live
test as a passed runtime gate. A Go module download may be needed for Arrow's
Parquet transitive dependencies; keep existing versions pinned.

The independent writer tests verify DuckDB can consume the required MAP
annotation and repeated/empty entries. Additional tests cover zero rows,
row/byte batch boundaries, an oversized single record, input budget accounting,
failed disk/footer writes, source closure, cancellation and exclusive bounds.
The microbenchmark is diagnostic throughput/allocation evidence only, not a
comparison with the removed production path or an end-to-end speed claim.

## Paired HTTP comparison

Reuse the dedicated container setup from exp100, but run **split placement**
with border `2025-03-01T12:00:00Z`. Build a new candidate from this checkout.
For the baseline, use the retained integrated reduction image
`beehive-data-api:aggregation-reduce-1793922815`, digest
`sha256:c31360fa869622d3729d9d303399d918856115aca7d011ee70788c2bd8bf2546`.
Do not use the older dictionary baseline from exp96: that would confound this
staging change with the earlier cold reduction.

Use separate request scratch mounts and the same verified historical hot
bucket, immutable warehouse, compression, thread settings and resource limits.
Bind only dedicated loopback ports. Keep other benchmark traffic idle.
`EXP101_BASE_CONTAINER` / `EXP101_CANDIDATE_CONTAINER` and matching port
variables below refer to those newly started dedicated containers. Neither the
driver nor these tests deploys to production.

```sh
python sage-data-rework/tiered_system/experiments/runbooks/exp100_aggregation_http.py \
  --baseline "http://127.0.0.1:$EXP101_BASE_PORT/api/v1/query" \
  --candidate "http://127.0.0.1:$EXP101_CANDIDATE_PORT/api/v1/query" \
  --baseline-container "$EXP101_BASE_CONTAINER" \
  --candidate-container "$EXP101_CANDIDATE_CONTAINER" \
  --reference sage-data-rework/tiered_system/experiments/results/exp100-host-1793922689/split-control/report.json \
  --reference-candidate --placement split --workload short --reps 3 \
  --run-dir "$EXP101_ROOT/split-control"
```

The driver pins the baseline to the completed exp100 candidate image and checks
matching settings/resources. It alternates whole-sequence backend order,
excludes one warmup, tests both wire formats and compares every body, including
head/tail. This is 160 split HTTP responses at three measured repetitions.
For diagnostic Arrow profiles, recreate the same images with separate profile
mounts, then repeat with:

```sh
python sage-data-rework/tiered_system/experiments/runbooks/exp100_aggregation_http.py \
  --baseline "http://127.0.0.1:$EXP101_BASE_PORT/api/v1/query" \
  --candidate "http://127.0.0.1:$EXP101_CANDIDATE_PORT/api/v1/query" \
  --baseline-container "$EXP101_BASE_CONTAINER" \
  --candidate-container "$EXP101_CANDIDATE_CONTAINER" \
  --baseline-profiles "$EXP101_BASE_PROFILES" \
  --candidate-profiles "$EXP101_CANDIDATE_PROFILES" \
  --reference sage-data-rework/tiered_system/experiments/results/exp100-host-1793922689/split-control/report.json \
  --reference-candidate --placement split --workload short --reps 1 \
  --control "$EXP101_ROOT/split-control/report.json" \
  --run-dir "$EXP101_ROOT/split-profile"
```

Compare full HTTP latency/first result, not merely Parquet write throughput.
The old baseline has `aggregate_hot_read_stage_seconds` plus
`aggregate_stage_queue_seconds` and `aggregate_stage_copy_seconds`; the candidate
has one direct `aggregate_hot_read_stage_seconds` covering source iteration,
validation, Parquet writing and closure. Compare the shared
`aggregate_staging_complete_since_backend_start_seconds` marker as well; do not
add it to its overlapping phases. Candidate `aggregate_input_bytes` must equal
the baseline's `aggregate_ndjson_bytes` for the same hot input, though Parquet
file sizes may differ. Check row counts, final execution/spill, peak container
memory, scratch usage, OOM/restarts and scratch cleanup. The driver's before/
after resource snapshots are not continuous peak memory sampling; use the host
monitor alongside it. Staging allocates bounded Go batches outside DuckDB's
memory limit and can overlap between requests.

## Retained outcome (2026-11-06)

The [completed host gates and review](../../plans/13_aggregation_queries.md#direct-hot-parquet-staging-host-gates-2026-11-06)
pass correctness, independent codec consumption, exact byte accounting,
cleanup and all paired response comparisons. Direct staging is retained as
the sole API path for its scratch-space reduction (~188 MB to ~4 MB), accepting
the measured day-mean Arrow latency increase from 3.88s to 4.20s. The removed
NDJSON/COPY implementation is not available as a runtime option. These commands
remain for reproducing the comparison against the retained historical image;
that image is benchmark evidence, not a production dependency. Sampled RSS
does not establish a process-memory improvement.
