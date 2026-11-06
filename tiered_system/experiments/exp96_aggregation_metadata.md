# Aggregation metadata dictionary (exp96)

The candidate reduces per-observation metadata work in the production cold and
split aggregate compiler. Ordinary extracts and hot-only Flux aggregates keep
their existing paths. This does not change the warehouse or public schema.

The SQL scans filtered raw input once into a materialized CTE, deduplicates
raw `(name, meta, promoted tags)` tuples, and reconstructs/canonicalizes metadata
on those distinct tuples. Canonical `(name, meta)` identities receive numeric
series IDs. A null-safe dictionary join assigns each original observation its
ID; head/tail and aggregation operate on the ID. Complete metadata is restored
at output. This retains observations with identical tags and preserves series
identity when raw map order, promoted-tag overrides or null/empty raw metadata
differ. Hot metadata is already merged and supplies null promoted columns.

This removes expensive repeated merging/sorting and carrying full metadata
through the reduction. It still hashes/compares raw tuples when building and
joining the dictionary, materializes raw input, and has additional joins. High
metadata cardinality can reduce or reverse the benefit; measure rather than
assuming every workload improves. No fallback flag or alternate runtime path
was added. The earlier source is retained only as benchmark evidence.

## Local evidence

[Artifacts](results/exp96-metadata-20261105):

- Production compiler against the captured oracle and actual local Iceberg:
  182 cold/split cases pass with DuckDB 2.0.0.
- Twenty focused native CLI regressions: five functions × all/head0/head1/tail1.
  They cover reordered keys, promoted overrides, already-merged hot metadata,
  null and empty raw metadata, repeated tuples, distinct non-promoted tags,
  combined-series limit selection and empty windows.
- Local one-million-row Parquet mean benchmark, 16 series, two DuckDB threads,
  3 GB engine limit, one excluded warmup plus three alternating measurements:
  final-source baseline median **1.490 s**, dictionary median **0.220 s**,
  about **6.8×**. See `final/sequential/local-benchmark.json`.
  All eight executions return the same 320 rows/values. Native process startup
  is included equally. SQL, source snapshots, fixture and measurements are
  retained in the artifact directory.

The earlier smoke run also passed (1.555 s → 0.231 s). An intermediate run
overlapped a correctness suite and is explicitly excluded from performance
analysis; the final-source timing above ran sequentially after that suite.

The local benchmark does **not** measure Iceberg/Pelican, HTTP, hot staging or
historical production latency. The pinned Go native library is unavailable in
this sandbox, so the host gates and historical timing run remain required.

## Host/VM correctness gates

From `beehive-data-api`, reuse the existing local exp91 fixture environment
(`COLD_AGGREGATION_MANIFEST`, `COLD_AGGREGATION_DUCKDB2`). Do not reseed it:

```sh
AGGREGATION_PARITY_PROFILE=1 CGO_ENABLED=1 \
  go test -tags=duckdb_arrow -count=1 -v ./internal/backend/cold \
  -run '^(TestAggregateCompilerOracle|TestAggregateMetadataDictionary|TestAggregationProductionHTTP|TestHotAggregateStage|TestColdQueryProfile)'
CGO_ENABLED=1 go test -tags=duckdb_arrow ./...
CGO_ENABLED=1 go vet -tags=duckdb_arrow ./...
```

Also rerun `TestAggregationLiveTierParity` using the retained verified
`AGGREGATION_PARITY_SEED_REPORT` and `INFLUX_TOKEN`, with a fresh
`AGGREGATION_HTTP_OUTPUT_DIR`, then replay its 546 responses as described in the
[API validation guide](../../../beehive-data-api/docs/aggregation-validation.md).
The dedicated 36-row parity bucket is separate from historical performance data.

## Historical comparison

Reuse the read-only adopted warehouse and retained **exp95-hot** Influx bucket
from `exp95-bench-1793897282`. Do not change the old API image/containers,
published data or reference reports. Follow the [exp95 runbook](exp95_aggregation_performance.md)
with a fresh `BENCH_ROOT`, image tag, container names and unused loopback ports.
Build the current source; configure the same Influx data/network, static
borders, execution settings and resource limits as the baseline. Run the
unprofiled control and separate diagnostic Arrow sweep against the new image.
A diagnostic sweep must reference its own new unprofiled control, since the
old image differs. Existing containers may stay running, but no benchmark
requests should overlap. Use the same host and avoid unrelated workloads.

Compare the two **unprofiled** reports offline, from the workspace root:

```sh
python sage-data-rework/tiered_system/experiments/exp96_aggregation_metadata_compare.py \
  --baseline sage-data-rework/tiered_system/experiments/results/exp95-bench-1793897282/control/report.json \
  --candidate "$BENCH_ROOT/control/report.json" \
  --output "$BENCH_ROOT/metadata-comparison.json"
```

The comparison permits a different image and requires matching workload,
execution settings and resource limits. It verifies body hashes, decodes every
response and checks baseline/candidate parity before reporting HTTP medians,
speedup and percentage reduction. Integer/timestamp comparisons remain exact;
floating tolerance is unchanged. The offline self-check against the old report
passes; its `self_check=true` output is not new performance evidence.

Inspect the diagnostic operator tree to confirm metadata MAP construction and
sorting operate on distinct tag tuples rather than every observation. Report
daily/monthly cold-only gains, the split penalty, small-query regressions,
engine memory/spill, and first-result time. Retain the candidate only after
native correctness and representative historical results support it. No
warehouse changes, commits or pushes are needed.
