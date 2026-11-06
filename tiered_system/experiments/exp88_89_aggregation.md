# Experiments 88–89: integer compatibility and actual Iceberg/HTTP fixture

These follow [plan 13](../../plans/13_aggregation_queries.md). Production
aggregation is still disabled. Only research scripts and an opt-in `_test.go`
fixture adapter are added. No existing bucket, API deployment, warehouse,
`/node-data`, or archived datasets are modified or deleted.

## 88: replay native integer behavior

From the workspace root:

```sh
python3 sage-data-rework/tiered_system/experiments/exp88_flux_integer_replay.py \
  --duckdb "$HOME/.local/bin/duckdb" \
  --source-report sage-data-rework/tiered_system/experiments/results/exp87-vm-1793830028/report.json \
  --run-dir "$PWD/sage-data-rework/tiered_system/experiments/results/exp88-$(date +%s)"
```

Use the Linux CLI path on a Linux VM. The script enforces DuckDB 2.0.x and
replays all 45 native storage count/sum/mean observations in both cold-only and
split-through-Parquet placements. It reads the captured raw fixture and hashes
the original oracle; it does not make HTTP calls. It has no tolerance for integer
or nanosecond changes; float comparison keeps the previous tolerance.

The candidate retains the baseline compiler by default. Exp88 explicitly selects
its research-only `flux64` policy: wider exact integer accumulation, explicit
modulo 2^64 reduction, signed/unsigned interpretation, and double conversion only
after wrapping for means. Empty-window results stay unchanged. No new production
flag or request field is introduced. Unsupported/mixed arithmetic input remains
an error rather than being silently coerced or omitted.

Local result: **90/90 comparisons passed** on DuckDB 2.0.0.
[Report](results/exp88_integer_replay_20261104/report.json).

## 89: prepare an actual local Iceberg v3 table

Build the small Go writer, from the workspace root:

```sh
(cd sage-data-rework/tiered_system/experiments/icebergo && \
  go build -o /tmp/exp89-iceberg-fixture ./cmd/aggregation-fixture)
```

The writer uses the research module's existing iceberg-go/SQLite dependencies.
It registers the generated Parquet in a new local SQL catalog, with format v3,
UTC day partitions, timestamptz_ns, MAP metadata, promoted tags, and value field
ID 11 typed VARIANT. Files are sorted plugin/vsn/time and plugin-pure with a
262,144-row group cap. The tiny fixture uses separate plugin/day files; it is
not a file-packing throughput or full-warehouse-layout benchmark.

Unsigned storage is a **known separate blocker**, so the supported fixture uses
an explicit exclusion and records every excluded case:

```sh
python3 sage-data-rework/tiered_system/experiments/exp89_aggregation_fixture.py \
  --exclude-unsigned --duckdb "$HOME/.local/bin/duckdb" \
  --fixture-writer /tmp/exp89-iceberg-fixture \
  --integer-report sage-data-rework/tiered_system/experiments/results/exp88_integer_replay_20261104/report.json \
  --baseline-report sage-data-rework/tiered_system/experiments/results/exp86-vm-1793828688/report.json \
  --run-dir "$PWD/sage-data-rework/tiered_system/experiments/results/exp89-$(date +%s)"
```

Omitting `--exclude-unsigned` preserves strict behavior and exposes the write
failure; it does not substitute floats, strings, or decimals. Every preparation
requires a fresh directory and retains outputs on failure. The baseline report
adds the other three functions, float series, null/empty windows, calendar and
nanosecond bounds, and head/tail combinations. Original Flux expectations stay
unchanged. Mixed-type omission and Flux panics are not copied.

The CLI verifies all raw stored rows, value kinds, full metadata, nanoseconds,
Iceberg schema/version, and every aggregate against the recorded Flux oracle.
It generates `http-manifest.json`, SQL, reports, and hot Parquet staging.
No REST catalog/server is needed. Local results: **30 raw rows verified and
136/136 aggregate comparisons passed**. All 23 unsigned-input cases are listed as
excluded. [Report](results/exp89_supported_fixture_20261104/report.json).
[Independent JSON scalar-type check](results/exp89_supported_fixture_20261104/public-value-types.json).

## Unsigned VARIANT storage finding

The actual Parquet step found an issue the in-memory replay could not:

| Native input | Parquet round trip on DuckDB 2.0.0 |
| --- | --- |
| `6::UBIGINT::VARIANT` | Numeric 6 survives, but `variant_typeof` becomes `INT64` |
| `18446744073709551612::UBIGINT::VARIANT` | Writer rejects the cast to INT64 |
| The same values converted to `DECIMAL(20,0)::VARIANT` | Exact JSON integers survive and the subtype remains `DECIMAL(20, 0)` |

[Diagnostic report](results/exp89_unsigned_variant_check_20261104/report.json).
Parquet's [VARIANT primitive encoding](https://github.com/apache/parquet-format/blob/master/VariantEncoding.md)
provides signed integer and decimal encodings; do not assume a native UINT64
subtype survives storage. This is separate from DuckDB's in-memory VARIANT support.
Decimal encoding is a **candidate only**. Using it would require an explicit
archival/reader convention and verification policy, including how original uint
kind is identified. No exporter, warehouse schema, current data, or API decoder
was changed to adopt it. Do not claim full unsigned compatibility from the
supported fixture or silently classify signed values as unsigned.

One intermediate fixture also caught an instrumentation issue: DuckDB reports
boolean VARIANT types as `BOOL_TRUE` and `BOOL_FALSE`. The experiment's kind
classifier now recognizes both. The false value itself was stored correctly;
it was not a boolean storage-loss finding. Failed preparations are retained.

## Current HTTP validation

The original precompiled-SQL HTTP/read-grant adapter was removed after the
production aggregation path passed exp100. Its historical result records below
are retained; they are not current execution instructions. Use the prepared
fixture with the [API validation guide](../../../beehive-data-api/docs/aggregation-validation.md)
and [exp100](exp100_aggregation_http.md). The native fixture, current-client
replay and standalone tiered lease tests remain maintained.

## Host HTTP probe (2026-11-04)

Ran on this Mac against the existing preparation, which already stores absolute
`file://` URLs on this filesystem. Go 1.26.5 darwin/arm64, `CGO_ENABLED=1`,
`-tags=duckdb_arrow`, `GOFLAGS=-mod=readonly`, and the API `go.mod` pins
(Arrow 18.5.1, DuckDB Go bindings/driver, JWT, protobuf, sys). The test loaded
Iceberg itself and required DuckDB `v2.0.`. No listener was bound.

`TestAggregationCandidateHTTPProbe` passed in 5.435s. **272 response cases**
(136 SQL cases × NDJSON and Arrow) plus the six lease scenarios (renew, expire,
changed pin, each format). Saved responses:
`results/exp89_supported_fixture_20261104/http-responses` (136 `.ndjson`, 136
`.arrow`). Log: `results/exp89_supported_fixture_20261104/http-probe.log`.

Offline harness: **32 tests OK** (`test_exp8[6789]*.py`). Production aggregation
routing stays disabled. Unsigned VARIANT storage is still excluded.

## Offline harness checks

```sh
python3 -m unittest discover \
  -s sage-data-rework/tiered_system/experiments -p 'test_exp8[6789]*.py' -v
```

Reports from separate runs are retained. No automatic historical cleanup or
implicit reuse is performed.

## Follow-up: decode the saved HTTP bodies with the current client

The host HTTP probe passed, but a separate client check found four precision
failures after DataFrame construction. Run the current repository client against
all saved responses without making any network calls:

```sh
# Use an environment with the Sage client dependencies (pandas/pyarrow/numpy).
python3 sage-data-rework/tiered_system/experiments/exp90_aggregation_client.py \
  --manifest "$EXP89_ROOT/http-manifest.json" \
  --responses "$EXP89_ROOT/http-responses" \
  --output "$EXP89_ROOT/client-check-$(date +%s).json"
```

The script imports the workspace client explicitly, hashes both decoder modules
and every body, and compares timestamps/metadata/nulls and values. Harmless dtype
changes are counted separately; exact integer changes are failures. It never
modifies the client, input bodies, or a previous report.

[Recorded client check](results/exp90_client_checked_20261104/report.json):
**268/272 pass; four fail** on nullable signed integers, in Arrow and NDJSON for
both placements. The HTTP body retains `9223372036854775807`; DataFrame float64
inference rounds it to `9223372036854775808`. Nullable integer precision needs a
client fix and regression replay before aggregation is considered client-ready.

### Completed client precision fix (2026-11-04)

The nullable-integer failure is fixed in both workspace client decoders, using
shared inference before integer values can become floats. The final offline
replay passes **272/272**, including all four previously failing responses:
[validated report](results/exp90_client_fixed_20261104/report_validated.json).
Original HTTP bodies and earlier failure reports remain unchanged. Reports hash
the Arrow/NDJSON decoders plus `_values.py`. Nullable integer values now use
`Int64`/`UInt64` where possible; mixed integer/float and wider integer columns
use objects. Float-only columns retain float64. See plan 13's completed client
precision section for regression checks and the local decode timing comparison.
This replay does not enable API aggregation or settle unsigned storage policy.
