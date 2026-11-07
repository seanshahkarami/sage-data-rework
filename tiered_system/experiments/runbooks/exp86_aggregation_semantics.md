# Experiment 86: aggregation semantics before API implementation

Saved runs cited below as `results/...` are paths inside the split archive in the parent directory (`../results.z01`, `../results.z02`, `../results.z03`, and `../results.zip`). See [recorded results](../README.md#recorded-results). A command that creates a new run directory still writes local untracked output.

This is a small synthetic correctness experiment for
[plan 13](../../plans/13_aggregation_queries.md). It creates one new Influx bucket
named `agg-exp86-<UUID>` and a new private local run directory. It never changes
an existing bucket, API, warehouse, catalog, or `/node-data`, and never issues a
delete request. Fixture buckets and reports remain available for inspection.

The full run compares the existing Flux operations with candidate DuckDB 2.0 SQL
in two placements: all rows in a local VARIANT relation (`cold_only`) and rows
split at a border with the hot half reconstructed from a primitive Parquet file
(`split`). The former represents cold input logically; it does **not** exercise
Iceberg, Pelican, physical row-group pruning, or the production API. Raw fixture
readback must match before aggregate comparisons begin. The deliberately mixed
field-type series is excluded from that preflight so its expected schema
collision cannot mask baseline integrity; it has separate aggregate cases.

## Run the Flux/DuckDB 2.0 comparison on the VM

From the workspace root, using a running InfluxDB 2.x fixture/demo service and a
matching native DuckDB 2.0.x CLI:

```sh
# Use a fixture credential that can create buckets in this organization.
export INFLUX_TOKEN='<fixture token>'
python3 sage-data-rework/tiered_system/experiments/runbooks/exp86_aggregation_semantics.py \
  --duckdb "$PWD/beehive-hot-to-cold-loader/bin/duckdb" \
  --influx-url http://localhost:18086 --org waggle \
  --run-dir "$PWD/sage-data-rework/tiered_system/experiments/results/exp86-vm-$(date +%s)"
```

The demo in this workspace uses port 18086 and organization `waggle`. You may
instead use a dedicated Influx fixture endpoint and its own credential. Keep the
URL to the intended fixture. The script reads the token from the environment and
does not save it. It requires no third-party Python packages, Docker access from
the script, Pelican key, or cloud credentials. The supplied DuckDB binary must
match the VM architecture. An incompatible engine is rejected before any Influx
mutation. It does not install/download a replacement engine.

There are 60 cases: 57 candidate comparisons in both placements, plus three
oracle-only observations (compound month/day windows and all-null count/mean).
They cover all five functions, empty windows, partial-window stop labels, 2ns
windows, pre-epoch floor division, weekly/monthly/yearly and compound windows,
large signed/unsigned integers, strings/booleans, overflow, mixed field kinds,
series observed only on one side, reversed hot metadata key order, and per-series
head/tail **before** aggregation. The weighted-mean fixture has
cold 10/20/30 and hot 100, whose correct mean is 40 rather than 60.

`report.json` records exact results, value kinds, timestamps, metadata, engine
version, source hash, error categories, diagnostics, and the retained bucket ID.
Each case also retains its Flux, annotated CSV when available, SQL, DuckDB stdout,
and stderr. Failed comparisons are useful findings; an exit status of 1 means
at least one mismatch, not permission to change expected results or discard files.

- `PASS`: values, kinds, metadata, and timestamps agree (float tolerance
  `rel=1e-12`, `abs=1e-10`; integers and nanoseconds are exact).
- `BOTH_ERROR`: both engines reject input in the same recognized type/overflow
  category. This is not proof of matching HTTP status or exact error messages.
- `FAIL`: result/error mismatch, value/type/timestamp mismatch, or an unclassified
  failure. Infrastructure, CSV parser, and SQL binder failures cannot count as
  successful error parity.
- Oracle-only cases retain the Flux result/error but do not claim implementation
  of that construct. In particular, mixed calendar/fixed boundaries are deferred.

The candidate uses null filling for empty sums and rejects mixed numeric
kinds in a series. After the first Flux run it also retains null windowed
min/max results, preserves observed series through zero-row limits, and floors
nanoseconds before the calendar bucketing overload. Those are hypotheses under test, not confirmed compatibility
policies. Any mismatch here should update the plan before production coding.

Unwindowed timestamps are mapped to `_start` exactly as the current hot adapter
does. Windowed timestamps use Flux `_time`. This maps the adapter's record
semantics; it is not an HTTP Arrow/NDJSON integration test. Later experiments must
cover the real API, grant renewal/cancellation, both wire formats, actual v3
scans, and performance/concurrent-query impact after semantic parity is settled.

## Local preview when the target services are unavailable

```sh
python3 sage-data-rework/tiered_system/experiments/runbooks/exp86_aggregation_semantics.py \
  --local-smoke --duckdb /path/to/local/duckdb \
  --run-dir /absolute/path/to/new/exp86-local-preview
```

This mode permits a different engine version **explicitly for candidate
self-consistency**. It runs no Flux, Influx HTTP, API, Iceberg, or Pelican work.
It compares whole/split candidates and checks eight independent expected values.
The report records the actual engine and must not be described as target-version
or Flux compatibility evidence. The normal full-run version guard remains strict.
Native VARIANT JSON conversion is recorded diagnostically; result comparisons use
explicit typed scalar serialization to preserve integers and avoid relying on
another engine version's VARIANT JSON behavior.

Offline harness tests:

```sh
python3 -m unittest discover \
  -s sage-data-rework/tiered_system/experiments/runbooks -p 'test_exp86*.py' -v
```

These test parsing/precision, exact bounds, source splitting, guards, failure
classification, and operation order; they do not replace the VM experiment.

## Laptop CLI selection

This workspace's tool shell resolves `/opt/homebrew/bin/duckdb` (1.5.6) before
`~/.local/bin/duckdb` (2.0.0). The conda Python package is also 1.5.6 independently.
Use `--duckdb "$HOME/.local/bin/duckdb"` explicitly for the native 2.0 CLI on this
laptop. The repeated local 2.0 candidate screen passed all 57 cases and eight
manual controls. It still has no Flux reference because this sandbox cannot
reach the local Influx endpoints; see the full-run instructions above.

## Replay an existing Flux run without modifying it

After fixing candidate SQL, compare it with the recorded Flux reference locally:

```sh
python3 sage-data-rework/tiered_system/experiments/runbooks/exp86_aggregation_semantics.py \
  --duckdb "$HOME/.local/bin/duckdb" \
  --replay-report sage-data-rework/tiered_system/experiments/results/exp86-vm-1793828688/report.json \
  --run-dir "$PWD/sage-data-rework/tiered_system/experiments/results/exp86-replay-$(date +%s)"
```

On the VM, substitute its native 2.0 executable. This mode requires a completed
reference report with verified raw input and its adjacent `fixture.json`. It
makes no Influx calls and does not need a token. It hashes the source report and
fixture, reuses the stored expectations unchanged, and saves SQL/results into a
new directory. It is a recorded-oracle regression check, not a new live Flux or
API run. A nonzero exit is expected while unresolved parity cases remain.

The first safe-fix replay on DuckDB 2.0.0 measured **86 PASS, 8 BOTH_ERROR, 20
FAIL**, versus 66/8/40 originally. Fixes covered timestamp rounding at calendar
boundaries, empty windowed min/max results, and zero-limit observed-series
membership. Arithmetic quirks, mixed field types, and unsupported min/max error
classification remain unresolved. Saved VM results were not rewritten.
