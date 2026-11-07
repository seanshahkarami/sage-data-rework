# Experiment 87: arithmetic and mixed-type storage probes

Saved runs cited below as `results/...` are paths inside the split archive in the parent directory (`../results.z01`, `../results.z02`, `../results.z03`, and `../results.zip`). See [recorded results](../README.md#recorded-results). A command that creates a new run directory still writes local untracked output.

This follows [experiment 86](exp86_aggregation_semantics.md) and
[plan 13](../../plans/13_aggregation_queries.md). It diagnoses the remaining
integer and mixed-kind mismatches before choosing production behavior.
It does not change the API, Iceberg, Pelican, published warehouses, or `/node-data`.

## Questions

1. Are both mixed-type observations visible in raw storage queries, individually
   and together? Does the chosen type depend on chronological order or insertion
   order? Does explicit conversion recover both observations?
2. Do large unsigned means and overflowing sums behave the same in storage-backed
   Flux, explicit-map input, and pure `array.from` input? Does `aggregateWindow`
   produce the same result as an unwindowed function?
3. How does a pure Flux union of separately typed arrays behave when grouped,
   and when converted to float before grouping?

The existing exp86 bucket is **read-only**. The default run also creates one
new `agg-exp87-<UUID>` bucket with infinite retention and 16 synthetic points:
three two-point mixed-kind series plus five arithmetic pairs. One mixed series
is int then float, another is float then int, and a third has the first series'
time/value assignment but reversed insertion order. The two dates are January
and March to separate source shards. No bucket is deleted or reused for writes.

## Run on the VM / fixture host

Use the same fixture credential as exp86, with permission to create the new bucket:

```sh
export INFLUX_TOKEN='<fixture token>'
python3 sage-data-rework/tiered_system/experiments/runbooks/exp87_aggregation_type_probes.py \
  --duckdb "$PWD/beehive-hot-to-cold-loader/bin/duckdb" \
  --influx-url http://127.0.0.1:18096 --org waggle \
  --source-report sage-data-rework/tiered_system/experiments/results/exp86-vm-1793828688/report.json \
  --run-dir "$PWD/sage-data-rework/tiered_system/experiments/results/exp87-vm-$(date +%s)"
```

Use a native executable matching the host. On this laptop, the macOS 2.0 CLI is
`$HOME/.local/bin/duckdb`; the service's `bin/duckdb` is Linux ARM. If running the
Linux binary inside the same Python image used for exp86, share the **dedicated
fixture container's network**, so localhost refers to its Influx listener:

```sh
export INFLUX_TOKEN='<fixture token>'
docker run --rm --user "$(id -u):$(id -g)" \
  --network container:exp86-influx-1793828580 \
  -e INFLUX_TOKEN -v "$PWD:$PWD" -w "$PWD" \
  python:3.12-slim-bookworm \
  python3 sage-data-rework/tiered_system/experiments/runbooks/exp87_aggregation_type_probes.py \
    --duckdb "$PWD/beehive-hot-to-cold-loader/bin/duckdb" \
    --influx-url http://127.0.0.1:8086 --org waggle \
    --source-report sage-data-rework/tiered_system/experiments/results/exp86-vm-1793828688/report.json \
    --run-dir "$PWD/sage-data-rework/tiered_system/experiments/results/exp87-vm-$(date +%s)"
```

The existing isolated container must be running; substitute its actual name if
it was renamed. The Docker command removes only its transient Python container.
The Influx fixture, newly created bucket, and local reports remain intact.
The script itself uses stdlib Python and HTTP, without Docker commands or new
Python dependencies. DuckDB 2.0.x is validated before creating any bucket.

Add `--read-only` to skip creating a chronology fixture. That mode still queries
the old mixed series and runs pure Flux arrays, but cannot compare reversed
storage chronology or new stored arithmetic variants. It records 127 probes;
the default complete run records **331 probes**. Each query has a bounded timeout
and tiny input; this is not a historical-day performance benchmark.

## What is recorded

`report.json` includes engine health/version, source report hash, new bucket
identity, exact fixture values, and each probe's results or errors. Generated
Flux and successful annotated CSV are saved separately. DuckDB SQL/stdout/stderr
record five independent arithmetic controls. Tokens and request headers are not
saved. Transport and authentication failures abort the run rather than count as
semantic observations. Engine query errors are retained as observations.

Mixed storage series are queried raw and with count/mean/sum over the whole
range, January only, and March only. The whole range is also tested after
`toFloat()` and explicit float mapping, and with a one-year aggregate window.
Those conversion results help locate where input is omitted or rejected.

Arithmetic probes use count/sum/mean with no window, 5-minute windows (separate
points), and 15-minute windows (both points together, clipped to query end):

- Pure Flux arrays with exact native int/uint values, constructed from strings.
- Pure Flux arrays explicitly converted to float before arithmetic.
- Native typed storage input.
- Storage input followed by an identity map.
- Storage input followed by `toFloat()`.

An identity map is an **attempt to distinguish optimized storage execution**,
not proof of a particular physical plan. Its optimizer may remove or push it;
pure arrays provide the independent no-storage reference. No production decision
should rest on the identity-map label alone.

Output is descriptive, not a pass-percentage contest. `completed: true` means
all planned probes were captured; it does not mean behavior is compatible or
mathematically correct. Review raw row counts/types and compare the three input
shapes before deciding what the API should preserve. Do not emulate arbitrary
row omission or an Influx panic just to eliminate a mismatch.

## Local arithmetic-only check

```sh
python3 sage-data-rework/tiered_system/experiments/runbooks/exp87_aggregation_type_probes.py \
  --local-only --duckdb "$HOME/.local/bin/duckdb" \
  --run-dir "$PWD/sage-data-rework/tiered_system/experiments/results/exp87-local-$(date +%s)"
```

No token or network is used. This validates exact wider sums and explicit modulo
2^64 arithmetic against independent Python integers, and reports mathematical
means beside hypothetical wrapped-accumulator means. It is **not** Flux proof.
The full run repeats these controls before contacting Influx.

```sh
python3 -m unittest discover \
  -s sage-data-rework/tiered_system/experiments/runbooks -p 'test_exp8[67]*.py' -v
```

The offline tests cover generated probe coverage, native integer literals,
chronology/insertion ordering, read-only guards, failure handling, and arithmetic
control construction. They do not replace the real Flux execution.
