# Historical aggregation performance (exp95)

Saved runs cited below as `results/...` are paths inside the split archive in the parent directory (`../results.z01`, `../results.z02`, `../results.z03`, and `../results.zip`). See [recorded results](../README.md#recorded-results). A command that creates a new run directory still writes local untracked output.

This is a read-only benchmark of the assembled production API. It neither seeds
Influx nor changes Pelican, starts containers, or deletes data. Run it on the
host/VM with Docker access after building the current API. Python needs PyArrow.
Use dedicated static API instances, with no other clients and no archival
writer/deletion touching their historical bucket during the run.

The cold reference is the adopted March/April plugin → vsn → time warehouse,
with plugin-pure row groups capped at 262,144 rows:

```
https://nrdstor.nationalresearchplatform.org:8443/sage/cold-storage-demo-v3-bounded-full-1793018629285065000/warehouse
```

The second API uses that same warehouse and `COLD_BORDER=2025-03-01T12:00:00Z`.
Its Influx bucket must already contain the **same historical March 1 rows from
noon through midnight**, with original tags, kinds, values and nanoseconds.
The small `agg-exp86-*` correctness bucket is unsuitable. Do not reseed or
change published warehouses merely to run this script. If the historical hot
slice is absent, prepare a separate isolated fixture before measuring.

## Workload and measurement

| Case | Range / filter | Function / window | Placements |
| --- | --- | --- | --- |
| `series_day_count/mean/sum/min/max` | March 1, wxt.env.temp / W0A0 | Each function / 5m | Cold and split |
| `series_hour_mean` | March 1 11:30–12:30, same series | mean / 1m | Cold and split |
| `fleet_minute_count` | March 1 11:59:30–12:00:30, all data | count / whole range | Cold and split |
| `temperature_fleet_hour_mean` | March 1 11:30–12:30, wxt.env.temp / all nodes | mean / 5m | Cold and split |
| `series_month_mean` | March, wxt.env.temp / W0A0 | mean / 1h | Cold only |

The month case avoids requiring a month of raw hot input. Fleet count exercises
heterogeneous measurements without attempting arithmetic on strings/booleans.
Use `--case` repeatedly for a smaller smoke run. The default is one excluded
warmup and three measured runs per placement/format, with order rotated within
each case. There is no claim of controlled OS/Pelican cold caches. Results
include first complete record/batch, full HTTP time, body bytes, rows and client
receive CPU. Arrow includes IPC parsing; NDJSON receives lines/bytes. This is
not a Sage client dataframe benchmark. Validation runs outside the timer.

All bodies are retained, hashed and decoded. Every response must match the
cold reference: exact timestamps, series metadata, integer values, and floating
values within relative 1e-12 / absolute 1e-10. Nonempty results, unique identities,
bounds and integer count outputs are checked. A disagreement stops the run and
leaves `report.json` marked failed. Historical cold/split agreement is a useful
coverage check, not an independent proof of original production ingestion.

## Native profiling gate

Re-run the existing production HTTP fixture with the new opt-in profiling
assertions before the historical benchmark. From `beehive-data-api`, retain the
same prepared `COLD_AGGREGATION_MANIFEST` and `COLD_AGGREGATION_DUCKDB2` used for
the passing host correctness run:

```sh
AGGREGATION_PARITY_PROFILE=1 CGO_ENABLED=1 \
  go test -tags=duckdb_arrow -count=1 -v ./internal/backend/cold \
  -run '^(TestHotAggregateStage|TestColdQueryProfile|TestAggregationProductionHTTP)'
CGO_ENABLED=1 go test -tags=duckdb_arrow ./...
CGO_ENABLED=1 go vet -tags=duckdb_arrow ./...
```

The HTTP assertion requires **one shared profile**, including staging and the
native query, with complete status and all split-stage fields. It also retains
the existing response parity and scratch/grant cleanup assertions.

## Start dedicated benchmark APIs

Example from the workspace root. Supply `BENCH_ENV` as an existing private
Docker env file containing the historical `INFLUXDB_URL`, `INFLUXDB_TOKEN`,
`INFLUXDB_BUCKET`, and any necessary hot settings. The URL must be reachable
from the selected Docker network. Supply `PELICAN_BENCH_KEY` as the absolute
path to the existing trusted read key and `BENCH_NETWORK` as an existing Docker
network. Static mode requires loader coordination connection settings unset.
Keep those settings out of this dedicated env file; do not reuse dynamic service
configuration. No secrets should be copied into result reports.

```sh
export BENCH_ROOT="$PWD/sage-data-rework/tiered_system/experiments/results/exp95-$(date +%s)"
export BENCH_IMAGE="beehive-data-api:aggregation-perf-$(date +%s)"
export BENCH_SUFFIX="$(date +%s)"
export BENCH_WAREHOUSE='https://nrdstor.nationalresearchplatform.org:8443/sage/cold-storage-demo-v3-bounded-full-1793018629285065000/warehouse'
# Set BENCH_ENV, PELICAN_BENCH_KEY and BENCH_NETWORK in this shell first.
mkdir -m 700 "$BENCH_ROOT"
docker build -t "$BENCH_IMAGE" beehive-data-api

for mode in control profile; do
  for role in cold split; do
    border='2025-05-01T00:00:00Z'
    port=18081
    if [ "$role" = split ]; then border='2025-03-01T12:00:00Z'; port=18082; fi
    profile_dir=''
    if [ "$mode" = profile ]; then profile_dir=/profiles; port=$((port + 2)); fi
    mkdir -m 700 "$BENCH_ROOT/$mode-$role-profiles" "$BENCH_ROOT/$mode-$role-scratch"
    docker run -d --name "agg-$mode-$role-$BENCH_SUFFIX" \
      --network "$BENCH_NETWORK" -p "127.0.0.1:$port:10000" \
      --env-file "$BENCH_ENV" \
      -e COLD_FRONTIER=static -e "COLD_BORDER=$border" \
      -e "COLD_WAREHOUSE=$BENCH_WAREHOUSE" \
      -e COLD_ASYNC_THREADS=8 -e COLD_READ_AHEAD_DEPTH=-1 \
      -e ARROW_COMPRESSION=zstd -e "COLD_PROFILE_DIR=$profile_dir" \
      -e COLD_AGGREGATE_STAGING_DIR=/scratch \
      -e COLD_AGGREGATE_MAX_STAGING_BYTES=17179869184 \
      -e PELICAN_KEY_PATH=/run/secrets/pelican.pem \
      --mount "type=bind,src=$PELICAN_BENCH_KEY,dst=/run/secrets/pelican.pem,readonly" \
      --mount "type=bind,src=$BENCH_ROOT/$mode-$role-profiles,dst=/profiles" \
      --mount "type=bind,src=$BENCH_ROOT/$mode-$role-scratch,dst=/scratch" \
      "$BENCH_IMAGE"
  done
done
```

Check startup logs before running. These examples impose no extra CPU/memory
limits; if setting limits, apply identical limits to both placements and phases.
Scratch must have room for bounded NDJSON plus Parquet and engine spill. Do not
run two benchmark processes simultaneously. Containers and artifacts are left
for operator inspection; this guide performs no automatic removal.

## Primary unprofiled sweep

```sh
python sage-data-rework/tiered_system/experiments/runbooks/exp95_aggregation_performance.py \
  --cold http://127.0.0.1:18081/api/v1/query \
  --split http://127.0.0.1:18082/api/v1/query \
  --cold-container "agg-control-cold-$BENCH_SUFFIX" \
  --split-container "agg-control-split-$BENCH_SUFFIX" \
  --run-dir "$BENCH_ROOT/control"
```

This measures both Arrow and NDJSON. The script inspects only a whitelist of
non-secret configuration, image/container identities, resource limits, restart
counts and cgroup OOM events. Changed or mismatched execution settings fail.
Use Docker env settings, not overriding CLI flags, for these dedicated APIs so
the inspection describes the actual runtime configuration.

## Diagnostic Arrow sweep

Use the same selected `--case` options as the control, if any:

```sh
python sage-data-rework/tiered_system/experiments/runbooks/exp95_aggregation_performance.py \
  --cold http://127.0.0.1:18083/api/v1/query \
  --split http://127.0.0.1:18084/api/v1/query \
  --cold-container "agg-profile-cold-$BENCH_SUFFIX" \
  --split-container "agg-profile-split-$BENCH_SUFFIX" \
  --cold-profiles "$BENCH_ROOT/profile-cold-profiles" \
  --split-profiles "$BENCH_ROOT/profile-split-profiles" \
  --control "$BENCH_ROOT/control/report.json" \
  --run-dir "$BENCH_ROOT/profile"
```

The diagnostic phase requires the same image, resources and execution settings
as the control (apart from profile directory), and checks values against saved
control bodies. It pairs exactly one fresh complete timing/native profile to
each request and requires positive staged input for split queries.

New fields in `.timing.json`:

- `aggregate_hot_open_seconds`: opening the raw Flux source.
- `aggregate_hot_read_stage_seconds`: consuming CSV, validating and encoding
  NDJSON, flushing/closing files and closing the source. Includes network waits.
- `aggregate_stage_queue_seconds`: waiting for the shared cold-engine mutex.
- `aggregate_stage_copy_seconds`: executing the NDJSON → Parquet COPY.
- `aggregate_hot_rows`, `aggregate_ndjson_bytes`, `aggregate_parquet_bytes`:
  eligible staged records and artifact sizes.
- `aggregate_staging_complete_since_backend_start_seconds`: elapsed staging
  marker; overlaps the substeps and must not be added to them.

The existing backend lifetime/first-batch/EOF markers now start **before hot
staging** for split aggregates. Cold-only and extract markers retain their
existing start. DuckDB's native profile still describes the final aggregate
query, not the staging COPY. Native operator work is not additive wall time.
NDJSON profiling remains disabled; use its unprofiled HTTP measurements.
The inner native result finishes its profile before the outer staging directory
is removed; HTTP return and existing cleanup tests cover that final ownership
step. Do not interpret backend lifetime as full coordinator/HTTP latency.

Report the unprofiled medians separately from profile diagnostics. Determine
whether hot input transfer/encoding, COPY, scan/group/window/sort, or delivery
is the limiting phase before changing the implementation. This experiment does
not benchmark dynamic lease overhead, concurrent throughput or mixed-kind Flux
completeness.
