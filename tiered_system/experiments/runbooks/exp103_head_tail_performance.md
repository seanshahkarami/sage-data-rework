# Historical standalone head/tail benchmark and profiles

Saved runs cited below as `results/...` are paths inside the split archive in the parent directory (`../results.z01`, `../results.z02`, `../results.z03`, and `../results.zip`). See [recorded results](../README.md#recorded-results). A command that creates a new run directory still writes local untracked output.

The correctness gates are complete in [exp102](exp102_spanning_head_tail.md).
This measures the current feature on the adopted full March/April warehouse
and retained `exp95-hot` historical Influx slice. No seeding, publication,
warehouse updates or deletion is needed. The benchmark driver sends only
queries and inspects dedicated containers; it does not deploy/configure them.

## Workload and interpretation

Sixteen cases cover head/tail at limits **1 and 1000**, each over:

| Workload | Range (UTC, March 1) | Filter |
| --- | --- | --- |
| Series day | 00:00–next midnight | `wxt.env.temp`, `W0A0` |
| Series hour | 11:30–12:30 | `wxt.env.temp`, `W0A0` |
| Fleet minute | 11:59:30–12:00:30 | All names/series |
| Temperature fleet hour | 11:30–12:30 | `wxt.env.temp`, all nodes |

Every range crosses noon, the split border, and uses the same already-verified
historical hot input as exp101. A cold-only instance reads the same logical
observations entirely from Iceberg. Both instances use one newly built image.
This compares **placement costs**, not old-versus-new implementations. Head/tail
returns original rows, so these times are not directly comparable to the
288-row windowed aggregation responses.

Each response is validated for schema (through the existing wire reader),
exclusive time bounds, exact filters, timestamp ordering, duplicate identities
and at most N rows per series. All placements/formats/repetitions must match
the first cold-only response: integer/scalar types and timestamps are exact;
floats use rel=1e-12/abs=1e-10. This historical reference supplements the exp102
independent small-fixture oracle; it is not an independent proof that the cold
implementation selects the right rows on every possible dataset.

The unprofiled control runs Arrow and NDJSON, one excluded warmup and three
measured repetitions per case/placement/format: **256 responses**. Request
order rotates within each case. Reused-engine/external caches remain in play;
no cold-cache claim is made. A separate Arrow diagnostic run uses one excluded
warmup plus one measured repetition: **64 responses**. Profiles must match the
control's image, settings, resources and hashed response bodies. Run the two
phases sequentially with no unrelated benchmark traffic.

## VM setup

From the workspace root, reuse the private `BENCH_ENV` file (Influx URL/org/token,
no loader-coordination variables), `BENCH_NETWORK` and absolute
`PELICAN_BENCH_KEY` from exp100/101. Do not print the environment file or key.
The cold-only and split instances need access to the same historical hot bucket,
although only the split requests read it. The paired driver pins settings and
the hot bucket against the completed exp101 split control. It never requires
that older image to run.

```sh
export EXP103_SUFFIX="$(date +%s)"
export EXP103_ROOT="$PWD/sage-data-rework/tiered_system/experiments/results/exp103-host-$EXP103_SUFFIX"
export EXP103_IMAGE="beehive-data-api:head-tail-bench-$EXP103_SUFFIX"
export EXP103_REFERENCE="$PWD/sage-data-rework/tiered_system/experiments/results/exp101-host-1793968135/split-control/report.json"
export EXP103_COLD_PORT=18481
export EXP103_SPLIT_PORT=18482
mkdir -m 700 "$EXP103_ROOT"
docker build -t "$EXP103_IMAGE" beehive-data-api
```

This helper creates dedicated benchmark instances with isolated scratch and
profile mounts. It does not change an existing service or warehouse. Docker
startup needs the existing credentials above. Leave existing data services
running. If these loopback ports are already occupied, choose unused ports.

```sh
start_exp103_pair() {
  local phase="$1"
  local role border port container profiles
  local -a profile_mount
  for role in cold split; do
    border='2025-05-01T00:00:00Z'
    port="$EXP103_COLD_PORT"
    if [ "$role" = split ]; then
      border='2025-03-01T12:00:00Z'
      port="$EXP103_SPLIT_PORT"
    fi
    container="headtail103-$phase-$role-$EXP103_SUFFIX"
    mkdir -m 700 "$EXP103_ROOT/$phase-$role-scratch"
    profiles=''
    profile_mount=()
    if [ "$phase" = profile ]; then
      mkdir -m 700 "$EXP103_ROOT/profile-$role"
      profiles='/profiles'
      profile_mount=(--mount "type=bind,src=$EXP103_ROOT/profile-$role,dst=/profiles")
    fi
    docker run -d --name "$container" --network "$BENCH_NETWORK" \
      -p "127.0.0.1:$port:10000" --env-file "$BENCH_ENV" \
      -e COLD_FRONTIER=static -e "COLD_BORDER=$border" \
      -e COLD_WAREHOUSE='https://nrdstor.nationalresearchplatform.org:8443/sage/cold-storage-demo-v3-bounded-full-1793018629285065000/warehouse' \
      -e INFLUXDB_BUCKET=exp95-hot \
      -e COLD_ASYNC_THREADS=8 -e COLD_READ_AHEAD_DEPTH=-1 \
      -e ARROW_COMPRESSION=zstd -e "COLD_PROFILE_DIR=$profiles" \
      -e COLD_AGGREGATE_STAGING_DIR=/scratch \
      -e COLD_AGGREGATE_MAX_STAGING_BYTES=17179869184 \
      -e PELICAN_KEY_PATH=/run/secrets/pelican.pem \
      --mount "type=bind,src=$PELICAN_BENCH_KEY,dst=/run/secrets/pelican.pem,readonly" \
      --mount "type=bind,src=$EXP103_ROOT/$phase-$role-scratch,dst=/scratch" \
      "${profile_mount[@]}" "$EXP103_IMAGE"
  done
}
start_exp103_pair control
```

Check startup logs and the configured DuckDB 2.0/two-thread/3 GB engine settings
before measurement. Examples add no CPU/memory caps; any added caps must match
between the pair and both phases. The driver checks image identity, settings,
resource limits, OOM counters and restarts without storing credentials.

## Unprofiled HTTP control

```sh
python sage-data-rework/tiered_system/experiments/runbooks/exp103_head_tail_performance.py \
  --cold "http://127.0.0.1:$EXP103_COLD_PORT/api/v1/query" \
  --split "http://127.0.0.1:$EXP103_SPLIT_PORT/api/v1/query" \
  --cold-container "headtail103-control-cold-$EXP103_SUFFIX" \
  --split-container "headtail103-control-split-$EXP103_SUFFIX" \
  --reference "$EXP103_REFERENCE" --reps 3 \
  --run-dir "$EXP103_ROOT/control"
```

Require `report.json` status `complete`. Responses and partial reports remain
on failure. Latency includes parsing/spooling through the full response, but
excludes semantic comparison and container inspection. It does not measure
Sage-client dataframe construction. Use optional repeated `--case` arguments
for a shorter run, with the identical selection for diagnostics.

Runtime snapshots are not peak-memory sampling. If needed, run a separate
`docker stats` collector while each phase runs, and sample the corresponding
scratch mounts. Report sampled resource peaks separately from per-query native
engine counters. Check that request scratch directories are empty after each
phase. Native profiles persist in their own profile mounts by design.

## Separate Arrow profiles

Stop only these dedicated control instances, preserving their data and reports:

```sh
docker stop "headtail103-control-cold-$EXP103_SUFFIX" "headtail103-control-split-$EXP103_SUFFIX"
start_exp103_pair profile
```

After startup checks, run:

```sh
python sage-data-rework/tiered_system/experiments/runbooks/exp103_head_tail_performance.py \
  --cold "http://127.0.0.1:$EXP103_COLD_PORT/api/v1/query" \
  --split "http://127.0.0.1:$EXP103_SPLIT_PORT/api/v1/query" \
  --cold-container "headtail103-profile-cold-$EXP103_SUFFIX" \
  --split-container "headtail103-profile-split-$EXP103_SUFFIX" \
  --cold-profiles "$EXP103_ROOT/profile-cold" \
  --split-profiles "$EXP103_ROOT/profile-split" \
  --reference "$EXP103_REFERENCE" --reps 1 \
  --control "$EXP103_ROOT/control/report.json" \
  --run-dir "$EXP103_ROOT/diagnostics"
```

Require 64 complete fresh timing/native profile pairs and matching response
bodies. Profiles are Arrow-only; NDJSON primary latency comes from the control.
The report contains per-case medians and raw per-request phase/operator data;
original `.timing.json`/`.duckdb.json` files stay in the profile mounts.

Inspect `aggregate_hot_rows`, `aggregate_input_bytes`, `aggregate_parquet_bytes`,
`aggregate_hot_read_stage_seconds` and the staging-complete marker for split
requests. Compare native prepare/fetch, dictionary join, window/ranking and sort
operator work, engine buffer peak and spill with cold-only execution. The
staging marker overlaps its subphases; do not sum them. Engine operator CPU
can overlap workers and is not additive wall time. Bytes-read counters are not
direct Pelican traffic measurements. This uses the existing DuckDB/phase
profiles, not a Go CPU or allocation profile; the latter remains separate work.

Finish by stopping only the dedicated profile instances:

```sh
docker stop "headtail103-profile-cold-$EXP103_SUFFIX" "headtail103-profile-split-$EXP103_SUFFIX"
```

Keep result/profile directories and stopped containers for review; do not prune
storage. Add results to plan 13, including failures, skips (if any), full HTTP
medians, parity, staging/native breakdown and resource/cleanup observations.
No published warehouse, `/node-data`, Influx data or production configuration
should change in this experiment.

## First historical run: incomplete

The [plan 14 results](../../plans/14_head_tail_queries.md#historical-standalone-headtail-placement-run-2026-11-06)
record a stall after hot staging in the split path. Only two cold-only warmup
responses completed; they are not medians. The interrupted report remains
`running`, and the profile phase was not started. Preserve the failed artifacts.
Use [exp104](exp104_head_tail_stall.md) to isolate native execution and capture
a failed/cancelled request's phase sidecar before rerunning this full workload.
