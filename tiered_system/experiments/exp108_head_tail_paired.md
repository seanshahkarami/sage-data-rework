# Paired HTTP gate for raw-candidate head/tail selection

> Archived comparison after raw-candidate adoption (2026-11-06). The live API
> has removed the superseded standalone selectors and diagnostic probes.
> Reproducing this experiment requires its retained pre-adoption source
> snapshot. Current regression commands are in
> [API validation](../../../beehive-data-api/docs/aggregation-validation.md#standalone-headtail-across-the-border).


Exp107 passed all 64 native trials. Day native lifetime improved 2.3–3.9×,
but hour cases were flat or slower (hour tail 1: 1.514s → 2.373s). Hot staging
was excluded. Run this full request/fleet gate before deciding on adoption.
Use the retained exp108 baseline source snapshot when reproducing this archived comparison.

The preparation tool copies two minimal build/test contexts into a fresh
isolated directory. It changes only the candidate: rename the retained builder,
insert the exact exp107 wrapper as the live selector, and redirect two diagnostic
test baselines. The patch and per-file SHA-256 manifest are saved. No runtime
flag, dependency update, source-checkout edit, seeding or warehouse write.
Both copies include the unchanged Grafana dashboard and Prometheus alerts
required by the existing full-suite asset test. These files are hashed in the
manifest and are outside the four-file selector patch.

## Prepare and test the candidate source

From the workspace root on the VM, reuse the working fixture environment:
`COLD_AGGREGATION_MANIFEST`, `COLD_AGGREGATION_DUCKDB2`,
`AGGREGATION_PARITY_SEED_REPORT` and `INFLUX_TOKEN`. Paths must be absolute;
all retained inputs are read-only. Use the existing `BENCH_ENV`,
`BENCH_NETWORK` and absolute `PELICAN_BENCH_KEY`; do not print credentials.
`BENCH_ENV` must contain the same Influx URL/org/token and no loader coordination
variables, as in exp106. Do not use `/node-data` for any input or output.

Run shell blocks in order and stop if any command fails. Keep failed evidence.

```sh
export EXP108_SUFFIX="$(date +%s)"
export EXP108_ROOT="$PWD/sage-data-rework/tiered_system/experiments/results/exp108-host-$EXP108_SUFFIX"
export EXP108_REFERENCE="$PWD/sage-data-rework/tiered_system/experiments/results/exp106-host-1793985281/performance/control/report.json"
export EXP108_BASELINE_IMAGE="beehive-data-api:head-tail-baseline-$EXP108_SUFFIX"
export EXP108_CANDIDATE_IMAGE="beehive-data-api:head-tail-candidate-$EXP108_SUFFIX"
export EXP108_BASELINE_PORT=18481
export EXP108_CANDIDATE_PORT=18482
mkdir -m 700 "$EXP108_ROOT"
python sage-data-rework/tiered_system/experiments/exp108_prepare_head_tail_candidate.py \
  --api "$PWD/sage-data-rework/tiered_system/experiments/results/exp108-host-1793991377/source/baseline" \
  --output "$EXP108_ROOT/source"
cat "$EXP108_ROOT/source/candidate.patch"
```

Check that the manifest lists exactly four changed files, including two test
files. Both snapshots have identical module pins, staging, aggregation and I/O
policy. Run gates **inside the candidate copy**, so HTTP tests exercise the
candidate as the live selector. Setting `HEAD_TAIL_VALIDATED_REPORT=` disables
only the optional exp105 byte-for-byte direct-plan capture, which is a
baseline-only assertion. It must not be interpreted as a candidate fixture
skip. Remote diagnostic probes are intentionally disabled in the ordinary suite.

```sh
export GOWORK=off
HEAD_TAIL_VALIDATED_REPORT= AGGREGATION_PARITY_PROFILE=1 \
  AGGREGATION_HTTP_OUTPUT_DIR="$EXP108_ROOT/http" \
  go -C "$EXP108_ROOT/source/candidate" test -tags=duckdb_arrow -count=1 -v \
  ./internal/backend/cold -run '^TestHeadTailProductionHTTP$' \
  > "$EXP108_ROOT/http.log" 2>&1
HEAD_TAIL_VALIDATED_REPORT= AGGREGATION_PARITY_PROFILE=1 \
  AGGREGATION_HTTP_OUTPUT_DIR="$EXP108_ROOT/live-http" \
  go -C "$EXP108_ROOT/source/candidate" test -tags=duckdb_arrow -count=1 -v \
  ./internal/backend/cold -run '^TestHeadTailLiveTierParity$' \
  > "$EXP108_ROOT/live.log" 2>&1
HEAD_TAIL_VALIDATED_REPORT= HEAD_TAIL_ISOLATION_OUTPUT= HEAD_TAIL_REDUCTION_OUTPUT= \
  AGGREGATION_HTTP_OUTPUT_DIR= \
  go -C "$EXP108_ROOT/source/candidate" test -tags=duckdb_arrow -count=1 ./... \
  > "$EXP108_ROOT/suite.log" 2>&1
go -C "$EXP108_ROOT/source/candidate" vet -tags=duckdb_arrow ./... \
  > "$EXP108_ROOT/vet.log" 2>&1
go -C "$EXP108_ROOT/source/candidate" test -race -count=1 ./internal/backend/tiered \
  > "$EXP108_ROOT/race.log" 2>&1
```

Require **512 synthetic and 768 live HTTP cases**, with no fixture skips,
plus suite/vet/race success. These cover exact nanoseconds/integers, mixed
scalars, canonical aliases, head/tail limits, native closure/grant release and
scratch cleanup. Do not proceed if a required fixture or CLI/native test skips.

## Build and start the matched pair

Build both images from the two snapshots in the same session/platform. The
driver records image IDs and source hashes; retain both build logs. This
separates code behavior from the older exp106 machine/cache timings.

```sh
docker build -t "$EXP108_BASELINE_IMAGE" "$EXP108_ROOT/source/baseline" \
  > "$EXP108_ROOT/build-baseline.log" 2>&1
docker build -t "$EXP108_CANDIDATE_IMAGE" "$EXP108_ROOT/source/candidate" \
  > "$EXP108_ROOT/build-candidate.log" 2>&1
export EXP108_BASELINE_ID="$(docker image inspect --format '{{.Id}}' "$EXP108_BASELINE_IMAGE")"
export EXP108_CANDIDATE_ID="$(docker image inspect --format '{{.Id}}' "$EXP108_CANDIDATE_IMAGE")"

start_exp108_pair() {
  local phase="$1"
  local role port image container profiles
  local -a profile_mount
  for role in baseline candidate; do
    port="$EXP108_BASELINE_PORT"
    image="$EXP108_BASELINE_ID"
    if [ "$role" = candidate ]; then
      port="$EXP108_CANDIDATE_PORT"
      image="$EXP108_CANDIDATE_ID"
    fi
    container="headtail108-$phase-$role-$EXP108_SUFFIX"
    mkdir -m 700 "$EXP108_ROOT/$phase-$role-scratch"
    profiles=''
    profile_mount=()
    if [ "$phase" = profile ]; then
      mkdir -m 700 "$EXP108_ROOT/profile-$role"
      profiles='/profiles'
      profile_mount=(--mount "type=bind,src=$EXP108_ROOT/profile-$role,dst=/profiles")
    fi
    docker run -d --name "$container" --network "$BENCH_NETWORK" \
      -p "127.0.0.1:$port:10000" --env-file "$BENCH_ENV" \
      -e COLD_FRONTIER=static -e COLD_BORDER=2025-03-01T12:00:00Z \
      -e COLD_WAREHOUSE='https://nrdstor.nationalresearchplatform.org:8443/sage/cold-storage-demo-v3-bounded-full-1793018629285065000/warehouse' \
      -e INFLUXDB_BUCKET=exp95-hot \
      -e COLD_ASYNC_THREADS=8 -e COLD_READ_AHEAD_DEPTH=-1 \
      -e ARROW_COMPRESSION=zstd -e "COLD_PROFILE_DIR=$profiles" \
      -e COLD_AGGREGATE_STAGING_DIR=/scratch \
      -e COLD_AGGREGATE_MAX_STAGING_BYTES=17179869184 \
      -e PELICAN_KEY_PATH=/run/secrets/pelican.pem \
      --mount "type=bind,src=$PELICAN_BENCH_KEY,dst=/run/secrets/pelican.pem,readonly" \
      --mount "type=bind,src=$EXP108_ROOT/$phase-$role-scratch,dst=/scratch" \
      "${profile_mount[@]}" "$image"
  done
}
start_exp108_pair control
```

Choose unused ports if needed. Check both startup logs and readiness; keep the
same DuckDB 2.0/two-thread/3 GB engine settings. Both roles use the **same noon
border**, warehouse and hot bucket. No CPU/memory caps are added here; any
caps must match across roles/phases. Existing services stay running.

## Unprofiled end-to-end control

```sh
python sage-data-rework/tiered_system/experiments/exp108_head_tail_paired.py \
  --baseline "http://127.0.0.1:$EXP108_BASELINE_PORT/api/v1/query" \
  --candidate "http://127.0.0.1:$EXP108_CANDIDATE_PORT/api/v1/query" \
  --baseline-container "headtail108-control-baseline-$EXP108_SUFFIX" \
  --candidate-container "headtail108-control-candidate-$EXP108_SUFFIX" \
  --baseline-image "$EXP108_BASELINE_ID" --candidate-image "$EXP108_CANDIDATE_ID" \
  --source-manifest "$EXP108_ROOT/source/manifest.json" \
  --reference "$EXP108_REFERENCE" --reps 3 --timeout 120 \
  --run-dir "$EXP108_ROOT/control"
```

Require **256 responses**, complete status, exact selected identities/scalar
types and numeric parity (existing float tolerance) against hashed exp106
cold NDJSON bodies. Four workloads cover day/hour single series, fleet minute
and temperature-fleet hour; both head/tail 1/1000, Arrow and NDJSON. Each cell
has one excluded warmup and three measured requests. Requests are sequential,
with rotating role/format order, and saved bodies/hashes on success. Failed
bodies and a failed report remain for investigation. `comparisons` records
same-run baseline/candidate medians and speedup ratios.

Latency includes full response receive/parsing/spooling, excluding semantic
validation and inspection. It is not Sage-client dataframe latency. The VM
may have warm remote caches; this is not a cold-cache or throughput claim.
Check request scratch is empty and no OOM/restarts occurred before proceeding.

## Separate Arrow profiles

```sh
docker stop "headtail108-control-baseline-$EXP108_SUFFIX" "headtail108-control-candidate-$EXP108_SUFFIX"
start_exp108_pair profile
```

After both new instances are ready:

```sh
python sage-data-rework/tiered_system/experiments/exp108_head_tail_paired.py \
  --baseline "http://127.0.0.1:$EXP108_BASELINE_PORT/api/v1/query" \
  --candidate "http://127.0.0.1:$EXP108_CANDIDATE_PORT/api/v1/query" \
  --baseline-container "headtail108-profile-baseline-$EXP108_SUFFIX" \
  --candidate-container "headtail108-profile-candidate-$EXP108_SUFFIX" \
  --baseline-image "$EXP108_BASELINE_ID" --candidate-image "$EXP108_CANDIDATE_ID" \
  --source-manifest "$EXP108_ROOT/source/manifest.json" \
  --baseline-profiles "$EXP108_ROOT/profile-baseline" \
  --candidate-profiles "$EXP108_ROOT/profile-candidate" \
  --reference "$EXP108_REFERENCE" --control "$EXP108_ROOT/control/report.json" \
  --reps 1 --timeout 120 --run-dir "$EXP108_ROOT/diagnostics"
docker stop "headtail108-profile-baseline-$EXP108_SUFFIX" "headtail108-profile-candidate-$EXP108_SUFFIX"
```

Require **64 eligible timing/native profile pairs** (32 excluded warmups),
correct rows, unchanged resources/settings, no OOM/restarts and empty request
scratch. Check staging, native preparation/fetch, projection/window work,
spill and buffer peaks. Profiled latency is diagnostic, separate from the
primary medians. Worker timings can overlap; blocked-thread time does not by
itself explain wall latency. Buffer peaks are not process RSS; runtime
snapshots do not continuously sample memory or scratch.

Retain all containers, logs, source snapshots, profiles and bodies for review.
Put results in [plan 14](../../plans/14_head_tail_queries.md). We will assess
the day benefit, hour tail regression and large-N/fleet overhead together.
Adoption remains pending; no source checkout change, production deployment,
warehouse mutation, commit or push is part of this gate.
