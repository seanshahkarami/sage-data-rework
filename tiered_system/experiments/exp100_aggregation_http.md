# Integrated reduction: HTTP and reused-engine validation (exp100)

The workspace API now reduces aggregates without head/tail before buffering.
Queries with head/tail retain the dictionary path so observations are selected
before aggregation and empty-window membership remains correct. This is
selection by query semantics, not an operator fallback flag. The 3 GB engine
budget, backend mutex, staging, warehouse and public schema are unchanged.
Deployment has not been updated by this work.

`aggregateValueSQL` is shared by both executors. The integrated reduction
builder accepts the raw source directly; it no longer extracts production
logic from experimental SQL strings. Numeric edge tests retain a real
independent comparison through the live head selection path, selecting every
fixture observation. The superseded exp98/99 probes and adapters were removed.
The main compiler/metadata oracle exercises the
actual dispatch, including all head/tail cases; redundant experimental oracle
wrappers were removed.

## Host correctness gate

Reuse the existing exp91 fixture variables (`COLD_AGGREGATION_MANIFEST`,
`COLD_AGGREGATION_DUCKDB2`), verified 36-row seed report, and its loopback
`INFLUX_TOKEN`. From the workspace root:

```sh
export EXP100_ROOT="$PWD/sage-data-rework/tiered_system/experiments/results/exp100-host-$(date +%s)"
mkdir -m 700 "$EXP100_ROOT"
AGGREGATION_PARITY_PROFILE=1 CGO_ENABLED=1 \
  go -C beehive-data-api test -tags=duckdb_arrow -count=1 -v ./internal/backend/cold \
  -run '^(TestAggregateCompilerOracle|TestAggregateMetadataDictionary|TestAggregateNumericAndTypeSemantics|TestAggregateNullKeyPresence|TestReducedAggregateRejectsObservationLimits|TestAggregationProductionHTTP|TestHotAggregateStage|TestColdQueryProfile)' \
  > "$EXP100_ROOT/correctness.log" 2>&1
CGO_ENABLED=1 go -C beehive-data-api test -tags=duckdb_arrow ./...
CGO_ENABLED=1 go -C beehive-data-api vet -tags=duckdb_arrow ./...
go -C beehive-data-api test -race ./internal/backend/tiered
```

Export the actual synthetic-hot HTTP bodies with a fresh
`AGGREGATION_HTTP_OUTPUT_DIR` if replaying all 364 through the client. Then run
the live three-tier gate, reusing the retained verified seed rather than
seeding again:

```sh
export AGGREGATION_PARITY_SEED_REPORT="$PWD/sage-data-rework/tiered_system/experiments/results/parity-seed-1793895351/report.json"
export EXP100_LIVE="$EXP100_ROOT/parity-live-http"
AGGREGATION_PARITY_PROFILE=1 AGGREGATION_HTTP_OUTPUT_DIR="$EXP100_LIVE" \
  CGO_ENABLED=1 go -C beehive-data-api test -tags=duckdb_arrow -count=1 -v \
  ./internal/backend/cold -run '^TestAggregationLiveTierParity$' \
  > "$EXP100_ROOT/live.log" 2>&1
python sage-data-rework/tiered_system/experiments/exp90_aggregation_client.py \
  --manifest "$EXP100_LIVE/client-manifest.json" --responses "$EXP100_LIVE" \
  --output "$EXP100_LIVE/client-report.json"
```

Expect **546/546** client checks, exact integer/nanosecond fidelity, complete
profiles, and existing staging/read-grant cleanup checks. Paths must match the
prepared fixtures on this host. The pinned native Go library is unavailable
in the local sandbox, so these runtime gates remain host work; CLI compiler
parity does not replace them.

## Paired dedicated APIs

Use two dedicated APIs at a time: retained **dictionary image**
`beehive-data-api:aggregation-meta-1793902541`, and a fresh image built from this
checkout. The driver verifies the baseline digest against exp96, requires a
different candidate image and matching resource limits/execution settings.
It performs only queries and Docker inspection; it never configures or
restarts containers itself.

Supply the existing private `BENCH_ENV` file from exp95/96 with the historical
Influx URL/token and `INFLUXDB_BUCKET=exp95-hot`; supply `BENCH_NETWORK` and the
absolute `PELICAN_BENCH_KEY` path. Leave loader coordination connection settings
out of this static configuration. No keys/tokens belong in result reports.

```sh
export EXP100_IMAGE="beehive-data-api:aggregation-reduce-$(date +%s)"
export EXP100_SUFFIX="$(date +%s)"
docker build -t "$EXP100_IMAGE" beehive-data-api
export EXP100_PLACEMENT=cold
export EXP100_BASE_PORT=18481
export EXP100_CANDIDATE_PORT=18482
```

Run this block for the selected placement (cold first, split later):

```sh
export EXP100_BORDER='2025-05-01T00:00:00Z'
if [ "$EXP100_PLACEMENT" = split ]; then export EXP100_BORDER='2025-03-01T12:00:00Z'; fi
export EXP100_BASE_CONTAINER="agg100-$EXP100_PLACEMENT-base-$EXP100_SUFFIX"
export EXP100_CANDIDATE_CONTAINER="agg100-$EXP100_PLACEMENT-new-$EXP100_SUFFIX"
for layout in base new; do
  image='beehive-data-api:aggregation-meta-1793902541'
  port="$EXP100_BASE_PORT"
  container="$EXP100_BASE_CONTAINER"
  if [ "$layout" = new ]; then image="$EXP100_IMAGE"; port="$EXP100_CANDIDATE_PORT"; container="$EXP100_CANDIDATE_CONTAINER"; fi
  mkdir -m 700 "$EXP100_ROOT/$EXP100_PLACEMENT-$layout-scratch"
  docker run -d --name "$container" --network "$BENCH_NETWORK" \
    -p "127.0.0.1:$port:10000" --env-file "$BENCH_ENV" \
    -e COLD_FRONTIER=static -e "COLD_BORDER=$EXP100_BORDER" \
    -e COLD_WAREHOUSE='https://nrdstor.nationalresearchplatform.org:8443/sage/cold-storage-demo-v3-bounded-full-1793018629285065000/warehouse' \
    -e COLD_ASYNC_THREADS=8 -e COLD_READ_AHEAD_DEPTH=-1 \
    -e ARROW_COMPRESSION=zstd -e COLD_PROFILE_DIR= \
    -e COLD_AGGREGATE_STAGING_DIR=/scratch \
    -e COLD_AGGREGATE_MAX_STAGING_BYTES=17179869184 \
    -e PELICAN_KEY_PATH=/run/secrets/pelican.pem \
    --mount "type=bind,src=$PELICAN_BENCH_KEY,dst=/run/secrets/pelican.pem,readonly" \
    --mount "type=bind,src=$EXP100_ROOT/$EXP100_PLACEMENT-$layout-scratch,dst=/scratch" \
    "$image"
done
```

Check startup logs before measurement. These examples have no extra CPU/memory
limits; any limits must match between the pair and be recorded by the report.
Keep other benchmark clients idle. Do not run the cold and split comparisons
at the same time, or direct unrelated traffic to these APIs.

## Unprofiled mixed/repeated HTTP sequence

```sh
python sage-data-rework/tiered_system/experiments/exp100_aggregation_http.py \
  --baseline "http://127.0.0.1:$EXP100_BASE_PORT/api/v1/query" \
  --candidate "http://127.0.0.1:$EXP100_CANDIDATE_PORT/api/v1/query" \
  --baseline-container "$EXP100_BASE_CONTAINER" \
  --candidate-container "$EXP100_CANDIDATE_CONTAINER" \
  --reference sage-data-rework/tiered_system/experiments/results/exp96-bench-1793902541/control/report.json \
  --placement "$EXP100_PLACEMENT" --run-dir "$EXP100_ROOT/$EXP100_PLACEMENT-control"
```

Cold mixed sequence:

1. Full March mean and an immediate identical repeat.
2. Overlapping March 8–29 mean, retaining 1h windows.
3. All five daily functions, hourly series mean, fleet minute count, all-node
   temperature hour mean, then daily head/tail mean.
4. Return to the original full March mean.

The split phase uses the ten short/day/limit queries only: the retained hot
bucket covers March 1 windows, not a full month after the noon border. It
cannot honestly measure a month-long split without different data. Every split
query crosses the border. Cold-only monthly tests and split day tests together
cover the intended integrated routes without seeding or warehouse changes.

A full sequence runs on one backend before switching to the other; backend
order alternates per repetition. One excluded whole-sequence warmup and three
measured repetitions are default. Both Arrow and NDJSON are measured; their
native engines are distinct and persist within each API, while the mutex still
serializes them. `month_start` names a position, not a cold-cache state. Neither
engine nor OS/Pelican caches are reset. This measures reused-engine behavior.

Every saved response is decoded outside the receive timer and checked against
the old-image baseline, including limits and overlap queries. The first
baseline warmup establishes a reference per exact query; all later baseline
and candidate responses must match it across formats. Reference/binary-image
configuration, response hashes, queries and order are retained. Runtime drift,
restarts, OOM events, empty results or parity failures stop the run and retain
a failed report. Endpoint URLs must be loopback.

Default cold run: **224 HTTP responses**, including 64 monthly responses across
images/formats/warmups. Allow substantial time. Use `--reps 1` for an initial
smoke, `--workload short` to omit monthly ranges, or `--format arrow` to select
one format; keep the chosen scope explicit in the results. Default split run:
**160 responses**. Wire parsing/spooling is timed; semantic validation and
container inspection are outside the receive timer.

After the cold run, stop only the two dedicated containers just created to
release their cached engines; preserve their logs and mounted outputs:

```sh
docker stop "$EXP100_BASE_CONTAINER" "$EXP100_CANDIDATE_CONTAINER"
export EXP100_PLACEMENT=split
```

Then rerun the startup block and comparison command with split placement. The
same loopback ports are free after stopping those two containers. No containers
or datasets are removed by this procedure. The adopted warehouse remains
read-only and the historical Influx bucket is reused.

## Optional separate Arrow diagnostics

Create a dedicated profiled pair of the same image digests and matching
resource/settings for a placement, with separate host profile mounts and
`COLD_PROFILE_DIR=/profiles`. Use fresh container names. Add these arguments to
the benchmark command, retaining the same workload:

```
--baseline-profiles /absolute/isolated/base-profile-mount
--candidate-profiles /absolute/isolated/new-profile-mount
--control /absolute/placement-control/report.json
--reps 1
```

Diagnostics run Arrow only and pair one fresh complete native profile per
response. They must match the unprofiled control's values, images, settings and
limits. Inspect scan/read metrics and spills around monthly repeats/overlap and
returns. These are not primary latency numbers, and byte counters alone do not
prove Pelican traffic. NDJSON remains unprofiled.

## Adoption gate

Require the synthetic/live HTTP correctness gates, then inspect both formats
and placements: warm monthly benefit, mixed-range reuse, small-query cost,
head/tail parity, and staging/resource cleanup. Keep resource and latency
conclusions separate. This integration is local code, not deployment; no
operator fallback flag was added. Do not claim all queries became faster.
