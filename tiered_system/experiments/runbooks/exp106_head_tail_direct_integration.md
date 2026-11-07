# Validate the direct standalone head/tail plan

Saved runs cited below as `results/...` are paths inside the split archive in the parent directory (`../results.z01`, `../results.z02`, `../results.z03`, and `../results.zip`). See [recorded results](../README.md#recorded-results). A command that creates a new run directory still writes local untracked output.

> Archived comparison after raw-candidate adoption (2026-11-06). The live API
> has removed the superseded standalone selectors and diagnostic probes.
> Reproducing this experiment requires its retained pre-adoption source
> snapshot. Current regression commands are in
> [API validation](../../../beehive-data-api/docs/aggregation-validation.md#standalone-headtail-across-the-border).


Exp105 isolated the failing remote dictionary/CTE selection: source scans,
raw union and direct window completed correctly, while dictionary selection
stalled with read-ahead enabled and disabled. The API's standalone crossing
selector now uses the direct window. It still stages hot data with the retained
writer and shares source filters, scalar restoration and ownership with
aggregation. Aggregation SQL and all global I/O settings are unchanged.

Canonical metadata is now merged/sorted per observation before `row_number()`
partitions by `(name, meta)`. This trades the dictionary optimization for the
plan that completed on remote data. It is not an HTTP speedup claim, and its CPU
cost needs measurement. No runtime fallback/selector was added. The failing
pre-fix SQL remains only in the opt-in exp105 diagnostic tests for reproduction;
it can be retired after the integration gates pass.

## Full correctness first

Reuse the working exp102/104 fixture environment on the VM:
`COLD_AGGREGATION_MANIFEST`, `COLD_AGGREGATION_DUCKDB2`, the verified
`AGGREGATION_PARITY_SEED_REPORT` and its `INFLUX_TOKEN`. Do not reseed or write
any warehouse. From the workspace root:

```sh
export EXP106_SUFFIX="$(date +%s)"
export EXP106_ROOT="$PWD/sage-data-rework/tiered_system/experiments/results/exp106-host-$EXP106_SUFFIX"
export HEAD_TAIL_VALIDATED_REPORT="$PWD/sage-data-rework/tiered_system/experiments/results/exp105-host-1793984157/native/report.json"
mkdir -m 700 "$EXP106_ROOT"
AGGREGATION_PARITY_PROFILE=1 AGGREGATION_HTTP_OUTPUT_DIR="$EXP106_ROOT/http" \
  go -C beehive-data-api test -tags=duckdb_arrow -count=1 -v \
  ./internal/backend/cold -run '^TestHeadTailProductionHTTP$' \
  > "$EXP106_ROOT/http.log" 2>&1
AGGREGATION_PARITY_PROFILE=1 AGGREGATION_HTTP_OUTPUT_DIR="$EXP106_ROOT/live-http" \
  go -C beehive-data-api test -tags=duckdb_arrow -count=1 -v \
  ./internal/backend/cold -run '^TestHeadTailLiveTierParity$' \
  > "$EXP106_ROOT/live.log" 2>&1
AGGREGATION_HTTP_OUTPUT_DIR= HEAD_TAIL_ISOLATION_OUTPUT= \
  go -C beehive-data-api test -tags=duckdb_arrow -count=1 ./... \
  > "$EXP106_ROOT/suite.log" 2>&1
go -C beehive-data-api vet -tags=duckdb_arrow ./... \
  > "$EXP106_ROOT/vet.log" 2>&1
go -C beehive-data-api test -race -count=1 ./internal/backend/tiered \
  > "$EXP106_ROOT/race.log" 2>&1
```

Require all **512 synthetic** and **768 live** HTTP cases to pass without
fixture skips, including head/tail 0/1/2/1000, booleans/strings, exact integers,
nanoseconds, canonical series identity, grant release after native closure and
scratch cleanup. The opt-in remote isolation test is intentionally disabled in
the normal suite; its old dictionary cases are expected to fail and are not
production fallbacks. Other optional test skips must not be mistaken for
successful fixture/live gates.

`TestSelectionCapturedDirectPlan` renders the production query using the exp105
snapshot/input path and verifies it exactly matches the saved successful SQL.
It performs no remote I/O. The suite also reruns native local scale and numeric
aggregation regressions. Preserve failed evidence if any gate does not pass.

## Fresh current-image historical control

Build a new API image; running the retained exp103 image would still execute
the old stalled plan. Reuse the dedicated setup helper `start_exp103_pair`
from the [exp103 runbook](exp103_head_tail_performance.md), with the same
`BENCH_ENV`, `BENCH_NETWORK` and `PELICAN_BENCH_KEY`. The reference remains the
completed exp101 split report, which pins historical input/settings, not an old
image for these paired placements.

```sh
export EXP103_SUFFIX="$EXP106_SUFFIX"
export EXP103_ROOT="$EXP106_ROOT/performance"
export EXP103_IMAGE="beehive-data-api:head-tail-direct-window-$EXP106_SUFFIX"
export EXP103_REFERENCE="$PWD/sage-data-rework/tiered_system/experiments/results/exp101-host-1793968135/split-control/report.json"
export EXP103_COLD_PORT=18481
export EXP103_SPLIT_PORT=18482
mkdir -m 700 "$EXP103_ROOT"
docker build -t "$EXP103_IMAGE" beehive-data-api \
  > "$EXP106_ROOT/build.log" 2>&1
start_exp103_pair control
```

After startup checks, run the unprofiled two-format control. Use a 120-second
per-request client timeout to retain a failed report promptly if the stall
recurs; this is a benchmark diagnostic bound, not a new API default.

```sh
python sage-data-rework/tiered_system/experiments/runbooks/exp103_head_tail_performance.py \
  --cold "http://127.0.0.1:$EXP103_COLD_PORT/api/v1/query" \
  --split "http://127.0.0.1:$EXP103_SPLIT_PORT/api/v1/query" \
  --cold-container "headtail103-control-cold-$EXP103_SUFFIX" \
  --split-container "headtail103-control-split-$EXP103_SUFFIX" \
  --reference "$EXP103_REFERENCE" --reps 3 --timeout 120 \
  --run-dir "$EXP103_ROOT/control"
```

Require **256 responses**, completed status, body parity, no restarts/OOM and
empty request scratch after completion. A first response is a useful smoke
check, but does not complete the control. Single-case exp105 timings and the
old exp103 warmups are not comparison medians. Keep output of this new run
separate from all prior artifacts.

## Separate Arrow diagnostics after control completes

```sh
docker stop "headtail103-control-cold-$EXP103_SUFFIX" "headtail103-control-split-$EXP103_SUFFIX"
start_exp103_pair profile
```

After startup checks:

```sh
python sage-data-rework/tiered_system/experiments/runbooks/exp103_head_tail_performance.py \
  --cold "http://127.0.0.1:$EXP103_COLD_PORT/api/v1/query" \
  --split "http://127.0.0.1:$EXP103_SPLIT_PORT/api/v1/query" \
  --cold-container "headtail103-profile-cold-$EXP103_SUFFIX" \
  --split-container "headtail103-profile-split-$EXP103_SUFFIX" \
  --cold-profiles "$EXP103_ROOT/profile-cold" \
  --split-profiles "$EXP103_ROOT/profile-split" \
  --reference "$EXP103_REFERENCE" --reps 1 --timeout 120 \
  --control "$EXP103_ROOT/control/report.json" \
  --run-dir "$EXP103_ROOT/diagnostics"
```

Require **64 complete profile pairs** and response parity. Inspect direct
metadata/window/sort work, staging time, first result/full response, memory and
spill. Profile operator times can overlap and are not additive wall time.
Diagnostic/profiled response latency is separate from primary control medians.
Sampled resource peaks are not engine-only costs or continuous process peaks;
retain the same sampling caveats as exp103.

Stop only these dedicated profile instances after collecting outputs:

```sh
docker stop "headtail103-profile-cold-$EXP103_SUFFIX" "headtail103-profile-split-$EXP103_SUFFIX"
```

Keep stopped containers, outputs, original failed scratch and all reports for
review. Put results in [plan 14](../../plans/14_head_tail_queries.md). No
production deployment, published warehouse update, `/node-data` access, commit
or push is part of this gate.

## Completed outcome

The [exp106 results and review](../../plans/14_head_tail_queries.md#review-direct-selection-retained-after-exp106-2026-11-06)
pass 512 synthetic / 768 live wire cases, suite/vet/race, all 256 historical
control responses and all 64 eligible diagnostic responses. The first-fetch
stall did not recur. Artifact replay verified all 320 historical bodies and
1,280 fixture bodies. The direct plan is retained; the failed dictionary is
not an API option. Day split queries are roughly 7s and hour split queries
roughly 0.85s on this workload; staging and native projection remain costs.
These commands remain regression gates. No deployment or universal performance
claim follows from this run.
