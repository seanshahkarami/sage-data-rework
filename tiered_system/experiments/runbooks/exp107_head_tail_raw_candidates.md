# Raw candidate selection before metadata canonicalization

Saved runs cited below as `results/...` are paths inside the split archive in the parent directory (`../results.z01`, `../results.z02`, `../results.z03`, and `../results.zip`). See [recorded results](../README.md#recorded-results). A command that creates a new run directory still writes local untracked output.

> Archived comparison after raw-candidate adoption (2026-11-06). The live API
> has removed the superseded standalone selectors and diagnostic probes.
> Reproducing this experiment requires its retained pre-adoption source
> snapshot. Current regression commands are in
> [API validation](../../../beehive-data-api/docs/aggregation-validation.md#standalone-headtail-across-the-border).


The exp106 direct window is the **unchanged production default**. This opt-in
experiment targets its native projection cost, without touching hot staging,
aggregation, global I/O settings or the public response schema.

## Candidate and correctness argument

First select head/tail N within each raw tuple:
`(name, meta, plugin, vsn, node, host, task, job, zone)`.
Materialize that candidate set, then run the retained canonical direct window
and final per-series N. The first window does not merge/sort metadata maps.
The materialized CTE has one producer and one consumer, without the failed
dictionary graph's shared consumers and joins. Remote completion must still be
tested; a different CTE shape is not itself proof that the stall is avoided.

Every raw tuple deterministically maps to one canonical public series. If a
point is discarded by its raw tuple's top-N, that tuple already contains N
points at least as early/late, all in the same canonical series. The discarded
point cannot be necessary to the final top-N, subject to the API's existing
unspecified ordering among equal timestamps. The final window is essential:
raw aliases can merge, and simply returning every tuple's N would over-return.
Values are carried intact, with no numeric reduction or reassociation.

The materialization boundary keeps canonical map work after candidate pruning.
Large limits or many raw tuples may yield little reduction and add overhead;
this must be evaluated before adoption. Hot extraction/staging still reads all
hot observations. This experiment cannot remove the measured ~3.4s day staging
cost or establish full HTTP speedup.

## Local checks

The SQL-only parity test covers head/tail 0/1/2/10/1000, promoted overrides,
key order, hot merged aliases, null/empty maps, absent versus present-null tags,
and mixed uint64/int64/boolean/string/null/float values. Synthetic local Parquet
scale tests check the selected exact timestamp/value over 1,043,680 rows.

```sh
export COLD_AGGREGATION_DUCKDB2="$(command -v duckdb)"
(
  cd beehive-data-api
  go test -count=1 -v internal/backend/cold/sql.go \
    internal/backend/cold/aggregate_sql.go internal/backend/cold/aggregate_reduce.go \
    internal/backend/cold/selection_sql.go internal/backend/cold/selection_scale_test.go \
    internal/backend/cold/selection_reduce_probe_test.go
)
go -C beehive-data-api test -tags=duckdb_arrow -count=1 -v \
  ./internal/backend/cold -run '^TestSpanningSelectionNativeScale$'
```

The native scale gate now includes both retained and candidate plans through
Arrow and database/sql. It needs the pinned native library and CLI; a skipped
gate is not a pass. Local CLI runs were encouraging (~0.11s candidate versus
~2.8–3.0s retained on the synthetic Parquet head/tail queries), but these are
single synthetic executions, not historical or adoption medians.

## Retained historical input experiment in the VM

This uses the same **read-only warehouse**, current runtime image and actual
staged hot bytes from the prior runs. It does not read Influx or rerun staging.
Use the existing `BENCH_NETWORK` and absolute `PELICAN_BENCH_KEY`.

Choose the stopped exp104 hour file (21,912 rows, 197,586 bytes) and one stopped
exp103 day file (521,840 rows, 4,015,648 bytes). The experiment copies them to
its new private output mount and records hashes/row counts. Originals remain
read-only and unchanged. Never choose `/node-data`.

```sh
find "$PWD/sage-data-rework/tiered_system/experiments/results/exp104-host-1793974848/remote-probe/profile-split-scratch" \
  -type f -name hot.parquet -print
find "$PWD/sage-data-rework/tiered_system/experiments/results/exp103-host-1793972898/control-split-scratch" \
  -type f -name hot.parquet -print
export EXP107_HOUR='/absolute/path/to/exp104/hour/hot.parquet'
export EXP107_DAY='/absolute/path/to/exp103/day/hot.parquet'
export EXP107_SUFFIX="$(date +%s)"
export EXP107_ROOT="$PWD/sage-data-rework/tiered_system/experiments/results/exp107-host-$EXP107_SUFFIX"
export EXP107_RUNTIME='beehive-data-api:head-tail-direct-window-1793985281'
export EXP107_PLATFORM="$(docker image inspect --format '{{.Os}}/{{.Architecture}}' "$EXP107_RUNTIME")"
export EXP107_BUILDER="beehive-data-api:head-tail-raw-candidate-builder-$EXP107_SUFFIX"
export EXP107_REFERENCE="$PWD/sage-data-rework/tiered_system/experiments/results/exp106-host-1793985281/performance/control"
mkdir -m 700 "$EXP107_ROOT"
```

The completed exp106 control and cold NDJSON bodies are the output oracle.
Each trial checks the request shape, saved reference-body SHA256, and exact
selected timestamps, metadata and scalar JSON. Only the fixed day/hour
`wxt.env.temp` / `W0A0` queries are included here. The full fleet workload and
all API correctness gates remain necessary before adoption.

Compile using the existing platform-matched builder and unchanged pins:

```sh
docker build --platform "$EXP107_PLATFORM" --target builder -t "$EXP107_BUILDER" beehive-data-api \
  > "$EXP107_ROOT/build.log" 2>&1
docker run --platform "$EXP107_PLATFORM" --name "headtail107-compile-$EXP107_SUFFIX" \
  --mount "type=bind,src=$EXP107_ROOT,dst=/diagnostics" \
  "$EXP107_BUILDER" \
  go test -tags=duckdb_arrow -c -o /diagnostics/raw-candidate.test ./internal/backend/cold \
  > "$EXP107_ROOT/compile.log" 2>&1
```

Run the opt-in native experiment inside the retained exp106 runtime image:

```sh
if docker run --platform "$EXP107_PLATFORM" --name "headtail107-probe-$EXP107_SUFFIX" \
  --network "$BENCH_NETWORK" --entrypoint /diagnostics/raw-candidate.test \
  -e HEAD_TAIL_REDUCTION_OUTPUT=/diagnostics/native \
  -e HEAD_TAIL_REDUCTION_KEY=/run/secrets/pelican.pem \
  -e HEAD_TAIL_REDUCTION_HOUR=/retained/hour.parquet \
  -e HEAD_TAIL_REDUCTION_DAY=/retained/day.parquet \
  -e HEAD_TAIL_REDUCTION_REFERENCE=/reference/report.json \
  -e HEAD_TAIL_REDUCTION_REPS=3 \
  --mount "type=bind,src=$EXP107_ROOT,dst=/diagnostics" \
  --mount "type=bind,src=$PELICAN_BENCH_KEY,dst=/run/secrets/pelican.pem,readonly" \
  --mount "type=bind,src=$EXP107_HOUR,dst=/retained/hour.parquet,readonly" \
  --mount "type=bind,src=$EXP107_DAY,dst=/retained/day.parquet,readonly" \
  --mount "type=bind,src=$EXP107_REFERENCE,dst=/reference,readonly" \
  "$EXP107_RUNTIME" \
  -test.v -test.run '^TestHeadTailRawCandidateRemote$' -test.timeout=20m \
  > "$EXP107_ROOT/probe.log" 2>&1; then
  printf '%s\n' 0 > "$EXP107_ROOT/probe.exit"
else
  printf '%s\n' "$?" > "$EXP107_ROOT/probe.exit"
fi
```

Eight cases cover day/hour × head/tail × N=1/1000. Both plans get one excluded
warmup and three measured repetitions, alternating order: **64 profiled native
trials**. Each trial uses a fresh production backend with its SQL/Arrow engines,
two CPU threads, 3 GB and default async/read-ahead settings. Each query has a
45-second deadline after backend startup; a stall or mismatch stops the run
promptly with a failed incremental report and retained evidence. Tokens are
refreshed per trial, with one pinned metadata object across the run.

These timings exclude source-hint resolution, Influx extraction and staging.
Trial elapsed includes backend construction, Arrow execution and Go decoding of
the selected rows; sidecar lifetime starts after SQL backend startup. Both are
profiled diagnostics. External caches are uncontrolled and no HTTP, cold-cache
or concurrent-throughput claim is made. Output cardinality is identical, so
Go result decoding is common to the pair. No secret setup SQL/token is saved.

## Summarize and decide

Only summarize a complete run with all output parity and native profiles:

```sh
python sage-data-rework/tiered_system/experiments/runbooks/exp107_head_tail_candidate_summary.py \
  --report "$EXP107_ROOT/native/report.json" \
  --output "$EXP107_ROOT/native/analysis.json"
```

The analyzer excludes warmups, requires every case/plan/repetition and eligible
DuckDB 2.0 profiles, and reports native phase/CPU/projection/spill medians.
Inspect the per-case operator lists and raw native JSON: canonical map work
should occur on the reduced set, while raw grouping and materialization add
work of their own. Compare total native lifetime as well as fetch; moving work
into pending/materialization is not a speedup. Do not sum operator CPU and
elapsed phases. Buffer counters are not process RSS.

If the experiment fails, retain its failed report/logs and do not report medians
from the partial run. If it passes and improves the intended cases, the next
step is a candidate integration and **full two-format/fleet HTTP control**,
including large-limit cases with little pruning. The retained direct plan
remains the API default until that separate decision/gate. Keep all artifacts
and stopped compile/probe containers; no storage cleanup, deployment, commit
or push is part of this experiment. Put results in [plan 14](../../plans/14_head_tail_queries.md).
