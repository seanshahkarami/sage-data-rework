# Isolate the historical split head/tail stall

Saved runs cited below as `results/...` are paths inside the split archive in the parent directory (`../results.z01`, `../results.z02`, `../results.z03`, and `../results.zip`). See [recorded results](../README.md#recorded-results). A command that creates a new run directory still writes local untracked output.

> Archived comparison after raw-candidate adoption (2026-11-06). The live API
> has removed the superseded standalone selectors and diagnostic probes.
> Reproducing this experiment requires its retained pre-adoption source
> snapshot. Current regression commands are in
> [API validation](../../../beehive-data-api/docs/aggregation-validation.md#standalone-headtail-across-the-border).


The exp103 historical control did **not** complete. Its two cold-only responses
are excluded warmups, not medians. Split staging completed, then queries waited
until client cancellation. Keep the interrupted report, stopped containers and
retained scratch unchanged. Small-fixture correctness from exp102 still stands,
but historical split execution remains blocked.

The local production SQL completes over a 1,043,680-row synthetic UNION and a
pair of private Parquet files at that scale, with differing cold/hot row groups.
Those CLI tests check exact selected timestamps/values. They do not reproduce
native Go execution or the remote Iceberg path. Do not change SQL, memory limits
or I/O settings based only on these successes.

## First distinguish native Go execution from remote scanning

On the VM with the pinned dependencies, set the actual DuckDB 2.0 CLI path.
These tests create only private synthetic temporary Parquet files; they need
no credentials or network storage. They exercise range/parquet inputs through
both the native Arrow pending-streaming interface and `database/sql`.

```sh
export COLD_AGGREGATION_DUCKDB2="$(command -v duckdb)"
export EXP104_ROOT="$PWD/sage-data-rework/tiered_system/experiments/results/exp104-host-$(date +%s)"
mkdir -m 700 "$EXP104_ROOT"
go -C beehive-data-api test -tags=duckdb_arrow -count=1 -v \
  ./internal/backend/cold \
  -run '^(TestSpanningSelectionScale|TestSpanningSelectionParquetScale|TestSpanningSelectionNativeScale)$' \
  > "$EXP104_ROOT/native-scale.log" 2>&1
```

Each CLI/native request has a 20-second context deadline. Native cases cover
head over a generated range union and head/tail over local Parquet. Require no
skips and exactly one correct observation per case. If native local cases stall
while the CLI completes, that isolates an execution-interface difference
without involving Pelican. If both complete, continue to the remote probe.
Native scale tests have not yet run successfully in the local sandbox because
the pinned library/dependencies are unavailable; their VM results are pending.

## Capture the failing remote request's phase timings

Use the **same failing image**, not an unproven SQL change. Reuse the setup
helper `start_exp103_pair` from the [exp103 runbook](exp103_head_tail_performance.md),
with the existing `BENCH_ENV`, `BENCH_NETWORK`, and `PELICAN_BENCH_KEY`. Set fresh
names/mounts and unused loopback ports. This creates new dedicated profiled
instances; leave the previous failed-run containers and scratch untouched.

```sh
export EXP103_SUFFIX="$(date +%s)"
export EXP103_ROOT="$EXP104_ROOT/remote-probe"
export EXP103_IMAGE='beehive-data-api:head-tail-bench-1793972898'
export EXP103_COLD_PORT=18481
export EXP103_SPLIT_PORT=18482
mkdir -m 700 "$EXP103_ROOT"
start_exp103_pair profile
```

Check startup logs first. The only session-setting change from the failed
control is enabling profiles. Do not use the exp103 performance driver's
`--control`: its matching-completed-control requirement is correct, and the
interrupted control is not eligible. This is a bounded failure diagnostic,
not a benchmark phase or a latency estimate.

The hour case also stalled previously, despite much less hot input. Send it
once per placement with a 45-second deadline:

```sh
for role in cold split; do
  port="$EXP103_COLD_PORT"
  if [ "$role" = split ]; then port="$EXP103_SPLIT_PORT"; fi
  if curl --silent --show-error --max-time 45 \
    -H 'Content-Type: application/json' \
    -H 'Accept: application/vnd.apache.arrow.stream' \
    --data '{"start":"2025-03-01T11:30:00Z","end":"2025-03-01T12:30:00Z","filter":{"name":"wxt.env.temp","vsn":"W0A0"},"head":1}' \
    --dump-header "$EXP103_ROOT/$role.headers" \
    --output "$EXP103_ROOT/$role.arrow" \
    --write-out 'http=%{http_code} bytes=%{size_download} total=%{time_total}\n' \
    "http://127.0.0.1:$port/api/v1/query" \
    > "$EXP103_ROOT/$role.curl.txt" 2> "$EXP103_ROOT/$role.curl.err"; then
    printf '%s\n' 0 > "$EXP103_ROOT/$role.curl.exit"
  else
    printf '%s\n' "$?" > "$EXP103_ROOT/$role.curl.exit"
  fi
done
```

After each request has completed or canceled and the API has logged cleanup,
retain its profile sidecar even if status is failed/cancelled or no native JSON
profile exists. Inspect the reached phases before requiring an eligible
complete profile. Use this summary after both requests:

```sh
python - <<'PY'
import json, os
from pathlib import Path
root=Path(os.environ['EXP103_ROOT'])
reports=[]
for role in ('cold','split'):
    for path in (root/('profile-'+role)).rglob('*.timing.json'):
        report=json.loads(path.read_text())
        reports.append(dict(role=role,file=str(path),status=report['status'],
            rows=report.get('rows'),native_profile_present=report.get('duckdb_profile_present'),
            phases=report.get('phases',{})))
(root/'phase-summary.json').write_text(json.dumps(reports,indent=2)+'\n')
print(json.dumps(reports,indent=2))
PY
docker logs "headtail103-profile-cold-$EXP103_SUFFIX" > "$EXP103_ROOT/cold.log" 2>&1
docker logs "headtail103-profile-split-$EXP103_SUFFIX" > "$EXP103_ROOT/split.log" 2>&1
```

If no timing file appears after cancellation, retain that absence and logs as
evidence; do not call it a completed native profile. A missing phase can mean
its operation never returned: for example, `prepare_seconds` is recorded only
after prepare finishes, and `fetch_chunks_seconds` after fetch returns. A
`native_ready_since_backend_start_seconds` marker with no first batch narrows
the wait to subsequent fetch/execution. Phase values overlap and are not
additive wall time. Existing Arrow streaming-error logs already suggest the
Arrow query reached result iteration; the sidecar should make this explicit.

Stop only these new dedicated instances once the sidecars/logs are captured:

```sh
docker stop "headtail103-profile-cold-$EXP103_SUFFIX" "headtail103-profile-split-$EXP103_SUFFIX"
```

Do not remove retained Parquet files after uncertain native closure or rewrite
the original report's `running` status to `complete`. Further isolation should
follow the reached phase and local-native result: remote I/O, the specific
native execution interface, or the combined query plan are hypotheses, not
confirmed causes. Put results in [plan 14](../../plans/14_head_tail_queries.md).
No deployment, published warehouse updates or `/node-data` access is needed.

## Completed phase isolation and follow-up

The [exp104 results in plan 14](../../plans/14_head_tail_queries.md#stall-isolation-2026-11-06)
show native local cases complete, but the remote split query stalls in the
first fetch after a native result is ready. Staging and prepare/pending returned.
The failed native profile is empty, so it does not identify the blocking scan
or operator. Use [exp105](exp105_head_tail_plan_isolation.md) to separate the
remote cold scan, retained hot read, union and selection graph. The production
query and read-ahead default remain unchanged pending that evidence.
