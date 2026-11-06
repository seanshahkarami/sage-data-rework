# Head/Tail queries on the tiered backend

Current status: the raw-candidate standalone head/tail selector validated by
exp108 is now the workspace default for cross-border Arrow and NDJSON queries.
The superseded direct selector and standalone dictionary/comparison probes
have been removed. Aggregation's separately validated dictionary remains.
Adoption details and validation limits are recorded at the end of this plan.
No production deployment, commit or push has been performed.
Cross-border orchestration now lives in `internal/backend/tiered`; the cold
backend supplies its own raw scan and engine through `query.PlannedSourceBackend`.

Further head/tail performance work is deferred. Retain the adopted selector
and its documented fleet-minute N=1000 tradeoff; the current implementation
is sufficient for now. See the deferred optimization section below.

## Standalone head/tail across hot and cold (2026-11-06)

Implemented standalone crossing head/tail through a dedicated
`query.HeadTailBackend` capability on the cold backend. The tiered router sends
the complete request to one executor, rather than applying two independent
limits. Relative bounds and an omitted end use one request clock, and caller
queries remain unchanged. Hot-only/cold-only paths and aggregation dispatch
retain their behavior.

The combined executor shares the retained direct Parquet staging and raw-union
builder with aggregation. Cold rows are strictly before the pinned nanosecond
border; staged hot rows start at it. Distinct raw tag tuples receive canonical
merged/sorted metadata and common series IDs, so key order and promoted-tag
collisions do not split a public series. DuckDB applies one row-number window
per series, in ascending time for head or descending time for tail, then emits
selected original measurements in ascending timestamp order. No arithmetic,
empty-window padding or mixed-numeric-kind rejection is applied. Value/name/
metadata/timestamp wire fields remain unchanged in Arrow and NDJSON. Equal-time
tie ordering remains unspecified, as in the existing API.

The shared lifecycle validates the managed single `value`-field input, source
bucket, finite supported scalar values, exclusive end and int64 nanosecond
range. Both limits, negative limits and a standalone window are invalid. Zero
returns no rows. Hot input is staged before selection/streaming, even for a
small limit; existing staging-byte/directory settings and JSON-equivalent input
charges apply. A Go-side series map is not introduced; the native dictionary,
ranking and sort use existing DuckDB memory/spill settings. Source closure,
lease renewal, pinned metadata, scratch ownership and cleanup use the same
mechanisms as spanning aggregation. Existing `aggregate_*` staging profile keys
are retained for compatibility and also describe standalone crossing limits.
No performance improvement is claimed.

Validation added: an independent Go per-series observation oracle checks eight
limit/direction combinations against the production selection SQL using DuckDB
2.0, including empty/null metadata, tag-order/promoted collisions, uint64 max,
int64 min, booleans, strings and null values. A private Parquet-union fixture
checks that overlapping cold rows at/after the border cannot duplicate or
replace the hot observation. Input validation and a single frozen-bound routing
executor are tested. These checks, the existing 40 numeric/type cases and
null-key/limit guards pass locally; the tiered race suite passes. The full native
Go suite is blocked locally by missing pinned dependencies and native library,
not recorded as passing.

Added `TestHeadTailProductionHTTP` and `TestHeadTailLiveTierParity` to the real
API fixture gate. They derive limits 0/1/2/1000 from original fixture rows and
verify both encoders, dynamic grant release after native/staging closure and
cleanup. Live tests reuse the verified retained seed; no reseeding is needed.
The [exp102 runbook](../tiered_system/experiments/exp102_spanning_head_tail.md)
provides the host commands. HTTP/live execution remains pending on the VM. Local
[validation logs](../tiered_system/experiments/results/spanning-head-tail-20261106/summary.json)
record the passing compiler/vet/race gates and blocked native suite.
Published storage, deployment, commits and pushes were not changed.

## Standalone crossing head/tail host gates (2026-11-06)

The compiler file list passed against DuckDB CLI `v2.0.0`:
`TestSelectionValidation`, `TestSpanningSelectionSQL`, and
`TestSpanningSelectionRawUnion`, in 0.631s. The tiered race package passed
in 1.953s, including the single-executor routing check. No fixture case was
skipped.

[Synthetic HTTP](../tiered_system/experiments/results/exp102-host-1793971895/http.log)
passed in 4.385s with **512/512** wire subtests and no skips. The retained
exp91 manifest supplied the rows. Limits 0, 1, 2, and 1000 were checked in
both Arrow and NDJSON, and the test released each dynamic read grant after
native and staging closure.
[Live parity](../tiered_system/experiments/results/exp102-host-1793971895/live.log)
passed in 6.043s with **768/768** subtests and no skips. It reused the
verified 36-row seed and compared hot-only, cold-only, and split results
with the same observation oracle. Nothing was reseeded. Exported bodies are
under `http/` and `live-http/` in that result directory, each with its
client manifest. The old aggregation numeric client comparator was not used
for string or boolean parity; the HTTP gate checks those scalars directly.

The tagged
[suite](../tiered_system/experiments/results/exp102-host-1793971895/suite.log)
exited 0, with the cold package `ok` in 36.451s while the fixture and seed
environment were still set. Vet exited 0. A second tiered race run passed in
1.972s. No HTTP performance gain is claimed: every hot input row is staged
before selection streams. No warehouse writes, commits, pushes, or deployment
were made.

## Review: crossing head/tail validation complete (2026-11-06)

Verified the uploaded logs directly: all **512 synthetic HTTP** and **768 live
parity** wire subtests pass, with no failures or skipped fixture cases. Both
exports contain every expected Arrow/NDJSON body (256 and 384 request cases,
respectively). The tagged suite and tiered race checks pass; vet reports no
findings. The live gate covers hot-only, cold-only and split placements with
one shared independent observation oracle. The production-path tests also
assert release of each dynamic read grant only after native closure and
request scratch cleanup.

Independently decoded all **1,280 exported responses** with Python JSON and
PyArrow readers. Compared every row against the exported observation oracle:
row counts, name/metadata, exact nanosecond instants, integers without float
narrowing, boolean/string types, and numeric values pass. Arrow retains the
public four-column schema and nanosecond timestamps. Cold and split output is
ordered by timestamp. This independent wire replay is not a claim that the old
aggregation-only numeric client comparator validates strings or booleans.

Standalone head/tail across hot and cold is now implemented and host-validated,
alongside the existing hot-only/cold-only support. Limits apply once per series
over the combined observations, without an aggregation function. Zero returns
no rows; oversized limits return the available observations. This closes the
previous cross-border feature gap. Keep the staged-input budget and complete
hot-read cost documented: small output limits do not imply a cheap scan. No
new latency improvement, performance benchmark or production deployment is
claimed. Updated API validation documentation; no runtime code, published
storage, commits or pushes changed during this review.

## Historical standalone head/tail benchmark and profiles prepared (2026-11-06)

Added [exp103](../tiered_system/experiments/exp103_head_tail_performance.md). No historical API benchmark has been executed from this workspace;
there are no new latency numbers or bottleneck conclusions yet.

The driver compares cold-only and noon-split placement on the same newly built
API image, adopted immutable warehouse and retained `exp95-hot` input/settings.
It pins the input configuration to the completed exp101 split control and uses
separate private scratch/profile mounts. Sixteen cases cover head/tail limits
1/1000 on series day/hour, fleet minute and temperature-fleet hour ranges.
This is a placement benchmark of the current implementation, not an old/new
comparison or a comparison with windowed aggregate output sizes.

The default unprofiled Arrow/NDJSON control is 256 responses (one excluded
warmup plus three measured repetitions per case/placement/format), rotating
order while reusing engines. The separate Arrow diagnostic phase is 64 responses
(one excluded warmup and one measured repetition), requiring the same control
image/resources/settings/queries and hashed control bodies. All responses must
match cold-only reference rows with exact identities/scalar types and integer
values, float tolerance only for floats, exclusive bounds and per-series counts.
The existing exp102 small-fixture oracle remains the independent correctness
proof; this historical baseline comparison supplements it.

Diagnostics retain hot rows, JSON-equivalent input charge, Parquet output,
staging/prepare/fetch phases and native dictionary/window/sort operator data,
buffer peaks and spill. These are existing phase/DuckDB profiles, not Go CPU
or allocation profiles. Read/stage includes source iteration/CSV decoding and
Parquet work. Operator work is not additive wall time; engine byte counters
are not Pelican network measurements. Before/after runtime snapshots detect
OOM/restarts, not continuous process/scratch peaks; optional host sampling is
documented. No cold-cache or concurrent-throughput claim is made.

Six new driver tests cover unaggregated workloads, exact scalar types,
per-series/range validation, retained configuration, warmup/order rotation,
control-to-profile verification and failed-report/body retention. All **101
maintained Python tests pass**, including these driver checks. The runbook's
shell blocks pass syntax checks. [Harness validation](../tiered_system/experiments/results/head-tail-benchmark-harness-20261106/summary.json)
records preparation only. Runtime implementation, warehouse data, deployment,
commits and pushes are unchanged. VM execution/results remain pending.

### Historical standalone head/tail placement run (2026-11-06)

The unprofiled control was started and then stopped. Split head/tail produced
no response body, so the 256-response control and the 64-response profiled
Arrow diagnostic were not completed. There are no placement medians and no
split-versus-cold parity result.

Image `beehive-data-api:head-tail-bench-1793972898`, manifest digest
`sha256:462f649e70d25d02fe20182f697468e0b4438053a2fc031b116612d43b250c11`.
Both containers used the exp101 candidate settings, the adopted read-only
warehouse, and retained `exp95-hot`. Cold-only border
`2025-05-01T00:00:00Z`; split border `2025-03-01T12:00:00Z`. DuckDB reported
`v2.0.0` with two threads and a 3 GB limit. Evidence is
[exp103-host-1793972898](../tiered_system/experiments/results/exp103-host-1793972898).

Cold-only series-day head 1 (`wxt.env.temp`, `W0A0`, 2025-03-01) returned 1
row: Arrow 7.041s, NDJSON 6.581s. Those two responses are the only rows in
`control/report.json`. The report status is still `running` because the
driver process was killed before its failure handler could rewrite it.
Cold-only series-hour head 1 (11:30–12:30) returned HTTP 200, 346 bytes, 1
record, in 0.778s.

The same day query on the noon-split API wrote `hot.parquet` (4,015,648
bytes, 521,840 rows, timestamps from the noon border through the last point
before 2025-03-02T00:00:00Z) within a few seconds, then returned nothing.
After that write, the API process used about 0.02s of CPU over a 15s sample,
`read_bytes` stayed 0, and the connection to the warehouse host stayed
established with empty queues. The Arrow call was canceled after about 7.4
minutes (`failed to close results`, `INTERRUPT Error: Interrupted!`). A
following NDJSON call was canceled after 120s inside `Query` (`failed to
query backend`). The split hour head 1 probe also returned 0 bytes in 90s
and was canceled inside `Query`. Cancel unblocked the API; this was not the
earlier token-expiry failure, and host memory was available.

`stagedResults.Close` removes the staging directory only when the native
result close succeeds. The two canceled Arrow day calls left identical
`hot.parquet` files under `control-split-scratch`. Cold scratch is empty.
Containers `headtail103-control-cold-1793972898` and
`headtail103-control-split-1793972898` were stopped and not removed. Profile
containers were never started. No deployment, commit, or push.

## Review of the interrupted historical run (2026-11-06)

Checked `control/report.json` and both API logs. The report has exactly two
results, both `warmup=true`, and remains `running` because the process was
killed. Independently decoded both cold bodies: each contains the same original
observation at **1740787200026403194 ns**, value **11.7**, with equal metadata.
The 7.041s Arrow / 6.581s NDJSON values are individual first-query observations,
not measured medians or a format-speed conclusion. No split response exists
for historical parity. The 256-response control and 64-response diagnostic
phases did not finish. Historical performance validation is **not complete**.

The saved logs distinguish Arrow iteration cancellation/close errors from
NDJSON backend-query cancellation. This suggests the Arrow path had returned
a result before waiting during iteration, whereas the row path waited inside
`Query`. Both stall after successful direct staging, so the observations do
not support treating this as slow CSV/Parquet output. Almost-idle CPU, small
resident demand and reported available host memory argue against CPU or host
RAM exhaustion; they do not distinguish an engine dependency wait, asynchronous
I/O wait or an execution-interface problem. The remote connection alone does
not prove that Pelican is the cause. No root cause is confirmed yet.

Added private scale regressions using the production selection compiler:
1,043,680 rows from a synthetic union and a cold/hot Parquet pair with different
row-group sizes. Local DuckDB 2.0 CLI runs complete in under a second per query,
selecting exactly the expected nanosecond instant and value for head/tail.
This rules out row count alone as a sufficient local reproducer; it does not
validate the native Go interfaces or real remote Iceberg scan. Added a separate
`TestSpanningSelectionNativeScale` for Arrow pending-streaming and database/sql
on the same private inputs, with bounded cancellation. Its pinned-library VM
execution is pending. No runtime query rewrite or resource-setting change was
made without reproducing the blocker.

The next gate is [exp104 stall isolation](../tiered_system/experiments/exp104_head_tail_stall.md):
run native local scale tests, then reproduce the short hour query against new
dedicated instances of the exact failing image with profiles enabled and a
45-second client deadline. Preserve failed/cancelled timing sidecars rather
than requiring an eligible complete benchmark profile. Reached setup/prepare/
execute/fetch phases should identify the next isolation step. The interrupted
control must not be supplied as a successful profile control or overwritten.
Retained scratch, stopped containers and existing warehouse objects remain
untouched. Updated moved-plan links and API validation status. No deployment,
commit or push was made.

Local [stall-review validation](../tiered_system/experiments/results/head-tail-stall-review-20261106/summary.json)
records passing selection/scale/numeric CLI tests and runbook shell syntax.
The native Go scale gate is blocked by unavailable pinned dependencies locally,
so no successful native reproduction or fix is claimed. The failed exp103
artifacts and their report status were preserved.

### Stall isolation (2026-11-06)

Native local scale tests and one profiled remote hour probe are in
[exp104-host-1793974848](../tiered_system/experiments/results/exp104-host-1793974848).
The interrupted exp103 report is still `running`. Its containers and scratch
were left in place. No SQL, memory, or I/O setting was changed. No deployment,
commit, or push.

`TestSpanningSelectionScale`, `TestSpanningSelectionParquetScale`, and
`TestSpanningSelectionNativeScale` passed with DuckDB CLI v2.0.0 and no skips
(`native-scale.log`, package 3.073s). The native cases cover Arrow
pending-streaming and `database/sql` for the generated range union and the
local Parquet head/tail pair. Each selected one row. The slowest native subtest
was 0.22s; each CLI selection was under 0.33s.

The remote probe used the same image,
`beehive-data-api:head-tail-bench-1793972898`
(`sha256:462f649e70d25d02fe20182f697468e0b4438053a2fc031b116612d43b250c11`),
on new containers `headtail103-profile-cold-1793974892` and
`headtail103-profile-split-1793974892`. Both logged DuckDB v2.0.0, two threads,
and a 3 GB limit. Cold border `2025-05-01T00:00:00Z`; split border
`2025-03-01T12:00:00Z`. The only setting change from the failed control was
`COLD_PROFILE_DIR=/profiles`. The hour request was series head 1 for
`wxt.env.temp` / `W0A0`, 11:30–12:30, Arrow, with a 45-second client deadline.

Cold-only returned HTTP 200, 1,048 bytes, 1 record, in 2.194s. Its timing
sidecar is `complete`, with a native JSON profile. `native_ready` is at 1.101s,
the first batch at 2.187s, and `fetch_chunks_seconds` is 1.087s.

Split returned 0 bytes at 45.005s (curl exit 28). Hot staging finished first:
21,912 rows, 7,545,022 JSON-equivalent input bytes, and a 197,586-byte
`hot.parquet`, with `aggregate_staging_complete_since_backend_start_seconds`
at 0.174s. Prepare (0.372s), pending (0.228s), and execute-pending returned.
`native_ready_since_backend_start_seconds` is 1.000s. There is no first-batch
or end-of-stream marker. `fetch_chunks_seconds` is 44.015s, and
`total_backend_lifetime_seconds` is 45.017s. The API then logged
`context canceled` and `failed to close results` with
`INTERRUPT Error: Interrupted!`. The timing sidecar status is `failed`, rows 0.
Its `.duckdb.json` is empty, so `duckdb_profile_present` is false. The staged
Parquet remains under `profile-split-scratch`. Both new containers were stopped
and not removed.

The recorded wait is the first `FetchChunk` after the streaming result exists.
Hot staging, SQL build, hint HTTP (0.165s), prepare, and pending had already
returned. Local native execution of the same selection shape completed, so this
probe does not isolate a local Arrow or `database/sql` wrapper stall. It also
does not identify whether that first chunk is waiting on the remote Iceberg
scan or on the combined scan-plus-window plan. The empty native profile cannot
separate those. Historical placement medians remain unavailable.

## Review of exp104 and next scan/plan isolation (2026-11-06)

Verified the native log has no failures/skips and the remote sidecars agree
with the recorded results: cold-only is complete with one row/native profile;
split has zero rows, failed status, a result-ready marker at **1.000s**, and
**44.015s** in fetch before cancellation. Its native profile is empty. The
21912-row hot stage completed in **0.174s**. This now localizes the recorded
wait to the first native fetch, after prepare and pending returned. It does
not establish whether the wait comes from the remote scan, the union or the
selection graph. Local native interface completion rules out the supplied
local scale fixtures as reproducers; it does not prove every native path or
remote interaction is safe. There are still no historical placement medians.

Added an opt-in, test-only [exp105 scan/plan probe](../tiered_system/experiments/exp105_head_tail_plan_isolation.md).
It runs inside the retained failing runtime image's container environment,
using a test binary compiled for the same platform with the existing builder
and pinned modules. It reads one pinned snapshot of the adopted warehouse and
a private byte-for-byte copy of the retained exp104 hour file, without reading
Influx, rerunning staging or changing any published object. The original source
file, stopped containers and old reports remain untouched.

Six fresh-engine, 45-second cases isolate: remote cold-half scan; local hot
read; raw combined union; a direct canonical per-series window; the current
dictionary selection; and that same current SQL with read-ahead disabled.
All other execution settings remain two CPU threads / 3 GB / eight async
threads. The direct alternative and I/O setting are diagnostics only, not
runtime selectors or an adopted fallback. Successful source reads form a
bounded Go observation oracle for this fixed hour/sensor; successful combined
cases must match the full union or first observation per canonical series.
Native phase profiles, raw/partial rows, query SQL, copied-input hash, errors
and an incremental report are retained, including on timeout. Test failure
is expected if the original blocker reproduces; later cases still run.

A local mixed-value/promoted-collision fixture confirms the diagnostic direct
window matches the production selector for head and tail. Local selection,
scale and numeric CLI regressions pass. The opt-in native probe cannot be
compiled/executed in this sandbox with missing pinned dependencies and denied
Docker access; its VM run is pending. These are plan/scan completion diagnostics,
not HTTP latency measurements or a validated fix. No production SQL, defaults,
module pins, deployments, commits or pushes were changed. Candidate adoption
still requires the full correctness gates and historical two-format control.

Local [exp105 preparation validation](../tiered_system/experiments/results/head-tail-plan-isolation-20261106/summary.json)
records passing CLI semantic/scale/numeric checks and shell syntax validation;
the native build is blocked by unavailable pinned dependencies. The probe uses
the actual `Backend`/`queryArrow` setup, including both SQL and Arrow engines,
to match the API lifecycle. Backend startup precedes each 45-second query
deadline, with an eight-minute overall process timeout. Original exp103/104
artifacts remain unchanged. VM execution of this diagnostic is the next step.

### Scan and plan isolation (2026-11-06)

The diagnostic ran in the retained failing image and collected all six cases.
Evidence is [exp105-host-1793984157](../tiered_system/experiments/results/exp105-host-1793984157).
Report status is `diagnostics_collected`. The test process exited 1 because
the current selection cases hit their deadlines; that exit is the expected
failure signal, not an aborted collection. No production SQL, default, module
pin, deployment, commit, or push changed. The exp103 report is still
`running`. The original hour file is still 197,586 bytes with its 08:21
timestamp. Compile and probe containers exited and were left in place.

The binary was compiled for `linux/arm64` with the existing bookworm builder
and pinned modules, then executed inside
`beehive-data-api:head-tail-bench-1793972898`
(`sha256:462f649e70d25d02fe20182f697468e0b4438053a2fc031b116612d43b250c11`).
Each case used a fresh backend, DuckDB v2.0.0, two threads, a 3 GB limit, and
a 45-second query deadline. The copied hot input has SHA-256
`61a02797a9212b407e464b0ccd648ac46eaa0596d22ed9b0974836fc12d07bf9` and 21,912
rows. The pinned metadata object is
`00061-63fa1d9d-f966-4227-8ad4-ffa5f62b1cd8.metadata.json` in the adopted
warehouse. The query is series head 1 for `wxt.env.temp` / `W0A0`, 11:30–12:30.

| Case | Status | Rows | Parity | Elapsed | First fetch |
| --- | --- | ---: | --- | ---: | --- |
| `cold_scan` | complete | 21,963 | source only | 4.138s | batch at 2.588s |
| `hot_read` | complete | 21,912 | source only | 0.215s | batch at 0.104s |
| `union_scan` | complete | 43,875 | pass | 1.664s | batch at 1.503s |
| `direct_window` | complete | 1 | pass | 2.629s | batch at 2.570s |
| `dictionary` | failed | 0 | unavailable | 45.115s | no batch; fetch 44.378s |
| `dictionary_no_ahead` | failed | 0 | unavailable | 45.119s | no batch; fetch 44.334s |

The union row count is the cold half plus the hot slice. Both dictionary
sidecars are `failed`, with `native_ready` at 0.666s and 0.696s, empty
`.duckdb.json` files, and `INTERRUPT Error: Interrupted!` after the deadline.
`dictionary_no_ahead` is the only case whose log shows `read_ahead_depth=0`;
the other five show `-1`. Disabling read-ahead did not let the current
selection return a row.

The remote cold scan, the local hot file, and their raw union all complete.
The diagnostic direct per-series window also returns the oracle's one head row.
The current dictionary selection stalls in the first `FetchChunk` on this same
snapshot and file, with read-ahead left on and with it set to zero. That
implicates the dictionary/CTE selection graph in this environment. The direct
window is a diagnostic result for this fixed hour and sensor, not an adopted
plan. Placement medians remain unavailable, and adoption still requires the
exp102 correctness gates and the exp103 two-format control.

## Exp105 review and direct-plan integration (2026-11-06)

Verified the six-case report and sidecars: cold scan **21,963 rows**, hot read
**21,912 rows**, and union **43,875 rows** complete. Independently replayed the
saved raw rows: the union is exactly the cold plus hot observations, and the
direct window matches the first observation per canonical series. The selected
row is **1740828600067131745 ns**, value **-2.3**. Both dictionary variants still
have zero rows and about 44.3s in first fetch before deadline cancellation,
including `read_ahead_depth=0`. Source reads and union therefore work on this
snapshot/input; the standalone dictionary graph is the failing execution
shape in this environment. This does not identify an upstream engine defect's
internal cause or establish a general DuckDB limitation.

Replaced `buildSelectionSQL` with the successful direct window for standalone
cross-border head/tail. Metadata is merged/sorted per observation, one window
partitions by `(name, meta)`, and the selected original rows retain the public
schema and ascending timestamp output. The staging writer, byte budget,
source filters/union, exact scalar restoration, resource/grant lifecycle,
aggregation plans and global thread/memory/read-ahead settings are unchanged.
There is no runtime dictionary fallback or selector. The failed dictionary SQL
is frozen only in exp105 diagnostic test code for reproduction until adoption
gates complete; it is not compiled into the API binary.

Added `TestSelectionCapturedDirectPlan`: using the retained exp105 report's
snapshot and staged path, the production builder emits exactly the captured
SQL from the successful remote `direct_window` case. This render-only check
passes without contacting storage. Local per-series/zero/oversized limit,
metadata collision/order/null-map, exact scalar/nanosecond and scale checks
pass against DuckDB 2.0. The direct candidate is also compared against the
frozen dictionary on a small mixed-value fixture for head and tail. The
original 512/768 host correctness results are prior-plan evidence, not a new
validation of this replacement through the API.

Direct metadata work is now per observation rather than per distinct raw tag
tuple. This can increase CPU cost; accepting a completed hour diagnostic does
not establish full-workload performance or justify changing aggregation's
working dictionary plan. The 2.629s single profiled direct result is not a
placement median or an old/new speedup measurement.

The [exp106 integration gate](../tiered_system/experiments/exp106_head_tail_direct_integration.md)
provides the full synthetic/live HTTP, suite/vet/race and native-scale checks,
then a fresh-image exp103 control (256 responses) and separate Arrow diagnostic
(64 responses). The client timeout is shortened to 120s for promptly recording
another stall; no API timeout default changes. Full VM validation remains
pending because local native compilation is blocked by missing pinned modules
and library. Existing failed artifacts, source warehouse, deployments, commits
and pushes were not changed.

Local [direct-integration validation](../tiered_system/experiments/results/head-tail-direct-integration-20261106/summary.json)
records passing selection/scale/numeric tests, the exact captured remote SQL
match and runbook shell syntax. Native Go suite execution remains blocked
locally by missing pinned dependencies; the exp106 VM gates are still required.

### Direct-plan integration gates (2026-11-06)

The direct window passed the exp106 correctness gates and both historical
phases. The noon-split stall did not recur. Evidence is
[exp106-host-1793985281](../tiered_system/experiments/results/exp106-host-1793985281).
Image `beehive-data-api:head-tail-direct-window-1793985281`, manifest digest
`sha256:03a08a0ee64cc64ee3263326514bd35038c399434a03c904b9573501b84b4c86`.
No deployment, warehouse change, commit, or push. The interrupted exp103
report was not rewritten. The exp105 dictionary SQL remains only in the
diagnostic test.

`TestHeadTailProductionHTTP` passed 512 subtests in 3.417s and
`TestHeadTailLiveTierParity` passed 768 subtests in 5.334s. Both `-v` logs
have no skips. The `./...` suite, `vet`, and tiered race check exited 0.
The cold package in the suite took 82.299s; the race check took 2.015s.
Dedicated containers had no memory cap, no OOM, and no restart. Request
scratch was empty after the control and after the diagnostic. Both profile
containers were stopped and left in place, as were the control containers.

The unprofiled control is `complete`: 256 responses, 64 of them excluded
warmups, all `parity_passed`. Client timeout was 120s and was not hit. DuckDB
reported v2.0.0, two threads, and a 3 GB limit. Medians below are the three
measured repetitions. Arrow and NDJSON agree within a few tenths of a second
except the 98,211-row fleet-minute responses, where NDJSON is slower.

| Case | Rows | Cold Arrow | Split Arrow |
| --- | ---: | ---: | ---: |
| series day, head or tail, limit 1 or 1000 | 1 or 1,000 | 3.29–3.42s | 6.89–7.34s |
| series hour, head or tail, limit 1 or 1000 | 1 or 1,000 | 0.68–0.70s | 0.84–0.89s |
| fleet minute, limit 1 | 1,995 | 0.82–1.06s | 1.23–1.27s |
| fleet minute, limit 1000 | 98,211 | 0.92–1.56s | 1.41–2.09s |
| temperature fleet hour, limit 1 | 16 | 2.19–2.22s | 4.17–4.35s |
| temperature fleet hour, limit 1000 | 15,060 | 2.27–2.35s | 4.29–4.50s |

The separate Arrow diagnostic is `complete`: 64 responses, 32 excluded
warmups, parity with the control bodies, and a native profile on every
response. These are single measured repetitions, not the control medians.
Temp-directory size is 0 on every measured split profile. Peak buffer is about
46–51 MB for a series day, 44–46 MB for a series hour, 78–162 MB for a fleet
minute, and 89–162 MB for a temperature fleet hour.

On the measured split day, staging 521,840 hot rows finished at 3.35–3.46s
and wrote a 4,015,648-byte Parquet file. The first batch arrived at about
7.1s, with fetch around 3.2s. Operator CPU is dominated by projection, about
5.8s, beside about 0.3s of scan and 0.2–0.4s of hash grouping. The measured
split hour staged 21,912 rows and 197,586 bytes in 0.14–0.15s and returned its
first batch at 0.82–0.87s. Projection there is about 0.27s. Operator times
count CPU and are not additive wall time. The day split cost above cold-only
is the hot stage plus this per-observation projection, on this workload only.

## Review: direct selection retained after exp106 (2026-11-06)

Checked the uploaded logs and reports: **512 synthetic** and **768 live** wire
cases pass without skips; suite, vet and tiered race checks pass. The historical
control is complete with **256 responses**, excluding 64 warmups from medians;
all response parity checks pass. The diagnostic is complete with **64 eligible
profiles**, excluding 32 warmups, and matches control bodies. The historical
stall did not recur. Independently replayed all **320 historical saved bodies**
and checked their recorded hashes, schema, time bounds/order, exact filters,
per-series limits and cross-format/placement parity. Separately decoded all
**1,280 fixture bodies** against their original observation oracle, including
exact nanoseconds/integers and string/boolean types. These are wire checks,
not an additional Sage-client dataframe performance measurement.

Representative three-repetition unprofiled medians (the preceding range table
covers the other head/tail variants):

| Query | Rows | Cold Arrow | Split Arrow | Cold NDJSON | Split NDJSON |
| --- | ---: | ---: | ---: | ---: | ---: |
| Series day, head 1 | 1 | 3.419s | 7.105s | 3.391s | 7.342s |
| Series hour, head 1 | 1 | 0.675s | 0.867s | 0.686s | 0.851s |
| Fleet minute, head 1000 | 98,211 | 1.007s | 1.517s | 1.557s | 2.093s |
| Temperature fleet hour, tail 1000 | 15,060 | 2.285s | 4.428s | 2.337s | 4.501s |

The small output limits do not avoid the full input work: day head/tail 1 and
1000 have similar times despite very different output counts. Arrow's clearest
format benefit is the large fleet payload (split head 1000: 1.517s versus
2.093s, about 27.5% lower receive latency in this run). That timing includes
format-specific parsing/spooling, excludes semantic validation/dataframe
construction, and is not proof of an isolated server or network speedup.

In the separate measured **day head 1 profile**, split staging completes at
3.460s; total backend lifetime is 7.231s versus 3.307s cold-only. After removing
that measured staging duration, split's remaining lifetime is about 3.771s,
only about 0.463s above cold-only in this diagnostic. Thus most of the *extra
split wall time* here comes from hot staging. Native fetch is 3.236s versus
2.788s. Projection work dominates native CPU on **both** placements: 5.821s
split versus 4.766s cold. These CPU/operator times overlap workers and cannot
be added to stage or wall times. The dominant node projects `timestamp`,
`name`, `value`, `meta`; its aliases alone do not prove all that time is
metadata sorting. Any future expression-level attribution needs another
isolated probe. Do not mix these single diagnostic phases with control medians.

All measured split profiles have zero spill. Buffer-manager peaks do not
establish whole-process RSS or include the Go staging heap. Historical request
scratch was reported empty after both phases; fixture tests also verify
cleanup/read-grant ordering. Containers had no recorded restart/OOM, and the
interrupted exp103 evidence was preserved.

**Retain the direct standalone plan.** This completes the requested feature's
correctness and measured historical gate across cold and split storage, with
hot-only parity established by the live fixture. Keep aggregation's separately
validated plans and the global I/O policy unchanged. Future work can revisit
shared hot-staging CPU/allocations (also recorded in plan 13) and native
projection cost, while preserving canonical identities and avoiding the failed
standalone dictionary graph. No further performance implementation was made
in this review. The old graph remains diagnostic test code only; there is no
API fallback.

[Review validation](../tiered_system/experiments/results/head-tail-direct-review-20261106/summary.json)
records the completed artifact replays. Updated API validation and the runbook
outcome. No runtime code, published warehouse, deployment, commit or push
changed during this review.

## Experiment: select raw candidates before canonical metadata (2026-11-06)

The retained direct plan remains the API default. The next candidate targets
native projection by selecting head/tail N per **raw** tuple
`(name, meta, plugin, vsn, node, host, task, job, zone)`, materializing that
smaller set, then applying the existing canonical direct window and final N.
The first window does not merge or sort metadata maps. The materialized CTE
has a single consumer; the failed dictionary/CTE join graph is not restored.
Remote completion still requires testing rather than assuming all other CTE
shapes are safe.

Correctness follows from the raw partition being finer than public series:
all observations in one raw tuple map deterministically to one canonical
series. A point discarded by raw top-N already has N equally early/late or
better points in its own tuple, all in that canonical series. It is not needed
for the final top-N. Equal-time tie ordering remains unspecified. Multiple raw
aliases can merge, so **the final canonical per-series window is still
required**. This is selection of original observations, without numeric
subtotals, reassociation or a public value/schema change.

Added test-only `buildRawCandidateSelection` and a 2048-row mixed-type fixture
with promoted overrides, map key-order aliases, hot merged tags, null versus
empty maps and absent versus present-null tags. Head/tail 0/1/2/10/1000 exactly
match the retained selector. Local synthetic Parquet scale checks over
1,043,680 rows also select the expected exact nanosecond/value. Single CLI
executions were encouraging: about 0.11s candidate versus about 2.8–3.0s retained
on this synthetic file-backed head/tail input. These are not historical API
measurements or adoption medians. Native local scale coverage now includes
both plans through Arrow and database/sql; pinned-library VM execution is
still required locally.

The [exp107 runbook](../tiered_system/experiments/exp107_head_tail_raw_candidates.md)
prepares a read-only retained-input experiment in the current exp106 runtime
image. It copies the stopped exp103 day and exp104 hour hot files into a fresh
isolated mount, checks hashes/row counts, pins one adopted warehouse snapshot,
and compares each plan against the hashed exp106 cold NDJSON response. Eight
cases (day/hour × head/tail × N=1/1000), one excluded warmup and three measured
repetitions per plan give **64 profiled native trials**, with alternating order,
fresh production backends and per-query deadlines. Failed trials stop promptly
with their report, rows and profiles retained. Fresh scoped read tokens are
created per trial. No Influx read, staging rerun, publication or original-file
mutation is needed.

The summary tool requires complete parity, eligible DuckDB 2.0 profiles and the
full case/repetition matrix, excludes warmups and reports native phase/CPU/
projection/spill medians. It does not measure HTTP latency, Go/Sage-client
dataframe time, hot extraction or staging. The day staging cost (~3.4s in
exp106) remains. Large N/high-cardinality cases may get little pruning and pay
for an extra selection/materialization pass; after any promising isolated
result, the full fleet/two-format HTTP gate remains mandatory before adopting
this candidate. Aggregation, global I/O policy, module pins and live API SQL
are unchanged. No deployment, commit or push was made.

Local [raw-candidate preparation validation](../tiered_system/experiments/results/head-tail-raw-candidate-20261106/summary.json)
records passing semantic/scale/numeric CLI checks. The additional million-row
N=1000 head/tail check verifies exact row count, distinct timestamp count,
cutoffs and values. All **103 Python tests pass**, including summary validation
for complete matrices, warmup exclusion, invalid paths, failed parity and
ineligible profiles. The runbook shell blocks pass syntax checks. The native
probe is blocked locally by unavailable pinned modules/library. The production
selector's SHA matches the retained exp106 integration source; VM execution of
exp107 is pending.

### Raw-candidate retained-input comparison (2026-11-06)

The candidate completed all 64 profiled trials with exact parity against the
exp106 cold NDJSON bodies. It is still test-only. The production selector
remains the direct window. Evidence is
[exp107-host-1793990111](../tiered_system/experiments/results/exp107-host-1793990111).
No deployment, warehouse change, commit, or push. The original day and hour
files kept their timestamps and sizes.

`TestSpanningSelectionNativeScale` passed both plans through Arrow and
`database/sql` in 37.023s with no skips. On that local synthetic Parquet pair
the retained plan took 8.05–9.22s per subtest and the candidate took
0.11–0.13s. Those are not the historical medians below.

The remote probe ran inside
`beehive-data-api:head-tail-direct-window-1793985281`
(`sha256:03a08a0ee64cc64ee3263326514bd35038c399434a03c904b9573501b84b4c86`)
against pinned metadata
`00061-63fa1d9d-f966-4227-8ad4-ffa5f62b1cd8.metadata.json`. Copied inputs are
the exp103 day file SHA-256
`d53f6604f2fd9ae8b767d8e1c9d06184ffeec7c605e805e512f246ebd8ec222c` and the
exp104 hour file SHA-256
`61a02797a9212b407e464b0ccd648ac46eaa0596d22ed9b0974836fc12d07bf9`. DuckDB
reported v2.0.0, two threads, a 3 GB limit, and read-ahead depth -1. The test
exited 0 in 214.42s. Compile and probe containers exited and were left in
place. The summary status is `complete`.

Medians below exclude the one warmup per case and plan. They are native
backend lifetime after SQL startup, not HTTP latency and not hot staging.
Every measured profile has temp-directory size 0. Both plans read the same
bytes: 31,104,683 on a day and 4,702,269 on an hour.

| Case | Rows | Direct lifetime | Candidate lifetime | Ratio |
| --- | ---: | ---: | ---: | ---: |
| series day head 1 | 1 | 5.436s | 2.074s | 2.62 |
| series day head 1000 | 1,000 | 8.416s | 2.153s | 3.91 |
| series day tail 1 | 1 | 5.681s | 2.186s | 2.60 |
| series day tail 1000 | 1,000 | 6.452s | 2.768s | 2.33 |
| series hour head 1 | 1 | 1.647s | 1.660s | 0.99 |
| series hour head 1000 | 1,000 | 1.473s | 1.498s | 0.98 |
| series hour tail 1 | 1 | 1.514s | 2.373s | 0.64 |
| series hour tail 1000 | 1,000 | 1.640s | 1.694s | 0.97 |

On the day, projection CPU falls from about 6.0s to 0.03–0.05s while the scan
stays about 1.0s and 1,048,491 rows. A measured direct plan spends 6.010s in
one projection of those 1,048,491 rows. The matching candidate projection of
that row count is 0.003s; its hash grouping emits 2 rows on this one series.
The hour projection falls from about 0.27s to 0.003–0.024s, and that saving
is absorbed by scan, prepare, and pending. Hour tail 1 is slower for the
candidate, with blocked-thread time 1.119s against 0.562s. Peak buffer stays
about 41–45 MB on a day and 17–22 MB on an hour.

The day result is a projection reduction on one already-staged series, with
the same bytes read. It does not remove the exp106 hot-staging cost, and the
hour cases do not improve. The fleet workload and the two-format HTTP control
were not run. The direct plan stays the API default.

### Review and next gate: paired candidate HTTP/fleet test (2026-11-06)

Independently replayed all **64** exp107 saved native outputs against the
hashed exp106 cold NDJSON bodies and revalidated the full timing/native
profile matrix. All pass. The 2.33–3.91× day improvement above is native
lifetime with retained hot input; it is not a measured full-request speedup.
The hour tail-1 candidate is **57% slower** (1.514s → 2.373s). Its native CPU
also increases (~0.604s → 0.706s); increased blocked-thread time alone cannot
explain the wall difference because worker timings overlap. The other hour
cases are flat to slightly worse. All measured trials report zero spill.

Keep the direct selector as the API default until the fleet and HTTP gates
complete. The candidate's early pruning clearly reduces day projection work,
but large N and many distinct raw tuples could pay for an extra window with
little pruning. Hot extraction/staging is unchanged.

Prepared [exp108: paired HTTP/fleet gate](../tiered_system/experiments/exp108_head_tail_paired.md).
Its source preparation tool makes **two isolated build/test copies** with a
per-file hash manifest and reviewable patch. Only the candidate copy installs
the exact exp107 selector as the live standalone path. The API checkout,
module pins, aggregation, staging and global I/O policy remain unchanged;
there is no runtime selector. Candidate gates run inside that copy, including
512 synthetic / 768 live HTTP cases, suite/vet/race, before building the pair.
The optional exp105 SQL byte-shape capture is explicitly baseline-only.

Both dedicated APIs then use the same noon border, historical hot bucket,
adopted warehouse, resources and settings. The driver checks recorded image
IDs/source hashes and runs **256 unprofiled responses**: day/hour single
series, fleet minute and temperature-fleet hour, head/tail 1/1000, Arrow and
NDJSON, one excluded warmup plus three measured repetitions. Every response
is compared against the hashed exp106 cold NDJSON oracle. A separate **64
Arrow timing/native profile pairs** follow only a matching completed control.
The report includes same-run baseline/candidate latency medians and ratios.
Failed bodies/reports and stopped containers remain for inspection.

Local [preparation/review checks](../tiered_system/experiments/results/head-tail-paired-preparation-20261106/summary.json)
pass candidate SQL correctness, mixed scalar/canonical metadata cases,
million-row selection and N=1000, **106 Python tests**, and six runbook shell
blocks. The candidate intentionally skips only the baseline SQL capture in
this local SQL test invocation. Full native HTTP/fleet execution still needs
the VM with its pinned dependencies; it has not run locally. No deployment,
warehouse mutation, commit or push was made.

### Paired HTTP and fleet gate (2026-11-06)

The candidate passed the paired gate on correctness and is faster on the day,
hour, and temperature-fleet workloads. Fleet-minute limit 1000 is slightly
slower. The API checkout still uses the direct selector. Evidence is
[exp108-host-1793991377](../tiered_system/experiments/results/exp108-host-1793991377).
No deployment, warehouse change, commit, or push.

The candidate snapshot's synthetic HTTP test passed 512 subtests in 3.895s
and the live fixture passed 768 in 6.154s, with no skips. The first full
suite failed only in `TestDashboardUsesExportedMetricsAndScopedTargets`
because the snapshot omitted `observability/grafana/beehive-data-api.json`.
The same unchanged dashboard and alert files were then added to both
snapshots and recorded in the manifest; they are not part of the four-file
selector patch. The rerun suite, vet, and tiered race check exited 0. The
cold package in that suite took 35.703s and the race check took 1.978s.

Both images use the noon border, adopted warehouse, and `exp95-hot`, with
DuckDB v2.0.0, two threads, a 3 GB limit, and no container memory cap.
Baseline image `sha256:5d0a62c7f5a5e73602e13a556bf6e374849b8db40b03b09b541c5fbf3326ac91`.
Candidate image `sha256:380398233194237bc43bdda9c44b15c7e08a4a7b2a696cc5966fa96d9b98854b`.
Neither role recorded an OOM or a restart. Request scratch was empty after
both phases. All four containers were stopped and left in place.

The unprofiled control is `complete`: 256 responses, 64 excluded warmups, and
exact parity with the exp106 cold NDJSON bodies. Ratios below are baseline
median divided by candidate median. Arrow and NDJSON agree within a few
tenths except the 98,211-row fleet responses, where NDJSON is slower for both.

| Workload | Rows | Baseline Arrow | Candidate Arrow | Ratio |
| --- | ---: | ---: | ---: | ---: |
| series day, head or tail, 1 or 1000 | 1 or 1,000 | 6.89–7.17s | 4.17–4.37s | 1.61–1.72 |
| series hour, head or tail, 1 or 1000 | 1 or 1,000 | 0.84–0.96s | 0.70–0.73s | 1.17–1.37 |
| fleet minute, limit 1 | 1,995 | 1.25–1.28s | 0.95–0.99s | 1.27–1.32 |
| fleet minute, limit 1000 | 98,211 | 1.42–1.48s | 1.46–1.61s | 0.92–0.97 |
| temperature fleet hour, limit 1 | 16 | 4.16–4.18s | 2.65–2.72s | 1.53–1.58 |
| temperature fleet hour, limit 1000 | 15,060 | 4.26–4.30s | 2.78–2.97s | 1.44–1.53 |

Fleet-minute limit 1000 NDJSON ratios are 0.985 for head and 0.905 for tail.
The exp107 hour-tail slowdown does not appear in this same-run HTTP pair:
hour tail 1 is 0.866s baseline and 0.710s candidate on Arrow.

The Arrow diagnostic is `complete`: 64 responses, 32 excluded warmups, all
eligible v2.0 profiles, and parity with the control. These are single
measured repetitions. Temp-directory size is 0 on every one. Hot-row counts
match across roles: 521,840 for a series day, 21,912 for a series hour,
47,963 for a fleet minute, and 289,674 for a temperature fleet hour. Staging
time matches within about 0.2s. On a series day the baseline projection is
about 5.8–5.9s and the candidate projection is about 0.03–0.04s, while both
still stage for about 3.3s. Fleet-minute limit 1000 is the overhead case:
candidate CPU is 1.83s and 1.69s against baseline 0.96s and 0.85s, and the
buffer peak rises from about 168–173 MB to about 220–225 MB. Projection time
there is already about 0.6s for both plans. Operator CPU is not additive wall
time, and buffer peaks are not process RSS.

### Review: candidate ready for adoption with a documented tradeoff (2026-11-06)

Independently verified all **320 historical response bodies** and their hashes
against the retained exp106 cold NDJSON oracle, reloaded all **64** complete
DuckDB 2.0 timing/native pairs, and recomputed the full repetition matrix and
latency ratios. Replayed the **512 synthetic and 768 live fixture bodies**
against exp106 as well. All pass. Source manifest hashes match both snapshots,
the only four differing source files are the intended selector/test patch,
and the API checkout still matches the measured baseline. The unchanged
observability assets and module pins match across both copies. Review evidence:
[head-tail-paired-review-20261106](../tiered_system/experiments/results/head-tail-paired-review-20261106/summary.json).

**Recommendation: adopt raw-candidate selection for standalone spanning
head/tail.** It improves 28 of 32 measured workload/format cells. Day requests
improve by 1.60–1.86× across Arrow/NDJSON, hour requests by 1.17–1.37×, and
temperature-fleet hour by 1.44–1.58×. The earlier isolated hour-tail regression
does not reproduce in this paired full-request gate. These are medians of
three measured requests per cell, with one excluded warmup; they are not
confidence intervals or a concurrency benchmark.

The tradeoff is all four fleet-minute N=1000 cells: full-response latency
increases by **1.5–10.5%** (roughly 0.03–0.23s). The separate single measured
Arrow profiles show native CPU increasing **1.90–2.00×**, and peak buffers
increasing **30–31%**, about 168–173 MiB to 220–225 MiB. These short, broad
queries retain many of their 98,211 observations, so the extra raw selection
pass provides little benefit. Both plans still report zero spill. Buffer
peaks are not process RSS, and this test does not establish concurrent capacity.
The improvement is a workload tradeoff, not a universal speedup. Do not add
an automatic threshold based only on this small matrix.

After adoption, the next substantial target is **hot extraction/staging**,
which remains about **3.3s of a ~4.2s day request**. Its timer combines source
iteration, decoding, validation and Parquet writing; isolate those costs
before choosing another optimization. The shared aggregation staging path is
already direct-to-Parquet, so reintroducing an NDJSON-to-Parquet experiment
would repeat work that has been retired.

Fixed the exp108 preparation tool to copy the unchanged Grafana dashboard and
Prometheus alerts into both test snapshots automatically. The focused three
Python tests pass, including asset hashes, checkout preservation, paired
control/diagnostic flow and rejection of mismatched inputs. Original VM
artifacts remain unchanged. This review does **not** install the candidate in
the API checkout: the retained direct selector is still its default. No
production deployment, storage mutation, commit or push was made.


### Adopted raw-candidate selection and retired standalone probes (2026-11-06)

Adopted the exp108 selector in `beehive-data-api/internal/backend/cold/selection_sql.go`.
It is the single standalone spanning head/tail implementation for Arrow and
NDJSON. It first selects N per raw tuple, materializes candidates, then
canonicalizes metadata and selects N per public series. The final canonical
window remains necessary because raw aliases can merge. There is no operator
flag, threshold, alternate direct selector or dictionary fallback.

Flattened the validated wrapper and final projection into one builder. An
isolated comparison against the retained exp108 candidate sources verified
**32 byte-for-byte SQL matches**: raw and full staged/cold builders, head/tail,
N=0/1/2/1000, explicit/omitted end. The only changed runtime/module file versus
the measured baseline is `selection_sql.go`; dependency pins, staging,
aggregation and global I/O settings are unchanged. The measured fleet-minute
N=1000 latency/CPU/buffer tradeoff above remains accepted.

Removed four obsolete standalone experiment files (928 lines):
`selection_reduce_probe_test.go`, `selection_isolation_test.go`,
`selection_remote_isolation_test.go` and `selection_reduce_remote_test.go`.
Also removed the superseded exp105 direct SQL capture and duplicate native
candidate-versus-baseline runs. No remaining API references use their symbols.
The original source snapshots, failed-run evidence, warehouse data and VM
artifacts remain unchanged. Archived comparison runbooks now identify their
source requirements; exp108 preparation explicitly requires a pre-adoption
snapshot instead of silently rewriting the adopted API.

Preserved useful semantic coverage as `TestSpanningSelectionCanonicalAliases`
in `selection_semantics_test.go`: Go independently merges metadata/promoted
tags and chooses per-series observations for head/tail 0/1/2/10/1000, including
null/empty/present-null maps, key-order aliases and mixed exact scalar values.
No old SQL selector is needed as its oracle. Million-row native/CLI tests now
exercise only the live plan; N=1000 checks retain exact counts, distinct
nanosecond timestamps, bounds and values.

Local validation: compiler/SQL correctness, raw-union border checks,
million-row head/tail and large-limit checks, independent Go oracle, targeted
SQL vet, query/tiered tests, **106 Python tests**, archived shell syntax and
API whitespace checks pass. The native tagged scale rerun is blocked locally
by missing pinned native/transitive dependencies with offline module lookup;
it is not reported as passing. Exp108 already passed the candidate's 512/768
HTTP gates and full suite/vet/race on the VM. Current VM regression commands
are in the API's `docs/aggregation-validation.md`.

Updated architecture, development and validation docs. Evidence:
[head-tail-adoption-20261106](../tiered_system/experiments/results/head-tail-adoption-20261106/summary.json).
No deployment, warehouse mutation, commit or push was made.

### Deferred optimization: hot extraction/staging (2026-11-06)

The current head/tail implementation is retained. Further optimization is
deferred until we return to this work; no additional benchmark or performance
change is planned for now.

The next candidate is hot extraction/staging, measured at roughly **3.3s of
a 4.2s day query** in exp108. This is specific to the measured workload. The
combined timer includes reading the hot slice from Influx through Flux,
decoding and validating records in Go, and writing temporary Parquet. The
current measurements do not identify which component dominates, so increasing
DuckDB threads alone is not an established solution.

When work resumes, first add separate substep timings and Go CPU/allocation
profiles, then benchmark the same retained hot slice in isolation. Use that
evidence to choose between extraction, decoding/validation and Parquet-writing
optimizations. Direct-to-Parquet staging is already implemented; retain it
rather than reintroducing the retired NDJSON intermediate.


### Cross-border ownership moved to tiered (2026-11-06)

Moved combined aggregate/head/tail orchestration out of the cold backend.
`tiered/execute.go` now owns validation, raw hot reads, Parquet staging,
the raw union, combined-plan selection and scratch lifetime.
`tiered/selection.go` owns the adopted standalone selector.
`cold.Query` continues to serve cold-only queries, and `hot.Query` continues
to serve hot-only queries. Ordinary crossing extracts retain ordered result
concatenation.

The neutral `internal/query/aggregate` package contains the single shared
numeric/window/metadata compiler. `query.PlannedSourceBackend` exposes a bounded,
authenticated raw cold source and the reused DuckDB engine to the tiered
planner. Cold execution no longer accepts a hot backend or staged-input
context. It retains snapshot pinning, scalar-marker decoding and result
closure. Staging configuration fields moved to `tiered.Backend`; environment
variables and flags are unchanged. `query.QueryProfile` bridges staging and
native profiling without changing keys, file formats or lease ownership.
The former `AggregationBackend` and `HeadTailBackend` delegation interfaces
were removed.

Preserved the SQL plans byte for byte in **184** before/after comparisons,
covering cold-only and combined aggregates (five functions, windows, limits,
unsigned marker) plus cold-only Arrow/NDJSON and combined head/tail. The
retained-data compiler oracle passes; new tiered tests verify input slicing,
frozen bounds, hot-source closure before native execution, early bucket
validation, setup-failure cleanup and scratch retention on uncertain closure.

The full tagged suite, vet, and tiered/writer race tests pass locally using
the exact pinned dependencies recovered from local module archives. Optional
fixture gates are not counted as passes: explicitly invoking the aggregation
and head/tail HTTP gates is blocked by the sandbox's loopback-listener denial.
VM commands for all four synthetic/live gates are in the API's
`docs/aggregation-validation.md` under “Cross-border ownership refactor”.
Evidence is retained in
[the refactor results](../tiered_system/experiments/results/tiered-execution-refactor-20261106).
No benchmark was added, and performance work remains deferred. No deployment,
warehouse mutation, commit or push was made.
