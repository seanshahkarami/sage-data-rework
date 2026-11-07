# Experiments 91–92: unsigned persistence and complete mixed-type hot input

Saved runs cited below as `results/...` are paths inside the split archive in the parent directory (`../results.z01`, `../results.z02`, `../results.z03`, and `../results.zip`). See [recorded results](../README.md#recorded-results). A command that creates a new run directory still writes local untracked output.

Follow-up to [plan 13](../../plans/13_aggregation_queries.md). These are explicit
research paths. No production API, exporter policy, published warehouse, Pelican,
`/node-data`, or existing fixture data is changed. Output directories must be new.
Nothing is committed or pushed.

## 91: marked unsigned encoding inside the existing VARIANT column

Native unsigned VARIANTs do not preserve their kind through the pinned Parquet
writer and large values fail. The experimental convention writes **only source
uint64** as `DECIMAL(20,0)::VARIANT`, reserving this representation under table
property `sage.value.encoding=variant-uint64-decimal-v1`. Other supported source
kinds remain unchanged. The table still has one VARIANT value column, field ID 11.
The source contract currently has no decimal input kind, so the reservation is
unambiguous for newly created marked tables. Native decimals cannot later be
introduced without revisiting it.

The actual read returned `DECIMAL(38,0)` for the 128-bit persisted decimal, even
though an earlier standalone probe showed precision 20. The reader accepts the
20/38 zero-scale forms only under the explicit marker, validates the uint64
range, then restores a native UBIGINT VARIANT for kind-sensitive arithmetic.
Negative values and values above UINT64_MAX fail. Missing/unknown markers do not
automatically reinterpret decimals. Float/signed/string/bool values do not become
unsigned. JSON values remain integer tokens; no public response columns change.

The existing exp89 fixture tool now has the opt-in `--unsigned-encoding decimal20`.
Its default native baseline remains unchanged. The tiny fixture writer accepts
only the known experimental marker. This is not yet a production storage policy,
and cannot restore signedness already lost in previously written files.

### Local results, 2026-11-04

[Final boundary report](results/exp91_unsigned_boundaries_20261104/report.json):
DuckDB **2.0.0**, **51 raw rows verified**, **182/182 cold-only/split aggregate
comparisons passed**, **zero excluded unsigned cases**. Fifteen added raw controls
include signed/unsigned zero and six, 2^53+1, INT64_MAX, 2^63, UINT64_MAX,
INT64_MIN, float six, true/false and string six. Nanosecond instants, all tags,
logical source kinds and exact values survived actual Iceberg-go publication and
DuckDB Iceberg scans. Invalid negative/overflow decimal controls also rejected.

The first preparation failed because it assumed declared decimal precision was
unchanged. The second recovered exact integers but exposed a checker issue:
public JSON cannot distinguish signed from unsigned integer tokens. The corrected
checker verifies **logical kinds separately in typed SQL**, then independently
classifies public JSON scalars and compares values exactly. It never borrows an
observed kind from the expected row. Both typed and public checks must pass.
Earlier runs are retained as diagnostic evidence.

The native API HTTP rerun has **not** passed in the agent sandbox: the writable
offline Go cache lacks pinned `go.duckdb.org/libduckdb/darwin-arm64 v0.20000.0`.
No dependency versions were substituted. Run the following in the host terminal.

### Prepare on whichever host will run the Go test

From the workspace root:

```sh
(cd sage-data-rework/tiered_system/experiments/icebergo && \
  go build -mod=readonly -o /tmp/exp91-iceberg-fixture ./cmd/aggregation-fixture)
export EXP91_ROOT="$PWD/sage-data-rework/tiered_system/experiments/results/exp91-host-$(date +%s)"
python3 sage-data-rework/tiered_system/experiments/runbooks/exp89_aggregation_fixture.py \
  --unsigned-encoding decimal20 --duckdb "$HOME/.local/bin/duckdb" \
  --fixture-writer /tmp/exp91-iceberg-fixture \
  --integer-report sage-data-rework/tiered_system/experiments/results/exp88_integer_replay_20261104/report.json \
  --baseline-report sage-data-rework/tiered_system/experiments/results/exp86-vm-1793828688/report.json \
  --run-dir "$EXP91_ROOT"
```

Use the native Linux DuckDB 2.0 binary path on the VM. The local SQL catalog has
absolute file URLs; regenerate on the target host rather than copying its
metadata to a different path/OS. If testing the already-created laptop fixture,
set `EXP91_ROOT` to its absolute `exp91_unsigned_boundaries_20261104` directory
instead of preparing it again.

### HTTP validation after fixture preparation

The precompiled-SQL HTTP adapter and its one-command runner were retired after
production aggregation and exp100 validation passed. Reuse a prepared exp91
fixture with the [current API validation guide](../../../beehive-data-api/docs/aggregation-validation.md)
and [exp100](exp100_aggregation_http.md). Those tests compile the real request,
fetch/stage hot input where applicable, and export bodies for the maintained
`exp90_aggregation_client.py` replay. Do not use the removed prototype adapter
or its historical export settings. Fixture preparation and the result records
below remain supported/preserved.

### Host result, 2026-11-05

`results/exp91-http-1793886680` passed the native probe and is retained. Its
export check rejected the two empty no-series NDJSON bodies. Those queries
expect zero rows; their Arrow bodies contain the schema frame. Empty NDJSON is
now accepted only for an empty expected row list.

[Completed report](results/exp91-http-1793886760/report.json): Go 1.26.5,
read-only API pins, **364 responses**, **6 leases**, and a current-client
replay of **364/364** with pandas 3.0.6, pyarrow 25.0.1, and zero integer
precision failures.

## 92: actual shard-scoped extraction from the retained mixed fixture

The existing exp87 fixture has three two-point series: int then float, float then
int, and reversed insertion order. Whole-range reads omitted the later kind.
`exp92_complete_hot_reads.py` queries this existing isolated bucket **read-only**.
It reads actual live shard-group start/end/truncation/deletion metadata, clips
half-open reads to the requested range, and compares all recovered rows and
kinds with the original fixture. It refuses overlapping active groups rather
than declaring those reads complete. It does not assume a current shard duration
also describes historical shards, nor assume hour/day splitting is safe.

It retains whole-range reads and type-filter queries as diagnostic controls;
neither control is assumed to recover missing rows. Raw shard-scoped reads must
recover **all six exact observations**. Once complete input is verified, count
must return two per series and arithmetic on mixed kinds must explicitly reject,
following the current homogeneous-first recommendation. The test does not
silently choose floating coercion or copy historical omission.

Shard manifests are available in Influx's [metadata-backup API](https://docs.influxdata.com/influxdb/v2/api/backup/).
That endpoint returns KV/SQL metadata too. The script discards those fields and
unrelated buckets without saving or logging the raw backup. It saves only an
allowlist of identities and shard bounds for the exact fixture bucket. A
metadata token with sufficient privileges may be necessary; set
`INFLUX_METADATA_TOKEN` separately if the query token is insufficient. Do not
broaden production API credentials to run this fixture. A previously filtered
manifest can be supplied with `--shard-manifest` instead.

On the retained fixture host, with its existing Influx container running:

```sh
# Supply the fixture query token in INFLUX_TOKEN; optionally a separate
# metadata-capable fixture token in INFLUX_METADATA_TOKEN. Tokens stay in env.
python3 sage-data-rework/tiered_system/experiments/runbooks/exp92_complete_hot_reads.py \
  --duckdb "$HOME/.local/bin/duckdb" \
  --influx-url http://127.0.0.1:18096 --org waggle \
  --source-report sage-data-rework/tiered_system/experiments/results/exp87-vm-1793830028/report.json \
  --run-dir "$PWD/sage-data-rework/tiered_system/experiments/results/exp92-host-$(date +%s)"
```

Substitute the fixture's actual listener and native DuckDB binary on the VM.
This script makes no Docker, engine-export, write, bucket-create or delete calls.
Successful full validation expects **18 gates**: three complete raw reads and 15
complete-input aggregate count/rejection checks. Control-query differences remain
visible and are not counted as correctness failures of the complete-read path.
This live test is still pending; local synthetic checks do not establish storage
read completeness.

### Live result, 2026-11-04

The first host run, `results/exp92-host-1793847673`, is retained and incomplete.
The metadata backup is `multipart/mixed`, not a single JSON document. The reader
now selects the `buckets` part and still saves only the allowlisted fixture
manifest. Nine offline tests pass, including that discard check.

[Live report](results/exp92-host-1793847958/report.json): DuckDB 2.0.0, Influx
2.7.7, **18/18 gates**, **6 raw rows**. Two clipped non-overlapping groups,
`[2025-01-01, 2025-01-06)` and `[2025-02-24, 2025-03-02)`, recovered every
mixed point. Counts are 2. Mixed arithmetic rejects. Whole-range and
type-predicate controls still return only the January point.

### Local checks and remaining production work

The local synthetic-input SQL check passed **15/15**:
[report](results/exp92_local_mixed_contract_20261104/report.json).
It uses all six fixture values as supplied input; it does **not** read Influx or
validate shard metadata. Reproduce with the exp92 command above plus `--local-only`.
Eight offline unit tests cover marker enforcement, kind boundary fixtures,
non-hour-aligned groups, clipping, truncation, deletion, overlap rejection,
metadata allowlisting, and rejecting mutations before network calls:

```sh
python3 -m unittest discover -s sage-data-rework/tiered_system/experiments/runbooks \
  -p 'test_exp91_92*.py'
```

Even if live exp92 succeeds, production needs a minimal filtered shard-metadata
provider, explicit metadata refresh/concurrency behavior and support for or
rejection of overlapping groups. The API should not download full backup
metadata per query. The chosen mixed-kind policy must be explicit: complete
counts and clear arithmetic errors are the current candidate, rather than
claiming new mixed-numeric arithmetic compatibility. Exporter staging,
reconciliation and deletion proofs must use the same unsigned logical-kind
convention before adopting it in production. These experiments do not enable
production aggregation or change the current warehouse.
