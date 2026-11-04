# Dynamic Influx → Iceberg → Pelican archival

> **Read-timeout policy update (2026-11-03):** The official service now expires
> read grants after a configurable unrenewed lease lifetime (`--read-lease-duration`,
> default 30m), followed by a one-minute deletion grace. The managed API renews
> active requests in the background; healthy queries may exceed the initial
> lease, while lease loss cancels the query. This supersedes historical nonexpiring-reader guidance below; writer
> permits still require explicit release/recovery. See the
> [current operator guide](https://github.com/waggle-sensor/beehive-hot-to-cold-loader/blob/main/README.md).

> **Official service location:** [`beehive-hot-to-cold-loader`](https://github.com/waggle-sensor/beehive-hot-to-cold-loader/blob/main/README.md).
> Current service code and validation commands live there. Historical
> `archive-exporter` paths below describe the original research runs; saved data,
> frozen source and preparations remain at `sage-data-rework/archive-exporter/state`.
> The deferred improvements in this plan apply to the official service.


Design accepted 2026-10-27. Implementation is local and unreleased. No production
configuration, published demo warehouse, or source data was changed. **Do not
enable deletion until the VM/native and isolated storage gates below pass.**

## Decisions

- Extend [archive-exporter](../archive-exporter/README.md) into a continuous Go
  service. Integrate admission/fencing into `beehive-influxdb-loader`, rather
  than introducing a write gateway.
- Default hot retention: **30 days (`720h`)**, configurable by
  `--hot-retention`. `--archive-before` optionally bounds validation/backfill
  scheduling at an exclusive UTC day. Process complete UTC days oldest-first. A day's end must
  be at or before `now - retention`; scheduling and failures can extend hot
  retention. Increasing retention does not restore archived days to Influx.
- Cover every observation in the selected `waggle` bucket, including records
  without plugin tags. This bucket follows the established single `value`
  field schema. Check that contract and stop on violations; never silently
  omit fields or values to make an export succeed.
- Keep Influx's own retention unlimited. Explicit deletion follows verified
  archival; automatic age-based expiration must not outrun it.
- Use a new isolated managed prefix, `cold-storage-archive-<unique>`. The
  adopted demo replica remains unchanged.

## Physical warehouse contract

The adopted architecture from [plan 10](10_iceberg_v3.md) remains:

1. Iceberg v3, partitioned by `day(time)`.
2. `time TIMESTAMPTZ_NS`, `name`, seven promoted tags, `meta MAP(VARCHAR,VARCHAR)`,
   and one `value VARIANT` at field ID **11**. MAP key/value IDs are 12/13.
3. Form chronological batches of at most **10,000,000 rows** within a UTC day.
4. Sort each batch **plugin → vsn → time**, then write one Parquet file.
5. Close a row group at **262,144 rows** or a plugin change, whichever comes
   first. Groups are plugin-pure. Large plugins span multiple groups; small
   plugins share files through separate groups. Final groups may be small.

Data directories use the actual Iceberg day partition field name and epoch-day
value under `warehouse/sage/obs/data/`; discovery remains under
`warehouse/sage/obs/metadata/`. The managed catalog validates schema, partition
transform and adopted layout properties on restart.

The Go packer is promoted from the validated native layoutpack experiment. It
checks VARIANT/field ID, UTC nanosecond timestamps, file row limits, and sort
order while packing bounded Arrow batches. Full decoded multiset comparisons
verify the packed files against their fixed source batches and complete day.

The coordinator uses the DuckDB **2.0.x native CLI** in separate processes for
spillable sorting, VARIANT construction and scans. This avoids embedding an
additional CGO runtime in the Go coordinator. The API continues using its native
DuckDB Go Arrow path and still requires `-tags=duckdb_arrow`.

## Components and durable interfaces

The archiver is a singleton with an exclusive process lock, private persistent
state/PVC, SQLite control journal and SQLite Iceberg SQL catalog. SQLite uses
WAL and `synchronous=FULL`. Pelican holds immutable Parquet, manifest and metadata
objects. The catalog performs authoritative commits; a filesystem catalog or
Pelican compare-and-swap is not assumed.

Direct remote commits replace the demo's whole-replica-copy workflow. The HTTPS
FileIO validates the configured host and table prefix on every operation,
streams data through disk, rejects redirects, and signs prefix-scoped tokens.
Automatic object deletion is disabled. Metadata is published before its
`version-hint.text`; the hint contains the metadata stem without a newline.
The normal iceberg-go CreateTable path renumbers nested field IDs, so creation
uses an atomic catalog transaction with explicit schema IDs to preserve value 11.

The internal HTTP API uses distinct loader, reader and operations credentials:

| Endpoint | Role | Durable behavior |
| --- | --- | --- |
| `POST /v1/writes/acquire` | Loader | Accepts bucket, request ID, owner and UTC days; returns permitted hot days |
| `POST /v1/writes/release` | Loader | Completes a write permit after synchronous Influx completion |
| `POST /v1/late` | Loader | Accepts request ID and typed observations; commits inbox receipt before success |
| `POST /v1/quarantine` | Loader | Retains invalid original messages before source acknowledgement |
| `POST /v1/reads/acquire` | Reader | Pins metadata URL, frontier, coverage start and generation under a read grant |
| `POST /v1/reads/release` | Reader | Releases a grant after the result closes |
| `POST /v1/status` | Operations | Reports backlog, grants, frontier and errors |
| `GET /metrics` | Operations | Exposes aggregate `sage_archive_*` gauges |

Every POST includes the configured bucket. IDs make inbox retries durable and
payload-sensitive. Late records encode nanosecond time as an integer, complete
string tags, and value as a kind plus lossless text (`float`, `int`, `uint`,
`string`, `bool`). These are internal interfaces, not public query schema changes.

### Loader behavior

All writers of `waggle` must use the updated loader protocol, including imports.
The deployed recent-loader writes **`waggle-recent`**, so it is outside this
coordinator's bucket unless a separate service is configured for that bucket.
A pod UID plus an automatically appended process UUID identifies each loader
incarnation; the API uses the same owner convention. This keeps recovery of a
stopped incarnation separate from a restarted container. Per batch:

1. Parse and validate messages, including the existing node/user relationship.
2. Obtain a permit before sending any hot records to Influx.
3. Split mixed-day batches: allowed days go to Influx; fenced/already-cold days
   go to the durable inbox with their original timestamps.
4. Complete synchronous Influx writes, complete the permit, then acknowledge
   source RabbitMQ deliveries. Never acknowledge a partial/ambiguous write.
5. Invalid messages require durable quarantine. Unmanaged mode uses a confirmed,
   persistent RabbitMQ quarantine publication instead of discarding a log line.

Existing integer-to-float normalization remains consistent between hot and late
routes. This project does not silently change loader value semantics. Delivery
is at least once; broker redelivery may repeat an observation. Corrections use
last durable inbox acceptance order, not a claim of global publisher ordering.

A fence prevents new permits for its day and waits for every existing permit.
Grants never expire automatically. A stopped or unreachable process might still
have a request executing at Influx. Explicit owner recovery requires confirmed
termination and drained/cancelled requests.

### API behavior

Managed mode acquires a read view before routing each request. That exact
frontier determines the split, and that exact metadata URL is passed into cold
execution. Result closure releases the read grant. The wrapper preserves native
Arrow streaming through the existing composite-results interface.

Coordination failure does not fall back to a stale static watermark. A query
before declared coverage or for a different bucket fails. Static configuration
remains available for unmanaged deployments. Public Arrow/NDJSON fields and the
Sage Data Client remain unchanged.

## Archival and late-data lifecycle

Durable archive states:

```text
FENCING → EXPORTING → EXPORTED → RECONCILED → BUILT → COMMITTED
  → REMOTE_VERIFIED → ROUTED → DELETING → DELETE_REQUESTED → COMPLETE
```

A job records its creation time, frozen eligibility cutoff and retention setting.
Later configuration changes do not silently change an in-progress job's policy.

- **Fence:** atomically stop hot admission for the day, then drain granted writes.
- **Export:** use the matching `influxd inspect export-lp` binary against a
  read-only engine mount. Parameterize bucket ID, engine path and executable.
  Capture subprocess failures and enforce exact `[day, next day)` bounds.
  Stream sequenced raw records to disk without retaining a day-wide point map.
- **Reconcile:** externally sort the export by timestamp, measurement, canonical
  complete tags and descending replay sequence; stream the first row of each
  identity into a normalized export. Independently query the fenced day through Influx's API in
  hourly streaming CSV slices. Compare both complete typed multisets using
  spillable DuckDB `EXCEPT ALL` comparisons: time, measurement, complete tags,
  value type and value. Counts/hashes alone do not authorize deletion.
  Mismatches retain the fence and return to engine export on retry.
- **Build:** stage chronological batches on disk, sort within each file, pack
  bounded plugin groups, and compare complete packed output against input.
- **Commit:** upload and SHA-256 verify immutable objects, then atomically commit
  the day's files. Persist job identity in catalog properties/snapshot summary.
  An uncertain commit is resolved against the catalog before attempting replay.
- **Verify:** scan the exact committed remote snapshot through Pelican and
  compare the complete logical day against expected data. Verify object hashes
  and published hint. Persist verification reports and snapshot identity.
- **Route:** atomically activate the verified snapshot and contiguous frontier.
  Empty days also require successful export, reconciliation and verification.
- **Drain readers:** wait for API requests using an older hot frontier to close.
- **Delete:** only when explicitly enabled, reverify the remote snapshot, validate
  source bucket identity/organization and unlimited retention, delete the exact
  day, then query to confirm emptiness. Retry ambiguous deletion without another
  export or Iceberg append.
- **Complete:** persist completion before cleaning transient local data. Preserve
  JSON journals and verification evidence. `--keep-staging` retains data for
  debugging; completed cleanup is retried at startup.

Deletion-disabled mode continues publishing days while retaining hot copies;
routed deletion jobs remain durable. Late work and archival can continue without
waiting for deletion to be enabled. Automatic snapshot expiry, orphan cleanup,
compaction and remote object GC are outside this release.

### Reconnecting edge nodes

Original measurement timestamps decide placement, regardless of arrival time.
A measurement arriving for a fenced or cold day is durably queued before its
RabbitMQ acknowledgement. By default, late jobs are scheduled every five minutes,
plus processing/backlog time. Each job captures a bounded inbox slice (at most
50,000 records / approximately 64 MiB of serialized input).

For an affected day, merge the active cold day with captured inbox records using
measurement, full tags and nanosecond timestamp as identity. The `value` field
is fixed by the bucket contract. The latest accepted correction wins. Rebuild
the same physical layout and atomically replace that day's Iceberg files in one
snapshot. Verify remotely before activating it or completing inbox entries.
Old snapshots remain available to pinned readers. Later inbox arrivals stay
pending for the next replacement.

The explicit coverage start must include the intended historical domain. Input
older than that start is retained in the inbox but requires an explicit backfill;
the API does not claim unverified earlier coverage.

### Bootstrap of previously verified history

Optional `--bootstrap-warehouse` plus exclusive `--bootstrap-before` incorporates
an already audited warehouse, such as the adopted March/April replica. Pin its
metadata location in durable configuration. Before that boundary, build each day
from its source cold snapshot plus reconciled hot data; hot values win matching
identities. Publish only to the new managed prefix. The source stays read-only.
Bootstrap source/boundary and source bucket identity are bound to the state
directory; changing them in place is refused.

## Rollout, validation and current status

1. Build the exporter, loader and API locally; preserve `duckdb_arrow` in API builds.
2. Start `--coordinate-only` against isolated fixtures. This admits writes and
   serves read views without fencing, export, publication or deletion.
3. Move every selected-bucket writer to coordination; enforce bypass restrictions.
   Set loader owner to pod UID. Then enable archival with `--writers-coordinated`.
4. Use a fresh managed prefix and audited bootstrap source if required. Enable
   managed API read views and compare both HTTP formats with the source.
5. Prove engine export/query equivalence under WAL writes, overwrites, tombstones,
   compaction, and midnight boundaries on the matching Influx version.
6. Pass the full native lifecycle and an isolated real-Pelican publication.
7. Drain static readers. Only then enable
   `--delete-enabled --readers-coordinated`. No production rollout is automatic.

Local checks cover concurrent and durable fences/receipts, full-day eligibility, contiguous
frontiers, read pinning/draining, role isolation, retry behavior, failed verification,
partial deletion, CSV precision/types, explicit Iceberg schema IDs, and scoped
streaming transport. The native test is opt-in:

```sh
cd sage-data-rework/archive-exporter
SAGE_ARCHIVE_DUCKDB2=/absolute/path/to/duckdb go test -count=1 -v ./...
```

`TestNativeLifecycle` uses real DuckDB 2.0, native Parquet packing and Iceberg
commits with an isolated local TLS object fixture and simulated Influx. It covers
a complete day, deletion, a late correction and old-snapshot readability. It
requires local listener permission and does not contact real Pelican.

On this workstation DuckDB CLI/Python are **1.5.6**. The 2.0 lifecycle, the
matching Influx engine, the pinned API suite, and the loader SDK tests were run
in the Rancher Desktop VM. Results are below. Deletion stays disabled: real
Pelican publication and a live broker run are still open.

See the [operator guide](../archive-exporter/README.md) for startup flags, secret
roles, metrics, disk cleanup, backup and explicit stopped-owner recovery. No
commit, push, live deployment, source deletion or real Pelican publication was
performed while implementing this service.

The matching Influx engine gate also has an isolated Compose fixture under
`archive-exporter/testing`. `TestInfluxEngineConsistency` creates its own bucket,
checks immediate WAL/cache overwrites and a later post-snapshot pass against the
query API, and proves the delete stop boundary retains the next day's first
nanosecond. It is opt-in through `SAGE_ARCHIVE_FIXTURE_*`; see the operator guide.

### Local implementation checks (2026-10-27)

- Exporter: **23 unit tests passed**, three native/source integration tests
  skipped without their explicit fixture configuration; build and vet passed.
- Coordinator/runner race checks passed, including concurrent permit/fence admission.
- API managed routing and existing tiered/query tests and targeted vet passed.
- The real HTTP handler's managed Arrow/NDJSON regression passed in a temporary
  fixture using cached Arrow 18.8 dependencies. This does not replace the pinned
  API/native suite; the repository's dependency pins were unchanged.
- Loader: nine tests passed with temporary dependency stubs for unavailable SDK
  imports. The VM run below repeats them with the real packages installed.
- Markdown links, shell example syntax, Python syntax and fixture YAML checked.

`TestNativeLifecycle` includes an initially empty day, then a nonempty day,
verified deletion, a late correction and old-snapshot readability.
`TestNativeBoundedPluginPacking` checks a 600,005-row mixed-plugin file, including
large-plugin splitting, small plugins sharing files, null plugins, VARIANT values
and nanosecond timestamps.

### VM checks (2026-10-27)

Ran in Linux containers on the Rancher Desktop VM (aarch64, DuckDB CLI **2.0.0**,
Go **1.26.5**, `influxd` **2.7.7** from the official image). Demo containers were
left running. The isolated fixture used port 18087 and was stopped afterward.
No commit, push, production change, source deletion, or real Pelican publication
was performed.

DuckDB 2.0 required three coordinator adjustments before the native tests passed:

- List filters use `lambda x:`. The `x ->` form is rejected.
- An HTTP secret ignores the global certificate setting. The lifecycle fixture
  alone adds `VERIFY_SSL false` to its read secret.
- `-json` emits one document per statement. Callers keep the last document, which
  is the query result after `CREATE SECRET`.

InfluxDB 2.7.7 required three export/delete adjustments before the engine gate passed:

- `inspect export-lp` treats `--end` as inclusive and accepts only whole-second
  RFC3339. The exporter drops that single next-midnight instant so the day stays
  `[start, next midnight)`.
- The same export replays the earlier WAL value and the overwrite. The later line
  wins, matching the query API. That collapse holds one day's logical points in
  memory before the spillable DuckDB comparison.
- The delete API keeps its stop instant. A day delete stops at the previous
  nanosecond, so the next midnight remains.

Results:

- Exporter suite with `SAGE_ARCHIVE_DUCKDB2` set: passed.
  `TestNativeLifecycle` passed (empty day, nonempty day, verified deletion, late
  correction, old snapshot). `TestNativeBoundedPluginPacking` passed.
  `TestInfluxEngineConsistency` was skipped in this run because the fixture
  variables were unset.
- `TestInfluxEngineConsistency`: passed in 4.0s. Covered the immediate WAL
  overwrite, a later snapshot pass (TSM file count went from 0 to 1), an empty
  deleted day, and the retained next midnight. Tombstones and a full compaction
  cycle were not part of this fixture run.
- API `CGO_ENABLED=1 go test -tags=duckdb_arrow ./...`: passed for arrow, cold,
  hot, managed, tiered, and HTTP. `TestIcebergV3RoundTrip` stayed skipped. It was
  not pointed at the running demo catalogs.
- Loader `python -m unittest discover` with installed `pywagglemsg` 0.3,
  `influxdb_client` 1.26, and `pika` 1.2: **9 tests passed**. External IO is
  still mocked. A live RabbitMQ and Influx run remains open.

Deletion remains disabled. The open rollout gates are an isolated real-Pelican
publication, the live loader path, and draining static readers before
`--delete-enabled --readers-coordinated`.

### Review after VM validation (2026-10-27)

The recorded VM runs close the DuckDB 2.0 native lifecycle/packing, pinned API
build/test, and real loader-package import gates. The lambda syntax, final JSON
result selection, and fixture-only HTTP-secret TLS override match the tested
runtime behavior. Production read secrets retain certificate verification.
The export/delete endpoint adjustments preserve the intended logical half-open
day; keep them covered against the pinned InfluxDB version.

**Resolve the export memory bound before a full-day rollout.** The WAL-overwrite
adjustment in `internal/storage/influx.go` now retains one `control.Record` and
one composite string key per unique observation in `latest`. Memory grows with
all observations in a UTC day. At approximately 140 million unique points,
64-byte record structs plus 16-byte string-key headers alone account for about
10.4 GiB, before key contents, per-record tag maps, strings and map overhead.
DuckDB's 3 GB limit does not constrain this Go map. The 30,000-point engine
fixture and the separate 600,005-row packer test do not establish full-day
export memory capacity.

The next implementation should stream raw export records with a monotonically
increasing export sequence, then perform spillable deduplication by timestamp,
measurement and canonical complete tags, retaining the greatest sequence.
Preserve value types and nanoseconds. Continue the full comparison against the
query API: export order alone is not sufficient verification of Influx's
logical current state. Remove the day-wide Go map; retain the tested endpoint
handling and all deletion guards. Benchmark peak Go/container memory and staging
space on realistic day coverage.

Strengthen the matching-engine fixture before deletion is enabled:

- Write an older value into TSM, then overwrite it in WAL and reconcile again.
- Exercise tombstones and a full compaction cycle; those were not covered by
  the recorded fixture run.
- Add observations at `start - 1 ns`, `start`, `end - 1 ns`, `end`, and
  `end + 1 ns`, checking that export/delete affect exactly the target day.

After the bounded-memory export passes those checks, proceed with the isolated
real-Pelican publication and live loader/broker path, then drain static readers.
Deletion remains disabled. This review changed documentation only and preserved
the VM implementation fixes; no real storage was accessed and no commits or
pushes were made.

### Bounded-memory export normalization implemented (2026-10-27)

This supersedes the day-wide `latest` map identified in the preceding review.
The implementation removes that map and its delimiter-based `seriesKey`:

1. `Influx.Export` emits raw NDJSON immediately, with a monotonically increasing
   positive `seq` for every retained physical replay record. It preserves the
   tested inclusive-export boundary handling, nanoseconds and exact typed value
   text. No per-day identity set is retained.
2. `DuckDB.DeduplicateExport` sorts primitive staging columns by `(time, name,
   canonical complete tag JSON, seq DESC)`. The external sort uses DuckDB's
   memory budget and spills to the state directory. This avoids a day-sized Go
   map and avoids relying on a day-sized window/hash aggregate for deduplication.
3. A streaming Go Parquet reader emits the first row per identity to
   `export-dedup.ndjson`. Reader batches are 65,536 rows, temporary sorted Parquet
   targets 122,880-row groups, and the preceding identity is retained across
   batch boundaries. Go memory depends on reader/row-group buffers, row widths
   and Parquet footer metadata rather than one record/map entry per observation.
4. The normalized output must still match the independent query-visible Influx
   multiset in full before build. Archive and bootstrap builds now consume that
   normalized file. The adopted Iceberg v3/VARIANT schema, day partitions,
   chronological file limits and bounded plugin row groups are unchanged.

Normalization publishes only after successful reading, flush, file sync and
rename/directory sync. Failure keeps the job fenced and prevents progression.
Interrupted scratch is reclaimed on retry. Missing normalization on an older
`RECONCILED` job rewinds to `EXPORTED`; older raw data without sequence is
re-exported. Completed build manifests remain reusable. Completed-job cleanup
removes raw/normalized/sorted staging, while retaining `export-dedup.json` counts
and artifact sizes. Sort spill is additional space, not included in those sizes.

`--duckdb-memory` / `DUCKDB_MEMORY` now makes the existing default `3GB` budget
configurable. This is a DuckDB buffer budget, not a process/container RSS cap.
Leave headroom for native allocations, the Go process and `influxd`; provision
staging disk for both NDJSON copies, temporary sorted Parquet, spill and later
reconciliation/build artifacts. There is no claim that arbitrarily small memory
limits will succeed.

#### Local validation and supplemental scale smoke

Unit/race coverage checks immediate emission before source EOF; positive replay
sequence; overwrite selection across reader batches/row groups; exact large
integers, unsigned integers, strings and booleans; delimiter-bearing complete
identities; empty exports; bad ordering/sequence rejection; half-open nanosecond
bounds; preservation of published output on sort failure; and safe fenced retry
and completed-job cleanup. Native lifecycle and matching-engine fixtures now
exercise normalization before reconciliation/build as well.

Local `go test ./...` passed 31 tests with four opt-in native/resource tests
skipped. `go test -race ./...`, `go vet ./...`, the service build and
`git diff --check` also passed.

This workstation has DuckDB **1.5.6**, not 2.0. A temporary copy outside the
workspace ran only the primitive-column sort/stream harness against that CLI;
the temporary copy omitted the harness's 2.0 startup check. The application and
checked-in tests retain strict DuckDB **2.0.x** checks; no 1.5 compatibility was
added. These supplemental results establish local sort/stream behavior, **not**
DuckDB 2.0 integration, real engine export or full-day capacity:

| Unique points | Raw replay rows | Overwrites removed | Parse/export | Sort + collapse | Go peak RSS | Largest child peak RSS | Sort peak spill |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2,000,000 | 2,200,000 | 200,000 | 1.71 s | 1.60 s | 102.1 MiB | 535.2 MiB | 6.7 MiB |
| 20,000,000 | 22,000,000 | 2,000,000 | 19.75 s | 16.85 s | 90.4 MiB | 656.6 MiB | 1.09 GiB |

Both successful runs used `512MB` DuckDB limits, two threads, reversed timestamps
and a two-tag synthetic integer workload. Every normalized row's identity,
value and retained sequence was checked. Sampled staging file peaks were about
0.50 GiB and 5.05 GiB respectively. Go/child RSS are separate high-water marks,
not a simultaneous combined peak; staging sampling can miss transient/unlinked
files. An initial 2-million-point run at `128MB` failed inside DuckDB allocation;
it did not publish normalized output or progress any archival state. The larger
run demonstrates disk spill with no day-sized growth in Go RSS, not a hard total
container bound.

#### VM confirmation

The [operator guide](../archive-exporter/README.md#bounded-export-normalization)
contains the reproducible `TestNativeExportDedupScale` command. Use a precompiled
binary and separate fresh directories/processes for 2 million, 20 million and
approximately 140 million points with the pinned DuckDB 2.0 CLI. The harness
requires no Influx/Pelican access, verifies every retained point and saves
`export-scale.json` with timing, artifact sizes, separate Go/child RSS, DuckDB
buffer/spill peaks and sampled staging space. A cgroup peak, when available,
covers the container lifetime; use a fresh container for a meaningful run peak.
Follow with real-day export/query reconciliation to cover representative row
widths and the real `influxd` resource footprint.

Rerun the native lifecycle and matching-engine gates on the updated code. The
TSM-to-WAL overwrite, tombstone/full-compaction and additional boundary probes
listed in the preceding review still require VM coverage, as do isolated real
Pelican publication and the live loader/broker path. Deletion stays disabled by
default. No production services, Pelican/NRDstor paths or `/node-data` were
accessed, and no commits or pushes were made.

#### VM confirmation results (2026-10-27)

Ran on the Rancher Desktop VM with a precompiled `archive-storage.test`, DuckDB
CLI **2.0.0**, a **512MB** sort budget, and a fresh container and directory per
run. Ten percent of identities were overwritten and timestamps were reversed.
Every retained row was checked. Reports are in
`archive-exporter/state/export-scale-{2m,20m,140m}.json`. The large staging
directories were removed after each run.

DuckDB 2.0 prints ANSI deprecation warnings on stdout for
`PRAGMA enable_profiling`, which broke JSON parsing, and it nests profile peaks
under `system`. The harness now uses `SET enable_profiling` /
`SET profiling_output` and reads `system.peak_buffer_memory` and
`system.peak_temp_dir_size`.

| Unique points | Raw replay rows | Export | Sort + collapse | Go peak RSS | Largest child RSS | Sort buffer | Sort spill | Sampled staging | Cgroup lifetime peak |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2,000,000 | 2,200,000 | 2.11 s | 2.18 s | 95.8 MiB | 519.6 MiB | 423.0 MiB | 0 | 506 MiB | 915 MiB |
| 20,000,000 | 22,000,000 | 26.47 s | 32.37 s | 101.1 MiB | 609.9 MiB | 519.4 MiB | 930 MiB | 5.08 GiB | 5.00 GiB |
| 140,000,000 | 154,000,000 | 166.29 s | 203.97 s | 111.7 MiB | 703.3 MiB | 503.8 MiB | 6.86 GiB | 35.98 GiB | 6.90 GiB |

Go RSS stayed near 100 MiB from 2 million through 140 million points. The 140
million run's container limit was 8 GiB; its cgroup peak includes page cache and
sat under that cap while staging on disk was 36 GiB. Child RSS is the largest
reaped DuckDB process, not a combined peak. The 140 million run also spent time
checking all retained rows; wall time for that process was 596 s.

`TestNativeLifecycle` passed (2.31 s) and `TestNativeBoundedPluginPacking`
passed (1.95 s) on this code. `TestInfluxEngineConsistency` passed (4.19 s) on
a fresh InfluxDB 2.7.7 fixture: the first pass saw 0 TSM files and 1 WAL file,
and the later pass saw 1 TSM file with the WAL still present. That is a
snapshot during an open WAL, not a completed compaction, a tombstone, or an
overwrite applied only after the older value was already in TSM. The five
explicit nanosecond boundary points are still not in this fixture. The engine
day is the small synthetic bucket, not a wide production day. The fixture was
stopped afterward. Deletion remains disabled. Real Pelican publication and the
live loader path remain open.

#### Review of bounded-export VM results (2026-10-27)

The three saved `state/export-scale-*.json` reports agree with the table above.
The **day-wide Go-map scale blocker is resolved for export normalization**:
unique points increased 70-fold from 2 million to 140 million while Go peak RSS
increased from 95.8 to 111.7 MiB. The largest run checked every retained point
and removed all 14 million superseded replay rows. DuckDB spilled 6.86 GiB
instead of requiring a Go record/map entry for each point. Keep this design.

The VM changes to `export_scale_test.go` are appropriate for the pinned runtime:
use `SET` for profiling rather than the warning-producing `PRAGMA` form, and
read the profile's nested `system` peaks. These changes affect the measurement
harness; the strict DuckDB 2.0 check and archival/deletion controls remain.
Local `go test ./...`, `go vet ./...` and `git diff --check` passed after review;
the four opt-in native/resource tests were not rerun on this workstation.

The 140-million-point parse/export and normalization stages took **370.26 s
(6 min 10 s)** combined; the reported **596 s** process wall time also includes
the harness's full output check and setup. Neither is an end-to-end archival
time: real `influxd`, independent Influx query/reconciliation, warehouse packing,
upload, remote verification and deletion are outside this scale measurement.

Capacity planning must use the measured **6.90 GiB cgroup peak** and **35.98 GiB
sampled staging**, not the `512MB` DuckDB setting or Go RSS alone. The fresh
8 GiB container passed this synthetic workload, but that does not establish
8 GiB as sufficient for the complete service or wider production rows. The
cgroup figure includes page cache; later build/reconciliation artifacts require
additional staging capacity. Keep the default service budget and container
sizing subject to a representative full-day lifecycle measurement.

The next source-consistency work is the explicit TSM-to-WAL overwrite,
tombstone/full-compaction and five nanosecond boundary probes listed above.
The existing snapshot pass does not substitute for those cases. Then validate
a representative real-day lifecycle in a new isolated managed prefix, including
real Pelican readback, and the live loader/broker path. Reader migration/drain
remains required before enabling deletion. No code changes, production/storage
operations, commits or pushes were performed in this review.

#### Expanded matching-engine gate VM run (2026-10-27)

`TestInfluxEngineConsistency` now tests the remaining source states explicitly:

- **WAL overwrite:** seed an earlier `waltemp` value, overwrite it, require both
  physical replay values, and reconcile the last value while no TSM exists.
- **TSM → WAL overwrite:** wait until a WAL-free private copy of immutable TSM
  exports the older `temp` value and the deletion victim. Only then write the
  newer value. Require both values in the live raw replay, the newer value in
  query/normalized output, and the older value still present without the newer
  value in the TSM-only view. A phase that advances too quickly fails coverage
  rather than being treated as a pass.
- **Second snapshot:** write another 30,000 filler points and wait for a TSM-only
  export to match the independent expected 60,008-point day, including the
  newer temperature. The private views copy TSM/tombstone files, never WAL or
  hard links, and never mutate the source engine.
- **Tombstones:** explicitly delete only the `victim` measurement in the
  test-created bucket. Require an observed tombstone and capture a private
  WAL-free tombstoned engine view. Its export must match the expected remaining
  60,007 points. Repeat full reconciliation against the running engine/query API.
- **Full compaction:** observe an increase in the completed full-compaction
  duration histogram for this exact bucket/shard, with no new full-compaction
  failures and no active full compaction. Also require replacement of the old
  TSM/tombstone files, remaining TSM and no remaining tombstones. Reconcile again
  and reject resurrection of the victim in the raw export. A cache snapshot,
  another shard's compaction or a completion counter alone is insufficient.
- **Nanosecond boundaries:** seed separate observations at `start - 1 ns`,
  `start`, `end - 1 ns`, `end` and `end + 1 ns`. Compare exports, query results
  and independent expected multisets for the target/adjacent days before and
  after deleting the target day. The target day must become empty, while the
  previous day retains one point and the next day retains two.

The dedicated Compose fixture now leaves a 10-second snapshot cold window and
30-second full-compaction cold window. Bounded readiness polling replaces the
old three-second sleep. Requests/native work inside readiness probes inherit
its two-minute deadline. The fixture rejects `/node-data` paths and symlinked
engine trees, creates a unique bucket, and only deletes within that bucket;
production archival deletion remains disabled.

The [matching-engine operator command](../archive-exporter/README.md#matching-influx-engine-gate)
now recreates the dedicated fixture with a fresh local state directory and sets
`SAGE_ARCHIVE_FIXTURE_REPORT_DIR` to a separate fresh report directory. Run it on
the VM using the pinned InfluxDB 2.7.7 and DuckDB 2.0 executables. The report
`engine-consistency.json` records success/failure, last stage, per-phase counts,
file inventories and full-compaction metrics; NDJSON and the captured tombstoned
engine remain available when using a persistent report directory. Failed test
buckets remain for inspection; a successful run removes only its own bucket.

Local unit checks cover metric scoping/rejection, WAL-free copy isolation and
symlink rejection, plus independent boundary/deleted-day expectations. All 34
local tests passed, with four opt-in native/resource tests skipped. Race checks,
vet, service build, Compose configuration validation and `git diff --check`
passed.

The expanded gate was then run on the Rancher Desktop VM with pinned InfluxDB
2.7.7 and DuckDB 2.0. A fresh fixture directory
`archive-exporter/state/source-fixture-1793145313` and a separate report
directory `archive-exporter/state/engine-gate-1793145313` were used. The test
failed at `waiting-for-full-compaction` after 156.68s. Bucket
`sage-archive-fixture-a2c7709e-bb8b-4563-9456-a3296338807f`
(`082b58396abfda93`, shard `1`) was not removed.

Phases that reconciled, from `engine-consistency.json`:

| Phase | Unique rows | Superseded | Engine at reconcile |
| --- | ---: | ---: | --- |
| WAL overwrite | 30,008 | 1 | 0 TSM, 1 WAL |
| TSM → WAL overwrite | 30,008 | 1 | 1 TSM plus WAL; the older temperature was proven in a WAL-free copy before the new write |
| Second snapshot | 60,008 | 1 | 2 TSM |
| Tombstone | 60,007 | 0 | tombstone observed; the captured WAL-free view matched the deleted day |

Full compaction did not. The scoped histogram stayed at
`completed_full=0`, `failed_full=0`, `active_full=0` for the whole two-minute
wait. During that wait the two TSM files and
`000000001-000000001.tombstone` were replaced by
`000000002-000000002.tsm`. Live `/metrics` recorded that replacement as
`storage_compactions_duration_seconds_count{level="1"} 1` and never emitted a
`level="full"` duration series. InfluxDB 2.7.7 plans `level="full"` only when
more than one generation is still present after the 30-second full-compaction
cold window. A tombstone lets the level-1 planner compact immediately, below
its usual eight-generation threshold, so it applied the tombstone and left one
generation before that window. The five boundary observations were seeded, but
the target-day deletion and adjacent-day checks did not run.

A follow-up write on this same bucket, after the failure, created a second
generation with no new tombstone. After the 30-second cold window,
`level="full"` completed once (`failed=0`, `active=0`) and replaced
`000000002-000000002.tsm` and `000000003-000000001.tsm` with
`000000003-000000002.tsm`. The fixture setting can produce a full compaction.
This test deletes while the shard still has only level-1 generations, so the
level planner wins and the `level="full"` assertion cannot pass. That extra
point means the engine directory is no longer the exact failure snapshot; the
JSON report is.

This run does not close the TSM/tombstone/full-compaction VM gate. Representative
full-day lifecycle, isolated real Pelican publication and live loader/broker
validation remain subsequent gates. No production storage was accessed, no
production code/deletion configuration was changed, and no commits or pushes
were made. The fixture container was stopped afterward; the report and engine
directory remain.

The observation strategy uses the documented Influx [compaction metrics](https://docs.influxdata.com/influxdb/v2/reference/internals/metrics/)
and [engine export interface](https://docs.influxdata.com/influxdb/v2/reference/cli/influxd/inspect/export-lp/).

#### Review and correction of compaction fixture sequencing (2026-10-27)

The saved `state/engine-gate-1793145313/engine-consistency.json` confirms the
reported failure: WAL and TSM→WAL overwrites reconciled at 30,008 unique points;
the second snapshot reconciled at 60,008; the captured tombstoned view and live
deleted-victim day matched 60,007 points. The original TSM/tombstone paths were
replaced, but the scoped full-compaction histogram remained zero. This is a
fixture sequencing error, not evidence of an archival data mismatch. The gate
correctly refused to count level-1 compaction as a completed full compaction.
The boundary deletion/survival assertions remain unvalidated by that failed run.

The test now separates the two compaction phases:

1. After captured-tombstone and live reconciliation, wait for replacement of the
   tombstoned TSM files and removal of tombstones at **any** compaction level.
   Reconcile the resulting 60,007 points again and reject resurrection of the
   victim in raw replay. Record this as `tombstone-compaction`, separately from
   full-compaction evidence.
2. Record the scoped full-compaction baseline, then rewrite the existing
   `filler` point at `1740787200000010000` with the same integer value `0`.
   This creates new physical work without changing the expected logical rows.
   Wait for an additional tombstone-free TSM file/generation and verify its
   TSM-only normalized view against the independent expected dataset. Save the
   phase's immutable input paths under `full-compaction-inputs`.
3. Require an increased **full** completion histogram, no new full failures,
   no active full compaction, replacement of these newly recorded input files,
   remaining TSM and no tombstones. Level-1 metrics still cannot satisfy this
   check. Only then continue post-full reconciliation and target/adjacent-day
   deletion checks.

This follows the successful follow-up generation described in the VM notes,
while avoiding its extra logical point. It preserves the strict full-compaction
assertion and the 60,007-point expected day. A local regression test replays the
observed level-1 file replacement with a zero full count, then distinct fresh
input/replacement files and a real full-count increase, including rejection of
leftover tombstones. The [operator command](../archive-exporter/README.md#matching-influx-engine-gate)
is unchanged; rerun it with fresh fixture/report directories. Do not reuse the
failed bucket or its modified engine as a pristine input.

Local unit tests, race checks, vet, service build and `git diff --check` passed
after the correction. The opt-in native lifecycle and packing gates were not
rerun for this sequencing change.

#### Corrected engine gate VM run (2026-10-27)

The operator command was rerun on the Rancher Desktop VM with pinned InfluxDB
2.7.7 and DuckDB 2.0, using fresh directories
`archive-exporter/state/source-fixture-1793148864` and
`archive-exporter/state/engine-gate-1793148864`. The earlier failed bucket and
its engine were not reused. `TestInfluxEngineConsistency` passed in 86.71s.
`engine-consistency.json` records `passed: true` and `stage: complete`. The
successful run removed only its own bucket,
`sage-archive-fixture-3ba3ec83-7a5d-4cf0-b242-8464ea98b2f5`
(`e78693484072f007`, shard `1`).

| Phase | Unique rows | Superseded |
| --- | ---: | ---: |
| WAL overwrite | 30,008 | 1 |
| TSM → WAL overwrite | 30,008 | 1 |
| Second snapshot | 60,008 | 1 |
| Tombstone, before and after cleanup | 60,007 | 0 |
| After full compaction | 60,007 | 0 |
| Previous day, before and after target-day deletion | 1 | 0 |
| Next day, before and after target-day deletion | 2 | 0 |
| Target day after deletion | 0 | 0 |

Tombstone cleanup replaced the tombstoned TSM files and left no tombstone, then
the live day still reconciled at 60,007 points with the victim absent from raw
replay. The full-compaction baseline was `completed_full=0`, `failed_full=0`,
`active_full=0`. After the same-value `filler` rewrite, the saved inputs were
`000000002-000000002.tsm` and `000000003-000000001.tsm`, with no tombstone.
Full compaction then completed once for this bucket and shard, with no failure
and no active full compaction, and replaced those inputs with
`000000003-000000002.tsm`. Raw replay after that compaction still omitted the
victim. Deleting the target day emptied it and left the previous day at one
point and the next day at two.

This closes the expanded matching-engine gate, including full compaction and
the five boundary instants. Representative full-day lifecycle, isolated real
Pelican publication and the live loader/broker path remain subsequent gates.
No production storage was accessed, deletion stayed disabled, and no commits
or pushes were made. The fixture container was stopped afterward; the report
directory remains.

#### Review of the successful corrected engine gate (2026-10-27)

Accepted: the expanded matching-engine gate is closed for the pinned InfluxDB
2.7.7/DuckDB 2.0 fixture. Review of the saved JSON confirmed `passed: true`,
`stage: complete`, all eleven reconciliation phase counts, tombstone replacement
and the scoped full count increasing from zero to one without failures or an
active full compaction. Both saved full-compaction input paths differ from the
final output path. The local post-delete logical NDJSON files independently
confirm the previous-day timestamp `1740787199999999999`, next-day timestamps
`1740873600000000000` and `1740873600000000001`, and an empty target day.
No further fixture correction is indicated by these results.

Current validation status:

| Gate | Status | Scope |
| --- | --- | --- |
| Bounded export normalization | Passed | Synthetic 140 million unique points / 154 million replay rows; full output checked |
| Matching-engine source consistency | Passed | WAL and TSM→WAL overwrites, snapshots, captured tombstones, cleanup, full compaction, exact day boundaries |
| Native lifecycle and packing | Previously passed | Local object fixture, Iceberg v3/VARIANT and bounded plugin groups |
| Real managed Pelican publication/readback | Pending | Fresh isolated managed prefix; existing demo warehouses remain unchanged |
| Live loader/broker integration | Pending | Admission/fencing, durable late arrivals and acknowledgement/replay behavior |
| Representative full-day archival capacity | Pending | Real source export, query/reconciliation, packing, publication and remote verification together |
| Production reader migration/drain | Pending | Required before enabling archival deletion |

Next, validate a small isolated live path through loader/broker → Influx →
archiver → real Pelican → managed API/client, including a late arrival and
readback after snapshot replacement. This will exercise the external integrations
before investing in a representative full-day lifecycle/resource run. The source
gate's direct fixture deletion does not authorize production deletion or replace
the archiver's verify/route/drain checks. Deletion remains disabled by default.

This review checked saved artifacts and updated documentation only; no native
tests were rerun, no live services or storage were changed, and no commits or
pushes were made.

#### Small live gate implemented (2026-10-27)

Added [the live gate harness](../archive-exporter/testing/live/README.md). From
the workspace root, run:

```sh
python3 sage-data-rework/archive-exporter/testing/live/run.py
```

It builds curated snapshots of the current workspace archiver, loader, API and
client. Each run creates a new private Compose project/network, Influx/RabbitMQ
services, local state directory and `cold-storage-archive-live-<uuid>/warehouse`
on real Pelican. No host ports, existing demo volumes/brokers or source buckets
are reused. The existing issuer key is mounted read-only, never copied into a
build context. Generated credentials are private to the run directory. Source
hashes, the selected warehouse and stage results are retained without requiring
a git commit.

The live sequence covers:

- Three initial confirmed/persistent broker messages written by the real loader
  into the private Influx bucket, and hot API/client readback.
- A held write permit and old hot read grant; a fenced late arrival durably
  accepted before broker acknowledgement; coordinator restart proving that the
  receipt, permit, fence and read grant survive, then release of the writer.
- Native export/reconciliation, managed Iceberg publication and real-origin
  readback. Record the first cold snapshot from the durable archive job, acquire
  a cold read grant, and check both through later replacements without relying
  on a race against the queued late job.
- A separately published late overwrite and reconnect measurement, including an
  actual unacknowledged broker delivery, requeue and observed redelivery before
  the real loader consumes it. Require acknowledgement counters to increase,
  not merely an empty queue, and require late jobs to finish.
- Exact complete expected multisets through Arrow HTTP, NDJSON HTTP and the real
  client's default request, independently for both days and their combined
  range. Observe the client's real response media type to require Arrow. Check
  four public columns, zstd IPC, JSON scalar/metadata types and nanosecond
  remainders, including a string `"123"` and metadata without a plugin.
- Enable deletion only for the isolated fixture after those checks. The held
  old hot read grant must prevent it; release that grant, require completion of
  verified March 1 deletion and retention of March 2, then recheck all five
  current API/client records and the unchanged two-point old cold snapshot.
- Require all work/grants drained. Audit actual published metadata for Iceberg
  v3, `timestamptz_ns`, VARIANT field 11, MAP field 10, day partitioning and
  bounded-plugin layout properties. Inspect persisted full-multiset/SHA-256
  remote verification reports for every job, including the archive's
  `DELETING` re-verification report.

The synthetic grants represent old readers/writers; this does not claim to hold
an actual API response open. The broker probe exercises real unacknowledged
redelivery, not a loader crash following an ambiguous Influx write. The default
30-day retention stays unchanged; the old synthetic dates qualify, and
`--archive-before=2025-03-02` bounds archival to one day. Production deletion
defaults/configuration remain unchanged. No remote objects are automatically
deleted, and state/catalog must remain paired with its allocated prefix.

`run.json` records overall phase/result; per-stage JSON, wire bodies, schema
metadata and verification evidence live under `reports/`. Services are stopped
on success or failure, with state/containers retained. The runner refuses to
resume a partially executed run against a reused warehouse; use fresh inputs
after a failure. `--prepare-only` creates reviewable frozen inputs without
Docker/network operations, and `--prepared <directory>` executes a never-started
preparation on a Docker-capable host.

Local validation passed 12 offline harness tests, including real Arrow/Pandas
and current client decoding, rejection of wrong types/duplicates/lost
nanoseconds, source CSV precision/types, schema/layout rejection, isolation and
refusing deletion after failed readback.

Three harness corrections were required before a live run could pass. The
archiver image now copies InfluxDB 2.7.7 `influxd` from `/usr/local/bin/influxd`.
The origin marker is written at `warehouse/validation/live-gate.json`, outside
`sage/obs`, because a file under that collection makes a new catalog refuse the
prefix. The broker healthcheck uses `check_port_connectivity`, and bootstrap
retries private queue setup, because `ping` can succeed before AMQP accepts
connections. Two earlier prefixes were left in place after those failures and
were not reused:
`cold-storage-archive-live-79e160c592db4a458483cb543c6aac0a` and
`cold-storage-archive-live-de6b0c33a6774f23a26fe7223bb33d17`.

#### Small live gate VM run (2026-10-27)

The fresh run `archive-exporter/state/live-ced9aa98edae4618aeb4f36b090b51ca`
passed in about 80 seconds on the Rancher Desktop VM. `run.json` records
`passed: true`, phase `complete`, and `services_stopped: true`. DuckDB was
v2.0.0. The private warehouse is
`https://nrdstor.nationalresearchplatform.org:8443/sage/cold-storage-archive-live-ced9aa98edae4618aeb4f36b090b51ca/warehouse`.
Its Influx bucket was `a5c85a00d5bde46e`. No demo stack, host port, or existing
warehouse was reused. Remote objects were not deleted.

Every stage passed: bootstrap, seed, fence, restart recovery, archive and pin,
fenced late publication, real broker redelivery, late correction, deletion
blocked by the held hot grant, then release and finish.

- Seed acknowledged 3 persistent messages. Hot readback was 2 March 1 points
  and 1 March 2 point through NDJSON, Arrow, and the real client. The client
  response was `application/vnd.apache.arrow.stream`.
- The fenced late point was durably receipted before acknowledgement
  (`ack` 4, queue empty) and was absent from Influx while the write permit
  held the job in `FENCING`. Receipt, permit, fence, and hot grant survived
  the coordinator restart.
- The archive job routed with 2 initial remote rows. The held cold grant and
  that first snapshot stayed unchanged after later replacements.
- The redelivery probe observed `redeliver: 1` and then the loader
  acknowledged the late overwrite and reconnect measurement. Final broker
  totals were 6 published, 6 acknowledged, and 2 redeliveries, with no ready
  or unacknowledged messages.
- After deletion was enabled only in this stack, the held hot grant kept the
  March 1 job at `ROUTED`. Releasing it let the job reach `COMPLETE`. Influx
  retained only March 2. Current API/client reads were 5 combined points, 4 on
  March 1, and 1 on March 2, again with the client using Arrow. The old cold
  snapshot remained readable. Readers, writers, inbox work, and jobs were then
  drained.
- Published metadata is Iceberg v3 at `sage/obs`, with `time` field 1
  `timestamptz_ns`, `meta` field 10 as a string map (key 12, value 13),
  `value` field 11 `variant`, and `day(time)` partitioning. Layout properties
  are `bounded-plugin-v1`, plugin/vsn/time order, time batch order, and
  262144-row groups.
- All four persisted verification reports had `full_multiset_equal: true`:
  the archive commit, its `DELETING` re-verification of the same Parquet SHA,
  and both late commits.

This closes the small live integration gate. It does not measure a
representative full-day lifecycle. Production deletion defaults were not
changed, and no commits or pushes were made. The fixture containers were
stopped; local state remains paired with this prefix. Demo containers were
left running.

#### Review of successful small live integration (2026-10-27)

Accepted: the small real-service integration gate is closed. Review checked
`live-ced9aa98edae4618aeb4f36b090b51ca/run.json` and every functional stage report:
all passed, the run completed, and its services were stopped. All four frozen
source contexts match their recorded hashes, and the current archiver/API/
loader/client sources plus probe/Compose configuration match those executed
snapshots. The three VM fixes are retained: the actual Influx image binary path,
a marker outside the managed table collection, and AMQP listener readiness with
bootstrap retry. The marker fix preserves the catalog's refusal to adopt an
occupied table root; no warehouse guard or TLS verification was weakened.

Independent checks of the saved final Arrow **and** NDJSON bodies confirmed
exact timestamps, scalar types, complete metadata and expected row multiplicities
for the mixed range (5), archived day (4) and retained hot day (1). The real
client received Arrow in all three ranges before and after deletion. Final
broker evidence is 6 published/acknowledged, 2 redeliveries, and zero ready or
unacknowledged messages. All final writer/reader, inbox, job, deletion and failure
counters are zero. All four persisted verification reports record full multiset
equality and object SHA-256 values; the archive commit and pre-deletion
re-verification cover the same object hashes.

The result demonstrates real scoped Pelican publication/readback, durable late
receipts across coordinator restart, broker redelivery, late snapshot replacement,
API/client schema and nanosecond compatibility, old-snapshot readability, and
fixture deletion gated by an old hot read grant. The remaining limits of the
small fixture still apply: held grants are synthetic and it does not simulate a
loader crash following an ambiguous Influx write or prove full-day capacity.

Updated current gate status:

| Gate | Status | Evidence/scope |
| --- | --- | --- |
| Bounded export normalization | Passed | Synthetic 140-million-point normalization run; not the complete lifecycle |
| Matching-engine source consistency | Passed | WAL/TSM overwrites, tombstones, full compaction and five boundary instants |
| Native lifecycle and packing | Passed previously | Local object fixture and bounded Iceberg v3/VARIANT packing |
| Real managed Pelican publication/readback | Passed | Fresh isolated managed prefix in the small live run |
| Live loader/broker and managed API/client integration | Passed for tested scenarios | Durable fenced receipt/restart, redelivery, corrections, both wire formats and guarded fixture deletion |
| Representative full-day archival capacity | Pending | End-to-end source export, independent reconciliation, packing, publication/readback and resource peaks |
| Production writer/reader migration and drain | Pending | Deployment enforcement and operator validation before production deletion |

Next, benchmark a representative complete day in another fresh isolated bucket
and managed prefix, preserving the source/demo warehouses. Measure each lifecycle
stage, peak Go/native/container memory, peak staging/spill/Influx disk and
publication/readback throughput. The earlier 140-million-point normalization
measurement and this five-point integration run must not be presented as a
full-day archival result. Production deletion defaults remain unchanged.

All 12 offline live-harness tests and `git diff --check` passed after reviewing
the VM corrections. This review updated documentation only; no live services,
remote objects or source data were changed, and no commits or pushes were made.

#### Representative full-day benchmark implemented (2026-10-27)

Added [the capacity harness](../archive-exporter/testing/capacity/README.md).
It extends the successful live fixture with a real historical day rather than
scaling the five synthetic points. The default read-only source is the adopted
bounded-plugin full warehouse, March 1, 2025; its audited **140,578,674** rows are
required for that default input. Another day/source can supply its own audited
`--expected-rows`. The source hint is read once and its immutable metadata URL
is recorded before extracting any data.

Each run uses a new UUID directory, Influx instance/bucket and managed Pelican
prefix. The source warehouse receives only scoped read credentials; the demo
and existing warehouses are not repointed or modified. Source extraction writes
primitive Parquet with integer nanoseconds, full canonical tags, exact native
VARIANT kind and JSON value. Bounded line-protocol batches seed the private
Influx bucket under a coordination permit while archival is disabled. Seeding
is separate from measured archival work; RabbitMQ/loader throughput is not part
of this benchmark because that functional path already passed the small gate.
Unsupported/unrepresentable rows fail instead of being dropped or sanitized.
The initial harness incorrectly rejected literal newlines in string fields;
the correction below distinguishes quoted string fields from identifiers.
Identifier newlines and unsupported CR/NUL remain rejected; such a failure
must not be renamed a faithful full-day clone.

The actual archiver then exports the day, normalizes and reconciles it through
Influx, packs the adopted bounded chronological plugin layout, uploads and
verifies remote data, and activates the frontier. Before fixture deletion can
be enabled, native DuckDB compares **both full multisets** between the pinned
source reference and destination, including time/name/complete tags/native
value kind/value. The run also requires the source row count, engine normalized
count and worker verification evidence. Bounded complete series/time-window
probes exercise Arrow, NDJSON and the real client's Arrow request without
creating a 140-million-row DataFrame. Fixture deletion requires the archiver's
usual full re-verification; the target day must become empty while a next-day
hot sentinel remains. Ordinary completed-job cleanup is exercised.

A default-off `--profile-steps` option records exact per-attempt wall time,
process/child CPU deltas and parent/largest-child lifetime RSS under the private
job directory. Waiting-only polls produce no profile. Atomic timing reports
survive cleanup. Profiling errors are logged without changing durable job/error
semantics; the capacity gate separately rejects missing required profiles.
Go tests cover successful transitions/cleanup and preserved failure/rewind
behavior with profiling enabled. Normal startup, query, warehouse and deletion
behavior are unchanged when the option is absent.

The runner samples only its own project containers and mounts, collecting
cgroup memory/peak, OOM/exit state and logical/allocated sizes for staging,
archiver spill, Influx, source/reference storage and probe spill. Retained
one-shot containers preserve OOM diagnostics. Metric scopes are explicit:
RSS marks are lifetime high-water values, kernel peaks include page cache,
disk sampling may miss transient/unlinked files, and maxima across phases/roles
are not simultaneous combined VM peaks. Publish/readback figures divide
compressed data-file bytes by whole worker-stage durations and include CPU,
metadata and hash readback, rather than claiming physical wire bandwidth.
Retries are retained. The extra pinned-source comparison and source preparation
have separate resource/timing reports.

The user chose an isolated directory. Preparation observed about **43 GiB** free
on the workspace filesystem, so isolation alone must not be mistaken for more
space. A pre-seed admission check estimates the actual day's row footprint and
requires at least four raw-NDJSON equivalents plus 20 GiB (32 GiB floor), with a
10 GiB VM `MemAvailable` floor by default. The estimate is a heuristic, not a
proven peak bound. On failure it records measured/free/required bytes without
starting import or archive/deletion. `--run-dir` selects a fresh directory on a
larger mounted filesystem when needed; nothing unrelated is deleted to make
room. Component/native limits and deadlines are explicit and saved with the
run. The default archiver buffer budget remains 3 GB, with an 8 GiB fixture
container limit; Influx and the native probe each have 4 GiB limits.

From the workspace root on a Docker-capable host:

```sh
python3 sage-data-rework/archive-exporter/testing/capacity/run.py \
  --run-dir /path/to/large-isolated-volume/full-day-20250301-unique
```

The chosen directory must not already exist. `--prepare-only` is available for
reviewable frozen inputs with no Docker/network operations, and `--prepared`
executes only a never-started preparation. Failed runs retain their private
state/catalog and are not automatically resumed against a reused prefix.

Local validation: nine capacity-harness tests and all twelve existing live-
harness tests passed. Coverage includes scalar types/large integers/escaping/
nanoseconds, bounded batches, rejection of unrepresentable inputs, narrowing API
windows instead of accepting truncation, full-multiset mismatch rejection,
redacted native resource diagnostics, source URL isolation and refusal to enable
fixture deletion after reference verification fails. Go unit/race checks, vet
and the CGO-disabled service build passed with the new profiling option. Compose
merges and Python syntax were validated offline.

#### Representative full-day VM run (2026-10-28)

With the application containers stopped, `MemAvailable` was 13.57 GiB. A fresh
run, `archive-exporter/state/full-day-20250301-1793193430`, passed bootstrap
and extracted the default March 1, 2025 source. The pinned snapshot
`00061-63fa1d9d-f966-4227-8ad4-ffa5f62b1cd8.metadata.json` contained exactly
140,578,674 rows: 140,474,618 floats and 104,056 strings. The primitive
reference Parquet is 374 MiB. The estimated raw NDJSON footprint is 49.49 GiB,
so admission required 217.97 GiB free; 241.86 GiB was free and seeding was
allowed. The source warehouse was not modified.

Seeding then failed in 1.6 seconds with `input cannot be represented faithfully
in line protocol`. Measurement names are representable. 2,772 string values
contain a newline and no carriage return or NUL: `mobotix.move.status` (2,769)
and `env.image.description` (3). The harness rejected these newlines before
sending the offending batch. The original interpretation that Influx could not
preserve them was incorrect; see the parser review and fix below. No archive
job or fixture deletion ran. The destination prefix
`cold-storage-archive-live-4251a767c41c47b3bb5b7dfc12d67ee3` received only the
origin validation marker. Fixture containers were stopped. This does not
establish a full-day archival lifecycle. No commits or pushes were made, and
production deletion defaults remain disabled.

#### Literal newline fix before the next benchmark (2026-10-28)

The seed failure was an overly strict harness check, not evidence that the
InfluxDB 2.7.7 write endpoint cannot store multiline strings. Review followed
the exact version's [HTTP points parser](https://github.com/influxdata/influxdb/blob/v2.7.7/http/points/points_parser.go)
into [models/points.go](https://github.com/influxdata/influxdb/blob/v2.7.7/models/points.go):
the line scanner ends a point at LF only outside a quoted string field.
String-field escaping handles backslash and quote; literal backslash-n is not
an alternative spelling of an actual newline. The general syntax documentation
was insufficient grounds for the earlier conclusion and backup-only suggestion.
The March 1 source can remain the intended benchmark input, subject to the live
check below; no rows need to be omitted or rewritten to get past this failure.

The capacity encoder now preserves literal LF in quoted string fields and
continues escaping backslashes/quotes. Identifier newline validation remains
strict, and CR/NUL string fields still fail explicitly. Logical-row/byte batch
limits do not split on physical newline characters. A local scan of the saved
March 1 reference encoded all **104,056** strings successfully, including all
**2,772** newline-bearing values, without dropping or substituting characters.

The older demo Parquet-to-Influx encoder had a related bug: it replaced actual
LF with literal backslash-n. It now preserves LF too. Existing backups/buckets
are not rewritten, and existing literal backslash-n must not be automatically
unescaped: it is valid distinct data and its origin cannot be inferred safely.
The production loader already passes strings through the Influx Python client;
no new storage encoding or API decoding convention was introduced.

Every new full-day run now has a small `strings` gate before the historical
scan/seeding. It creates a separate unlimited-retention bucket only inside the
fresh Influx fixture, writes seven gzip-compressed string observations and
checks annotated CSV for exact values, row multiplicity, tags and nanoseconds.
Cases cover leading/trailing/repeated LF, quotes/backslashes, literal backslash-n
and text resembling another LP point after an embedded LF. The check's bucket
is retained for inspection and is outside the archiver's `waggle` source.
Its report is `reports/strings.json`. The archiver remains disabled on failure.

Run only that small gate before another full-day attempt:

```sh
python3 sage-data-rework/archive-exporter/testing/capacity/run.py \
  --strings-only --run-dir /path/to/new-isolated-string-check
```

This uses the existing bootstrap/resource preflight and destination marker,
starts only the private Influx/probe containers, and stops them afterward. Its
manifest has `kind: string-roundtrip`, explicitly excluding any full-day capacity
claim. Fresh preparations are required; old frozen probe images are unchanged.

Regression coverage now checks LP export and independent annotated CSV decoding
against exact multiline string values, including distinct literal backslash-n,
then verifies JSON record boundaries and nanosecond identities. The opt-in real
Influx engine consistency fixture now carries seven multiline/string cases
through its existing WAL/TSM, tombstone, cleanup, compaction and deletion checks.
The opt-in native DuckDB lifecycle fixture also includes a multiline string for
packing/publication/readback equality. Those two extensions were not rerun with this string gate.

Local validation passed: all twelve capacity tests and twelve live-harness
tests, the demo encoder's unit/historical-row checks, Go unit/race tests and vet,
and offline preparation of a frozen `string-roundtrip` run. The offline gate
test also rejects a simulated newline-to-backslash-n readback change and verifies
that strings-only execution never starts archival or the historical scan.
The representative full-day lifecycle gate remains open. No benchmark was
started, no existing fixture or remote object was deleted, production deletion
defaults remain disabled, and no commits or pushes were made.

#### String round-trip VM run (2026-10-28)

The strings-only command was run on the Rancher Desktop VM. The first attempt,
`archive-exporter/state/string-roundtrip-1793195198`, stored all seven values
with their original LF bytes. A Flux length and carriage-return check found no
CR in storage. The comparison failed because InfluxDB 2.7.7 annotated CSV emits
a stored LF as CRLF inside the quoted field. The checker now turns those CSV
CRLFs back into LF, and a local regression covers that dialect.

The fresh rerun `archive-exporter/state/string-roundtrip-1793195407` passed.
`run.json` records `kind: string-roundtrip`, `passed: true`, phase `complete`,
and `services_stopped: true`. `reports/strings.json` records seven rows, literal
LF preserved, literal backslash-n left distinct, and nanoseconds preserved.
The check bucket is `d72efd1767692231` inside the private fixture. No historical
scan, archival, or fixture deletion ran. The destination prefix
`cold-storage-archive-live-df44de7a60d849e79141e5ff92475d89` received only the
origin validation marker. This does not close the full-day lifecycle gate.

#### Review of the passing string gate (2026-10-28)

Accepted for the tested seven-row HTTP/CSV scope. Review confirmed the saved
manifest is `kind: string-roundtrip`, `passed: true`, phase `complete`, with
services stopped. All four frozen build-context hashes match the manifest;
the current capacity encoder and probe match the executed copies. Bootstrap
and string reports passed. Resource sampling records no OOM or sampler errors;
the short probe's own report records an 18,268,160-byte cgroup peak, even though
the five-second host sampler missed that container's live peak.

Retain the VM correction in the Python checker: normalize annotated-CSV CRLF
to LF in the decoded string field. This matches the observed transport dialect
for the LF-only inputs; it does not establish arbitrary stored CR/CRLF fidelity,
and the seed encoder still rejects CR. Literal backslash-n remains distinct.
The production archiver uses Go's `encoding/csv`, which already normalizes
CRLF, including within quoted multiline fields. A regression now exercises
both LF and Influx-style CRLF CSV against the same exact seven expected values,
timestamps and row identities as the LP export. That test passed without any
production decoder change. All twelve offline capacity-harness tests also
passed with the VM correction retained.

The newline seeding blocker is closed for these cases. Next run a fresh normal
capacity preparation (omit `--strings-only`) against the original March 1
source. It will repeat the small gate before the historical scan. The actual
engine consistency and native lifecycle extensions, full-day reconciliation,
publication/readback, API/client probes and verified fixture deletion remain
unvalidated by this strings-only run. No services or remote objects were
changed during this review, and no commits or pushes were made.

#### Full-day VM run, default Influx limit (2026-10-28)

Fresh directory `archive-exporter/state/full-day-20250301-1793195750`, project
`sage-archive-live-39d12b526e4541c4920298f90f57b2bb`. Preflight saw VM
`MemAvailable` of 14,152,604 kB and about 238 GiB free on the host volume.
Only kube-system containers were running. The harness used its default 4 GiB
Influx container limit.

Bootstrap and the repeated seven-row string gate passed. The string bucket is
`2d117410ec0bbdc7`, and its annotated-CSV SHA-256 matches the accepted
string-roundtrip run. The historical scan passed: **140,578,674** rows, pinned
metadata `00061-63fa1d9d-f966-4227-8ad4-ffa5f62b1cd8.metadata.json`, primitive
Parquet 392,465,755 bytes. Admission passed with 255,180,800,000 bytes free
against the 234,045,515,296-byte estimate.

Seeding wrote **140,000,000** rows in 1,535 seconds, then the write connection
closed. `reports/resources.json` records one OOM: the Influx container exited
137, `oom_killed: true`, cgroup peak **4,209,508,352** bytes. That is the 4 GiB
container limit, not the Rancher VM memory cap. The probe report is
`RemoteDisconnected`. Archival, publication, API probes, and fixture deletion
did not start. `services_stopped` is true. The destination prefix
`cold-storage-archive-live-39d12b526e4541c4920298f90f57b2bb` received only the
origin validation marker. The source warehouse was not modified, and this
directory is not resumed.

#### Full-day VM rerun, 6 GiB Influx limit (2026-10-28)

The same fresh-run command was repeated with an explicit `--influx-memory 6g`.
Directory `archive-exporter/state/full-day-20250301-1793197961`, project
`sage-archive-live-0c5ea9f3d7eb4f46b8f75fca72ff8af0`. Docker confirmed the Influx
container limit as 6,442,450,944 bytes. Before the run, VM `MemAvailable` was
13,987,800 kB and the host volume had about 236 GiB free. Archiver and probe
limits stayed 8 GiB and 4 GiB.

Bootstrap and the seven-row string gate passed again. The string bucket is
`d970e56240b1ff74`, with the same annotated-CSV SHA-256 as the accepted
round-trip. The historical scan passed with the same **140,578,674** rows and
the same pinned metadata. Admission passed with 253,132,800,000 bytes free
against the same 234,045,515,296-byte estimate.

Seeding again reached the **140,000,000**-row progress checkpoint, at 1,551
seconds, and then the write connection closed. The Influx container was
OOM-killed: exit 137, `oom_killed: true`, cgroup peak exactly
**6,442,450,944** bytes. The probe error is again `RemoteDisconnected`.
Sampled Influx disk stayed about 1.5 GiB; this was not a host-disk admission
failure. Archival did not start. `services_stopped` is true. The destination
prefix `cold-storage-archive-live-0c5ea9f3d7eb4f46b8f75fca72ff8af0` received
only the origin validation marker. The source warehouse was not modified.

Both container budgets died at the cap during the final part of the seed. At
that point the Rancher VM had about 15.6 GiB total, and the archiver container
limit remained 8 GiB. Neither of those directories is resumed.

#### Full-day VM run, 10 GiB Influx limit (2026-10-28)

The VM was then about 19.5 GiB, with `MemAvailable` 18,282,320 kB. The shared
live compose file's Influx `mem_limit` and the capacity runner's default were
both raised from their previous values to **10g**. The full-day overlay is what
the seed actually uses; Docker confirmed 10,737,418,240 bytes on the container.
Archiver and probe limits stayed 8 GiB and 4 GiB.

Fresh directory `archive-exporter/state/full-day-20250301-1793200573`, project
`sage-archive-live-a1c3fbb61fbb4e0fb71b683ad49b3fea`. Bootstrap, the seven-row
string gate (bucket `6c21c8c69a4e5bf4`, same CSV SHA-256), and the historical
scan passed again: **140,578,674** rows, same pinned metadata.

Seeding reached **45,000,000** rows in 499 seconds, then the write returned
HTTP 500. This was not an OOM. `oom_events` is empty, the Influx cgroup peak
was **1,336,066,048** bytes, and the container was still running when the
probe failed. The probe stores only the status line, so the Influx error body
was not kept. Archival did not start. `services_stopped` is true. The
destination prefix `cold-storage-archive-live-a1c3fbb61fbb4e0fb71b683ad49b3fea`
received only the origin validation marker. The source warehouse was not
modified, this directory is not resumed, and no remote object was deleted.

#### Review and seed diagnostics before another capacity run (2026-10-28)

Review confirmed all three manifests failed in `seed`, with services stopped.
The 4/6 GiB runs each have one recorded Influx OOM, while the 10 GiB run has
none and its probe explicitly reports HTTP 500 from `/api/v2/write`. The
latest failure is therefore not evidence that a higher memory limit is needed.
Its error body and server logs were not saved, so the underlying cause cannot
be diagnosed from the retained HTTP status. Influx's cache/write pressure is
a possibility, not an established finding; no blanket retries or further
memory increase were added.

One interpretation of the earlier runs needs qualification: the 140-million-row
checkpoint is not the final acknowledged write count. The original seed function
performed a whole-day Flux count after the final writes and sentinel, all under
the same `seed` label. Thus the old reports cannot establish whether those OOMs
occurred during final ingestion or the following count. They remain genuine
container OOMs, but are not yet a measured ingestion-only memory requirement.

The harness now separates those phases in `reports/seed-progress.json`:
`writing`, `writes-complete`, `writing-sentinel`, `counting-source`,
`counting-sentinel`, and `counts-verified`. A failed write also records its exact
acknowledged row count, without assuming an ambiguous failed request wrote zero
points. During source counting it records the accumulated count, number of
completed windows and exact bound reached. A failed seed still leaves its
coordination permit held; archival/deletion are never enabled after that failure.

The auxiliary Influx counts now cover the complete half-open range as consecutive
five-minute queries and sum their exact results. This avoids asking a single
query to maintain a whole day's execution state. It is a mitigation to evaluate,
not proof of the old OOM cause or a hard bound on high-cardinality query memory.
Every interval is covered without gaps/overlaps, including nanosecond bounds;
the total still must equal 140,578,674 for the source and one for the hot sentinel.
The same helper checks the full fixture day after deletion. Malformed CSV,
negative counts, error tables and multiple uncollapsed results fail. Native
full-multiset reconciliation/reference verification and production API execution
are unchanged.

Fixture write/count HTTP errors now save `reports/influx-http-error.json`: status,
endpoint, and a credential-redacted response body capped at 8 KiB. This is
opt-in only for the private Influx endpoints; signed remote SQL/storage responses
are not copied into diagnostics. On run failure the host saves
`reports/influx-logs.json` before teardown: only the current project's Influx tail,
up to 200 lines/64 KiB with credentials redacted. Diagnostic failures preserve
the original failure and teardown. Existing failed preparations remain frozen
and are not resumed. The user's 10 GiB Influx default and Compose changes remain.

All seventeen offline capacity tests and twelve existing live-harness tests
passed, along with Python syntax checks and `git diff --check`. Added coverage
checks exact count-window boundaries and sums, query error rejection, distinguishing
completed writes from a subsequent count failure, bounded/redacted HTTP responses,
preservation of the original HTTP error if reporting fails, and project-scoped,
bounded/redacted Influx logs. These harness changes have not been run on the VM.

Next use another fresh full-day preparation with the existing explicit 10 GiB
Influx budget. Inspect the new seed phase, HTTP body and Influx logs if it fails;
do not treat another seed failure as an archival performance result. The
representative full-day lifecycle gate remains open. No existing data or remote
objects were deleted, production deletion defaults remain disabled, and no
commits or pushes were made.


#### Full-day VM run with seed-phase diagnostics (2026-10-28)

Fresh directory `archive-exporter/state/full-day-20250301-1793202279`, project
`sage-archive-live-353ac3dcdf0a43c3946daba125b266bc`, Influx limit confirmed at
10,737,418,240 bytes. Before the run, VM `MemAvailable` was 18,206,468 kB and
the host volume had about 234 GiB free. No Influx HTTP error file was produced.

Bootstrap, the seven-row string gate, and the March 1 scan passed.
`reports/seed-progress.json` reached `counts-verified`: **140,578,674**
acknowledged rows, the same counted total, and one retained hot sentinel.
Seeding took 5,163 seconds. The Influx cgroup peak for the whole run was
3,201,466,368 bytes. `oom_events` is empty. The separated count phase completed
without the container OOM seen when the count was one whole-day query.

Export then succeeded in 456 seconds. Dedup reported 140,578,674 unique rows
and 0 superseded. The following reconcile attempt ran 3,001 seconds and failed
back to `EXPORTING`. The archiver log records DuckDB exit status 1:
`Out of Memory Error: failed to offload data block of size 256.0 KiB (70.0 GiB/70.0 GiB used)`
because `max_temp_directory_size` had consumed the free space on the temp
volume. A retry exported again in 522 seconds. Host free space was then about
15 GiB and still falling, so the archiver container was stopped before the
volume filled. The routed probe reports `coordinator disappeared`; that message
is the stop, not a second reconcile finding. The archiver cgroup peak reached
the 8 GiB container limit, with no OOM kill. Services were stopped. Publication,
API probes, and fixture deletion did not run.

The three staging NDJSON files, about 145 GiB, were removed after those reports
were saved. `df` stayed at about 11 GiB free. `lsof` shows the unlinked files
still open in `com.apple.Virtualization.VirtualMachine` (pid 91390), along with
other unlinked virtiofs files totaling about 330 GiB. Rancher was not restarted.
The destination prefix received only the origin validation marker. The source
warehouse was not modified, this directory is not resumed, and no remote object
was deleted. The full-day lifecycle gate remains open.

#### Review: seed/export passed; reconciliation disk blocker (2026-10-28)

The saved seed report and final `counts-verified` checkpoint confirm 140,578,674
acknowledged rows, an equal independent Influx count and one next-day sentinel.
The 5,162.8-second seed time includes writes and count verification; it is not
ingestion-only throughput. There are no recorded OOM events. Full-day fixture
seeding for this day passed; the newline and count-phase blockers did not recur.

The first export completed in 456.3 seconds. Dedup confirms 140,578,674 input/
unique rows and zero superseded rows. Raw and normalized NDJSON are each
52,521,487,008 bytes (48.9 GiB), while the transient sorted Parquet is just
490,254,356 bytes (467.5 MiB). The following EXPORTED attempt failed after
3,000.8 seconds and rewound to EXPORTING; a later export took 522.0 seconds.
The manifest's `failed_phase: routed` is the outer wait phase, not the failing
worker operation. The final coordinator-disappeared error followed the stop.

Resource samples reached 230,051,422,973 logical bytes (214.3 GiB) under `/state`,
including 74,625,548,960 bytes (69.5 GiB) of spill. These paths overlap; do not
sum them. Influx was about 1.44 GiB logical on disk. DuckDB exhausted its temp
allowance; the archiver's 8 GiB cgroup high-water mark includes page cache and
does not establish an OOM. The initial free-space heuristic was insufficient
for this comparison, retained staging and retries.

`Worker.Reconcile` retains raw export, normalized export and independent query
NDJSON, then `DuckDB.Equal` canonicalizes both whole datasets and runs both
directions of `EXCEPT ALL`. The wide canonical strings/full-day multiset
operators are a plausible source of the large spill. The reports establish a
reconciliation-stage failure but do not separately profile its SQL operators.

Before another full-day run, target compressed canonical Parquet intermediates
and sequential, bounded partition comparisons that retain exact full-row
multiset equality. A stable partition hash may distribute records, but must
not replace comparison of the actual complete records and duplicate counts.
Cover every row exactly once and preserve nanoseconds, full tags and native
value kinds. Apply this scalable verification approach to build/readback and
the harness's pinned-source comparison too, so later stages do not recreate
the same spill. Add a disk-aware retry/admission guard before repeated exports
consume remaining space. These are next changes, not implemented/validated wins.

The VM report documents unlinked files held by the virtualization process;
deleting their names did not release their storage. Review still observed only
about 14 GiB host free. Plan a controlled Rancher VM restart after stopping the
relevant work, then check actual free space before another capacity attempt.
More deletions or a larger memory limit alone do not address this comparison.
This review did not restart services or delete files.

Publication, full remote verification, API/client probes and verified fixture
deletion remain pending. Production deletion defaults remain disabled. No
commits or pushes were made.

#### Compressed staging and bounded exact comparison implemented (2026-10-28)

Implemented the reconciliation/storage changes identified in the full-day review.
Production engine export streams directly into `export.parquet`; normalization
atomically produces `export-dedup.parquet`; the independent annotated-CSV query
streams into `influx.parquet`. Post-delete confirmation also uses Parquet. These
private files store int64 nanoseconds, measurement, complete tag JSON, scalar
kind, lossless scalar text and replay sequence, with Zstd compression. They do
not change the published Iceberg/Parquet schema or the API response schema.
No whole-day NDJSON copy is constructed on these production paths. Small
independent NDJSON fixtures remain supported by the test helpers.

The Go encoder flushes at 65,536 rows or approximately 8 MiB of record payload;
an individual oversized record and Parquet metadata still affect memory. Exact
large integers, native scalar kinds, multiline strings, full tags, replay order
and nanosecond timestamps are preserved. A bounded streaming reader still picks
the latest sequence across sorted batch boundaries. Publication requires a
completed footer, file sync and atomic rename; incomplete outputs never advance
the state machine. File-size fields in `export-dedup.json` now measure compressed
physical staging artifacts, so they must not be interpreted as NDJSON sizes.

Every `DuckDB.Equal` caller now uses compressed canonical primitive staging:
source reconciliation, each packed batch, the complete build, remote readback
and pre-deletion re-verification. Counts precheck both sources; the full
canonicalization reads each source once into 16 hash-routed local partitions.
Oversized partitions are recursively refined with the next four hash bits,
using only local files, until each comparison has at most 1,000,000 rows per
side. Both directions of `EXCEPT ALL` compare the actual complete canonical
records and duplicate counts. Hash/count agreement alone cannot establish
equality. A partition that remains oversized after all 64 hash bits fails closed.
The independent Python pinned-source verification uses the same algorithm.

Partition staging uses two threads, a 65,536-row flush threshold and at most
32 open files (16 per thread), avoiding a large number of simultaneous writers.
Only comparison scratch has this hash partitioning; warehouse data retains
daily partitions, chronological files of at most 10 million rows, plugin/vsn/time
sorting and plugin-pure groups capped at 262,144. Ordinary archive builds now
preserve the normalized file's existing chronological order through a single-
file projection/COPY with insertion-order preservation explicitly enabled.
They avoid a redundant global sort of expanded MAP/VARIANT rows. Bootstrap
merges and late corrections still explicitly sort by time.

Before native work, disk admission requires more than 8 GiB available on the
actual staging filesystem. DuckDB spill is capped at the smaller of 16 GiB and
available space minus that reserve. Parquet flushes recheck disk space. This is
a guard, not a filesystem quota or a guarantee of total memory/disk usage under
wide records, concurrent writers or large outputs. Reconciliation capacity
errors retain `EXPORTED` and its fence instead of forcing another export; actual
record mismatches retain the existing re-export behavior. Capacity retries wait
15 minutes after attempt completion, ordinary retries one minute after completion.
This also fixes the earlier retry deadline being measured from the start of a
potentially long attempt. Verification/deletion safeguards remain in place.

The capacity harness's new compressed-staging admission heuristic requires the
maximum of 64 GiB, the operator's configured floor, and 32 reference-Parquet
sizes plus 32 GiB. It records the formula and actual sizes; this is not yet a
measured full-day peak. The old raw-NDJSON estimate remains informational.
Native probe spill/reserve limits match the worker. Existing frozen failed runs
are not resumed or rewritten. Rancher and old run data were left untouched.

Local validation passed: Go unit/race tests, vet and a CGO-disabled build;
nineteen offline capacity tests and twelve live-harness tests; Python syntax
and whitespace checks. Regression coverage includes compressed scalar/tag/time/
sequence round trips across batch seams, latest-value normalization to Parquet,
LF and Influx-style CRLF decoding directly to Parquet, materially smaller
compressed artifacts, safe capacity classification/fenced retry state, native
probe disk admission, and reference-comparison staging/count/cleanup guards.

The new opt-in `TestNativeCanonicalPartitionComparison` checks every canonical
field, duplicate multiplicities, a deliberate routing-hash collision and
recursive refinement over 17 million local generated records under a 128 MB
DuckDB buffer budget. The existing matching-engine/scale fixtures now exercise
compressed production staging. Native DuckDB 2.0/Influx/VM gates have **not**
been run here; these changes do not close the representative full-day gate.

Before another full-day import, run the native comparison/lifecycle/packing
gates and matching-engine gate described in
[the exporter README](../archive-exporter/README.md#bounded-exact-verification-and-disk-guards).
Arrange a controlled VM restart to release the previously reported open deleted
files and verify recovered space first. Then prepare a fresh full-day run with
the existing 10 GiB Influx budget, retaining stage/resource reports. No commits
or pushes were made, and production deletion defaults remain disabled.

#### VM restart, native gates, and compressed full-day run (2026-10-28)

`rdctl shutdown --wait` closed Rancher Desktop. Host free space went from about
13 GiB to **241 GiB**, and the old virtualization process exited. `rdctl start`
brought the same VM back: 14 CPUs, about 19.5 GiB total, `MemAvailable`
18,210,392 kB before the full-day run. Free space was about 252 GiB at that point.

The native gates ran in `golang:1.26.5-bookworm` with the Linux DuckDB 2.0 CLI.
`TestNativeCanonicalPartitionComparison` passed in 28.67 seconds.
`TestNativeLifecycle` first failed in `DELETING` because an empty Parquet file
still has a footer, so a size check treated it as rows. Confirmation now uses
the Parquet row count. The rerun passed in 3.49 seconds, and
`TestNativeBoundedPluginPacking` passed in 2.16 seconds.

`TestInfluxEngineConsistency` passed in 92.09 seconds on
`archive-exporter/state/source-fixture-1793217874`, report
`state/engine-gate-1793217874`. The fixture compose project was removed afterward.

Fresh full-day directory
`archive-exporter/state/full-day-20250301-1793217993`, project
`sage-archive-live-595f34796cd0467fb83beee96638adb7`, Influx limit 10 GiB.
String gate bucket `b4a6e639678cf73b` matched the accepted CSV SHA-256. Admission
passed: 265,011,200,000 bytes free against a 68,719,476,736-byte compressed-staging
estimate. Seeding reached `counts-verified` for **140,578,674** rows and one hot
sentinel, in 5,355 seconds. Influx peaked at 3,177,443,328 bytes. `oom_events`
is empty.

Export wrote `export.parquet` of **1,453,035,194** bytes in 507 seconds. The
reconcile attempt then failed after 482 seconds and stayed in `EXPORTED`.
DuckDB reported temp space **15.9 GiB of 16.0 GiB**. Host free space stayed
about 238 GiB; this was the spill cap, not a full volume. The archiver was
stopped before the 15-minute capacity retry, so the routed probe reports
`coordinator disappeared`. The archiver cgroup peak reached the 8 GiB container
limit without an OOM kill. Publication, API probes, and fixture deletion did
not run. `services_stopped` is true. The destination prefix received only the
origin validation marker. No remote object was deleted, and this directory is
not resumed. The full-day lifecycle gate remains open.

#### Review: compressed export validated; normalization sort is next (2026-10-28)

The native comparison/lifecycle/packing gates passed as reported, and the saved
matching-engine `engine-consistency.json` records `passed: true`. Retain the VM
fix to `ConfirmEmpty`: an empty Parquet file has a nonzero footer, so confirmation
must test its row count. A local regression now exercises both zero and one row,
requiring that the zero-row file still has bytes and that a one-row file is
rejected. That test passed. No production deletion default was changed.

The full-day seed report confirms 140,578,674 rows and 5,354.6 seconds including
count verification. Compressed export completed in 507 seconds and left the
1,453,035,194-byte raw Parquet file. Compared with the earlier raw NDJSON size
of 52,521,487,008 bytes, this is about 36 times smaller (97.2% fewer bytes).
This is a storage comparison across runs, not a controlled export-speed result.

The next failing operation is specifically **normalization's global sort**,
not the new partitioned equality comparison. The job directory contains raw
`export.parquet` but no `export-dedup.json`, normalized Parquet or independent
query copy. `Worker.Reconcile` writes the dedup report only after
`DeduplicateExport` returns; this attempt therefore failed before reaching the
Influx query or `DuckDB.Equal`. `exportSortSQL` still globally orders all expanded
records by time, name, canonical complete tags and descending sequence. Compact
input files do not make that expanded in-memory/spilled sort equally compact.

The persisted job error identifies the 16 GiB temp cap. Samples show about
17.3 GiB under `/state`, including 15.8 GiB of spill; these paths overlap.
There are no recorded OOMs, the host retained ample free space, and the job
stayed at `EXPORTED`. The guard prevented the earlier disk-filling/re-export
pattern. The outer routed probe's coordinator-disappeared message followed
the operator stop and is not a separate data/verification error.

Next bound normalization itself: partition by the complete point identity
**(nanosecond time, measurement, canonical full tags)**, sort/deduplicate bounded
local partitions, and merge the survivors into one chronological normalized
Parquet stream. Value and kind must remain outside the routing identity so
overwrites with changed values/types meet in the same partition and the largest
replay sequence wins. Keep merge fan-in/batches bounded and preserve the
chronological file policy. Raising the spill limit alone does not fix this
remaining whole-day sort.

Evaluate that change first against this run's retained `export.parquet`, read
only, with a fresh isolated scratch directory. That can measure normalization
without another roughly 89-minute seed, resuming the failed run, or modifying
Influx/Pelican. Native correctness gates and another full lifecycle run still
follow; the complete-day publication/readback/API/deletion gate remains open.
This review changed only the regression test and documentation. No existing
run data or remote objects were deleted, and no services were restarted or
commits/pushes made.

#### Bounded normalization and retained-export VM benchmark implemented (2026-10-28)

Normalization no longer globally sorts the complete day's expanded records.
It canonicalizes complete tag JSON and routes by `(time, name, tags)`, explicitly
excluding value, kind and sequence. Partitions larger than 1,000,000 input rows
are refined 16 ways with subsequent identity-hash bits. Each terminal partition
is sorted by time/name/tags/descending sequence, and a streaming collapse keeps
the greatest replay sequence. Hash collisions are resolved by complete identity
comparison; hashes never select the winning value. A partition still oversized
after 64 bits fails closed with the existing capacity classification.

Sorted winners are merged chronologically with at most eight readers and
2,048-row Arrow batches per reader; multiple passes bound fan-in. The merge
checks strict identity order, rejects duplicates across supposedly disjoint
partitions, preserves scalar text/kind/sequence and validates footer/coverage
counts. Arrow batch views avoid rebuilding tag maps while merging. Only the
small preceding identity is retained across batch seams. The final footer is
completed and synced before atomic publication; failures preserve any previous
complete normalized output. Newly created attempt scratch is reclaimed on
return; interrupted processes can leave private scratch for inspection.

`export-dedup.json` now records strategy `identity-partitions-merge-v1`, input/
unique/superseded counts, terminal partition count/max rows, merge passes/fan-in,
partition/sort-collapse/merge times and compressed artifact byte counts.
Partition/sorted byte counters are cumulative writes, not simultaneous peaks;
phase timers exclude some count/metadata/cleanup work. The existing native spill
cap/free-space reserve and fenced capacity retry policy remain in effect.
The chronological normalized output preserves the adopted downstream warehouse
layout and allows ordinary archive builds to continue avoiding a second sort.

Added [the standalone normalization runner](../archive-exporter/testing/normalization/README.md)
to avoid another roughly 89-minute seed while evaluating this change. It freezes
the curated Go source and DuckDB CLI, compiles separately in the same Linux Go
image used for the previous gates, then runs two measured/runtime stages with
networking disabled. Input and binary mounts and the runtime root filesystem
are read only; only fresh scratch is writable. It does not contact Influx,
Pelican or coordination, publish objects, delete data, or resume a failed run.
Build/image/module acquisition may use networking; runtime verification does not
install extensions and checks only the pinned DuckDB 2.0 version.

The first stage, `TestNativePartitionedNormalization`, generates 50,000 unique
records plus 10,000 later overwrites with changed kinds/values and reordered
metadata keys. A small independent global window oracle verifies the complete
result, sequence and chronology; forced small partitions exercise recursion and
multiple merge passes. The gate also covers empty input and atomic refusal of
invalid sequences. Failure stops the runner before the full benchmark.

The second stage, `TestNativeExistingExportNormalization`, uses the retained
March 1 export. Review independently checked its footer: **140,578,674 rows**.
This is the audited all-unique input for this benchmark. It measures normalization
separately, requires all expected rows with zero superseded rows, and then runs
bounded exact multiset comparison of the entire original and normalized data,
including replay sequence as well as time/name/full tags/kind/scalar text.
SHA-256 before and after must establish the source stayed unchanged. This gate
is specific to the retained all-unique export; overwrite correctness belongs
to the preceding independent oracle test.

From the workspace root on the Docker-capable VM host:

```sh
python3 sage-data-rework/archive-exporter/testing/normalization/run.py \
  --source sage-data-rework/archive-exporter/state/full-day-20250301-1793217993/coordinator/jobs/5f363e63-52e5-43c6-9f0b-cd68df7a4848/export.parquet
```

Defaults are native buffer 3 GB, container 8 GiB, two CPUs and six hours for the
full benchmark; the correctness stage uses its own smaller native buffer budget.
A fresh UUID directory is printed. `--run-dir` selects another new isolated
directory; `--prepare-only` creates frozen inputs without Docker execution.
`run.json` saves code/binary hashes, settings and container exit/OOM state;
`data/results/normalization-benchmark.json` saves timings/counts/resources, source
hashes and exact verification status. Logs are separate for build/correctness/
benchmark. Runtime containers are retained for failure diagnostics. Source
hashing warms the input; OS caches are uncontrolled. RSS and cgroup peaks have
the lifetime/page-cache limitations described in the benchmark README, and
scratch samples exclude the retained input and can miss transient/unlinked files.

Local validation passed: Go unit/race tests, vet, service and test-binary builds;
nineteen capacity tests, twelve live-harness tests, three new runner tests and
Python syntax/whitespace checks. Merge tests cover interleaving across Arrow
batch seams, duplicate/fan-in rejection and cancellation. Runner tests check
curated preparation, source preservation, directory isolation, read-only source
mount/network restrictions and preventing the full benchmark after gate failure.
Offline preparation against the retained export succeeded. No native DuckDB 2.0
execution or VM benchmark has been performed for this change yet. The complete
archival lifecycle remains open; no prior data, remote objects or services were
changed, and no commits/pushes were made.

#### Retained-export normalization benchmark (2026-10-28)

Ran the standalone runner against the retained March 1 `export.parquet`
(1,453,035,194 bytes, footer 140,578,674 rows) in a fresh directory,
`archive-exporter/state/normalization-1793228964` (`run_id`
`bef8b80be9e749fdb897e230d6cb0b89`). Defaults were kept: DuckDB buffer 3 GB,
container 8 GiB, two CPUs. The host had about 17.2 GiB MemAvailable and 236 GiB
free before the run. Runtime networking stayed disabled. The retained export,
its run directory, remote objects, and other services were not changed.

`TestNativePartitionedNormalization` passed in 13.32s (container exit 0, not
OOM). `TestNativeExistingExportNormalization` then passed in 1,628.60s
(container exit 0, not OOM). `run.json` records `passed: true` and phase
`complete`.

Normalization wall time was 858.89s: partition 292.79s, sort/collapse 176.80s,
merge 388.67s. Strategy `identity-partitions-merge-v1` kept all 140,578,674
rows with zero superseded rows, using 256 terminal partitions (maximum
550,959 rows), three merge passes, and fan-in at most eight. Cumulative
partition writes were 2,650,935,699 bytes and cumulative sorted writes
1,601,810,531 bytes. The published normalized file is 774,898,186 bytes.
Through normalization, Go RSS peaked at 399,007,744 bytes, the largest child
at 1,780,076,544 bytes, sampled scratch at 3,527,533,791 bytes, and the cgroup
at 6,772,527,104 bytes. That cgroup figure includes page cache and stops at
the end of normalization; it is not a verification peak.

Exact full-source multiset comparison, including replay sequence, took 762.12s
and matched. Source SHA-256 before and after is
`98d55ba8c3b4c3f9dc1316d34d9b1703aea562f02e897dcee9326147159f07f9`. Comparison
scratch was reclaimed; `data/results/normalized.parquet` remains. Host free
space afterward was about 232 GiB. This validates normalization and exact
record preservation for the retained all-unique export. Publication, API
readback, and verified deletion were not run, so the full-day archival
lifecycle remains open. No commits or pushes were made.

#### Review: retained-export normalization accepted (2026-10-28)

Accepted for the measured normalization/record-preservation scope. Review checked
`state/normalization-1793228964/run.json`, both native logs and the complete
`normalization-benchmark.json`. The frozen Go source and DuckDB binary hashes
match the manifest, and the frozen Go source matches the current implementation.
Both containers exited 0 without OOM. Independent footer reads confirm exactly
140,578,674 rows in both the retained export and normalized output.

| Measurement | Accepted result | Scope |
| --- | ---: | --- |
| Normalization | 858.89 s (14m19s) | Excludes source hashing and subsequent verification |
| Exact full-source verification | 762.12 s (12m42s) | Complete records, full tags, scalar kinds/text and replay sequence; matched |
| Input / unique / superseded | 140,578,674 / 140,578,674 / 0 | This audited all-unique retained export |
| Terminal partitions / maximum input rows | 256 / 550,959 | Below the one-million-row cap |
| Merge passes / maximum fan-in | 3 / 8 | Chronological output with bounded readers |
| Go / largest-child RSS | 380.5 MiB / 1.66 GiB | Separate lifetime high-water marks, not additive simultaneous peaks |
| Sampled scratch peak | 3.29 GiB | Excludes the 1.35 GiB retained source; can miss transient/unlinked files |
| Normalization cgroup peak | 6.31 GiB | Includes page cache; not the later verification peak |
| Normalized output | 774,898,186 bytes | Retained private Parquet; not a published warehouse file |

The saved before/after source SHA-256 values match, and the report records
`source_unchanged: true` and `full_source_multiset_equal: true`. The separate
50,000-identity/10,000-overwrite oracle gate passed, covering behavior absent
from the all-unique full-day input. Together these close the bounded normalization
correctness/capacity blocker for these tested inputs. They do not prove arbitrary
row widths/skew or a complete archival lifecycle. The earlier failed global sort
and this successful run are not a controlled timing comparison; their disk/
measurement scopes also differ, so no whole-pipeline speedup ratio is claimed.

Keep this implementation and proceed with integration validation rather than
tuning the merge further first. Rerun the native lifecycle/matching-engine gates
with the current normalizer, then use a **fresh** full-day capacity preparation
with the existing budgets (10 GiB Influx, 8 GiB archiver, 3 GB native buffer).
Do not resume or repoint the retained failed warehouse. The remaining acceptance
work is independent Influx reconciliation, adopted-layout packing, real scoped
publication/readback, pinned-original comparison, Arrow/NDJSON/client reads and
re-verified fixture deletion while retaining the next-day sentinel. Resource
peaks must cover the complete lifecycle before making a full-day capacity claim.

This review updated documentation only. No existing run data, remote objects or
services were changed, and no commits or pushes were made.

#### Native gates, engine gate, and fresh full-day (2026-10-28)

Native gates ran in `golang:1.26.5-bookworm` against the current normalizer.
`TestNativeCanonicalPartitionComparison` passed in 34.13s,
`TestNativeLifecycle` in 3.70s, and `TestNativeBoundedPluginPacking` in 2.43s
(`ok` in 40.46s, exit 0). Log: `.tmp/native-gates-1793231488.log`.

A fresh matching-engine fixture, `state/source-fixture-1793231568`, passed
`TestInfluxEngineConsistency` in 92.56s. Compose came up and down cleanly.
Report: `state/engine-gate-1793231568`. The fixture was stopped afterward.

The fresh full-day is `state/full-day-20250301-1793231701`, project
`sage-archive-live-adaa0bae6efb47eda64cbfc7792f3414`, job
`9982f31e-865e-49b9-82bc-138c397ba47e`. Budgets stayed at 10 GiB Influx, 8 GiB
archiver, and 3 GB DuckDB. It did not reuse run `1793217993`. Before the run
the VM had about 17.2 GiB MemAvailable and 231 GiB free. `oom_events` is empty.
Services are stopped. The published prefix remains
`cold-storage-archive-live-adaa0bae6efb47eda64cbfc7792f3414` and must stay paired
with this local directory. No remote objects were deleted, and no commit or
push was made.

Seed acknowledged 140,578,674 rows in 5,216.63s, and the next-day sentinel count
was 1. Export took 483.21s and wrote `export.parquet` at 1,454,783,735 bytes.
The `EXPORTED` attempt then passed in 2,944.41s: identity-partition normalization
kept all 140,578,674 rows with zero superseded rows, 256 partitions, a maximum
of 550,959 rows, three merge passes, and fan-in at most eight. Partition,
sort/collapse, and merge took 303.76s, 189.16s, and 396.41s. The normalized file
is 774,832,276 bytes. That attempt also includes the independent Influx query
and the exact comparison. Packing took 2,817.54s and publication/commit took
423.12s. Fifteen adopted-layout data files total 457,763,714 bytes under
`day_time=20148`. Worker remote verification took 1,312.64s, with
`full_multiset_equal: true` and `hint_published: true`. Routing took 0.002s.
Startup-inclusive archive-to-routed wall time is 7,985.50s. The separate probe
comparison also matched all 140,578,674 rows in 728.78s across 256 partitions,
with Iceberg format 3 and layout `bounded-plugin-v1`.

Through these attempts the largest recorded Go RSS is 384,438,272 bytes and the
largest reaped child RSS is 3,341,127,680 bytes; those are separate lifetime
marks. Influx cgroup peak is 3,239,579,648 bytes. The archiver cgroup peak is
8,589,934,592 bytes, equal to its limit, with no OOM kill. The verify probe
cgroup peak is 4,294,967,296 bytes, equal to its limit, and that comparison
still passed. Sampled logical peaks were 8,568,699,359 bytes on `/state`,
1,008,632,064 bytes on its spill directory, 1,547,205,838 bytes on `/influx`,
and 4,886,988,607 bytes on `/bench`. Host free space afterward was about 225 GiB.

The run failed at the API probe before deletion. `api-before-delete.json`
records `URLError` connection refused after 1.13s. The API container had only
logged `connecting to influxdb` and exited 2, not from OOM, when the harness
stopped the stack. A later start of that same stopped container reached
`cold DuckDB v2.0.0` and `service listening on :10000` in about two seconds; it
was stopped again without a query, deletion, or publication. Arrow, NDJSON, and
client reads did not run. Fixture deletion, pre-delete re-verification, and the
post-delete sentinel check did not run. `run.json` is `passed: false`,
`failed_phase: api`. This is not a full-day capacity pass. The job remains
`ROUTED` with one deletion pending in the saved status.

#### Review: full-day publication verified; API startup wait corrected (2026-10-29)

Accepted through routing and independent original-source verification, not as a
complete lifecycle pass. Review checked the saved `verify.json`, worker SHA/full-
multiset evidence, stage profiles, resource report and API failure report for
`full-day-20250301-1793231701`. Every recorded worker attempt through routing
succeeded. The independent comparison matched all 140,578,674 original rows in
256 bounded exact partitions. Worker readback verified all 15 data-file hashes
and the full multiset before publishing the hint. The recorded warehouse contract
is Iceberg v3, `timestamptz_ns`, `value variant`, `meta map`, `bounded-plugin-v1`.

| Stage | Accepted wall time | Scope |
| --- | ---: | --- |
| Export | 483.21 s | Actual pinned-engine export to compressed staging |
| Reconcile | 2,944.41 s | Bounded normalization, independent Influx query and exact equality |
| Build/pack | 2,817.54 s | Adopted physical layout and exact packing validation |
| Publish/commit | 423.12 s | Fifteen files, 457,763,714 data bytes |
| Worker remote verification | 1,312.64 s | Object SHA checks, complete multiset and hint publication |
| Archive-to-routed | 7,985.50 s (2h13m06s) | Includes startup/polling; excludes seed and subsequent original-source probe |
| Independent original-source probe | 728.78 s | Complete pinned original versus published day; matched |

Sampled `/state` peaked at about 7.98 GiB, including a 0.94 GiB spill subtree;
these paths overlap. `/bench` peaked at 4.55 GiB and Influx at 1.44 GiB logical.
These maxima are not a simultaneous combined filesystem peak. No OOM was
recorded, but archiver/probe cgroup peaks touched their limits. This does not
establish concurrency or spare memory headroom. Earlier failed runs and this
successful-through-routing run differ in scope/configuration, so no controlled
whole-pipeline performance ratio is claimed.

The remaining failure is at API startup/readiness. The API probe failed with
connection refused in 1.13 seconds, before any Arrow/NDJSON/client response was
validated. The code started Compose's API service and immediately invoked the
probe, with no listener-readiness wait. A later successful restart is consistent
with that race; it does not prove why the original process exited 2 during
teardown, or establish any API query success. The original run remains failed,
stopped, paired with its published prefix and `ROUTED`, awaiting fixture deletion.

The capacity harness now runs an explicit `api-ready` stage: poll `/metrics`
with two-second requests for up to 240 seconds. In this API, that listener is
registered after cold backend initialization. The check does not execute a data
query or acquire a read grant. Actual Arrow/NDJSON/client checks still fail on
their first error. On failure the host also saves a bounded, credential-redacted
API log tail before stopping services, alongside existing Influx diagnostics.
A persistent startup failure is reported as `api-ready` and never enables deletion.

All 21 offline capacity tests and 12 live-harness tests passed after this fix.
New coverage simulates connection refusal followed by readiness, rejects an
unexpected endpoint response, and proves readiness failure prevents data probes
and deletion while capturing both service logs. No production API, storage or
verification behavior was changed. The readiness fix still needs VM validation.

Next run the small real live integration gate with current code before another
expensive full-day sweep. It already waits for real API reads during startup and
checks Arrow/NDJSON/client reads, routing, correction and guarded fixture deletion.
Then use a fresh full-day capacity preparation with the readiness fix to finish
the remaining full-day API/readback/deletion acceptance work. Do not automatically
resume, repoint or delete this run/prefix. This review made no service/remote-data
changes, and no commits or pushes were made.

#### Small live gate passed (2026-10-29)

The small live gate passed in about 93 seconds. Directory
`state/live-f26000ac415445ceb49406fc227504ab`, `passed: true`, phase `complete`,
services stopped. Its March 1 job `6b401dc6-6f3d-4985-9fe2-78325f1cc8d9` reached
`COMPLETE`. Final status has no pending jobs, readers, writers, or deletions.
Arrow, NDJSON, and the real client were checked, and the old cold snapshot stayed
readable. Prefix
`cold-storage-archive-live-f26000ac415445ceb49406fc227504ab` stays paired with
that directory.

#### Review: current small live integration accepted (2026-10-29)

Accepted for the tested small live scenarios with the current compressed/
partitioned normalization implementation. Review verified all four frozen build
context hashes against `run.json`, all eleven functional stage reports, and the
final archive job at `COMPLETE` with services stopped. Final status has zero
readers, writers, late work, pending/failed jobs, deletions and quarantine items.

Independent decoding of saved final Arrow **and** NDJSON responses matched the
complete expected records for all three ranges: mixed (5), archived March 1 (4),
and retained March 2 (1). Arrow timestamps remain nanoseconds, values/metadata
match, and the actual client report confirms Arrow for each range. The run also
records old-snapshot readability after corrections/deletion and the v3/VARIANT/
MAP/bounded-plugin warehouse contract.

The old hot grant demonstrably kept deletion blocked at `ROUTED`; releasing it
allowed re-verification and fixture deletion to complete. All four verification
journal entries have full multiset equality: archive commit, archive pre-delete
re-verification and both late commits. The archive commit/pre-delete object hash
maps match exactly. Final broker totals are six published, six acknowledged and
two redeliveries, with no queued or unacknowledged messages. This supports the
tested durable late/correction/redelivery and reader-drain behavior; it does not
add untested crash scenarios to the acceptance claim.

No new implementation change is required from this result. Next run a fresh
full-day capacity preparation using the current readiness fix and existing
budgets:

```sh
python3 sage-data-rework/archive-exporter/testing/capacity/run.py --influx-memory 10g
```

The small gate uses its existing wait for real API reads during startup; it does
not itself execute the full-day harness's new `api-ready` stage. That stage,
full-day Arrow/NDJSON/client probes, pre-delete re-verification, confirmed full-
day deletion and hot-sentinel retention still need to complete in one full-day
run before closing that capacity gate. Keep previous directories paired with
their prefixes; no failed run is automatically resumed or repointed. This review
updated documentation only: no existing data or services were changed and no
commits or pushes were made.

#### Archive-to-routed performance review and retained-artifact experiment (2026-10-29)

The latest full-day archive-to-routed time was **7,985.50 seconds
(2h13m06s)**. Most time is spent in reconciliation and local packing, followed
by remote verification. The pipeline uses parallelism inside DuckDB, but the
archival DuckDB sessions were fixed at two threads. Independent normalization
partitions, merge groups, packed files, uploads/readbacks and day jobs currently
run sequentially. The Go Parquet packing loop is also sequential. More server
cores alone therefore will not automatically accelerate all this work.

| Stage | Wall time | Approx. share of archive-to-routed | Average CPU cores used |
| --- | ---: | ---: | ---: |
| Reconcile | 2,944.41 s (49m04s) | 37% | 1.68 |
| Build/pack | 2,817.54 s (46m58s) | 35% | 1.95 |
| Worker remote verification | 1,312.64 s (21m53s) | 16% | 1.84 |
| Export | 483.21 s (8m03s) | 6% | 1.54 |
| Publication/commit | 423.12 s (7m03s) | 5% | 0.06 |

CPU figures are the measured self-plus-children CPU-second deltas divided by
stage wall time, not utilization of the entire host. Stage totals leave a small
startup/orchestration remainder. Reconciliation includes normalization, the
independent Influx CSV query and exact comparison. Build includes typed expected
materialization, chronological batching, plugin/vsn/time sorting, native VARIANT
packing, audits, per-file exact comparisons and a whole-build exact comparison.
Remote verification includes object SHA readbacks and a full remote exact scan;
its CPU usage suggests it is not exclusively waiting for the network.

The published day has 140,578,674 rows in 15 adopted-layout Parquet files totaling
457,763,714 bytes. Source validation against the original fixture separately took
728.78 seconds. The eight-GiB archive/four-GiB probe container peaks reached their
limits without an OOM. Those are laptop test constraints, **not production sizing
or architecture requirements**. The service will run on a larger server; thread,
memory and future worker settings should be measured there. The first controlled
thread comparison keeps native memory equal between arms and applies no container
CPU or memory cap by default.

Recommended optimization order:

1. Measure two versus four DuckDB threads on retained artifacts, with detailed
   build substeps, before spending time reseeding Influx. Partitioned-writer open
   files must scale with threads (16 initial hash partitions × thread count).
2. Use those timings to identify repeated verification cost. Each packed file is
   compared exactly with its chronological source window, then the complete build
   is compared again. These checks may contain avoidable repeated work, but any
   consolidation must still prove both correct file-window assignment and complete
   coverage. Do not replace exact comparison with counts or hashes alone.
3. Consider two bounded packing workers after the serial baseline. More DuckDB
   threads do not parallelize the Go packing loop. Use separate scratch paths and
   explicit server resource budgets for concurrent workers.
4. Measure two/four concurrent uploads and SHA readbacks, keeping a single catalog
   commit after all objects pass. The current upload includes a SHA readback and
   later worker verification reads objects again; freshness/integrity requirements
   must remain explicit before consolidating checks. Pre-delete verification stays.
5. Consider overlapping independent Influx query extraction with normalization,
   measuring source/database/storage contention rather than assuming a benefit.

DuckDB 2.0 already issues asynchronous remote Parquet reads. If remote bandwidth
is underused, inspect `async_threads` and `read_ahead_depth`; adding CPU threads is
not automatically the right network optimization. The API's async settings do not
configure the archiver. See the official [DuckDB workload tuning guide](https://duckdb.org/docs/2.0/guides/performance/how_to_tune_workloads).

As a sensitivity calculation only, halving reconciliation and packing would save
about 48 minutes, bringing this run to roughly 1h25m. This is **not a forecast**;
thread scaling, spilling, serial packing and storage throughput must be measured.

##### Implemented retained-artifact packing/verification benchmark

Production `Worker.Build` and the new benchmark share `MaterializeExpected` and
`PackExpected`. Small JSON reports (`expected-profile.json`,
`packing-profile.json`) record elapsed wall time for typed expected copy/fsync,
row counting, each chronological batch's sort/copy, Go packing, file fsync,
layout audit, per-file exact comparison and final whole-build exact comparison.
Exact comparisons additionally split count prechecks, canonical routing/refinement,
and exact leaf SQL, with leaf partition counts. Failed steps are marked; no SQL
or credentials are included. Existing verification,
10-million-row file cap, plugin/vsn/time order, plugin-pure groups capped at
262,144 rows and chronological batching remain in place.

`--duckdb-threads` configures archival native threads (default two). Partitioned
normalization/comparison writers now allow 16 × threads open files. This prepares
controlled testing; it does not yet promote four threads or introduce file-worker
concurrency.

The new `archive-exporter/testing/packing/run.py` freezes source and the Linux
DuckDB CLI, compiles a separate test binary, runs small native correctness gates
at both thread counts, then runs two and four threads serially in fresh offline
containers. Retained `expected.parquet` and `export-dedup.parquet` are mounted
read-only. The benchmark recreates the production typed expected file, runs every
production packing check, checks the audited row count, and performs an additional
full exact comparison against the retained expected file. SHA-256 before/after
and across arms proves unchanged identical inputs. This extra reference comparison
and input hashing are outside the primary build time. Native memory is equal in
both arms; default is 3GB. Container CPU/memory caps are optional; any CPU cap must
allow at least four CPUs.

Each arm reports build/materialization/packing/substep wall times, file bytes,
process RSS, cgroup peak when available and sampled scratch peak. Peak metrics are
captured through the build, before the extra reference comparison. OS caches are
uncontrolled and hashing warms source inputs. One repetition screens candidates;
`--reps 2` runs 2/4 then 4/2 to reduce order bias. Fresh directories and diagnostic
containers are retained, including failures; no original run is resumed or deleted.

Run instructions: [retained packing benchmark](../archive-exporter/testing/packing/README.md).
Local Go/Python checks validate the harness; actual native correctness and full-day
2/4-thread timings still require the VM. This local benchmark does not test
Pelican uploads, remote verification, API reads, routing or deletion and does not
close the outstanding uninterrupted full-day lifecycle gate.

Local implementation validation: `go test -race ./...`, `go vet ./...`, a native
host Go build, five packing-runner tests and all 21 capacity-runner tests passed.
The packing runner tests cover trial order, equal resource settings/read-only
mounts, `/node-data` rejection, fresh preparation/input preservation and stopping
before full trials when the first correctness gate fails. Native DuckDB gates
were skipped locally: Docker socket access is denied in this session and the
available CLI is a Linux executable. No native benchmark result or thread-count
winner is claimed yet. No existing artifacts were deleted, and nothing was
committed or pushed.

#### Retained packing thread sweep: 2, 4, 8, 16 (2026-10-29)

Ran one forward pass, `--threads 2,4,8,16`, at equal 3 GB DuckDB memory and with
no container CPU or memory cap. Directory `state/packing-1793281873`. The VM
reported 16 CPUs and about 21.5 GiB. Inputs were the retained
`expected.parquet` and `export-dedup.parquet` from job
`9982f31e-865e-49b9-82bc-138c397ba47e`. All four small gates passed in under a
second. All four trials exited 0 without OOM, kept 140,578,674 rows, matched
the retained expected file exactly, and left the inputs unchanged with identical
SHA-256 across arms. `run.json` is `passed: true`.

`build_seconds` is materialization plus production packing and its checks. The
extra retained-reference comparison is outside that time. Later arms ran after
earlier ones, so OS caches were warmer; this was not the counterbalanced
2-then-4 / 4-then-2 repeat.

| Threads | Build | Materialize | Pack total | Per-file exact | Whole-build exact | Sort/copy | Go pack | Extra reference |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2 | 2,854.40 s | 186.11 s | 2,668.29 s | 1,154.04 s | 1,217.85 s | 156.90 s | 136.26 s | 1,219.59 s |
| 4 | 1,898.98 s | 116.25 s | 1,782.73 s | 677.51 s | 757.87 s | 211.39 s | 132.60 s | 704.46 s |
| 8 | 1,751.46 s | 97.99 s | 1,653.46 s | 571.07 s | 559.49 s | 364.06 s | 155.25 s | 560.61 s |
| 16 | 1,388.69 s | 87.36 s | 1,301.32 s | 387.30 s | 442.08 s | 335.37 s | 133.13 s | 483.97 s |

Exact comparison time fell as threads increased. Chronological sort/copy did
not: it was slowest at 8 threads. Go packing stayed near 133–155 seconds.
Each arm wrote 15 files. Compressed sizes differed slightly
(457,626,515 / 457,962,662 / 457,788,286 / 457,373,808 bytes) while exact record
comparison still matched. Largest-child RSS stayed about 3.08–3.18 GiB. Cgroup
peaks were 7.16, 7.44, 6.75, and 7.57 GiB. Sampled scratch peaked around
4.93–5.09 GiB. No production thread count is selected from this single ordered
pass. The uninterrupted full-day lifecycle gate remains open. No remote objects
were changed, and nothing was committed or pushed.

##### Review: verification preparation dominates; use operation-specific threading next

Reviewed the saved evidence in `state/packing-1793281873`: `run.json`, all four
benchmark/packing reports and native gate logs. The frozen Go source tree matches
the recorded code SHA-256. Both retained input files still match the SHA-256
recorded by every arm. All four native gates passed, all eight containers exited
zero without OOM, and each arm records 140,578,674 rows, 15 files, 77 successful
packing steps, successful per-file/whole-build exact comparisons and an additional
successful retained-reference comparison. This accepts the tested local build
correctness at 2/4/8/16 threads; remote publication and the full lifecycle remain
separate acceptance work.

| Threads | Build | Observed speedup vs 2 | All production exact checks | Canonical routing/refinement inside those checks | Exact leaf SQL inside those checks |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 2 | 47m34s | 1.00× | 2,371.90 s (83.1% of build) | 2,074.43 s | 294.00 s |
| 4 | 31m39s | 1.50× | 1,435.38 s (75.6%) | 1,231.37 s | 200.16 s |
| 8 | 29m11s | 1.63× | 1,130.56 s (64.5%) | 920.35 s | 204.42 s |
| 16 | 23m09s | 2.06× | 829.39 s (59.7%) | 689.16 s | 135.26 s |

The comparison columns are nested: routing and exact leaf SQL are included in
all production exact checks, not additional build work. Remaining comparison
time includes count prechecks, filesystem inventories, cleanup and orchestration.
At 16 threads, canonical routing/refinement alone is **49.6% of total build time
and 83.1% of exact-check time**. The expensive part of verification is mostly
reading/converting records into canonical primitive columns and writing/refining
comparison partitions, rather than the final `EXCEPT ALL` operations. “Routing”
includes both initial source canonicalization/staging and recursive local
repartitioning; these timings do not isolate conversion CPU from disk I/O.

Code inspection explains repeated work: each per-file check canonicalizes its
expected window and packed output. The whole-build check then canonicalizes both
complete sides again. Whole-build verification starts with 16 hash partitions
and refines oversized partitions; it finishes with 256 nonempty leaf partitions,
versus 240 across the 15 per-file comparisons. The profiles combine initial and
recursive routing, so they do not establish the separate cost of that refinement.

The Go packer remains about 133 seconds at both 2 and 16 threads. At 16 threads
that is only 9.6% of build time. Even ideal two-worker acceleration of packing
alone would save roughly 67 seconds, before overhead/contention; parallelizing
packing is therefore a lower priority than verification preparation. File fsync,
layout auditing and counting are negligible in this run. This says nothing about
remote publication/readback costs, which this benchmark does not exercise.

The measured 16-thread build is 51.4% shorter than the two-thread arm. However,
chronological batch sort/copy regressed from 156.90 seconds at two threads to
335.37 seconds at sixteen (364.06 seconds at eight), while materialization and
verification improved. The sweep does not isolate why sorting regressed: equal
3GB memory with more threads may change spill behavior, but cache order, host
contention and execution differences were not separately controlled. Do not
attribute it conclusively to a memory limit or select sixteen threads globally.

**Recommended next experiment:** retain the same artifacts and every exact check;
compare a uniform 16-thread baseline with materialization/comparison at sixteen
threads and batch sort/copy at two threads, with a reversed-order repeat. This
requires operation-specific settings rather than changing the global default.
Validate identical rows, layout and full exact proofs for each candidate. The
mixed setting is a hypothesis assembled from separate arms, not a measured win.
After that, target canonical staging reuse or direct/adaptive initial fanout to
reduce repeated preparation. Any reuse design must preserve per-file source
window assignment, complete coverage, multiplicity, full value/type/metadata/ns
comparison and fail-closed behavior; counts or hashes alone are insufficient.
Fresh remote/pre-delete verification stays in place.

No production setting or verification gate was changed during this review. The
forward-only sweep remains susceptible to order/cache effects. It demonstrates
substantial local build improvement, not a measured twofold end-to-end archival
speedup. No existing artifacts or remote objects were changed or deleted, and
nothing was committed or pushed.

#### Split-thread packing comparison: uniform 16 versus sort/copy at 2 (2026-10-29)

Ran the recommended paired comparison in `state/packing-split-1793293612`:
materialization and exact comparison at 16 threads, with batch sort/copy either
at 16 or at 2. Equal 3 GB DuckDB memory, no container CPU or memory cap, same
retained `expected.parquet` and `export-dedup.parquet` from job
`9982f31e-865e-49b9-82bc-138c397ba47e`. `--reps 2` ran uniform 16, then 16 with
sort/copy at 2, then the reverse. Both small gates and all four trials exited 0
without OOM. `run.json` is `passed: true`. The frozen source tree matches code
SHA-256 `a39912ae…c4ee`. Host SHA-256 of both inputs still matches every arm
(`c91128eb…1f6e`, `b486bf37…356e`). Each arm kept 140,578,674 rows, 15 files, 77
successful packing steps, 240 per-file leaf partitions plus 256 whole-build
leaves, `source_unchanged`, and `full_reference_equal`.

`SortThreads` applies only to chronological batch sort/copy. Production leaves
it unset, so that step still follows the session thread count. The production
default remains two threads for every operation.

| Pass | Sort threads | Build | Materialize | Sort/copy | Go pack | Per-file exact | Whole-build exact | Extra reference |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 16 | 1,427.61 s | 72.73 s | 372.63 s | 135.96 s | 382.95 s | 459.86 s | 469.21 s |
| 1 | 2 | 1,193.44 s | 83.69 s | 198.22 s | 134.12 s | 351.17 s | 422.81 s | 434.41 s |
| 2 | 2 | 1,154.11 s | 80.59 s | 183.73 s | 133.77 s | 339.88 s | 412.85 s | 424.50 s |
| 2 | 16 | 1,238.54 s | 82.09 s | 303.96 s | 133.18 s | 316.20 s | 399.74 s | 391.48 s |

Sort/copy is the step that follows the sort-thread setting. It was 174.41 s
faster at two threads on the first pass and 120.23 s faster on the reversed
pass, where the two-thread arm ran first. Exact-check time moved the other way:
842.81 s, 773.99 s, 752.74 s, then 715.94 s, declining with run order while
routing stayed about 82–83% of those checks. That verification improvement is consistent with cache warming, but this run
does not isolate cache effects from other host variability. Go pack stayed 133–136 s. On the reversed pass the
two-thread sort arm was still 84.43 s faster end to end (1,154.11 s versus
1,238.54 s), because the sort saving exceeded the later arm’s warmer exact
checks.

Compressed sizes again differed slightly (457,382,110 / 457,336,868 /
457,515,292 / 457,497,493 bytes) while exact records matched. Child RSS stayed
about 3.08–3.17 GiB. Cgroup peaks were 7.58–7.62 GiB. Sampled scratch peaked
around 5.09 GiB.

This accepts the local hypothesis for batch sort/copy: two threads beat sixteen
at the same 3 GB limit in both orders. It does not change a production setting.
Verification preparation is still the majority of the build (about 65% on the
mixed arms), so canonical staging reuse or adaptive initial fanout remains the
larger follow-up. Remote publication, readback, and the uninterrupted full-day
lifecycle stay separate. No remote objects were changed, and nothing was
committed or pushed.


##### Review: split threading accepted for the tested build; adaptive comparison fanout next

Independently reviewed `state/packing-split-1793293612/run.json` and all four
trial reports. The frozen source SHA-256 matches the manifest; both original
retained input files still match the hashes recorded in every arm. All nested
comparison completion reports passed. The evidence supports retaining the
16-thread materialization/comparison plus two-thread batch sort configuration
as the local build candidate for the next experiment, at the tested 3GB native
memory setting. It does not establish an optimal setting on other hardware or
for reconciliation/remote verification, which were not part of these trials.

| Pair/order | Uniform 16 build | 16 with sort at 2 build | Mixed saving |
| --- | ---: | ---: | ---: |
| Uniform first | 1,427.61 s | 1,193.44 s | 234.17 s (16.4%) |
| Mixed first | 1,238.54 s | 1,154.11 s | 84.42 s (6.8%) |
| Mean of the two measurements | 1,333.07 s | 1,173.78 s | 159.30 s (11.9%) |

The mean is descriptive of two trials per configuration, not a statistical
confidence interval or a promised server speedup. More persuasive than the mean
is that sort/copy improved in both orders (174.41 and 120.22 seconds), even when
the mixed arm ran before the later, faster-verifying uniform arm. The mixed
builds now take about 19–20 minutes. Exact checking still accounts for about
65% of those builds, with canonical staging/refinement the largest component.
No further thread-count sweep or packing-worker implementation is needed before
addressing this repeated preparation work.

Recommended next candidate: **adaptive initial comparison fanout**, tested against
the current comparison at the same 16-thread/two-sort-thread configuration.
`EqualCanonical` currently starts at 16 partitions even for the 140-million-row
whole-build comparison. Its roughly 8.8-million-row initial buckets must then be
repartitioned to finish with 256 leaf partitions under the one-million-row cap.
Choosing the initial fanout from the already validated input counts could route
the full day directly into 256 buckets, while keeping 16 for ten-million-row
per-file comparisons. This aims to avoid a complete intermediate repartitioning
level; it does not eliminate initial canonicalization or the second whole-build
proof. Actual partition sizes must still be checked and oversized/skewed buckets
recursively refined. Hashes remain routing only; every leaf still gets the exact
two-way multiset comparison. Writer/file-buffer budgets must remain explicit as
fanout increases, and recursive refinement must consume the correct hash bits.

This is a narrower first change than sharing canonical artifacts across separate
proofs: it preserves the current per-file and whole-build verification boundaries.
Measure initial routing and recursive refinement separately in the next retained-
artifact benchmark to show whether the removed pass offsets the higher initial
fanout overhead. Use the same reversed-order comparison, identical input hashes,
full retained-reference proof and existing layout/correctness gates. Canonical
staging reuse remains a later candidate if preparation still dominates.

No production defaults, implementation, remote data or existing artifacts were
changed in this review. The full uninterrupted API/readback/deletion gate remains
open. Nothing was committed or pushed.

##### Implemented: adaptive initial comparison fanout and paired benchmark

`EqualCanonical` now supports an explicit `adaptive` mode alongside the unchanged
`fixed` default. After validating equal input counts, adaptive mode chooses the
smallest power of two from 16 to 256 whose estimated average bucket fits the
one-million-row comparison limit. Thus the full 140,578,674-row day starts at
256 buckets and each ten-million-row file comparison still starts at 16. Actual
bucket cardinalities are still verified on both sides. Oversized/skewed buckets
are refined sixteen ways using hash bits after those consumed by the initial
fanout. Exhausting the hash without reaching the row bound fails closed as before.
Every bounded leaf retains the full two-way `EXCEPT ALL` proof; both per-file
and whole-build checks remain, with no canonical-artifact reuse.

Comparison writers use the same `min(16 × threads, 256)` open-file budget in both
modes, retaining the 65,536-row partition flush threshold. This caps writer count
independently of initial fanout rather than multiplying buffers by 256 buckets.
At the tested sixteen threads the budget is the same 256 used in prior sweeps.
This is an open-writer bound, not a guarantee about total process memory or disk
usage; native memory/spill controls still apply.

Nested profiles now separate `initial_routing_seconds`, `refinement_seconds`
and `refinement_passes`, alongside `initial_fanout`, mode and writer budget.
`routing_seconds` remains the aggregate. All production comparison callers share
the implementation; rollout is explicit through
`--duckdb-comparison-partitioning adaptive`. The production default remains fixed
partitioning and two threads; the benchmark does not change a running service.

The retained-artifact runner adds `--compare-partitioning`. Combined with
`--threads 16 --sort-threads 2 --reps 2`, it runs fixed/adaptive/adaptive/fixed at
identical materialization, comparison, sort and memory settings. It first runs
an offline native comparison gate exercising both modes, small forced row caps,
actual 256-way initial routing, recursive skew refinement, equal-bucket record
collisions, equal-total/different-multiplicity failures, empty equality and
exhausted-hash failure. Each arm also runs the native v3/VARIANT/MAP/nanosecond
packing gate. The extra full retained-reference comparison uses the **fixed**
comparator for both arms, outside the primary build timing. Inputs remain mounted
read-only, hashes must match across arms, runtime networking is disabled and all
outputs go into a fresh isolated run directory.

Run from the workspace root on the VM:

```sh
python3 sage-data-rework/archive-exporter/testing/packing/run.py \
  --expected sage-data-rework/archive-exporter/state/full-day-20250301-1793231701/coordinator/jobs/9982f31e-865e-49b9-82bc-138c397ba47e/expected.parquet \
  --normalized sage-data-rework/archive-exporter/state/full-day-20250301-1793231701/coordinator/jobs/9982f31e-865e-49b9-82bc-138c397ba47e/export-dedup.parquet \
  --threads 16 --sort-threads 2 --compare-partitioning --reps 2
```

The runner keeps 3GB native memory in both arms and applies no container CPU or
memory cap unless requested. Keep/return `run.json`, gate logs and all trial
`results/benchmark.json` / `packing-profile.json`. Compare total build time,
whole-build initial staging versus refinement, exact SQL time, actual fanout/
refinement counts and resource peaks. A direct initial split may cost more, so
performance improvement remains unproven until this paired VM run completes.
The larger full lifecycle acceptance gate remains open; this experiment performs
no Pelican publication, routing, API reads or Influx deletion.

Local validation passed: `go test -race ./...`, `go vet ./...`, host Go build,
eight packing-runner tests and all 21 capacity-runner tests. New local tests
cover fanout thresholds/capping, unchanged fixed defaults, writer budgets,
identical paired thread/memory settings, reversed arm order, preparation records
and stopping before full trials when the adaptive correctness gate fails. Native
DuckDB tests were skipped locally; Docker socket access is denied and the retained
CLI is Linux-only. The new native gates and performance result remain pending
VM execution. No existing artifacts were deleted, no remote storage was accessed,
and nothing was committed or pushed.

#### Adaptive comparison fanout VM run (2026-10-29)

Ran the paired comparison in `state/packing-adaptive-1793306319` at 16
materialization/comparison threads, batch sort/copy at 2, and 3 GB native
memory, with no container CPU or memory cap. Order was fixed, adaptive,
adaptive, fixed. The VM reported 16 CPUs and about 21.5 GiB. Inputs were the
same retained `expected.parquet` and `export-dedup.parquet`. `TestNativeAdaptiveComparison`
passed in 15.60 s. Both packing gates passed. All seven containers exited 0
without OOM. `run.json` is `passed: true`. The frozen source matches code
SHA-256 `f7c9c244…6c6a`. Host SHA-256 of both inputs still matches every arm
(`c91128eb…1f6e`, `b486bf37…356e`). Each arm kept 140,578,674 rows, 15 files,
full per-file and whole-build proofs, `source_unchanged`, and a fixed-mode
retained-reference proof outside build time.

Adaptive mode started the 140,578,674-row whole-build comparison at 256
partitions, refined nothing, and finished with 256 nonempty leaves. Fixed mode
started that comparison at 16 partitions, refined all 16 once, and also finished
at 256 leaves. Every per-file comparison stayed at an initial fanout of 16, with
zero refinement passes and 240 leaves, in both modes. Writer budget stayed 256
open files.

| Pass | Mode | Build | Whole initial route | Whole refine | Whole exact SQL | Initial fanout | Refine passes |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | fixed | 1,116.43 s | 275.22 s | 65.86 s | 60.80 s | 16 | 16 |
| 1 | adaptive | 1,508.72 s | 689.18 s | 0 s | 112.57 s | 256 | 0 |
| 2 | adaptive | 1,557.59 s | 712.13 s | 0 s | 82.12 s | 256 | 0 |
| 2 | fixed | 1,152.06 s | 293.39 s | 72.45 s | 61.98 s | 16 | 16 |

Direct 256-way staging removed the 66–72 s refinement pass and replaced it with
about 414–419 s of extra initial routing. Whole-build comparison grew from
403.73 / 429.68 s to 803.26 / 795.53 s. Build time grew by 392.29 s on the first
pass and 405.53 s on the reversed pass, where adaptive ran first. Per-file
checks, two-thread sort/copy (171–190 s), and Go pack (131–138 s) stayed in the
same range. Adaptive cgroup peaks were 11.17 and 11.04 GiB, against 7.58 and
7.56 GiB for fixed. Sampled scratch peaked at 7.41 and 7.59 GiB, against 5.09
GiB. Child RSS was about 3.40 GiB adaptive and 3.06–3.13 GiB fixed. Compressed
sizes differed slightly (457,595,292 / 457,249,374 / 457,769,187 / 457,497,180
bytes) while exact records matched.

The measured local candidate remains fixed partitioning at 16 comparison threads
and two sort threads. Production still defaults to fixed partitioning and two
threads. Initial canonical routing is still the large fixed-mode cost (528.05 s
and 559.82 s across each build’s checks), so canonical staging reuse remains the
later preparation candidate. Remote publication, readback, and the uninterrupted
full-day lifecycle stay open. No remote objects were changed, and nothing was
committed or pushed.

##### Review: reject direct adaptive fanout for this workload

Independently checked `state/packing-adaptive-1793306319/run.json`, all four trial
benchmark reports and all three native gate logs. The frozen source SHA-256
matches the manifest. Both original input files still match the hashes recorded
in every arm. All seven containers exited zero without OOM, all four arms kept
140,578,674 rows in 15 files, and all 77 packing steps per arm passed. Nested
exact proofs passed in the requested modes, and every extra retained-reference
proof used the fixed comparator. This establishes tested correctness of the
adaptive implementation, but **rejects it as a performance improvement at the
tested settings**.

| Configuration | Mean build (two trials) | Mean whole-build proof | Mean whole initial staging | Mean whole refinement |
| --- | ---: | ---: | ---: | ---: |
| Fixed, 16 comparison / 2 sort threads | 1,134.24 s (18m54s) | 416.70 s | 284.31 s | 69.16 s |
| Adaptive, same threads and memory | 1,533.15 s (25m33s) | 799.40 s | 700.66 s | 0 s |

Adaptive builds were **35.2% slower on average**, adding 398.91 seconds (6m39s).
The regression appears in both orders, not just the first cold/warm pairing.
Removing the intermediate repartitioning level saved 69.16 seconds on average,
but the initial 256-way staging added 416.35 seconds. Final exact leaf SQL also
took longer (112.57/82.12 seconds versus fixed 60.80/61.98 seconds). All cases
finished with the same 256 nonempty whole-build leaves; matching final leaf
counts does not imply equivalent staging layout or execution cost.

Adaptive mode also increased cgroup and scratch peaks, as recorded above. The
open-writer budget was identical in both modes, so this is not evidence that the
configured writer limit increased. Higher partition fanout may cause more buffer
fragmentation, writer rotation, smaller files or different compression/read costs,
but the saved profiles do not isolate those mechanisms. Do not claim a particular
cause, or that a different memory budget could never make adaptive routing useful.
The result is sufficient to prefer the current fixed strategy without another
fanout/memory tuning sweep now.

**Retain fixed initial partitioning with 16 materialization/comparison threads and
two batch-sort threads as the measured local build candidate.** The service's
production defaults remain unchanged; this benchmark did not measure sixteen
threads for reconciliation or remote verification, nor complete the full-day
lifecycle gate. Do not enable adaptive partitioning for the next acceptance run.
The failed performance hypothesis and its read-only evidence remain retained.

Recommended next optimization: **reuse canonical staging within a single build**,
rather than changing its initial fanout. Fixed-mode initial staging still costs
528.05/559.82 seconds across the build's per-file and whole-build proofs. Those
proofs currently canonicalize the same expected and packed records separately.
A candidate should retain each per-file proof's canonical expected/output shards
and feed those verified shards into the whole-build exact proof, refining local
shards as necessary. Keep file-window assignment, complete row coverage,
multiplicity and full timestamp/name/tags/value-type/value comparison explicit.
Both per-file and whole-build exact checks remain; the saved staging is an
optimization of preparation, not replacement of equality with counts or hashes.

Scope reuse to a fresh build, bind the staged inputs to the corresponding
expected/packed artifacts and reject changed/missing artifacts. Benchmark against
the current fixed comparator at identical 16/2-thread settings in both orders,
with the independent fixed-mode retained-reference proof. Measure staging,
refinement, exact SQL and scratch costs separately: reuse may trade disk retention
for less repeated work. Never reuse these local proofs as remote publication or
pre-delete verification; those must still read and verify the published objects.
Implementation of this next candidate is not part of this results review.

This review changed documentation only. No production defaults or implementation
were changed, no existing artifacts were deleted, no remote storage was accessed,
and nothing was committed or pushed.

##### Implemented: build-local canonical staging reuse candidate

The new opt-in `--reuse-canonical-staging` path retains the initial canonical
expected/output shards from each successful per-file comparison. The whole-build
proof then merges those verified shard lists by the same sixteen initial hash
buckets, checks full coverage, recursively refines oversized buckets and executes
a new complete two-way `EXCEPT ALL`. It avoids another original-data canonical
copy while retaining **both** per-file source-window proofs and the whole-build
exact proof. Fixed partitioning remains required; adaptive fanout is not combined
with this candidate.

Each invocation creates a private `packing-canonical-*` directory and an in-memory
ledger, never loading earlier staging from disk. The ledger records contiguous
source offsets, row counts and generated output paths. SHA-256/size bindings guard
the original expected artifact, each packed output and every retained canonical
shard. Before and after the whole proof, bindings, shard inventories and expected
row count are checked. Per-file shards are also bound before their exact check
and validated afterwards. Missing/changed artifacts and incorrect source coverage
fail the build; hashes are integrity bindings, not a substitute for record equality.
Refinement scratch is removed after each comparison, and retained initial shards
are removed when the build invocation returns, on success or error. A process
crash may leave private scratch; it is not trusted by a subsequent build or
removed from an older run automatically.

The shared exact-comparison/refinement routine is used by baseline and reuse.
Publication, remote verification, and pre-delete verification still call the fresh
comparator; no build-local ledger is consulted there. Current defaults remain
fixed partitioning, two session threads and no reuse. The candidate is wired into
production code behind the explicit flag but is not promoted by this experiment.

Profiles now record `reused_initial`, binding time and retained canonical bytes.
For the reused whole proof, initial routing time is zero; actual recursive
refinement and exact SQL remain timed. Top-level packing binding time covers
additional expected/output hashing; nested comparison binding time covers retained
shard checks and whole-proof artifact validation. These times are included in
primary build time. Retained canonical bytes is a byte inventory, distinct from
sampled scratch peak. Increased hashing, retained files and shard-list overhead
may offset the saved copy, so no speedup is assumed before the VM result.

The paired retained-artifact harness adds `--compare-reuse`, holding fixed
partitioning, materialization/comparison at sixteen threads, batch sorting at two
and native memory at 3GB. Two repetitions run baseline/reuse/reuse/baseline.
Every trial rebuilds fresh, keeps the existing layout and verifies the complete
output against the original retained expected file using a **fixed, non-reusing**
comparator outside build timing. Inputs remain mounted read-only; no runtime
network access, service interaction or remote writes are needed.

Native gates precede expensive trials: the existing exact/collision/multiplicity
suite, a new multi-window canonical reuse gate and the v3/VARIANT/MAP/nanosecond
packing gate in each mode. New negative cases cover modified expected/packed/
canonical files, incomplete/overlapping source coverage and a deliberately altered
canonical shard with refreshed test-only bindings that must still fail exact SQL.
The existing comparison gate exercises empty inputs and exhausted-hash failure.

Run from the workspace root on the VM:

```sh
python3 sage-data-rework/archive-exporter/testing/packing/run.py \
  --expected sage-data-rework/archive-exporter/state/full-day-20250301-1793231701/coordinator/jobs/9982f31e-865e-49b9-82bc-138c397ba47e/expected.parquet \
  --normalized sage-data-rework/archive-exporter/state/full-day-20250301-1793231701/coordinator/jobs/9982f31e-865e-49b9-82bc-138c397ba47e/export-dedup.parquet \
  --threads 16 --sort-threads 2 --compare-reuse --reps 2
```

Keep/return `run.json`, all gate logs and all trial `results/benchmark.json` /
`packing-profile.json`. Compare complete build and whole-proof time, initial
staging/refinement/exact/binding costs, retained bytes, scratch and memory peaks.
Container CPU/memory caps remain optional and no laptop cap becomes a production
requirement. This experiment does not publish to Pelican or execute API reads,
routing or deletion; the uninterrupted full-day lifecycle gate remains open.

Local validation passed: `go test -race ./...`, `go vet ./...`, host Go build,
eleven packing-runner tests and all 21 capacity-runner tests. Tests cover
same-size artifact mutations/missing files/symlinks, paired settings and reversed
order, stopping at reuse-gate failure, and collection of all four distinct trial
reports in a mocked runner completion. Native DuckDB tests were skipped locally:
Docker socket access remains denied and the retained binary is Linux-only. The
new native correctness gates and real performance measurements require the VM;
no speedup or rollout acceptance is claimed yet. No existing artifacts were
deleted, no remote storage was accessed, and nothing was committed or pushed.

#### Canonical staging reuse VM run (2026-10-29)

Ran the paired comparison in `state/packing-reuse-1793316400` at fixed
partitioning, 16 materialization/comparison threads, batch sort/copy at 2, and
3 GB native memory, with no container CPU or memory cap. Order was baseline,
reuse, reuse, baseline. The VM reported 16 CPUs and about 21.5 GiB. Inputs were
the same retained `expected.parquet` and `export-dedup.parquet`.
`TestNativeAdaptiveComparison` passed in 16.00 s and `TestNativeCanonicalReuse`
passed in 5.64 s. Both packing gates passed. All eight containers exited 0
without OOM. `run.json` is `passed: true`. The frozen source matches code
SHA-256 `6115c07b…4e48`. Host SHA-256 of both inputs still matches every arm
(`c91128eb…1f6e`, `b486bf37…356e`). Each arm kept 140,578,674 rows, 15 files,
full per-file and whole-build proofs, `source_unchanged`, and a fresh fixed
retained-reference proof outside build time.

Reuse kept the per-file canonical shards and set whole-build initial routing to
0 s. That proof still refined all 16 initial buckets once, finished at 256
leaves, and ran a new `EXCEPT ALL`. Per-file checks stayed fresh in both arms:
fanout 16, zero refinement passes, 240 leaves. Retained canonical shards were
1,911,051,756 and 1,911,819,370 bytes (about 1.78 GiB).

| Pass | Arm | Build | Whole proof | Whole initial route | Whole refine | Whole exact SQL | Whole binding |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | baseline | 1,079.47 s | 393.93 s | 267.07 s | 66.16 s | 59.03 s | 0 s |
| 1 | reuse | 796.64 s | 117.34 s | 0 s | 38.66 s | 63.12 s | 14.10 s |
| 2 | reuse | 809.45 s | 133.81 s | 0 s | 48.68 s | 65.80 s | 17.79 s |
| 2 | baseline | 1,113.60 s | 410.56 s | 289.45 s | 51.64 s | 67.88 s | 0 s |

The whole proof was 276.59 s and 276.75 s shorter with reuse. Build time was
282.82 s shorter on the first pass (26.2%) and 304.15 s shorter on the reversed
pass (27.3%), where reuse ran first. Per-file exact checks stayed 290–308 s.
Sort/copy stayed 159–180 s and Go pack 134–138 s. Sampled scratch peaked at
about 5.09 GiB in every arm. Cgroup peaks were 7.57–7.64 GiB. Child RSS stayed
about 3.02–3.07 GiB. Compressed sizes differed slightly (457,614,869 /
457,563,189 / 457,741,590 / 457,756,595 bytes) while exact records matched.

This accepts build-local canonical reuse for the tested fixed 16-thread /
two-sort-thread configuration. The saved work is the second full canonical copy;
per-file preparation remains. Production still defaults to fixed partitioning,
two threads, and fresh canonical staging. Publication, remote verification, and
pre-delete verification still call the fresh comparator. The uninterrupted
full-day lifecycle stays open. No remote objects were changed, and nothing was
committed or pushed.

##### Review: canonical reuse accepted; integrate the measured build configuration next

Independently reviewed `state/packing-reuse-1793316400/run.json`, all four trial
reports and all four native gate logs. The frozen source SHA-256 matches the
manifest, and both original input files still match every arm's recorded hashes.
All eight containers exited zero without OOM. Every trial records 140,578,674
rows, 15 output files, 77 successful packing steps, fresh per-file exact proofs,
a successful whole-build exact proof and a fresh fixed-mode retained-reference
proof. Reuse is recorded only for the candidate's whole-build initial staging;
per-file checks stay fresh. Native mutation/coverage/semantic-mismatch gates passed.
This accepts the candidate for the tested build workload and configuration.

| Configuration | Mean build | Mean whole-build proof |
| --- | ---: | ---: |
| Fixed partitioning, 16 comparison / 2 sort threads, fresh staging | 1,096.53 s (18m17s) | 402.25 s (6m42s) |
| Same settings, build-local canonical reuse | 803.04 s (13m23s) | 125.57 s (2m06s) |

The observed mean build reduction is **26.8%**, saving **293.49 seconds
(4m53s)**; the whole proof is 68.8% shorter. The candidate wins by 26.2% and
27.3% in the two run orders. These are descriptive results from two trials per
configuration, not a statistical confidence interval or an end-to-end archival
speedup claim.

The intended work was removed: baseline whole-build initial staging took
267.07/289.45 seconds; reuse took zero. It still performed sixteen recursive
refinements, 256 exact leaf comparisons and artifact validation. Its final exact
SQL cost stayed comparable to baseline. Total additional binding/hashing time
(top-level packing plus nested comparisons) was approximately 18.59/22.24 seconds,
already included in the candidate build times. Those checks did not consume the
saved staging benefit.

The candidate retained about 1.78 GiB of canonical shards, but sampled scratch
peak remained about 5.09 GiB in all arms, with similar cgroup and largest-child
RSS peaks. This is evidence for the tested sequential build, not proof that reuse
has no disk cost or that concurrent builds have the same footprint. Retained shard
lifetimes change; the sampler may miss short-lived/unlinked files and cgroup
memory includes page cache. The run's exact comparison results also show that
small compressed-byte differences are not record differences.

**Recommendation: retain reuse with fixed partitioning and the measured 16/2
build threading; next integrate that configuration into the real archival
acceptance harness.** The service currently exposes global DuckDB threads and
reuse, but the two-thread sort override is wired only into the retained packing
benchmark. Add explicit build-scoped thread/sort controls so `Worker.Build` can
use the measured configuration while reconciliation and fresh remote/pre-delete
verification keep their existing settings. Do not infer performance of those
other stages from this local packing experiment or globally raise their threads
without measurement.

Then run the small real live gate with the candidate before a fresh uninterrupted
full-day lifecycle: reconciliation, build, publication, fresh remote verification,
API readiness, Arrow/NDJSON/client reads, reader drain, fresh pre-delete verification,
confirmed fixture deletion and hot-sentinel retention. Record stage/substep times
and check the retained-artifact savings against the actual build. The next
milestone is that complete run rather than another speculative packing change;
reconciliation and remote verification can be profiled afterwards using its data.

This review changed documentation only. Production settings remain unchanged;
no existing artifacts were deleted, no remote storage was accessed, and nothing
was committed or pushed. The uninterrupted full-day lifecycle gate remains open.

##### Adopted: canonical reuse is the only build verification path

Per the decision following the successful VM sweep, `PackExpected` now always
retains the verified per-file canonical shards and runs a new whole-build exact
comparison over them. The fresh-staging build branch has been removed rather
than left as an option. The DuckDB reuse switch, `--reuse-canonical-staging`,
`SAGE_PACK_REUSE`, and fresh/adaptive build comparison arms have been removed.
The previously experimental public `--duckdb-comparison-partitioning` flag was
also removed: builds require fixed partitioning. Internal adaptive-comparator
regression coverage remains; it is not exposed as a build policy.

All source-window coverage, SHA-256 bindings, shard inventory checks, recursive
refinement and per-file/whole-build exact proofs remain. `reuse_canonical_staging`
is now report-only policy evidence, not a setting. Every retained packing trial
uses reuse, and the harness rejects reports that claim a non-reusing build. It
can still compare thread counts or sort-thread overrides, with reversed repeats.
A native empty-build gate now checks the zero-row path under mandatory reuse.

Remote publication/verification, pre-delete verification and the benchmark's
independent retained-reference check still prepare fresh canonical comparison
data from the artifacts they actually read. Removing those fresh reads would
allow stale local proofs to stand in for verification of published objects; they
are separate from the removed redundant second canonical copy within a build.
No existing benchmark directories, frozen source snapshots or historical results
were removed. Earlier commands containing retired flags are historical only;
current instructions are in `archive-exporter/testing/packing/README.md`.

This adoption changes the build algorithm, not CPU configuration: production
still uses its existing thread counts. Build-scoped 16/2 controls and full live/
capacity acceptance remain follow-up work. A running VM/container is not changed
by this workspace edit. No remote storage was accessed and nothing was committed
or pushed.

Adoption validation passed locally: `go test -race ./...`, `go vet ./...`, host
Go build, seven current packing-runner tests and 21 capacity-runner tests. The
harness tests explicitly reject non-reusing build evidence and verify every
thread/sort trial uses the adopted policy. Native DuckDB integration tests,
including the new zero-row gate, require the VM; they were skipped locally.
Historical paired benchmark evidence remains unchanged.

##### Removed: adaptive initial comparison partitioning

The rejected adaptive strategy is now removed from active code, including its
DuckDB configuration field, fanout selection logic, variable-fanout SQL and
adaptive-only test cases. All exact comparisons start with sixteen hash buckets;
oversized buckets still undergo sixteen-way recursive refinement using the next
four hash bits, retaining the one-million-row leaf bound and full two-way exact
multiset checks. This is fixed initial routing with necessary bounded refinement,
not a return to the rejected direct 256-way initial staging. Canonical reuse
remains mandatory for builds. Warehouse/Parquet layout is unchanged.

The offline correctness gate is now `TestNativeBoundedComparison`; it retains
collision, multiplicity, empty-input, recursive-refinement and exhausted-hash
failure coverage on the fixed path. The packing harness invokes the renamed
gate. Report fields still identify `mode: fixed` and initial fanout 16; these
are evidence fields rather than configuration selectors. Historical adaptive
benchmark data and frozen source remain untouched.

Validation passed: `go test -race ./...`, `go vet ./...`, host Go build and all
seven packing-runner tests. Native DuckDB integration remains VM-only and was
skipped locally. No remote data or existing artifacts were changed or deleted,
and nothing was committed or pushed.

##### Wired: measured build-only 16/2-thread execution policy

`Worker.Build` now clones the base DuckDB session and applies sixteen threads to
expected-data materialization and exact comparisons, and two threads to batch
sort/copy. It applies to archive, bootstrap-merge and late-correction builds.
The clone preserves memory/spill settings, executable, HTTP setup and inherited
process lock. `Worker.Reconcile` and remote/pre-delete `Worker.Verify` still use
the unchanged base session at its existing two-thread default. The Go Parquet
packer remains sequential. Canonical reuse is mandatory and initial comparison
partitioning remains fixed; neither is an opt-in alternative.

Service flags are `--duckdb-build-threads=16` and
`--duckdb-build-sort-threads=2`, independently configurable from the existing
`--duckdb-threads=2` used outside builds. All counts are validated in 1–256 at
startup. The Worker also validates its overrides and defaults zero-valued build
fields to 16/2 for direct callers; it never mutates its shared base session.

The small live Compose command and full-day command override explicitly set all
three thread counts. Both new run manifests record `execution_policy`, including
mandatory reuse and fixed partitioning. The full-day harness still enables stage
profiling and native memory remains the existing configurable 3GB. Small live
memory remains 512MB. This change does not tune remote verification, reconciliation,
async I/O, concurrency, container resource caps or API query settings.

Local tests verify default/override/invalid thread settings, base-session isolation,
and agreement between the actual capacity command, live Compose flags and recorded
policy. The native lifecycle gate now also reads the actual job build profiles and
requires sixteen materialization/comparison threads, two sort threads and canonical
reuse while the Worker's base session stays at two threads.

Next run the small real live gate from the workspace root on the VM:

```sh
python3 sage-data-rework/archive-exporter/testing/live/run.py
```

After that passes, prepare/run a **fresh** uninterrupted full-day fixture:

```sh
python3 sage-data-rework/archive-exporter/testing/capacity/run.py --influx-memory 10g
```

Optional `--run-dir` must name a new isolated directory; do not reuse previous
prepared runs or their paired prefixes. Both harnesses freeze current source and
commands. The full-day run must finish publication, fresh remote proof, API
readiness and Arrow/NDJSON/client checks, reader drain, fresh pre-delete proof,
confirmed archived-day deletion and hot-sentinel retention. Retain/return
`run.json`, functional reports, stage/resource metrics and job build profiles to
confirm the measured build policy worked inside the complete pipeline.

Wiring validation passed locally: `go test -race ./...`, `go vet ./...`, host Go
build, 22 capacity-harness tests, 12 live-harness tests and seven packing-harness
tests. Native DuckDB lifecycle/profile assertions remain VM validation; they
were skipped locally. No containers or remote services were changed, no existing
artifacts were deleted, and nothing was committed or pushed.

#### Small live gate with 16/2 build policy (2026-10-30)

Ran `testing/live/run.py` from the workspace root. Directory
`state/live-4425dbe588744c8d87ea06d3f3c18d4c`, run id
`4425dbe588744c8d87ea06d3f3c18d4c`. `run.json` is `passed: true`, phase
`complete`, `services_stopped: true`. The recorded execution policy is reconcile
and verify at 2 threads, build at 16 threads, batch sort/copy at 2, canonical
reuse required, and fixed partitioning. Prefix
`cold-storage-archive-live-4425dbe588744c8d87ea06d3f3c18d4c` stays paired with
that directory.

Bootstrap, seed, fencing, recovery, publication, late correction, blocked
deletion, and release all passed. The finished job is `COMPLETE`. Deletions
pending, jobs pending, readers, and writers are 0. Arrow, NDJSON, and the client
read the mixed 5 rows, the archived day 4 rows, and the retained next-day
sentinel 1 row. `old_snapshot_still_readable` is true. The three job packing
profiles record 16 threads, sort/copy at 2, and canonical reuse. Expected-data
materialization profiles record 16 threads. Services were stopped after the
gate. No remote objects were deleted by the host, and nothing was committed or
pushed.

#### Fresh full-day with 16/2 build policy (2026-10-30)

Ran a new full-day at the 10 GiB Influx limit, 8 GiB archiver, 4 GiB probe, and
3 GB native memory. Directory `state/full-day-20250301-1793363945`, run id
`982ae3fc9c704132a2932909f0ac2fa5`. The recorded execution policy matches the
live gate: reconcile and verify at 2 threads, build at 16, sort/copy at 2,
canonical reuse required, fixed partitioning. `run.json` is `passed: false`,
`failed_phase: api`, `error_type: CalledProcessError`, `services_stopped: true`.
Prefix `cold-storage-archive-live-982ae3fc9c704132a2932909f0ac2fa5` stays paired
with that directory. The run was not resumed, and the remote prefix was not
deleted.

Seed passed: 5,593.59 s, 140,578,674 rows, 39,067,004,031 line-protocol bytes,
and the next-day sentinel retained. Job `4faa748c-1e08-4d4b-bf54-a804fb078318`
reached `ROUTED`. Normalization used `identity-partitions-merge-v1`: 256
partitions, max 550,959 rows, 0 superseded, partition 354.55 s, sort/collapse
238.42 s, merge 479.12 s, normalized 774,782,887 bytes. The packing profile
records 16 threads, sort/copy at 2, canonical reuse, 140,578,674 rows, 15 files,
858.01 s, and a reused whole-build proof (0 s initial routing, 56.58 s
refinement, 76.44 s exact SQL, 256 leaves). Materialization used 16 threads and
took 85.53 s. Published data files are 457,738,816 bytes. The independent probe
and the worker both report a full exact multiset match. The published contract
is Iceberg v3, `timestamptz_ns`, value `variant`, meta `map`, layout
`bounded-plugin-v1`.

| Phase | Elapsed |
| --- | ---: |
| Exporting | 554.00 s |
| Exported / reconcile | 3,353.51 s |
| Reconciled / pack | 943.74 s |
| Built / publish | 76.82 s |
| Committed / remote verify | 1,331.95 s |
| Independent probe verify | 782.23 s |

Sampled cgroup peaks: Influx 3,236,794,368 bytes, archiver exactly 8,589,934,592
bytes, verify probe exactly 4,294,967,296 bytes, API 281,223,168 bytes. The
archiver and probe reached their caps. No OOM kill is recorded.

`api-ready` passed in 2.04 s against `/metrics`. The API log shows cold DuckDB
v2.0.0 and `service listening on :10000` before the data probe. The first query,
a one-second window for `uncorrected_inner`, returned HTTP 500 in 1.60 s. The
API error is `invalid filter field pattern "^uncorrected_inner$"`. The full-day
probe anchors the sampled name and tags with `^` and `$`. The API allowlist is
letters, digits, `+`, `-`, `_`, `.`, `*`, `:`, `|`, and space, so those anchors
are rejected before a backend read. Patterns without `|` or `*` are exact
equality matches. Arrow, NDJSON, and client checks did not run. Deletion stayed
disabled; the job remains `ROUTED`, with deletion still pending. This is not a
full-day capacity pass. Nothing was committed or pushed.

##### Review: 16/2 live accepted; full-day API probe contract corrected (2026-10-30)

Independently reviewed the new live and full-day run manifests, all eight frozen
build-context hashes (four per run), actual job packing profiles, full-day
readiness/remote verification reports and the captured API log. All context hashes
match their manifests. All three live builds record sixteen comparison threads,
two sort threads, reuse and success. The live manifest is complete/passed with
services stopped; its recorded functional checks accept the tested small live
16/2-policy lifecycle.

The full-day job successfully built and published all 140,578,674 rows and
reached `ROUTED`. Its materialization used sixteen threads; packing used sixteen
comparison threads, two sort threads and reuse. Independent source-versus-remote
comparison and worker remote verification both passed. Its failure is specifically
the first bounded API request, not startup or a failed archival data comparison.
`api-ready` passed, and the captured request contains
`name: ^uncorrected_inner$`, `vsn: ^W099$` and an anchored node filter. The API
rejected that pattern before a backend read. Full-day Arrow/NDJSON/client success,
pre-delete verification and confirmed deletion are therefore **still unproven**.
The stopped failed run/prefix remains paired and is not resumed or repointed.

The capacity probe's `sample_query` is now corrected to use the API's literal
filter contract. Plain supported values are sent unchanged, with no anchors or
`re.escape`. Patterns without `|` or `*` already mean exact equality in both hot
and cold backends. The probe uses a conservative ASCII literal subset; values
containing wildcard/alternation, quotes, escapes or other unsupported characters
are omitted as filters. Crucially the same predicates are omitted from the source
SQL oracle, so the broader HTTP result is still compared completely. If every
filter is omitted, the oracle checks the complete bounded time window. Existing
row-cap detection and shrinking time windows remain; no source rows are silently
filtered out or response truncation accepted. The production API is unchanged.

All 25 offline capacity tests pass. New cases cover the literal sampled name and
vsn/node values, punctuation that must remain literal, unsupported/wildcard values,
a fully unfiltered time window, and unchanged complete-window cap handling.
Actual HTTP validation of this corrected probe still requires the VM.

| Observed milestone | Previous full-day | New full-day |
| --- | ---: | ---: |
| Build/materialize/pack phase | 2,817.54 s (46m58s) | 943.74 s (15m44s) |
| Startup-inclusive archive-to-routed | 7,985.50 s (2h13m06s) | 6,266.28 s (1h44m26s) |

These are separate runs, not a controlled whole-pipeline benchmark. The new build
phase includes 85.53 seconds of materialization and 858.01 seconds of packing/
proofs. Its optimized build behavior is confirmed, but network/cache/resource
variation and changes in other phases prevent attributing the entire observed
archive-to-routed reduction to reuse/threading. Cgroup limits were reached without
recorded OOM; that is recorded evidence, not proof of server sizing or concurrency
headroom and not the cause of this rejected-filter failure.

Next run a fresh full-day preparation with the corrected probe; the current small
live policy gate has already passed:

```sh
python3 sage-data-rework/archive-exporter/testing/capacity/run.py --influx-memory 10g
```

Use a new isolated directory/prefix, preserving previous runs. No previous job is
resumed and no deletion is enabled by this review. The full lifecycle gate closes
only after real API/client comparisons and guarded deletion/hot-sentinel checks
complete. This fix changed the test harness and documentation only: no production
API behavior, remote objects or existing run artifacts were changed; nothing was
committed or pushed.

#### Fresh full-day with the corrected API probe (2026-10-30)

Ran a new full-day at the 10 GiB Influx limit, 8 GiB archiver, 4 GiB probe, and
3 GB native memory. Directory `state/full-day-20250301-1793377577`, run id
`6ee469c86a5644d99872e32f3a3d1217`. `run.json` is `passed: true`, phase
`complete`, `services_stopped: true`. The execution policy is reconcile and
verify at 2 threads, build at 16, sort/copy at 2, canonical reuse required, and
fixed partitioning. Prefix
`cold-storage-archive-live-6ee469c86a5644d99872e32f3a3d1217` stays paired with
that directory. The previous failed run was not resumed.

Seed passed in 5,319.70 s: 140,578,674 rows, 39,067,004,031 line-protocol bytes,
and the next-day sentinel retained. Job
`d24e87b6-8f97-495c-8a3a-f50fd87d9ae4` reached `COMPLETE`. Normalization used
`identity-partitions-merge-v1`: 256 partitions, max 550,959 rows, 0 superseded,
partition 301.19 s, sort/collapse 183.50 s, merge 420.17 s. Materialization used
16 threads and took 69.49 s. Packing used 16 threads, sort/copy at 2, and
canonical reuse: 773.95 s, 15 files, 457,844,069 published bytes, reused
whole-build proof with 0 s initial routing, 56.93 s refinement, 64.92 s exact
SQL, and 256 leaves. The worker and the independent probe both report a full
exact multiset match. The contract is Iceberg v3, `timestamptz_ns`, value
`variant`, meta `map`, layout `bounded-plugin-v1`.

| Phase | Elapsed |
| --- | ---: |
| Exporting | 473.07 s |
| Exported / reconcile | 3,046.38 s |
| Reconciled / pack | 843.68 s |
| Built / publish | 78.48 s |
| Committed / remote verify | 1,213.06 s |
| Deleting | 2,078.25 s |

The summed job phases from fencing through `ROUTED` are 5,654.69 s. `api-ready`
passed in 1.54 s. The bounded API probe used literal filters
`uncorrected_inner`, `W099`, and `000048b02dd3c51f` over a one-second window.
Arrow, NDJSON, and the client each returned that one source row before deletion
(15.77 s) and the same archived row after deletion (4.65 s). Pre-delete
verification passed (`deletion_reverified: true`). Hot March 1 rows are 0 and
the retained hot sentinel is 1. Deletions pending, jobs pending, readers, and
writers are 0.

Sampled cgroup peaks: Influx 3,166,306,304 bytes, archiver exactly 8,589,934,592
bytes, verify probe exactly 4,294,967,296 bytes, API 358,952,960 bytes. The
archiver and probe reached their caps. No OOM kill is recorded. Largest Go RSS
was 395,358,208 bytes and largest child RSS was 3,385,516,032 bytes.

This closes the tested full-day lifecycle for this fixture: publication, fresh
remote proof, API readiness, Arrow/NDJSON/client checks, pre-delete
verification, archived-day deletion, and hot-sentinel retention. Production
deletion remains disabled outside this isolated run. No earlier prefix was
deleted, and nothing was committed or pushed.

##### Review: uninterrupted full-day lifecycle accepted (2026-10-30)

Accepted for the tested March 1 fixture and measured build policy. Independently
reviewed `state/full-day-20250301-1793377577/run.json`, all four frozen context
hashes, routed/verify/readiness/API-before/API-after/complete/summary reports,
job build profiles and both worker verification journals. All context hashes
match. The run is complete/passed with services stopped. Every recorded job
phase has one attempt and zero failures; no failed run was resumed.

The 140,578,674-row independent source-versus-published comparison passed full
multiset equality. Worker commit and pre-delete verification both record fresh
full equality, identical expected-artifact SHA-256, identical metadata and
identical hashes for all fifteen published objects. The Iceberg v3/ns-time/
VARIANT/MAP/bounded-plugin contract is recorded. Build profiles confirm sixteen
materialization/comparison threads, two sort threads, canonical reuse and success
for all 77 packing steps; all nested comparison completion reports pass.

The corrected API probe passed before and after hot deletion for the same
complete one-second sampled range: one Arrow record, one NDJSON record and one
record through the real client with Arrow negotiation. These are **bounded HTTP
checks**, not a 140-million-row HTTP extraction test; full-day data equality is
established separately by the exact storage comparisons. The small live gate
provides the mixed hot/cold, correction and reader-blocked-deletion scenarios.

The archived job reached `COMPLETE`. Its confirmed remaining hot-day row count
is zero and retained next-day count is one. Final status has zero pending/failed
jobs, pending deletions, readers, writers, late work and quarantine items. This
closes the previously outstanding full-day fixture acceptance gate: export,
reconciliation, build, publication, fresh remote verification, routing, API
readiness/reads, fresh pre-delete verification and confirmed fixture deletion
completed in one run. It does not authorize production deletion or claim broader
crash/concurrency/scale scenarios that were not tested.

| Archival stage | Wall time | Average CPU cores used |
| --- | ---: | ---: |
| Export | 473.07 s (7m53s) | 1.55 |
| Reconciliation | 3,046.38 s (50m46s) | 1.67 |
| Build/materialization/packing | 843.68 s (14m04s) | 7.61 |
| Publish/commit | 78.48 s (1m18s) | 0.08 |
| Fresh remote verification | 1,213.06 s (20m13s) | 1.98 |
| Pre-delete verification plus delete request | 2,078.25 s (34m38s) | 1.16 |

CPU averages are self-plus-child CPU-second deltas divided by stage wall time,
not whole-host utilization. `DELETING` combines re-verification and the delete
request; it does not isolate either cost. Archive-to-routed is 5,659.95 seconds
(1h34m20s), including restart/startup/polling. The summed job phases through
routing are 5,654.69 seconds. Independent probe verification, seeding, bounded API
checks and deletion are excluded from that archive-to-routed metric. This is
therefore not the duration of the complete lifecycle.

The build is no longer the largest stage: reconciliation is about 54% of worker
time through routing, versus about 15% for the build. Its recorded normalization
substeps total roughly 904.85 seconds (15m05s); the remaining roughly 2,141.53
seconds (35m42s) include independent Influx extraction, exact comparison and other
reconciliation overhead. Do not attribute that remainder entirely to comparison
or claim that increasing DuckDB threads alone fixes it.

Recommended next performance work: add reconciliation substep timings (independent
Influx extraction versus exact comparison, retaining existing normalization
profiles) and split the pre-delete stage's fresh verification from its delete
request. Then benchmark the expensive exact-comparison operations at higher
thread counts using retained isolated artifacts, keeping existing proofs and the
validated build configuration. Do not remove the fresh remote/pre-delete checks
or apply build-local canonical reuse across those verification boundaries. This
review does not implement or select another optimization.

Recorded cgroup peaks touched the test caps without reported OOM. They describe
this successful test configuration, not production sizing or concurrency limits;
larger-server settings remain independently configurable. Current code/policy is
retained. The successful directory stays paired with its published prefix.
This review changed documentation only: no services, remote objects or existing
artifacts were changed or deleted, and nothing was committed or pushed.

##### Implemented: reconciliation, fresh verification and delete substep profiling

Added atomic, unique `operation-<operation>-*.json` records for every worker
reconciliation, verification and delete attempt. Reports capture job/day/state,
start time, base DuckDB threads/memory, elapsed time, completed/failing substeps
and self/reaped-child CPU-second deltas. No SQL, operation error strings, tokens
or signed URLs are recorded. Failed attempts remain beside later successes,
and report files survive ordinary staging-data cleanup.

Reconciliation now times complete normalization (including its stats journal),
independent Influx extraction to compressed Parquet, and fresh full exact
comparison separately. Existing normalization stats still split partitioning,
sort/collapse and merge. Exact comparison profiles additionally split count
prechecks, initial canonical routing, recursive refinement and exact leaf SQL.
Verification records object SHA readback, read setup, full exact comparison,
commit-time hint publication and verification-journal hashing/writing. The
pre-delete `verify/DELETING` report is separate from `delete/DELETING`, whose
steps are source validation and the actual Influx HTTP delete request.

The underlying operation order, return errors, fresh exact comparisons, state
transitions, deletion guards, thread counts and memory controls are unchanged.
A failed verification still prevents the delete call. The report is written on
operation return; a reporting failure is logged and never retries an already
completed delete or changes an operation's result. Atomic temporary files avoid
partial final JSON. Missing evidence is instead rejected by the capacity harness.

`routed.json` now includes reconciliation and committed-verification operation
reports and requires complete successful substeps before the harness proceeds to
API reads/deletion. `summary.json` requires all four successful operation/state
pairs, including pre-delete verification and deletion. It retains all attempts
and aggregates wall/CPU and nested comparison times by operation/state/substep.
The harness requires fresh, fixed, successful exact comparison evidence and the
unchanged two-thread base policy. Operation totals exclude report serialization;
enclosing stage totals include that small cost. Nested comparison timings overlap
substep wall times rather than representing additional elapsed time. Parent CPU
deltas include other service goroutines; they are not exclusive per-query CPU.

New local tests cover success/failure attempt retention, failed-step marking,
no error/token serialization, pending-file cleanup, nonintrusive write failures,
missing/incomplete/mismatched evidence rejection, retry aggregation and rejection
of reused remote comparison evidence. The native lifecycle test additionally
requires all expected operation reports, complete CPU usage and fresh comparison
successes at the unchanged base threads. It remains a VM/native gate.

The completed full-day acceptance result remains valid. This is instrumentation
for selecting the next optimization, not a new performance claim. Obtain the
new measurements with a fresh full-day run:

```sh
python3 sage-data-rework/archive-exporter/testing/capacity/run.py --influx-memory 10g
```

Retain `reports/summary.json`, `reports/routed.json`, stage/resource reports and
all job `operation-*.json` records. The main questions are how much of the roughly
51-minute reconciliation is independent Influx extraction versus exact comparison,
and how much of `DELETING` is fresh verification versus source validation/delete.
No old run is resumed, no existing artifacts are deleted and no remote storage
is accessed by this implementation work. Nothing was committed or pushed.

Instrumentation validation passed locally: `go test -race ./...`, `go vet ./...`,
host Go build, 28 capacity-harness tests, 12 live-harness tests and seven packing-
harness tests. Native operation-profile assertions and actual timing measurements
still require the VM; native tests were skipped locally. `read_setup` measures
auth/setup construction; native extension/session setup is included in the exact
comparison commands that execute it. This change has not started services or
modified/deleted remote or existing run data.

#### Operation substep full-day (2026-10-30)

Ran a fresh full-day with the new operation reports. Directory
`state/full-day-20250301-1793392740`, run id
`2a4d95f2d4dd4c67b8fa0319c60048a9`. `run.json` is `passed: true`, phase
`complete`, `services_stopped: true`. Job
`6bc995cb-b397-402e-9225-ffca1df15d9c` reached `COMPLETE` for all 140,578,674
rows. Hot March 1 is empty, the hot sentinel remains, and pending deletions,
jobs, readers, and writers are 0. Prefix
`cold-storage-archive-live-2a4d95f2d4dd4c67b8fa0319c60048a9` stays paired with
that directory. Each recorded operation passed on its first attempt at the base
two threads and 3 GB. All three operation-level exact comparisons were fresh and fixed, with no
canonical reuse. Build-local canonical reuse remained enabled separately.

Reconciliation took 2,988.71 s. The enclosing `EXPORTED` stage is 2,988.72 s.

| Reconciliation substep | Wall time | Share | CPU seconds, self + child |
| --- | ---: | ---: | ---: |
| Normalization | 895.88 s (14m56s) | 30.0% | 1,417.93 |
| Independent Influx extraction | 581.05 s (9m41s) | 19.4% | 561.17 |
| Exact comparison | 1,511.77 s (25m12s) | 50.6% | 3,027.12 |

Normalization itself was partition 297.70 s, sort/collapse 185.51 s, and merge
411.95 s, with 0 superseded rows. The exact comparison spent 1,302.50 s on
initial canonical routing, 47.55 s refining 16 partitions, 160.22 s on leaf
SQL, and 0.42 s on the count precheck. It finished at 256 leaves. That routing
step is 43.6% of reconciliation. Its child CPU is 3,022.50 s over 1,511.77 s of
wall time, about two cores. Influx extraction used 561.17 self CPU seconds and
no child CPU, showing roughly one core of archiver-side HTTP/CSV decoding and
Parquet-writing work. These counters exclude the separate Influx server and do
not establish single-threaded server-side execution. The HTTP delete is the
other waiting step: 784.15 s of wall time and 1.89 CPU seconds.

`DELETING` was 2,022.95 s and splits cleanly into the two operation reports:

| `DELETING` substep | Wall time |
| --- | ---: |
| Pre-delete object readback | 8.93 s |
| Pre-delete exact comparison | 1,225.13 s |
| Pre-delete journal | 4.70 s |
| Source validation | 0.04 s |
| Influx delete request | 784.15 s |

The pre-delete comparison again spent 1,029.22 s routing, 49.75 s refining, and
137.98 s on leaf SQL. Commit-time verification was the same shape: 1,231.99 s
total, 1,223.51 s of comparison, and 1,029.48 s of initial routing. Object
readback, hint publication, and journal writes were 5.04 s, 0.21 s, and 3.23 s.

The measured cost inside reconciliation is the two-thread exact comparison, and
inside that comparison it is initial canonical routing. The same routing cost
dominates both fresh remote proofs. Influx extraction is smaller, and source
validation is negligible beside the delete request. These reports do not change
the build policy or move canonical reuse into remote or pre-delete verification.
No earlier run was resumed, and nothing was committed or pushed.


##### Review: operation profiling accepted; target exact-comparison threading next

Reviewed `state/full-day-20250301-1793392740/run.json`, all four frozen context
hashes, four operation reports, job packing profile, API-before/after reports,
independent full-day verification, completion and both worker verification
journals. All context hashes match. The run is complete/passed with services
stopped, 140,578,674 rows compared exactly, archived hot rows zero, retained hot
rows one and no pending/failed work or active grants. Before/after bounded API
checks passed for Arrow, NDJSON and the client. Commit and pre-delete journals
both report full equality with matching hashes for all fifteen objects and the
same expected artifact. Build profiling still confirms 16/2 threads and reuse.

There are **four operation reports but three fresh exact comparisons**: one in
reconciliation, one at commit, and one before deletion. The separate delete
operation does not compare records. Build per-file and whole-build exact checks
are additional, separately measured proofs.

| Fresh exact comparison | Total | Initial canonical staging | Average child CPU cores |
| --- | ---: | ---: | ---: |
| Reconciliation | 1,511.77 s (25m12s) | 1,302.50 s | 2.00 |
| Commit verification | 1,223.51 s (20m24s) | 1,029.48 s | 1.98 |
| Pre-delete verification | 1,225.13 s (20m25s) | 1,029.22 s | 1.98 |
| Total | 3,960.41 s (66m00s) | 3,361.21 s (56m01s) | — |

Initial staging consumes about 85% of these comparison times. The comparison
processes are using approximately their configured two cores; network latency
alone does not explain the wall time. These totals span pre-routing and
pre-deletion work, excluding the independent probe's own full comparison and
build verification. They must not be presented as archive-to-routed duration.

Reconciliation's exact comparison is 50.6% of its 49m49s total; normalization is
30.0% and independent Influx extraction 19.4%. The deletion request is genuinely
separate: 784.15 seconds (13m04s), versus 1,238.76 seconds (20m39s) of fresh
pre-delete verification. Source validation and object SHA readback are relatively
small. The delete client's low CPU does not distinguish server processing,
storage/lock waits or network delays; profiling the Influx service would be
required to explain that request further.

**Recommended next experiment:** fresh exact-comparison thread counts of 2, 8
and 16, using identical retained inputs, fixed sixteen-way initial partitioning,
the existing bounded refinement and full two-way `EXCEPT ALL`, and equal native
memory. Use reversed order repeats and record the existing comparison phase/CPU
metrics plus input hashes and resource use. A local retained-artifact comparison
can screen reconciliation candidates without reseeding Influx. Remote proofs
need their own read-only benchmark or a subsequent full lifecycle validation;
a local result alone does not establish remote performance.

Any service rollout should use a comparison-specific session copy for the three
`Equal` calls, rather than globally increasing `--duckdb-threads`. Normalization
uses the base DuckDB settings and should remain unchanged; raising its sorting
threads can have different performance effects. Preserve fresh independent
Influx extraction and fresh commit/pre-delete remote reads. Do not reuse a prior
proof across either remote verification boundary. Keep the already validated
16/2 build configuration, mandatory build-local reuse and fixed partitioning.
No new thread count is selected from the current two-thread profiling result.

This is the next performance candidate, not an implementation change in this
review. The successful full-day acceptance remains valid. No services, remote
objects or existing artifacts were changed/deleted, and nothing was committed
or pushed.

##### Implemented: retained fresh-comparison 2/8/16-thread benchmark

Added `archive-exporter/testing/comparison/run.py` and a native retained-input
comparison test. The benchmark screens the reconciliation exact-comparison path
without reseeding or querying Influx: both inputs are existing primitive Parquet
files, the normalized engine export and independent Influx query copy from the
same retained day. It invokes the same `SELECT * EXCLUDE(seq)` typed projection
and fresh `DuckDB.Equal` as `Worker.Reconcile`, preserving full nanosecond/time,
name, sorted tags, value-type/value and duplicate-multiplicity comparison.

Default arms run **2/8/16 then 16/8/2**, serially, with equal 3GB native memory.
Each arm has its own container, process and private scratch directory. Fixed
sixteen-way initial partitioning, bounded refinement and two-way `EXCEPT ALL`
remain unchanged; no canonical artifact or prior proof is reused. The normal
partition-writer budget follows thread count (32/128/256 open files), so this
measures threading under existing writer policy rather than holding writer
parallelism fixed separately.

Before full trials, each selected thread count executes native semantic and
typed-projection gates. The bounded semantic gate now accepts the benchmark's
thread count while retaining its existing default outside this harness. It checks
recursive refinement, same-bucket full-record collisions, duplicate multiplicity,
empty equality and fail-closed hash exhaustion. The new typed gate covers mixed
values, large signed/unsigned integers, string/newline and metadata handling,
and rejects a nanosecond mutation and a signed-to-unsigned type mutation.

Each full trial checks the audited row count of both retained inputs, hashes them
before/after and performs a new complete exact comparison. Reports must show a
fresh successful fixed proof, sixteen initial buckets, the requested thread/
memory/writer settings, CPU metrics and identical input hashes across arms. A
failed gate/proof or OOM aborts remaining trials. Source and Linux DuckDB binaries
are frozen once and mounted read-only, runtime networking is disabled and only
new isolated scratch/logs/reports are writable. No previous run is resumed or
modified. The two original files must be distinct; `/node-data` is prohibited.

`run.json` records every trial and descriptive per-thread medians. Trial
`results/benchmark.json` records full comparison time, phase timings, CPU deltas,
logical CPU count, Go scheduler setting, process/cgroup/sampled-scratch peaks,
leaf counts and before/after SHA-256. Version/footer admission and source hashing
are outside comparison wall/CPU timing; hashes warm inputs, so caches remain
uncontrolled. RSS is process lifetime, child RSS is not a concurrent summed peak,
and scratch sampling can miss transient/unlinked files.

Run on the VM from the workspace root:

```sh
python3 sage-data-rework/archive-exporter/testing/comparison/run.py \
  --export sage-data-rework/archive-exporter/state/full-day-20250301-1793231701/coordinator/jobs/9982f31e-865e-49b9-82bc-138c397ba47e/export-dedup.parquet \
  --influx sage-data-rework/archive-exporter/state/full-day-20250301-1793231701/coordinator/jobs/9982f31e-865e-49b9-82bc-138c397ba47e/influx.parquet
```

These two retained files still exist in this workspace. Their original failed
archival job is not resumed: only the input files are mounted read-only. Alternate
same-day retained paths are supported. `--run-dir` selects a new isolated volume;
`--container-memory` / `--cpus` are optional and omitted by default. The default
3GB native memory is equal across arms and can be changed for all arms together.
`--prepare-only` freezes the run without Docker. Full options and scope are in
`testing/comparison/README.md`.

Return `run.json`, all gate logs and every trial `results/benchmark.json`.
This measures **local reconciliation comparison only**, excluding normalization,
Influx HTTP extraction, build, remote reads and deletion. No service thread
setting or algorithm is changed and no thread-count winner is selected yet.
After local screening, remote verification requires its own read-only benchmark
or a complete lifecycle run before promoting comparison-scoped thread settings.
The previously completed full-day acceptance remains valid.

Local implementation validation passed: `go test -race ./...`, `go vet ./...`,
host Go build, six comparison-runner tests, 28 capacity-runner tests and seven
packing-runner tests. New runner tests cover reversed order, equal resources,
read-only/offline mounts, distinct inputs and isolated scratch, failed-gate early
termination, fresh-proof/hash evidence rejection and six-trial collection. Native
DuckDB tests were skipped locally: Docker socket access remains denied and the
available CLI is Linux-only. Actual per-thread native correctness and performance
are pending the VM command above. No existing artifacts were deleted, no remote
storage was accessed, and nothing was committed or pushed.

#### Retained reconciliation comparison at 2, 8, and 16 threads (2026-10-30)

Ran the retained exact-comparison screen in `state/comparison-1793411641`.
Inputs were the read-only `export-dedup.parquet` and `influx.parquet` from job
`9982f31e-865e-49b9-82bc-138c397ba47e`. Order was 2, 8, 16, then 16, 8, 2, at
equal 3 GB native memory and with no container cap. The VM reported 16 CPUs.
All six native gates passed. All twelve containers exited 0 without OOM.
`run.json` is `passed: true`. The frozen source matches code SHA-256
`4eb53e71…5b20`. Host SHA-256 of both inputs still matches every arm
(`b486bf37…356e`, `01a4f333…44c9`). Each arm compared 140,578,674 rows with a
fresh fixed proof, 16 initial buckets, 16 refinement passes, and 256 leaves.

| Pass | Threads | Comparison | Initial routing | Refinement | Exact SQL | Writers |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 2 | 1,558.96 s | 1,355.20 s | 60.21 s | 141.91 s | 32 |
| 1 | 8 | 625.06 s | 512.57 s | 40.32 s | 69.94 s | 128 |
| 1 | 16 | 508.62 s | 368.27 s | 79.71 s | 58.77 s | 256 |
| 2 | 16 | 478.07 s | 347.79 s | 71.58 s | 56.79 s | 256 |
| 2 | 8 | 554.11 s | 454.15 s | 30.81 s | 67.44 s | 128 |
| 2 | 2 | 1,540.06 s | 1,342.91 s | 52.73 s | 143.01 s | 32 |

Medians are 1,549.51 s at two threads, 589.59 s at eight, and 493.34 s at
sixteen. Eight threads was 2.63× the two-thread median. Sixteen was 3.14× that
median and 1.20× the eight-thread median. Sixteen stayed ahead of eight in both
orders, including when sixteen ran first. Two threads stayed near 1,540 s even
as the last, warmest arm. Initial routing is the part that scaled. Refinement
was slower at sixteen threads than at eight in both orders. Child CPU over wall
time was about 2.0, 7.7, and 13.5 cores.

Child RSS rose from about 1.73–1.84 GiB at two threads to 1.97–2.03 GiB at
eight and 2.51–2.54 GiB at sixteen. Cgroup peaks were 6.82, 7.15–7.16, and
7.23–7.24 GiB. Sampled scratch was about 4.00, 4.16, and 4.20 GiB.

This screens the local reconciliation comparison only. It does not include
normalization, the Influx reread, remote readback, or deletion, and it does not
change the two-thread base used by those proofs. Remote verification still needs
its own measurement before a comparison thread count is promoted. No existing
artifacts were deleted, and nothing was committed or pushed.

##### Review: sixteen threads is the local comparison candidate; remote screen next (2026-10-31)

Independently reviewed `state/comparison-1793411641/run.json`, all six trial
reports and native gate logs. The frozen source hash matches the manifest. Both
original retained input SHA-256 values match every before/after hash in all arms.
All twelve containers exited zero without OOM. All six trials report full exact
multiset equality over 140,578,674 rows, fresh fixed comparison, sixteen initial
buckets, sixteen refinement passes and 256 leaves. Writer budgets match the
selected counts (32/128/256). Native semantic and typed gates passed at each
thread count. This accepts correctness and the measured local performance result.

| Threads | Median comparison | Speedup vs 2 | Wall-time reduction vs 2 |
| ---: | ---: | ---: | ---: |
| 2 | 1,549.51 s (25m50s) | 1.00× | — |
| 8 | 589.59 s (9m50s) | 2.63× | 62.0% |
| 16 | 493.34 s (8m13s) | 3.14× | 68.2% |

The sixteen-thread candidate saves 1,056.16 seconds (17m36s) relative to the
observed two-thread median. It is 16.3% shorter than the eight-thread median,
saving another 96.24 seconds. It beats eight threads in both orders. The final
two-thread arm remained slow despite running last, strengthening the case that
the main gain is not merely later-arm cache warmth. These are still two
measurements per setting with uncontrolled caches, not statistical guarantees or
a measured end-to-end archival speedup. Thread count and the existing associated
writer-budget policy change together.

Initial staging accounts for the improvement. Native child CPU use rises from
about two cores to 7.7 and 13.5, so higher counts are doing additional parallel
work rather than simply masking HTTP waits in this offline benchmark. Refinement
is slower at sixteen threads than at eight, but the overall proof is still faster.
Do not introduce another refinement-specific thread setting yet; its smaller
potential saving is lower priority than validating the fresh remote path.
Recorded memory/scratch growth stayed within this uncapped VM run's resources,
without establishing concurrency headroom or a universal server resource limit.

**Retain sixteen threads as the measured local reconciliation-comparison
candidate. Next benchmark fresh remote exact comparisons read-only at 2/8/16
threads**, using an immutable published snapshot and a retained same-day expected
file, equal native memory and reversed repeats. Record object/read context,
initial routing, refinement/exact times, CPU and resource use. Do not substitute
local canonical shards, cached verification journals or count/hash-only checks
for a fresh remote scan. Local performance does not prove remote performance.

Service rollout must scope higher threads to exact comparisons. Do not globally
raise `--duckdb-threads`: that would also change normalization's native partitions/
sorts. Keep independent Influx extraction, normalization and deletion unchanged,
and keep the validated build configuration at 16/2 with mandatory canonical reuse
and fixed partitioning. Reconciliation and remote verification may use separate
comparison overrides if their measured winners differ. The current remote
verification default remains two threads pending its own result. This review
does not change any service defaults or implement the remote benchmark.

No new full-day reseed is needed for the remote screen. The existing complete
lifecycle acceptance remains valid; a final lifecycle validation should follow
any promoted comparison-specific settings. No existing artifacts or remote data
were changed/deleted, and nothing was committed or pushed.

##### Implemented: read-only pinned remote comparison 2/8/16-thread screen

Added `archive-exporter/testing/remote_comparison/run.py` and native benchmark
helpers. The inputs are a retained typed expected Parquet file and its successful
`verification-COMMITTED.json` journal. The journal supplies an immutable metadata
URL/day, expected artifact SHA-256 and the complete published object hash map.
The runner derives the isolated warehouse from the validated metadata URL; it
never resolves a head hint or starts/resumes an archiver, catalog, Influx or API.

The default source is the retained original full-day job
`9982f31e-865e-49b9-82bc-138c397ba47e`. That older stopped job and its prefix are
not modified or resumed. Only its expected file and already-published snapshot
are read. Metadata/object URL validation rejects cross-origin/prefix references,
queries/credentials, traversal and `/node-data`. The native manifest reader checks
v3 table location, current snapshot, manifest list/manifests, data-only content,
object inventory and aggregate row count against the journal/audited day before
DuckDB scans anything. Delete manifests or extra unjournaled current objects are
not accepted by this deliberately narrow benchmark.

A pin stage records snapshot ID and hashes of metadata, manifest list and leaf
manifests, then checks all remote object hashes. Each trial rereads that metadata
tree and object SHA-256 before comparison, and repeats those validations after
comparison. Every trial's local expected hash/count must match the journal; the
expected file is rehashed afterwards. Changed pin, objects or expected artifact
fails the sweep. Full equality still comes from a **fresh** `DuckDB.Equal` of the
local typed expected file and production `Worker.Scan` of the pinned snapshot/day,
with fixed sixteen-way initial routing, bounded refinement and complete two-way
`EXCEPT ALL`. No canonical shards or verification result are reused.

Default order is **2/8/16 then 16/8/2**, at equal 3GB native memory in separate
processes/containers and fresh scratch. Native semantic/typed gates at each thread
count run offline before the pin or trials. Only pin/trial containers have outbound
network access. The native runtime image preinstalls httpfs/iceberg for the exact
DuckDB CLI; runtime root and extension HOME are read-only. The image's context
allowlist contains only the Dockerfile and CLI. Signing key, expected data, target,
pin and test binary are bind-mounted read-only, never copied into the image.
Tokens use `NewPelicanReader` and carry only the selected prefix's read scope.
TLS validation stays enabled. There are no catalog commits, PUTs or DELETEs in
this benchmark. Local mock tests simulate changed object bytes without touching
actual storage.

Comparison wall/CPU timing includes session LOAD/setup and remote scan, excluding
Go metadata validation, object hash readbacks and expected admission/hashing.
Object-validation times are reported separately. Pre-comparison object readback
matches production verification's object-before-comparison order and warms server
caches; OS/server caches and network load remain uncontrolled. Each native command
uses a new connection/process; that does not clear external storage caches.
Reports retain default thread/memory/async/read-ahead/http-pool settings without
changing async behavior. Process/cgroup peaks include admission/readback work;
sampled scratch can miss transients, and separate peaks are not summed concurrent
memory. Writer budgets follow the existing thread policy (32/128/256).

Run on the VM from the workspace root:

```sh
python3 sage-data-rework/archive-exporter/testing/remote_comparison/run.py \
  --expected sage-data-rework/archive-exporter/state/full-day-20250301-1793231701/coordinator/jobs/9982f31e-865e-49b9-82bc-138c397ba47e/expected.parquet \
  --verification sage-data-rework/archive-exporter/state/full-day-20250301-1793231701/coordinator/jobs/9982f31e-865e-49b9-82bc-138c397ba47e/verification-COMMITTED.json
```

The existing issuer key/Linux CLI paths are defaults and can be overridden.
`--run-dir` selects a new isolated directory outside retained inputs and key
folders. Container CPU/memory caps are optional and omitted by default. The
usual six-hour read token is freshly issued per trial; changing `--timeout` alone
does not lengthen it. `--prepare-only` freezes safe inputs/source without remote
access. See `testing/remote_comparison/README.md` for full details.

Return `run.json`, gate/pin logs, `pin/results/pin.json` and all trial
`results/benchmark.json`. The runner reports six trial times and descriptive
medians but does not deploy or select a service setting. Service normalization,
build settings, fresh verification and deletion behavior remain unchanged. The
existing successful full-day lifecycle acceptance remains valid; a final lifecycle
validation should follow any comparison-specific thread rollout.

Local validation passed: `go test -race ./...`, `go vet ./...`, host Go build,
six remote-runner tests, six local-comparison runner tests and 28 capacity-runner
tests. Tests cover immutable URL/object scope, credential exclusion from build
context, read-only enforcement and object-byte mismatch, changed pin/expected
proof rejection, stopping after a failed pin and six distinct reversed trials.
The native fixture lifecycle additionally checks the pinned-manifest reader and
object-integrity guard against its own temporary mock warehouse. Native tests,
image builds and the actual remote sweep were not run locally because Docker
socket access is denied and the available CLI is Linux-only. VM correctness and
performance remain pending. No existing artifacts were deleted, no actual remote
storage was accessed, and nothing was committed or pushed.

#### Pinned remote comparison at 2, 8, and 16 threads (2026-10-31)

The first attempt, `state/remote-comparison-1793450518`, passed its offline gates and pinned snapshot `6916398151291571813` (140,578,674 rows, 15 objects), then `trial-1-2` exited in 20.43 s. `LOAD iceberg` tried to auto-install `avro` into the read-only extension home. The runtime image now installs and loads `httpfs`, `avro`, and `iceberg` before that home is read-only. That directory was not resumed.

The measured run is `state/remote-comparison-1793450730`, started 2026-10-31T12:45:30Z. Inputs were the retained `expected.parquet` and `verification-COMMITTED.json` for job `9982f31e-865e-49b9-82bc-138c397ba47e`. The warehouse came from that journal's metadata URL. Order was 2, 8, 16, then 16, 8, 2, at equal 3 GB native memory and with no container cap. The VM has 16 CPUs. DuckDB CLI SHA-256 is `f4c681bc…1f29`. Frozen Go source SHA-256 is `e859f540…6f57`. Native settings on every trial were `async_threads=64`, `http_client_pool_capacity=64`, `memory_limit=2.7 GiB`, and `read_ahead_depth=-1`.

Gates passed in 2.94/3.02/2.90 s (semantics) and 0.41/0.39/0.41 s (typed) at 2/8/16. The pin passed in 11.19 s. Five comparison containers exited 0. Each completed arm compared 140,578,674 rows with a fresh fixed proof, initial fanout 16, 16 refinement passes, and 256 leaves. Expected SHA-256 stayed `c91128eb…1f6e`. The pin, metadata hashes, and 15 object hashes matched before and after those five proofs. Pre-comparison object readback was 11.89, 10.73, 10.18, 7.53, and 8.08 s. Post-comparison validation was 8.82, 7.93, 7.45, 7.83, and 7.40 s. No container was OOM-killed.

| Pass | Threads | Comparison | Initial routing | Refinement | Exact SQL | Writers |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 2 | 1,268.24 s | 1,046.10 s | 55.39 s | 149.15 s | 32 |
| 1 | 8 | 476.66 s | 362.70 s | 30.85 s | 76.64 s | 128 |
| 1 | 16 | 374.74 s | 256.37 s | 52.34 s | 60.14 s | 256 |
| 2 | 16 | 362.48 s | 237.18 s | 54.43 s | 64.79 s | 256 |
| 2 | 8 | 441.16 s | 340.53 s | 24.72 s | 69.03 s | 128 |
| 2 | 2 | interrupted | 543.18 s | — | — | 32 |

Completed medians are 458.91 s at eight threads and 368.61 s at sixteen. Against the single completed two-thread proof, eight threads was 2.76× and sixteen was 3.44×; sixteen was 1.24× the eight-thread median. Sixteen stayed ahead of eight in both orders. Initial routing is the part that scaled. Refinement was slower at sixteen threads than at eight in both orders. Child CPU over comparison wall time was about 2.0, 7.5, and 13.2–13.4 cores.

Child RSS was 1.89 GiB at two threads, 2.27–2.29 GiB at eight, and 2.87 GiB at sixteen. Cgroup peaks were 5.94, 6.26–6.27, and 6.35–6.36 GiB. Sampled scratch was about 4.07, 4.20–4.21, and 4.23 GiB.

`run.json` is `passed: false` with `failed_phase` `trial-2-2` and `EXIT=1`. The reversed two-thread arm had already reread the pinned objects in 7.83 s. During fresh comparison, DuckDB reported `Could not resolve hostname` on an HTTP HEAD of the pinned metadata URL. Routing had reached 543.18 s; refinement and exact SQL had not started, and the test exited at 571.07 s. The partial report still showed the same snapshot and expected hash. That wall time is not a finished proof, so there is no two-thread median and no post-comparison object check for that arm.

This screens fresh remote verification of one pinned snapshot. It does not change the two-thread base used by reconciliation, remote verification, or pre-delete verification, and it does not select a service thread count. The published prefix and stopped job were only read. No existing artifacts were deleted, and nothing was committed or pushed.

#### Replacement two-thread remote proof (2026-10-31)
>NOTE: it was interrupted because my laptop was closed, so nothing to worry about

The interrupted arm was rerun on its own in `state/remote-comparison-1793468978`, started 2026-10-31T17:49:38Z, about three hours after the sweep stopped. The command was the same pinned inputs with `--threads 2 --reps 1` and a new directory. The earlier sweep directory was not resumed. Hostname resolution to the storage origin succeeded before the run (HTTP 403 in 0.12 s without a token). `run.json` is `passed: true`, phase `complete`, `EXIT=0`. Gates passed in 4.14 s and 0.44 s. The pin passed in 7.76 s on the same snapshot `6916398151291571813`, 140,578,674 rows, and 15 objects. The single trial exited 0 with no OOM. Frozen Go source and DuckDB CLI hashes match the sweep.

Comparison was 1,212.75 s: initial routing 1,005.92 s, refinement 51.77 s, exact SQL 146.53 s, count 7.43 s. It was a fresh fixed proof, fanout 16, 16 refinement passes, 256 leaves, and 32 writers. Full equality held. Expected SHA-256 stayed `c91128eb…1f6e`. Metadata and object hashes matched before and after. Object readback was 5.31 s and post-comparison validation was 6.61 s. Child CPU was 2,403.56 s, about 2.0 cores over the comparison. Child RSS was 1.82 GiB, cgroup peak 5.95 GiB, and sampled scratch 4.07 GiB.

The two completed two-thread proofs are 1,268.24 s and 1,212.75 s. Their median is 1,240.50 s. This sample is 55.49 s faster than the first and supplies a second completed proof from a separate run. Cache temperature was not measured; the three-hour gap does not establish cold caches or restore the interrupted reversed-order arm. Against that median, eight threads was 2.70× and sixteen was 3.37×. Sixteen remains 1.24× the eight-thread median from the completed pairs. The two-thread base for reconciliation, remote verification, and pre-delete verification is unchanged, and no service thread count is selected. The published prefix and stopped job were only read. No existing artifacts were deleted, and nothing was committed or pushed.


##### Review: remote results support comparison-scoped sixteen threads (2026-10-31)

Independently reviewed `state/remote-comparison-1793450730` and its replacement
`state/remote-comparison-1793468978`: both manifests, all six completed trial
reports, source hashes, pin context and native settings. Each frozen source hash
matches its manifest, and both runs used the same Go source and DuckDB CLI hashes.
The exact pin context matches across runs: snapshot `6916398151291571813`,
140,578,674 rows, fifteen data objects and identical root/manifest-list/manifest
hashes. Every completed trial records fresh fixed full equality, sixteen initial
buckets, sixteen refinements, 256 leaves, the expected thread/writer budget,
unchanged expected SHA-256 and successful before/after remote validation.

The original sweep remains failed because the last two-thread arm did not finish.
Its partial 571-second duration is excluded entirely. The user reports laptop
closure during that arm; this does not call for a pipeline recovery change. The
replacement is a successful new, one-trial run, not a resume of the original.
It supplies a second completed two-thread proof, with matching immutable inputs,
but cannot reproduce the original sweep's final cache/network conditions. No
cold-cache claim is made from its later start time.

| Remote comparison threads | Completed samples | Descriptive median | Speedup vs 2 |
| ---: | ---: | ---: | ---: |
| 2 | 1,268.24 / 1,212.75 s | 1,240.50 s (20m40s) | 1.00× |
| 8 | 476.66 / 441.16 s | 458.91 s (7m39s) | 2.70× |
| 16 | 374.74 / 362.48 s | 368.61 s (6m09s) | 3.37× |

Sixteen threads reduces comparison wall time by **70.3%** relative to the two-
thread median and **19.7%** relative to eight. It beats eight in both completed
orders; both completed two-thread proofs are substantially slower. Native child
CPU averages rise from about two to 7.5 and 13.2–13.4 cores. Initial canonical
routing is again the major scaling component. Refinement remains slower at
sixteen than eight, but does not overturn the total winner. No need for another
refinement-specific thread experiment before validating the combined policy.

All completed arms used the same default async/read-ahead/pool settings
(`async_threads=64`, `read_ahead_depth=-1`, HTTP client pool 64) and equal requested
3GB memory (reported as 2.7 GiB). This measures native comparison threading plus
its existing associated writer budget; it is not an async-I/O tuning comparison.
Uncontrolled server/OS caches, network conditions and the replacement trial mean
these two-sample medians are descriptive, not statistical guarantees. The local
and remote cases also use different input representations, so their absolute
times should not be used to infer that remote I/O is faster than local I/O.

The runtime image's explicit avro install/load fix is present in current source;
all extension dependencies must be ready before making HOME read-only. Its first
failed attempt is not performance evidence and is not resumed. No change to the
freshness or integrity checks is needed based on these results.

**Recommendation: wire sixteen threads specifically into reconciliation exact
comparison and fresh commit/pre-delete exact comparisons.** Both local and remote
screens now support that setting. Keep normalization and other base native work
at two threads, build materialization/comparison at sixteen, batch sort/copy at
two, mandatory build-local canonical reuse and fixed initial partitioning. Object
readback, independent Influx extraction, delete calls, permissions and full proof
boundaries remain unchanged. Use a private comparison session copy rather than
globally increasing `--duckdb-threads`.

The operation reports should distinguish base-operation threads from the exact
comparison's actual thread count so the new policy can be verified. Update live/
capacity manifests and assertions accordingly. After wiring, run the small live
gate, then one fresh uninterrupted full-day lifecycle to measure actual total
latency and validate API reads, fresh proofs and guarded deletion together. Do not
present the isolated speedups as measured pipeline savings before that run.

This review changes documentation only. No thread default was changed yet, no
remote objects or existing artifacts were changed/deleted, and nothing was
committed or pushed. Existing successful lifecycle acceptance remains valid.

#### Implemented comparison-specific sixteen-thread policy (2026-10-31)

The reviewed setting is now wired into the worker and fresh live/capacity runs.
`--duckdb-comparison-threads` defaults to sixteen and accepts 1–256. It applies
only to the fresh exact comparisons in reconciliation, committed-object
verification and pre-delete verification. Each uses a private copy of the base
DuckDB settings; executable, memory/spill configuration and process locking are
preserved. Invalid counts fail before doing the operation's work.

| Work | Threads | Policy |
| --- | ---: | --- |
| Normalization/base native work | 2 | Unchanged |
| Fresh reconciliation exact comparison | 16 | New comparison-specific override |
| Fresh commit/pre-delete exact comparison | 16 | Same override, independently fresh proofs |
| Build materialization and exact comparison | 16 | Mandatory build-local canonical reuse |
| Build batch sort/copy | 2 | Unchanged |

Fixed initial partitioning and recursive exact comparison remain unchanged.
Independent Influx extraction, object SHA readback, routing checks, verification
journals and guarded deletion retain their existing behavior. No concurrency was
added between lifecycle stages or between separate jobs.

Nested comparison profiles now record actual `threads` for fresh and reused
proofs. Operation profiles continue to record the base two-thread setting; the
nested exact proof records sixteen. Fresh live/capacity manifests separately
record `normalization_threads`, `comparison_threads`, `build_threads` and
`build_sort_threads`. Capacity acceptance requires sixteen-thread fresh fixed
exact proofs as well as the existing ordered steps, CPU evidence and two-thread
base policy. The retained local/remote comparison runners also check reported
proof threads against their requested trial count.

Local validation passed: `go test -race ./...`, `go vet ./...`, the service build,
and 52 Python harness tests (28 capacity, 12 live, six local comparison and six
remote comparison). Scope tests verify that the comparison override preserves
the base/runtime fields, supports an explicit override, leaves build settings
independent and rejects invalid counts. Native Docker/live tests remain pending
on the VM; local checks do not establish combined lifecycle performance.

On the VM, prepare a fresh small live run from the workspace root:

```sh
python3 sage-data-rework/archive-exporter/testing/live/run.py
```

After it passes, run a fresh uninterrupted full-day lifecycle:

```sh
python3 sage-data-rework/archive-exporter/testing/capacity/run.py --influx-memory 10g
```

Use new run directories; completed runs retain frozen older code and settings.
Return `run.json`, `routed.json`, `summary.json` and job operation/build reports.
Acceptance must still cover fresh proofs, API reads before/after deletion, the
empty archived hot day, preserved sentinel, and cleared grants/queues. Compare
measured total latency with the previous full lifecycle before claiming pipeline
savings. No existing artifacts or remote data were changed/deleted during this
implementation, and nothing was committed or pushed.

#### Sixteen-thread comparison lifecycle (2026-10-31)

The small live gate passed in `state/live-1793470860`, run id
`9a30791c2db146a8a7fb515818366842`, `EXIT=0`. Phase is `complete` and services
are stopped. Policy is normalization 2, fresh comparison 16, build 16, sort 2,
canonical reuse, fixed partitioning. Prefix
`cold-storage-archive-live-9a30791c2db146a8a7fb515818366842` stays paired.
Job `466d0888-573c-476a-ad74-7649d608b7c2` reached `COMPLETE`. Operation reports
keep the base session at 2 threads and record the nested exact proof at 16,
fresh and fixed. Final pending deletions, jobs, readers, and writers are 0.
The held old snapshot stayed readable, deletion stayed blocked while that grant
was open, and Arrow, NDJSON, and the client agreed before and after deletion.

The fresh full day passed in `state/full-day-20250301-1793470993`, run id
`4756f5ecc7714321983e991c056ad869`, started 2026-10-31T18:23:13Z, `EXIT=0`.
`run.json` is `passed: true`, phase `complete`, `services_stopped: true`. The
same execution policy is recorded. Prefix
`cold-storage-archive-live-4756f5ecc7714321983e991c056ad869` stays paired. Job
`49ae30ad-f39f-4ee1-be67-fa51d78b2c8f` reached `COMPLETE` for 140,578,674 rows.
Seed took 5,261.53 s and wrote 39,067,004,031 LP bytes. The hot sentinel is
`1740873600000000194`. Hot March 1 has 0 rows and the next-day sentinel remains.
Pending deletions, jobs, readers, writers, late work, and quarantine are 0.
No OOM event is recorded. Archiver cgroup peak is exactly 8 GiB and the verify
probe exactly 4 GiB. Influx peaked at 3,183,513,600 bytes and the API at
407,703,552 bytes. Largest Go RSS was 390,221,824 bytes and largest child RSS
was 3,347,611,648 bytes.

Startup-inclusive archive-to-routed time is **3,804.03 s (1h03m24s)**. The
accepted two-thread-comparison lifecycle, `full-day-20250301-1793377577`, was
5,659.95 s. The profiled two-thread lifecycle, `full-day-20250301-1793392740`,
was 5,566.12 s. This run is 1,855.92 s (32.8%) and 1,762.08 s (31.7%) shorter
on that metric. Seed time is outside it and is essentially unchanged.

| Archival stage | Wall time | Prior accepted | Average CPU cores |
| --- | ---: | ---: | ---: |
| Export | 468.13 s (7m48s) | 473.07 s | 1.54 |
| Reconciliation | 2,059.52 s (34m20s) | 3,046.38 s | 4.56 |
| Build/materialization/packing | 827.28 s (13m47s) | 843.68 s | 7.44 |
| Publish/commit | 74.53 s (1m15s) | 78.48 s | 0.08 |
| Fresh remote verification | 369.64 s (6m10s) | 1,213.06 s | 12.97 |
| Pre-delete verification plus delete request | 1,204.14 s (20m04s) | 2,078.25 s | 4.11 |

Against the profiled run, the three fresh exact comparisons account for the
reduction. Reconciliation comparison fell from 1,511.77 s to 543.47 s.
Commit-time comparison fell from 1,223.51 s to 359.81 s. Pre-delete comparison
fell from 1,225.13 s to 364.92 s. Together that is 2,692.21 s removed.
Normalization, Influx extraction, build, and the Influx delete request did not
produce that reduction: normalization was 950.91 s versus 895.88 s, extraction
565.12 s versus 581.05 s, build 827.28 s versus 786.21 s, and the delete request
823.58 s versus 784.15 s. The delete request still uses 2.04 CPU seconds, so it
is waiting on Influx.

Each fresh proof used 16 threads, fixed fanout 16, 16 refinement passes, 256
leaves, 256 writers, and no canonical reuse. Reconciliation routing was 388.52 s,
refinement 70.84 s, and leaf SQL 82.06 s. Commit routing was 245.39 s and
pre-delete routing 259.20 s. Child CPU over those comparison walls was about
13.3–13.5 cores. Commit and pre-delete object readback were 4.91 s and 10.43 s.
Both worker journals record full equality for the same 15 objects. The
independent probe also matched all 140,578,674 rows. Build stayed at 16 threads
and sort 2, with canonical reuse. Materialization was 88.03 s and packing
739.03 s, producing 15 files and 457,785,628 data bytes. The whole-build proof
reused its initial staging and finished in 128.91 s.

API readiness passed in 1.53 s. Before and after deletion, Arrow, NDJSON, and
the client each returned the same one archived row for `uncorrected_inner`,
`W099`, node `000048b02dd3c51f`, from 2025-03-01T00:00:00Z through 00:00:01Z.
`deletion_reverified` is true. Production deletion remains disabled outside this
isolated run. No earlier prefix was deleted, and nothing was committed or pushed.

##### Review: combined policy passes; normalization is the next target (2026-10-31)

Reviewed the saved live/full-day manifests, full-day routed/summary and API
reports, normalization statistics and individual operation reports against the
previous profiled lifecycle. Both fresh runs are complete, passed and stopped.
The full-day evidence confirms 140,578,674-row source equality, fresh fixed
sixteen-thread proofs at reconciliation/commit/pre-delete, two base threads,
successful bounded Arrow/NDJSON/client reads before and after deletion, and
`deletion_reverified`. The final report has no outstanding jobs, grants, late
work or quarantine. Retain the implemented comparison-specific sixteen-thread
policy together with build 16/sort 2, canonical reuse and fixed partitioning.
This validates the isolated lifecycle; it does not enable production deletion.

Timing scopes matter when assessing the improvement. Against profiled run
`full-day-20250301-1793392740`:

| Metric | Previous | Current | Reduction |
| --- | ---: | ---: | ---: |
| Startup-inclusive archive-to-routed | 5,566.12 s | 3,804.03 s | 1,762.08 s / 31.7% |
| Sum of worker stages through routing | 5,561.39 s | 3,799.12 s | 1,762.27 s / 31.7% |
| Sum of all worker stages, including deletion | 7,584.67 s | 5,003.65 s | 2,581.02 s / 34.0% |

The three fresh proof reductions total 2,692.20 s (about 44m52s), but the
pre-delete proof occurs after routing. Only reconciliation and commit proofs
contribute to archive-to-routed savings; other steps also vary between runs.
The all-stage sum excludes harness waits, independent probes and API checks,
so it is not measured end-to-end lifecycle wall time. These are observed run
comparisons, not repeated-run guarantees.

Current normalization is 950.91 s (15m51s), comprising 320.07 s partitioning,
192.93 s sort/collapse and 437.18 s merging. It produces 256 leaf partitions
(largest 550,959 rows), then performs three merge passes at fan-in eight. All
140,578,674 input rows remain unique in this fixture. The merge is Go work;
increasing native DuckDB threads cannot directly accelerate it. Independent
Influx extraction remains 565.12 s with about 560.73 s of archiver self CPU;
that CPU does not measure the Influx server. The 823.58-second delete request
has little archiver CPU, which places the wait outside archiver computation
without establishing which Influx subsystem is responsible.

**Next recommendation:** benchmark normalization against a retained immutable
export, with fresh isolated outputs and the current implementation as baseline.
Measure partition routing, sort/collapse and merge separately. First screen
native normalization threads at 2/8/16 while keeping merge policy unchanged;
then assess merge fan-in or bounded merge concurrency separately if merging
remains dominant. Preserve identity-based partitioning, latest-sequence winner
semantics and final ordering. Validate complete output equality against baseline
and duplicate/type/nanosecond edge cases; this all-unique day alone does not
exercise winner selection. Hash inputs before/after and leave existing artifacts
untouched. Do not apply another service default or rerun the complete lifecycle
until an isolated normalization improvement is demonstrated.

This review updates documentation only. No code settings, existing artifacts or
remote data were changed, and nothing was committed or pushed.

#### Fixed sixteen-thread fresh comparison policy (2026-10-31)

The accepted policy is now unconditional in the service: reconciliation and
commit/pre-delete exact proofs always clone the base DuckDB settings with sixteen
threads. Removed `Worker.ComparisonThreads`, the default/override/validation
branches and `--duckdb-comparison-threads`. Live/capacity commands no longer pass
that flag. The base thread setting cannot lower fresh proof threads; normalization
remains at two by default, builds at sixteen and build sort at two.

Operation evidence and execution manifests still record actual proof threads,
and capacity acceptance still requires fresh fixed sixteen-thread exact proofs.
The historical 2/8/16 benchmark runners retain selectable trial counts for
reproducibility; they do not select service behavior. Existing frozen run
artifacts and the history above are unchanged. New service invocations must omit
the removed flag; current harnesses do so.

Local validation passed: service build, `go vet ./...`, `go test -race ./...`,
and 52 Python tests across capacity/live/local-comparison/remote-comparison
harnesses. Scope coverage verifies that changing the base count does not change
sixteen-thread fresh proofs or mutate base runtime settings. No additional VM run
was performed for this cleanup; the accepted sixteen-thread lifecycle above is
the behavioral evidence. Normalization benchmarking remains the next performance
experiment. No existing artifacts or remote data were deleted, and nothing was
committed or pushed.

#### Prepared isolated normalization thread sweep (2026-10-31)

Added `archive-exporter/testing/normalization/sweep.py` around the existing
production `DeduplicateExport` implementation. The earlier single-run diagnostic
remains available. Default arms are 2/8/16 followed by 16/8/2; the first arm must
be the current two-thread baseline. Each gate and trial gets a fresh container
and directory. Only native normalization threads vary: merge fan-in stays eight,
identity partitioning and row limits remain unchanged, and Go runtime settings
are equal across arms. No service normalization policy has changed.

The native normalization gate runs separately at each requested thread count.
It checks recursive partitioning, multiple merge passes, final ordering against
an independent small window oracle, latest replay wins across changed values/
types and reordered tags, empty input and failed-publication atomicity. Its
fixture now includes large signed/unsigned scalar text, floating point, boolean,
embedded LF/literal backslash-n and nanosecond identity.

The retained-day test now reports requested threads, logical CPUs/GOMAXPROCS,
normalization self/child CPU, existing partition/sort-collapse/merge wall times,
RSS, cgroup peak and sampled scratch. Each output receives a separately timed
fresh fixed sixteen-thread exact source comparison, including scalar kind/text,
complete canonical tags, nanosecond time and replay sequence. On this audited
all-unique input every output must equal the same source exactly, so all arms
also equal the baseline multiset by transitivity. Counts and hashes alone are
not treated as equality. The merge enforces strict identity ordering while
emitting rows; the small oracle also checks final output order and overwrite
selection. Source SHA-256 must match before/after each trial and across arms.

The runner freezes curated Go source and the Linux DuckDB executable, records
their hashes, runtime configuration, per-container exit/OOM states and medians.
Measured containers have networking disabled, read-only roots/input/binaries and
only fresh writable scratch. They contain no Pelican credentials and do not
start Influx, seed data, publish, route or delete measurements. No old run is
resumed or removed. Defaults are 3GB native memory and no container CPU/memory
caps; optional equal caps are recorded. Hashing warms input and cache temperature
is uncontrolled; reversing order reduces order bias but does not prove cold I/O.

From the workspace root on the VM:

```sh
python3 sage-data-rework/archive-exporter/testing/normalization/sweep.py \
  --source sage-data-rework/archive-exporter/state/full-day-20250301-1793231701/coordinator/jobs/9982f31e-865e-49b9-82bc-138c397ba47e/export.parquet
```

This retained source exists locally and is mounted read-only. Default audited
row count is 140,578,674; the native footer admission check must confirm it on
the VM. Return `run.json`, gate/trial logs and every
`trial-*/results/normalization-benchmark.json`. Trial normalized Parquet outputs
are retained. The benchmark uses its own unique `normalization-sweep-*`
directory; `--run-dir` can select a new isolated directory on a larger volume.

Local checks passed: Go race tests/vet, nine normalization harness tests and six
comparison harness tests. The harness tests cover ordering, read-only/network
isolation, equal resource settings, input preservation, rejected incomplete or
changed evidence, fail-fast gates and six-trial aggregation. Native VM gates and
performance measurements remain pending: local Docker socket access is denied.
After this screen, assess merge fan-in/concurrency separately if merge remains
dominant. No existing data was deleted, and nothing was committed or pushed.

#### Retained normalization at 2, 8, and 16 threads (2026-10-31)

Ran the retained normalization screen in `state/normalization-sweep-1793483418`,
started 2026-10-31T21:50:18Z. The source was the read-only `export.parquet` from
job `9982f31e-865e-49b9-82bc-138c397ba47e`. Order was 2, 8, 16, then 16, 8, 2,
at equal 3 GB native memory and with no container cap. The VM has 16 CPUs.
`GOMAXPROCS` stayed 16 on every arm; only the normalization thread count
changed. Merge fan-in stayed 8. `run.json` is `passed: true`, phase `complete`,
`EXIT=0`. Frozen source SHA-256 is `39b293c0…cbbf` and the DuckDB CLI is
`f4c681bc…1f29`. Gates passed in 16.06, 15.00, and 14.47 s. All nine containers
exited 0 with no OOM. Source SHA-256 stayed `4d259aea…d901` before and after
every trial.

Every arm emitted 140,578,674 unique rows, 0 superseded rows, 256 partitions,
max partition 550,959 rows, and 774,832,276 normalized bytes. Each output then
passed a separate fresh fixed 16-thread exact comparison against the source.
That check is outside the normalization times below. It took 362–415 s, used
256 leaves, and did not reuse canonical staging.

| Pass | Threads | Normalization | Partition | Sort/collapse | Merge |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 2 | 897.56 s | 300.86 s | 196.33 s | 399.56 s |
| 1 | 8 | 717.35 s | 129.98 s | 188.06 s | 398.06 s |
| 1 | 16 | 712.55 s | 115.74 s | 189.23 s | 406.76 s |
| 2 | 16 | 710.96 s | 124.94 s | 188.60 s | 396.34 s |
| 2 | 8 | 728.26 s | 132.31 s | 191.22 s | 403.90 s |
| 2 | 2 | 921.93 s | 320.33 s | 202.23 s | 398.51 s |

Medians are 909.75 s at two threads, 722.80 s at eight, and 711.76 s at
sixteen. Eight threads was 1.26× the two-thread median. Sixteen was 1.28× that
median and 1.02× the eight-thread median. Sixteen stayed ahead of eight in both
orders, by 4.80 s and 17.30 s. Partitioning is the part that scaled: its
medians are 310.59 s, 131.15 s, and 120.34 s. Sort/collapse stayed near
189–199 s. Merge stayed near 399–402 s at every thread count, about 44% of the
two-thread median and 56% of the sixteen-thread median. Three merge passes and
fan-in 8 were unchanged.

Self plus child CPU over normalization wall time was about 1.6, 2.5, and
3.2–3.4 cores. Child RSS rose from 1.62–1.71 GiB at two threads to 2.04–2.05
GiB at eight and 2.60–2.65 GiB at sixteen. Cgroup peaks were 6.17–6.25,
6.50–6.72, and 7.50–7.75 GiB. Sampled scratch stayed about 3.28–3.35 GiB.

This screens normalization of one already-exported day. It does not change the
two-thread normalization policy. Merge remains the dominant step once
partitioning has been given eight or sixteen threads, so merge fan-in is the
next measurement rather than another thread count. The source file was only
read. No existing artifacts were deleted, and nothing was committed or pushed.

##### Review: native scaling is measured; screen merge fan-in next (2026-10-31)

Independently checked the sweep manifest, frozen source/CLI hashes and all six
trial reports. Frozen hashes match the manifest. All nine gate/trial containers
exited successfully without OOM. Trials agree on source SHA-256 before/after and
across arms, audited row counts, fixed normalization strategy, partition bound,
merge fan-in and CPU/memory settings. Every complete output passed fresh fixed
sixteen-thread exact source equality, including replay sequence. This supports
comparing the arms; it is not a new full lifecycle measurement.

| Native normalization threads | Median wall | Reduction vs 2 | Median merge |
| ---: | ---: | ---: | ---: |
| 2 | 909.75 s (15m10s) | — | 399.03 s |
| 8 | 722.80 s (12m03s) | 20.5% | 400.98 s |
| 16 | 711.76 s (11m52s) | 21.8% | 401.55 s |

Sixteen is the measured winner in both orders, but its median advantage over
eight is only 11.05 s (1.5%). Two samples per setting and uncontrolled caches
do not establish a repeatable small margin. Retain sixteen as the candidate
native setting for the next isolated screen, without changing the deployed or
harness normalization default yet. Use the sweep's two-thread baseline for
these percentages; the earlier 950.91-second lifecycle normalization used a
different retained export and execution context.

The next experiment should hold native threads at sixteen and vary **merge
fan-in 8/16/32**, reversing order on the second repetition. With 256 leaves,
fan-in eight needs 256 → 32 → 4 → 1, while sixteen needs 256 → 16 → 1 and
thirty-two needs 256 → 8 → 1. That removes one complete intermediate merge
pass without changing identity ordering or winner selection. This is an
algorithmic reason to test, not a measured speedup: larger fan-in also raises
open cursor/decoder memory and heap work per output row. Measure actual peak
memory, bytes and phase CPU/wall times alongside complete equality.

Keep merge concurrency at one initially so the fan-in effect can be attributed.
Only test bounded concurrent independent merges afterward if the results justify
it. Preserve source isolation, latest-sequence semantics, strict ordering,
native oracle gates and fresh full exact proofs. Do not relax verification or
change fixed sixteen-thread reconciliation/commit/pre-delete comparison policy.
Select a combined normalization policy only after this screen, then validate it
with the live/full-day lifecycle gates.

This review changes documentation only. No service defaults, existing artifacts
or remote data were changed, and nothing was committed or pushed.

#### Prepared isolated merge fan-in sweep (2026-10-31)

Added `testing/normalization/merge_sweep.py`, sharing the existing sweep
orchestration. Default order is 8/16/32, then 32/16/8. Every gate and trial uses
sixteen native normalization threads, equal 3GB native memory and merge
concurrency one. Partitioning, sort/collapse, merge batch rows, compression and
full verification remain unchanged. The production default is still fan-in
eight and normalization two threads; larger fan-in uses a private test override,
with no service flag/environment setting. Bounds are validated before output
work, and the same merge implementation checks ordering/duplicate identities
and closes all cursors on failure for every arm.

Normalization statistics now also record merge self/child CPU and cumulative
`merge_written_bytes`: compressed intermediate merge files plus the final
normalized file, excluding partition and sort writes. CPU deltas include other
concurrent Go goroutines. Phase timings/CPU are nested inside total normalization
cost. Normalization RSS/cgroup peaks are not merge-only memory measurements;
report those alongside scratch peaks without treating them as additive.

The runner requires actual maximum fan-in and merge-pass counts consistent with
the arm and leaf inventory, one merge worker, finite phase/CPU metrics, unchanged
source hashes and fresh fixed sixteen-thread exact source equality. All gate/
trial containers have network disabled, read-only roots/inputs/binaries, private
scratch and equal optional resource caps (none by default). Reports retain code/
CLI hashes, order, actual threads/fan-in, exit/OOM states and per-arm medians.
Failed gates prevent trials; failed evidence prevents success. Existing exports,
run directories and remote storage are untouched.

On the Docker-capable sixteen-CPU VM, from the workspace root:

```sh
python3 sage-data-rework/archive-exporter/testing/normalization/merge_sweep.py \
  --source sage-data-rework/archive-exporter/state/full-day-20250301-1793231701/coordinator/jobs/9982f31e-865e-49b9-82bc-138c397ba47e/export.parquet
```

A new `state/normalization-merge-sweep-<uuid>` directory is used by default.
`--run-dir` can select a new isolated directory on another volume;
`--prepare-only` freezes inputs without starting Docker. Return `run.json`, all
gate/trial logs and each `trial-*/results/normalization-benchmark.json`. Preserve
the full normalized outputs until review. We will compare merge time, total
normalization time, passes/bytes and memory before selecting a new policy; no
merge concurrency experiment or full lifecycle rerun is needed before review.

Local validation passed: Go race tests/vet and 47 Python harness tests (thirteen
normalization, twenty-eight capacity, six comparison). New Go coverage exercises
8/16/32-way ordering across batch seams, scalar/newline/nanosecond preservation,
excess fan-in/duplicate rejection and the unchanged production bound. The native
small oracle now applies the requested fan-in as well as requested native
threads, preserving overwrite/atomicity checks. Native gates and full-day timing
remain pending on the VM; local Docker access is unavailable. Nothing was
committed or pushed, and no existing data was deleted.

#### Merge fan-in 8, 16, and 32 at sixteen normalization threads (2026-10-31)

Ran the retained merge screen in `state/normalization-merge-sweep-1793491122`,
started 2026-10-31T23:58:42Z. The source was the same read-only `export.parquet`
from job `9982f31e-865e-49b9-82bc-138c397ba47e`. Every arm used 16 normalization
threads, 3 GB native memory, and one merge worker. Order was fan-in 8, 16, 32,
then 32, 16, 8, with no container cap. The VM has 16 CPUs. `run.json` is
`passed: true`, phase `complete`, `EXIT=0`. Frozen source SHA-256 is
`60d4807b…9d3b` and the DuckDB CLI is `f4c681bc…1f29`. Gates passed in 15.55,
14.73, and 15.84 s. All nine containers exited 0 with no OOM. Source SHA-256
stayed `4d259aea…d901`.

Every arm emitted 140,578,674 unique rows, 0 superseded rows, and 774,832,276
normalized bytes. The separate fresh fixed 16-thread source comparison passed
on every arm in 384–459 s and is outside the normalization times below.

| Pass | Fan-in | Normalization | Partition | Sort/collapse | Merge | Passes | Merge bytes written |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 8 | 736.78 s | 130.96 s | 196.65 s | 408.16 s | 3 | 4,243,360,827 |
| 1 | 16 | 613.86 s | 142.59 s | 197.39 s | 272.89 s | 2 | 2,590,085,248 |
| 1 | 32 | 625.14 s | 148.86 s | 197.67 s | 277.60 s | 2 | 2,479,415,266 |
| 2 | 32 | 658.27 s | 156.41 s | 209.54 s | 291.27 s | 2 | 2,479,415,266 |
| 2 | 16 | 650.12 s | 163.09 s | 200.47 s | 285.63 s | 2 | 2,590,085,248 |
| 2 | 8 | 821.63 s | 183.40 s | 206.42 s | 430.77 s | 3 | 4,243,360,827 |

Normalization medians are 779.21 s at fan-in 8, 631.99 s at 16, and 641.70 s at
32. Merge medians are 419.47 s, 279.26 s, and 284.44 s. Fan-in 16 was ahead of
both 8 and 32 in both orders. Against fan-in 8 it removed one merge pass and
140.20 s of median merge time, and the total normalization median was 147.22 s shorter.
Fan-in 32 also uses two passes and writes 110,669,982 fewer merge bytes than
fan-in 16, but its merge median is 5.18 s longer and its normalization median is
9.71 s longer. The second pass was slower for every fan-in; fan-in 8 was slowest
both when it ran first and when it ran last.

Reported merge self CPU was 554.74–591.92 s at fan-in 8, 352.47–369.67 s at 16,
and 369.17–384.19 s at 32. Merge child CPU was 0 on every arm, indicating no reaped-child CPU in the
merge interval. A single merge worker does not imply a single CPU core: process
self CPU includes all Go goroutines and can exceed wall time when they run in
parallel. These counters do not attribute CPU to individual goroutines. Child RSS stayed 2.53–2.64 GiB.
Cgroup peaks were 7.33–7.68 GiB. Sampled scratch stayed 3.34–3.35 GiB. Those
peaks cover the whole normalization, not the merge alone, and they are not
additive.

This does not change the production fan-in of 8 or the two-thread normalization
setting. The measured normalization median is shortest at fan-in 16. The source
file was only read. No existing artifacts were deleted, and nothing was
committed or pushed.


##### Review: select fan-in sixteen for combined lifecycle validation (2026-10-31)

Checked the saved manifest, all six trial reports, container exit/OOM records,
and frozen source/CLI hashes against their recorded digests. All match. Every
trial used sixteen native threads, sixteen logical CPUs/GOMAXPROCS, one merge
worker and equal native memory. Source hashes agree before/after and across
arms. All outputs preserved 140,578,674 unique rows with no superseded rows,
and all six fresh fixed sixteen-thread exact proofs passed without reuse.
Actual fan-in and pass counts match the requested arms. No OOM was recorded.

| Merge fan-in | Median normalization | Median merge | Passes | Merge bytes written |
| ---: | ---: | ---: | ---: | ---: |
| 8 | 779.21 s (12m59s) | 419.47 s | 3 | 4,243,360,827 |
| 16 | 631.99 s (10m32s) | 279.26 s | 2 | 2,590,085,248 |
| 32 | 641.70 s (10m42s) | 284.43 s | 2 | 2,479,415,266 |

Fan-in sixteen reduces total normalization median by **18.9%** and merge median
by **33.4%** relative to eight within this sweep. It writes **39.0% fewer merge
bytes**, consistent with removing one complete intermediate pass. Sixteen beats
eight and thirty-two in both trial orders. Thirty-two saves another 110.67 MB
of merge writes but provides no measured time advantage. The small sixteen vs
thirty-two timing margin is descriptive, not a statistical guarantee; cache/load
conditions were uncontrolled and every second repetition ran more slowly.

The earlier thread screen and this screen support a combined candidate of
**sixteen normalization threads, fan-in sixteen, merge concurrency one**.
Keep builds at sixteen/sort two, fixed fresh comparison threads at sixteen,
mandatory build-local canonical reuse and fixed partitioning. Select this
candidate before adding merge concurrency; another simultaneous algorithm change
would make the next lifecycle result harder to interpret. No fan-in thirty-two
or concurrent-merge service path is needed based on this evidence.

Next implementation should wire that combined policy into fresh live/capacity
runs, update manifest/profile assertions to require normalization sixteen and
fan-in sixteen, and then run the small live gate followed by a fresh uninterrupted
full-day lifecycle. Operation reports currently label base normalization threads
as two, so their acceptance checks must be updated with the policy. Keep the
standalone historical benchmark controls for reproducibility. Preserve full
source/remote equality, read grants, API probes and fresh pre-delete verification.
Do not infer an end-to-end archival saving from isolated medians or add the two
screens' percentages; measure the combined lifecycle directly.

This review corrects the median merge reduction above from 135.21 s to 140.20 s.
It changes documentation only: normalization defaults remain two threads/fan-in
eight until implementation and validation. No existing artifacts or remote data
were changed/deleted, and nothing was committed or pushed.

#### Implemented combined normalization policy (2026-10-31)

Promoted the measured candidate in current source: default base/normalization
threads are sixteen and production merge fan-in is sixteen, with one merge
worker. `--duckdb-threads` remains an explicit base-thread override; no service
fan-in or concurrency option was added. Historical benchmark runners retain
explicit 2/8/16-thread and 8/16/32-fan-in controls, so their frozen measurements
remain reproducible. Build materialization/comparison stays sixteen, build
sort/copy two, fresh reconciliation/commit/pre-delete proofs fixed at sixteen,
canonical reuse mandatory for builds and initial partitioning fixed.

`export-dedup.json` now records actual native `threads`, configured `merge_fan_in`
and `merge_concurrency`, alongside maximum actual fan-in, passes and existing
phase/CPU/byte metrics. Default construction and CLI now agree on sixteen
threads. Scope tests continue to allow deliberate base overrides without changing
the fixed fresh-proof or build-sort policies.

Fresh live/capacity commands explicitly select sixteen base threads. Their
manifests record normalization sixteen, fan-in sixteen, concurrency one and the
remaining execution policy. The shared probe policy check requires configured
16/16/1, actual maximum fan-in consistent with the leaf count and the expected
number of merge passes. The small live verification-journal audit applies it to
archive jobs before fixture deletion; the capacity routed probe also applies it
before enabling deletion. Capacity operation acceptance now requires base
threads sixteen as well as fresh sixteen-thread exact proofs. New tests reject
old base-thread evidence, wrong fan-in/concurrency and incorrect pass counts.
The native lifecycle test inspects the actual normalization report as well as
build and fresh-proof profiles.

Local validation passed: service build, `go test -race ./...`, `go vet ./...`,
and 67 Python tests (30 capacity, 12 live, 13 normalization, six comparison and
six remote comparison). Docs were updated for the current service policy while
preserving historical benchmark settings. No new native/live VM measurement was
performed locally; the combined lifecycle gate is still pending.

From the workspace root on the VM, run a fresh small live fixture first:

```sh
python3 sage-data-rework/archive-exporter/testing/live/run.py
```

After it passes, run a new uninterrupted full-day lifecycle:

```sh
python3 sage-data-rework/archive-exporter/testing/capacity/run.py --influx-memory 10g
```

Use fresh directories rather than completed frozen runs. Return `run.json`,
`routed.json`, `summary.json`, `export-dedup.json` and the job operation/build
profiles. Acceptance must still show complete source/remote equality, fresh
pre-delete proof, Arrow/NDJSON/client probes before and after deletion, empty
archived hot day, preserved next-day sentinel and cleared grants/queues. Compare
archive-to-routed and worker-stage totals against accepted sixteen-comparison
run `full-day-20250301-1793470993`; do not substitute isolated medians for the
combined measured saving. No existing artifacts or remote data were changed or
deleted, and nothing was committed or pushed.

#### Combined sixteen-thread normalization and fan-in sixteen lifecycle (2026-11-01)

The first live attempt, `state/live-1793538677`, stopped before archival. Its
RabbitMQ broker exited while reading `/var/lib/rabbitmq/.erlang.cookie`
(`eacces`). The Docker VM disk was 90% full, with 10 GiB free. Unused build
cache and exited sage containers were removed, bringing that disk to 29.6 GiB
free. That attempt was not resumed, and the archiver never started.

The retry passed in `state/live-1793538978`, run id
`51276f791aa143aa9ab5cf2c357ccddf`, `EXIT=0`. Phase is `complete` and services
are stopped. Policy is normalization 16, merge fan-in 16, merge concurrency 1,
fresh comparison 16, build 16, sort 2, canonical reuse, and fixed partitioning.
Prefix `cold-storage-archive-live-51276f791aa143aa9ab5cf2c357ccddf` stays paired.
The archived job's `export-dedup.json` records threads 16, fan-in 16, and
concurrency 1. Final pending deletions, jobs, readers, and writers are 0. The
held old snapshot stayed readable, and deletion stayed blocked while that grant
was open.

The fresh full day passed in `state/full-day-20250301-1793539326`, run id
`32ad661098044c4caed122d193d8be7d`, started 2026-11-01T13:22:06Z, `EXIT=0`.
`run.json` is `passed: true`, phase `complete`, `services_stopped: true`, with
the same policy. Prefix
`cold-storage-archive-live-32ad661098044c4caed122d193d8be7d` stays paired. Job
`df16d7c5-28b0-43ce-ab32-4ac340309ee0` reached `COMPLETE` for 140,578,674 rows.
Seed took 5,453.42 s. The hot sentinel is `1740873600000000194`. Hot March 1 has
0 rows and the next-day sentinel remains. Pending deletions, jobs, readers,
writers, late work, and quarantine are 0. No OOM event is recorded. The verify
probe cgroup peak is exactly 4 GiB. The archiver peaked at 7,525,961,728 bytes,
Influx at 3,150,204,928 bytes, and the API at 330,788,864 bytes. Largest Go RSS
was 408,317,952 bytes and largest child RSS was 3,260,383,232 bytes.

Startup-inclusive archive-to-routed time is **3,464.06 s (57m44s)**. The
accepted sixteen-thread comparison lifecycle,
`full-day-20250301-1793470993`, was 3,804.03 s. This run is 339.97 s shorter on
that metric. Seed is outside it and was 191.90 s longer than that run's
5,261.53 s.

| Archival stage | This run | Prior sixteen-comparison run | Change |
| --- | ---: | ---: | ---: |
| Export | 495.49 s | 468.13 s | +27.36 s |
| Reconciliation | 1,726.24 s | 2,059.52 s | −333.28 s |
| Build/materialization/packing | 814.35 s | 827.28 s | −12.93 s |
| Publish/commit | 76.72 s | 74.53 s | +2.19 s |
| Fresh remote verification | 345.54 s | 369.64 s | −24.10 s |
| Pre-delete verification plus delete request | 1,202.62 s | 1,204.14 s | −1.52 s |

The reconciliation reduction is the normalization step: 625.01 s versus 950.91 s
on the prior run. Inside this run it was partition 132.31 s, sort/collapse
198.71 s, and merge 293.21 s, with 256 partitions, maximum fan-in 16, and 2
merge passes. The prior run's normalization report recorded maximum fan-in 8, 3 passes,
partition 320.07 s, sort/collapse 192.93 s, and merge 437.18 s. Influx extraction
was 595.98 s versus 565.12 s, and the fresh reconciliation comparison was
505.23 s versus 543.47 s. Both comparisons were fresh, fixed, fanout 16, 256
leaves, and not reused. Build stayed at 16 threads and sort 2, with canonical
reuse, and packed 140,578,674 rows into 15 files and 457,326,952 data bytes in
713.62 s.

Commit verification was 345.52 s, including 335.22 s of fresh comparison.
Pre-delete verification was 349.85 s, including 336.12 s of fresh comparison.
The Influx delete request was 852.76 s and is the bulk of the deleting stage.
Both worker journals record full equality for the same 15 objects. The
independent probe matched all 140,578,674 rows. API readiness passed in 1.80 s.
Before and after deletion, Arrow, NDJSON, and the client each returned one row
for `uncorrected_inner`, `W099`, node `000048b02dd3c51f`, over the same
one-second window. `deletion_reverified` is true.

The combined lifecycle saving against `1793470993` is the 339.97 s
archive-to-routed reduction, and 325.90 s of that is the normalization step in
the same run. Production deletion remains disabled outside this isolated run.
No earlier prefix was deleted, and nothing was committed or pushed.


##### Review: combined normalization policy accepted (2026-11-01)

Reviewed the successful small live run and fresh full-day manifest, routed/
summary reports, normalization statistics and all four operation reports against
accepted run `full-day-20250301-1793470993`. Both new runs are complete, passed
and stopped. Actual normalization reports confirm sixteen native threads,
fan-in sixteen, one merge worker and two passes for 256 leaves. The three fresh
proofs independently record sixteen threads, fixed partitioning, no canonical
reuse and successful equality; build-local canonical reuse remains separate.
The full independent probe matched all 140,578,674 rows. Bounded Arrow/NDJSON/
client reads passed before and after deletion, `deletion_reverified` is true,
and final jobs/grants/deletions/late/quarantine counts are zero. The small live
reports additionally confirm old grants block deletion and the old snapshot
remains readable afterward. Retain the implemented combined policy as the
validated default. Production deletion remains disabled outside isolated tests.

| Metric | Prior accepted policy | Combined policy | Observed reduction |
| --- | ---: | ---: | ---: |
| Startup-inclusive archive-to-routed | 3,804.03 s (1h03m24s) | 3,464.06 s (57m44s) | 339.97 s / 8.9% |
| Normalization | 950.91 s (15m51s) | 625.01 s (10m25s) | 325.91 s / 34.3% |
| Sum of all worker stages, including deletion | 5,003.65 s | 4,661.45 s | 342.20 s / 6.8% |

Normalization partitioning fell by 187.76 s and merge by 143.97 s; sort/collapse
rose by 5.78 s. These explain most of the observed routing improvement without
adding isolated benchmark percentages. Worker stage totals exclude harness
waits, independent verification and API probes; they are not total lifecycle
wall time. The runs seeded the same logical day but produced different physical
export files/replay ordering, and load/cache conditions are uncontrolled. This
is a successful combined-policy measurement, not a repeated-run guarantee.

The failed first live preparation remains separate from this evidence: it
stopped before archival with RabbitMQ cookie access failure. The documented disk
cleanup and successful retry do not justify changing archival correctness or
recovery logic based on that attempt. No earlier failed attempt is counted as a
performance sample.

Further normalization tuning can wait. Remaining before-routing costs include
build/materialization/packing at 814.35 s, normalization at 625.01 s, independent
Influx query extraction at 595.98 s and reconciliation proof at 505.23 s. The
852.76-second Influx delete request occurs after routing and therefore cannot
improve archive-to-routed time. Little archiver CPU during deletion does not
identify the responsible Influx subsystem.

**Next recommendation:** profile the independent Influx CSV-to-Parquet path
before choosing another optimization. Its 595.98-second step uses 567.13 s of
archiver self CPU (about 0.95 core) and no child CPU. Current `Influx.Query`
processes hourly responses sequentially through `DecodeCSV` and one encoder.
These measurements suggest inspecting CSV parsing, tag/value conversion,
allocation and primitive Parquet encoding; they do not prove that the Influx
server is the bottleneck. Start with a bounded representative retained CSV
fixture or isolated read-only query, collect CPU/allocation profiles and validate
complete scalar/tag/nanosecond preservation. No full-day reseed or higher query
concurrency is needed merely to identify where CPU goes. Preserve the independent
source proof and fence throughout; do not remove reconciliation to save time.

This review updates documentation only. No code defaults, existing artifacts or
remote data were changed/deleted, and nothing was committed or pushed.

#### Finalized normalization policy and removed superseded controls (2026-11-01)

The accepted normalization policy is now unconditional: `DeduplicateExport`
clones the caller's session value, fixes native threads at sixteen and uses only
fan-in sixteen with one merge worker. A caller's generic DuckDB thread setting
cannot lower normalization threads, and its session is not mutated. Removed the
`--duckdb-threads` service flag and its main/harness plumbing. Fresh comparison
proofs remain fixed at sixteen; build sixteen/sort two remains unchanged.

Removed the private fan-in override field/resolver, variable merge bound,
normalization thread/fan-in test environment controls, and active historical
normalization sweep scripts/tests (`sweep.py`, `merge_sweep.py` and their sweep
tests). The native correctness gate and ordered/duplicate/bound tests now
exercise the single accepted implementation. Existing benchmark outputs and
frozen source directories under `state` were not modified or removed; historical
results remain available without retaining the old branches in current code.

The retained-export `testing/normalization/run.py` diagnostic now measures only
16/16/1, records that policy, and requires actual profile evidence and a fresh
sixteen-thread exact proof. Its default runtime has no container CPU/memory cap;
optional resource caps do not select another algorithm. A new harness check
rejects two-thread evidence. This runner's shared path/hash helpers remain in
place for other retained comparison experiments.

Fresh live/capacity commands omit the removed flag. Their existing actual
normalization/operation checks continue to require 16/16/1. Custom invocations
must also remove `--duckdb-threads`. The retained historical normalization
benchmark commands earlier in this plan describe frozen past experiments, not
current entry points; use the current diagnostic README for new runs.

Local validation passed: service build, Go race tests/vet and 58 Python tests
(30 capacity, 12 live, four normalization, six comparison and six remote
comparison). The native normalization oracle additionally verifies that an
explicit two-thread caller still yields sixteen-thread normalization metadata
without changing its original settings. Native VM tests were not rerun during
this cleanup; the accepted combined lifecycle above remains the measured
behavioral evidence. Docs were updated. No existing test data, saved runs or
remote objects were deleted, and nothing was committed or pushed.

#### Added extraction CPU/allocation profiling and local smoke replay (2026-11-01)

Added `testing/extraction/run.py` and a native Go diagnostic that calls the
unchanged production `DecodeCSV` and primitive `newRecordEncoder` paths. It
builds bounded annotated CSV from a retained primitive export prefix, emitting a
new table when series/type changes. This is a **derived fixture**, not captured
Influx HTTP output: it cannot measure server execution, network/HTTP buffering,
hourly request boundaries or the server's exact grouping/defaults layout.
Production code, concurrency and archival correctness gates are unchanged.

The diagnostic separately profiles decode-to-counting-sink, encode-predecoded-
records-to-Parquet and the complete streaming CSV-to-Parquet path. Each produces
CPU pprof, before/after allocation profiles and CPU/allocation top reports. CPU,
wall, total allocated bytes, mallocs and GC cycles accompany each phase.
Encode/full includes encoder setup, record encoding, compression, footer, sync
and close. Decode/full release the expected-record array before measurement;
encode-only deliberately preloads records. Fixture generation, source hashes,
independent equality checks and text report generation are outside timed/profiled
work. Profiling overhead and fixed phase order still influence measurements.
Phase costs are separate experiments, not additive exact production substeps.

The generated CSV must decode back to every selected primitive source record.
Every encode/full repetition is independently read and compared in order,
including complete tags, nanosecond identity, scalar type/text and the query
encoder's zero replay sequence. Native fixture gates also cover large signed/
unsigned integers, floats, booleans, multiline/literal newline strings, error
CSV and unknown fields. Before/after source SHA-256 must match. No equality
claim relies only on counts or hashes.

Ran a local macOS smoke replay using 250,000 rows and three repetitions per
phase. Final artifacts are at
`/private/tmp/sage-extraction-profile-1793551791013`; the earlier harness smoke
remains separate at `/private/tmp/sage-extraction-profile-1793551686092` and is
not the final streaming-memory measurement. Final `profile.json` passed and
source SHA stayed `4d259aea…d901`. The derived CSV is 94,464,676 bytes, with
236,096 float and 13,904 string values, nineteen measurement names and thirty
tag keys. It is a prefix mix, not a representative whole-warehouse guarantee.

| Local phase | Replayed rows | Wall | Cumulative bytes allocated per row |
| --- | ---: | ---: | ---: |
| Decode only | 750,000 | 0.921 s | 2,444 B |
| Encode only | 750,000 | 1.082 s | 3,460 B |
| Full CSV → Parquet | 750,000 | 2.057 s | 6,330 B |

Allocation profiles identify CSV record allocation and decoder allocations
(including tag maps), JSON map serialization/reflection and Arrow buffer
allocation as concrete candidates. Full-path allocation sampling attributes
about 19.5% cumulatively to `json.Marshal` and about 19.1% flat to Arrow's Go
allocator in this sample; cumulative and flat percentages overlap and must not
be added as disjoint components. Compression also allocates working buffers.
Allocated bytes are cumulative traffic, not live/peak memory. The macOS CPU
stacks have substantial runtime/syscall attribution and these short sampled
profiles do not establish Linux CPU shares or explain the entire 595.98-second
production extraction. Use Linux VM stacks before choosing an optimization.

On the Docker-capable VM, no Influx startup or full-day seed is needed:

```sh
python3 sage-data-rework/archive-exporter/testing/extraction/run.py \
  --source sage-data-rework/archive-exporter/state/full-day-20250301-1793231701/coordinator/jobs/9982f31e-865e-49b9-82bc-138c397ba47e/export.parquet
```

Defaults are one million source-prefix rows and three repetitions per phase,
with no container CPU/memory cap. `--rows`, `--reps`, optional resource caps,
`--run-dir` (new isolated directory) and `--prepare-only` are documented in
`testing/extraction/README.md`. The runner freezes curated Go source, builds a
matching Linux test binary separately, runs offline with read-only input/binary
and private writable scratch, and produces pprof top reports using the bundled
Go tool. It needs no DuckDB executable or credentials. It never seeds, publishes,
routes, deletes measurements or removes old artifacts.

Return `run.json`, `profile.log`, `data/results/profile.json`, each phase's
`cpu-top.txt`/`alloc-top.txt` and raw pprof files. Keep `archive-storage.test` for
further symbolized analysis. Review allocation and Linux CPU attribution before
selecting map/tag reuse, parsing or encoding changes. If derived layout bias
prevents a clear conclusion, capture a bounded real annotated response through
an isolated read-only query rather than assuming the replay models server time.

Local validation passed: Go race tests/vet, exact retained-prefix replay, the new
typed/newline round-trip test and 49 Python harness tests (three extraction,
four normalization, thirty capacity and twelve live). Source hashes and all
encode/full outputs passed. No production code/default was modified, no existing
data or saved run was deleted, and nothing was committed or pushed.

#### Linux extraction profile, one million prefix rows (2026-11-01)

`state/extraction-profile-1793552042` passed, `EXIT=0`, run id
`47bc77d1be154fbe90a6b6c2fd421914`. Correctness and profile containers exited 0
and were not OOM-killed. The source SHA stayed
`4d259aeaabcd442a59b37512876f8aeb7a40a2b45d5e3f527b86563e11c1d901`. The derived
CSV is 324,803,827 bytes: 984,134 floats, 15,866 strings, 204 measurement names
and 31 tag keys. It is the first million rows of the retained export, not a
captured Influx response and not a whole-day mix. `GOMAXPROCS` and logical CPUs
were 16. Child CPU was zero in every phase, meaning no reaped-child CPU was
recorded. Self CPU divided by wall time gives approximately 1.43 cores for
decode, 1.59 for encode and 1.26 for full replay, including Go runtime work.

| Linux phase | Rows replayed | Wall | Self CPU | Allocated bytes per row | Mallocs per row | GC cycles |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Decode only | 3,000,000 | 4.613 s | 6.597 s | 1,903 B | 10.18 | 809 |
| Encode only | 3,000,000 | 5.012 s | 7.982 s | 2,991 B | 27.73 | 9 |
| Full CSV → Parquet | 3,000,000 | 11.470 s | 14.427 s | 5,486 B | 36.92 | 828 |

Full-path throughput was 261,551 rows/s. Extending that prefix rate across
140,578,674 rows is about 537 s of this client replay. The production extraction
step was 595.98 s and also includes 24 hourly Influx queries, HTTP, and the
server's grouping. These are not a split of the same measurement.

Sampled allocation traffic matches the macOS candidates, with different shares
because this prefix is 98.4% floats. On the full path, flat allocation was
19.4% Arrow `GoAllocator.Allocate`, 18.8% `DecodeCSV`, 15.9% `encoding/csv`
`readRecord`, and 10.5% zstd history buffers. `json.Marshal` was 18.6%
cumulative and `mapEncoder.encode` 13.6% cumulative; those two cumulative
figures overlap each other. Arrow allocation is a separate flat site; arbitrary
flat and cumulative percentages must not be summed as independent shares. Decode-only allocation was 53.6% flat in `DecodeCSV` and
46.2% flat in CSV `readRecord`. Encode-only allocation was 36.0% flat in the
Arrow allocator and 33.7% cumulative in `json.Marshal`.

Linux CPU does not isolate one function. On the full path, flat time was 6.7%
`memmove`, 5.6% `memclr`, 5.4% `Syscall6`, and 4.0% CSV `readRecord`. Cumulative
time was 22.3% JSON map encoding, 19.1% `mallocgc`, 19.0% CSV `readRecord`, and
9.3% string-map assignment. `DecodeCSV` is cumulative 77.5% because it calls the
encoder sink. zstd encoding was 0.4% flat. Encode-only CPU was mostly GC:
`gcDrain` 35.4% cumulative, with JSON map encoding also 35.4% cumulative.
Decode-only spent 809 GC cycles in 4.6 s while building a tag map per row.

No map reuse, parser, or encoder change has been implemented. The profiles
confirm distributed client-side work; absence of a single dominant function does
not rule out substantial client-side cost. A bounded real annotated response
would strengthen workload validation before assigning a production saving to
client processing rather than Influx. The binary and
profiles remain in the run directory. No production default changed, nothing
was deleted, and nothing was committed or pushed.


##### Review: target repeated tag construction/serialization (2026-11-01)

Checked the manifest, frozen source hash, source before/after digests and each
phase report/top profile. The frozen source matches its manifest; both containers
exited successfully without OOM. CSV/source equality, output equality for every
encode/full repetition and unchanged retained input all passed. The reported
one-million-row sample and three repetitions match the requested configuration.
These are valid local replay profiles; no production-query timing was captured.

The **full streaming profile** is the best basis for choosing the next candidate:
JSON map encoding takes 22.3% cumulative CPU, CSV record parsing 19.0%, allocation
19.1% and map assignment 9.3%. Those are overlapping call-stack costs, not
independent percentages to add. `DecodeCSV`'s 77.5% cumulative CPU includes its
encoder call and therefore is not parser-only cost. Encode-only retains an
in-memory expected-record array, so its GC profile should not be treated as the
streaming service's GC share. Total full-path allocation is 5,486 bytes per row,
with 828 GC cycles over three million rows. zstd is not the first CPU target
based on this profile; a low flat compression sample is not a full codec cost
measurement.

The current path constructs a tag map for every CSV row, then the primitive
Parquet encoder marshals that map to JSON for every row. Repeated keys/values
therefore create work on both sides of the decoder/encoder boundary. The next
isolated candidate should:

- Build a column/type plan when the annotated CSV header changes, preserving
  defaults, mixed schemas, error-table checks and unsupported-field rejection.
- Compare tag cells with the previous tuple and reuse canonical tag JSON only
  when keys and values are identical. Invalidate on header/default/tag changes;
  do not assume that a table id or group flag alone guarantees constant tags.
- Feed canonical tags directly into the existing primitive Parquet representation
  where supported, avoiding the per-row map → JSON round trip while preserving
  complete tag identity and the generic encoder contract.

Keep the current implementation as the benchmark baseline while developing the
candidate. Compare full decode-plus-encode output, time, allocation, mallocs and
GC, with unchanged compression and bounded cache size. Preserve all scalar
kinds/precision, tags/defaults, multiline strings and nanoseconds. If CSV row
slice reuse is tested, retained header/type/default slices and sink-owned data
must not alias the reused row buffer. A memory-safety regression would invalidate
any speedup. No production default or independent equality proof should change
until exact comparisons and the isolated benchmark pass.

A bounded captured annotated response is useful to check that the derived
fixture's column/tag reuse matches real Influx layout. It can be obtained from
an isolated read-only query without a full-day seed or deletion. Retain the
existing derived replay as a controlled baseline and gate; do not label its
537-second linear projection as measured client time within the 595.98-second
production extraction. They use different data mixes, I/O paths and contexts.
Nevertheless the replay demonstrates enough local processing cost to justify a
focused optimization experiment rather than dismissing client work.

This review corrects the CPU-core and allocation-overlap interpretation above.
It updates documentation only: no parser/encoder change, defaults change,
existing artifact deletion or remote interaction was performed, and nothing was
committed or pushed.

#### Implemented cached CSV layout and canonical tag candidate (2026-11-01)

Added the candidate production decoder in `internal/storage/influx_csv.go`.
Column indices/types are compiled only when the actual header/type layout
changes, so identical repeated table declarations reuse the plan. CSV record
slice reuse is enabled, with independently owned header/type/default snapshots.
Each row still applies current defaults, parses and validates scalar/time/name
and checks query bounds.

The synchronous primitive Parquet sink can now accept canonical staging views
from the decoder. The decoder retains one previous effective tag tuple, its
validated map and canonical JSON. Every row's tag cells are compared; changed
effective values rebuild the tuple/JSON. Changed column/type layouts invalidate
the cache. Repeated identical headers/defaults do not force rebuilding when the
layout and effective values are unchanged. No reuse is inferred solely from a
table id or group flag. The Arrow writer copies staging view strings into its
builders synchronously. Generic encoders still receive separate owned
`control.Record` tag maps per row; mutable maps are never shared with those
sinks. The cache does not retain a growing dictionary of all historical tuples.

The old decoder is frozen in `_test.go` only, never in the service binary. There
is no production baseline/candidate flag. The candidate preserves primitive
schema, canonical tags, query replay sequence zero, ordering, scalar precision,
nanoseconds, compression and all archival verification boundaries. Malformed
duplicate header columns now fail closed instead of ambiguous duplicate-column
interpretation. Tests compare old/new generic and Parquet records across changed
headers, defaults, tag changes/removal and column order; they verify generic map
ownership and reject invalid type/nonfinite/error-table transitions. Existing
integer, unsigned, boolean, float, multiline/newline and nanosecond gates remain.
The typed fixture also exercises escaped and Unicode tags.

`testing/extraction/run.py` now defaults to baseline → candidate → candidate →
baseline in separate containers using one frozen source/binary. `--decoder
baseline` or `--decoder candidate` selects a single diagnostic arm. Each arm
retains all decode/encode/full CPU and allocation profiles, independent source/
output equality and source hashes; the manifest records per-arm reports and
median full-path times. The baseline is strictly test code. Each fixture reports
its derived table count to expose reconstructed-layout density.

Initial local instrumentation at
`/private/tmp/sage-extraction-ab-1793553775750` exposed unnecessary plan
recompilation: the 250,000-row fixture has 81,903 derived table headers. That
partial diagnostic remains intact and is not the final measurement. The final
frozen local A/B artifacts are at
`/private/tmp/sage-extraction-ab-1793554021629`; `local-ab.json` records native
macOS execution, the frozen source/binary hashes, four ordered trials and
medians. Its preparation `run.json` remains a preparation record; it is not a
Docker VM acceptance result.

Every final arm passed source/CSV equality, unchanged source SHA and independent
ordered readback of all encode/full repetitions. Each arm used 250,000 prefix
rows, three repetitions per phase, the same native binary and unchanged input.

| Local full-path arm | Wall for 750,000 records | Allocated bytes per record |
| --- | ---: | ---: |
| Baseline, first | 2.0066 s | 6,334 B |
| Candidate, first | 1.3833 s | 4,174 B |
| Candidate, second | 1.3679 s | 4,157 B |
| Baseline, second | 2.0232 s | 6,348 B |
| Median baseline | 2.0149 s | 6,341 B |
| Median candidate | 1.3756 s | 4,166 B |

The candidate reduces local full-path wall by **31.7%** and allocated bytes by
**34.3%**, winning both orders. These are short, warm macOS replay samples, not
Linux or end-to-end lifecycle guarantees. Candidate decode-only performs the
canonical-tag work formerly in the encoder; its slightly longer standalone time
is not an equivalent parser-only regression. Full-path timing/allocation is the
primary decision metric. Encode-only intentionally retains the original generic
record encoding work for diagnosis.

Run the Linux A/B experiment next, without seeding or starting Influx:

```sh
python3 sage-data-rework/archive-exporter/testing/extraction/run.py \
  --source sage-data-rework/archive-exporter/state/full-day-20250301-1793231701/coordinator/jobs/9982f31e-865e-49b9-82bc-138c397ba47e/export.parquet
```

Return `run.json`, `profile-*.log`, every
`profile-*/results/profile.json` and each phase's CPU/allocation reports. Compare
full-path time, allocation and GC across both orders with unchanged inputs and
exact outputs. Reconstructed CSV does not establish real Influx table-layout or
network savings; use the existing small live/full-day lifecycle gates after
reviewing the Linux screen, and capture a bounded real response if layout bias
remains material. All normalization/build/fresh-proof policies remain fixed as
previously accepted.

Local validation passed: service build, Go race tests/vet, exact four-arm replay
and 49 Python harness tests (three extraction, four normalization, thirty
capacity, twelve live). The extraction harness test covers reversed four-arm
aggregation and refuses incomplete equality evidence. Native DuckDB/Pelican
lifecycle tests were not rerun locally. No remote storage was contacted, no
existing artifacts/data were deleted, and nothing was committed or pushed.

#### Linux decoder A/B, one million prefix rows (2026-11-01)

`state/extraction-ab-1793554518` passed, `EXIT=0`, run id
`0337cbe6da8b483a881a2ed1f62018c7`. Order was baseline, candidate, candidate,
baseline. Correctness and all four profile containers exited 0 and were not
OOM-killed. Every arm kept source SHA
`4d259aeaabcd442a59b37512876f8aeb7a40a2b45d5e3f527b86563e11c1d901`, decoded the
derived CSV back to the source records, and passed ordered encode/full readback.
The prefix is 984,134 floats and 15,866 strings, 204 names, 31 tag keys, and
324,803,827 CSV bytes. It reconstructs **281,059 table headers** in 1,000,000
rows, about one header per 3.6 rows. `GOMAXPROCS` was 16 and child CPU stayed 0.

| Full path, 3,000,000 records | Wall | Allocated bytes per record | GC cycles | Mallocs per record |
| --- | ---: | ---: | ---: | ---: |
| Baseline, first | 10.986 s | 5,502 B | 829 | 36.92 |
| Candidate, first | 7.559 s | 3,802 B | 453 | 23.12 |
| Candidate, second | 7.804 s | 3,820 B | 466 | 23.12 |
| Baseline, second | 10.490 s | 5,494 B | 838 | 36.92 |
| Median baseline | 10.738 s | 5,498 B | 833.5 | 36.92 |
| Median candidate | 7.681 s | 3,811 B | 459.5 | 23.12 |

The candidate is faster in both orders. Median full-path wall is **28.5%**
shorter, allocated bytes **30.7%** lower, and GC cycles **44.9%** lower. The
second candidate trial is slower than the first, and the second baseline trial
is faster than the first. This is not a simple monotonic warm-up pattern, but
cache temperature remains uncontrolled and cannot be ruled out as a contributor.

Decode-only is not the decision metric. Its median wall rose from 4.605 s to
5.630 s because the candidate builds canonical tag JSON there. Encode-only stayed
near 5.0–5.2 s on both arms; it still encodes generic records and does not show
the cache. On the full path, JSON map encoding fell from about 24% cumulative
CPU to about 16–17%, and string-map assignment fell from about 7–8% to about
1.2%. Sampled CSV `readRecord` allocation fell from about 16% flat to about 8%
flat. Arrow allocation stayed near 3.1 GB, so its share of the smaller total
rose.

This reconstructed header density has not been validated against an actual
hourly Influx response. The 28.5% is a client replay result, not a measured
extraction or network saving. The small live gate and a fresh full-day remain
the check against real annotated responses. No service default was changed by
this run, no earlier artifact was deleted, and nothing was committed or pushed.


##### Review: cached decoder passes Linux screen; proceed to real lifecycle (2026-11-01)

Independently checked the manifest, frozen source hash, all four native reports
and container exit/OOM records. Frozen code matches its recorded hash. Correctness
and all four arms exited zero without OOM. Every arm used the same million-row
prefix and three repetitions, unchanged source hash, identical derived layout
(281,059 headers), source/CSV equality and independent ordered output equality
for every encode/full repetition. These measurements support the full-path
comparison; no timing from an incomplete or failed proof was included.

| Full streaming metric | Baseline median | Candidate median | Reduction |
| --- | ---: | ---: | ---: |
| Wall for 3,000,000 rows | 10.738 s | 7.681 s | 28.5% |
| Allocated bytes per row | 5,498 B | 3,811 B | 30.7% |
| Mallocs per row | 36.92 | 23.12 | 37.4% |
| GC cycles | 833.5 | 459.5 | 44.9% |

The candidate wins against baseline in both observed orders. Reduced allocation,
mallocs and GC corroborate the intended effect of removing repeated map/JSON
work; it is not only a small wall-time difference. These are descriptive
repetitions with warm/uncontrolled caches, not a statistical guarantee.
The cache result does not imply all serialization cost disappeared: Arrow buffers,
CSV parsing, remaining tuple changes and compression still do work. Standalone
decode includes canonical JSON in the candidate, so do not reject the candidate
based on decode-only time or add separate phase timings into an exact pipeline
breakdown. Full-path cost is the appropriate A/B decision metric.

**Retain the cached decoder and proceed to real-response lifecycle validation.**
The candidate is already the `DecodeCSV` implementation in current service
builds; the baseline and `--decoder` control exist only in diagnostic tests.
No extra production flag or new default needs wiring. Keep the test baseline
until lifecycle validation finishes so regressions remain easy to reproduce.
No further parser, map-pool, compression or concurrency changes are needed before
this gate. Do not multiply the 28.5% replay reduction by the previous 595.98 s
query-extraction stage and present that as measured production saving.

On the VM, prepare and run a fresh small live fixture first:

```sh
python3 sage-data-rework/archive-exporter/testing/live/run.py
```

After it passes, run a fresh uninterrupted full-day lifecycle:

```sh
python3 sage-data-rework/archive-exporter/testing/capacity/run.py --influx-memory 10g
```

Use new directories and current source. Return `run.json`, `routed.json`,
`summary.json`, normalization/build reports and all job operation profiles.
Compare extraction's wall/self CPU against accepted combined-policy run
`full-day-20250301-1793539326` (595.98 s and 567.13 s self CPU), then compare
archive-to-routed against 3,464.06 s. Actual lifecycle responses exercise the
server's annotated table layouts/defaults; if they reveal a mismatch, retain
that bounded failing response for regression rather than assuming the derived
layout was representative. Keep full source/remote equality, Arrow/NDJSON/client
checks, fresh pre-delete proof, retained sentinel and cleared grants/queues.
All normalization/build/proof execution policies stay unchanged.

This review also corrects the earlier cache-ordering inference and removes an
unsupported comparison with real hourly header density. It changes documentation
only. No new production configuration, saved artifacts or remote data were
changed/deleted, and nothing was committed or pushed.

#### Cached-decoder lifecycle (2026-11-01)

Before the gate, 29 exited archive containers and 6.463 GB of unused build cache
were removed. The Docker VM disk went from 22.6 GiB free to 28.5 GiB free.
Running kube-system containers were left up. No remote object was deleted.

The fresh live gate passed in `state/live-1793555660`, run id
`510f77e3768f4074b61723b50b25a2f0`, `EXIT=0`. Phase is `complete` and services
are stopped. Policy remains normalization 16, merge fan-in 16, merge concurrency
1, fresh comparison 16, build 16, sort 2, canonical reuse, and fixed
partitioning. Prefix
`cold-storage-archive-live-510f77e3768f4074b61723b50b25a2f0` stays paired. Every
listed phase passed, including deletion blocked while a grant was held and
release-and-finish.

The fresh full day passed in `state/full-day-20250301-1793555812`, run id
`8ab14ddb9b0c460c96c9424c202c7d86`, started 2026-11-01T17:56:52Z, `EXIT=0`.
`run.json` is `passed: true`, phase `complete`, `services_stopped: true`, with
the same policy. Prefix
`cold-storage-archive-live-8ab14ddb9b0c460c96c9424c202c7d86` stays paired. Job
`fa8660b0-95bb-4c3e-bc2f-53f257359485` reached `COMPLETE` for 140,578,674 rows.
Seed took 5,307.81 s. The hot sentinel is `1740873600000000194`. Hot March 1 has
0 rows and the next-day sentinel remains. Pending deletions, jobs, readers,
writers, late work, and quarantine are 0. No OOM event is recorded. The resource
sampler recorded one probe `CalledProcessError` while collecting a sample; that
did not fail a phase. The archiver cgroup peak is exactly 8 GiB and the verify
probe peak is exactly 4 GiB. Influx peaked at 3,060,641,792 bytes and the API at
390,787,072 bytes. Largest Go RSS was 419,983,360 bytes and largest child RSS
was 3,291,709,440 bytes.

Influx extraction on this run is **494.75 s** wall and **224.20 s** self CPU,
with child CPU 0. The accepted combined-policy run
`full-day-20250301-1793539326` was 595.98 s wall and 567.13 s self CPU, also
with child CPU 0. This extraction is 101.23 s shorter in wall time and 342.93 s
shorter in self CPU. That is the measured query-stage change. It is not the
28.5% replay reduction applied to 595.98 s.

Startup-inclusive archive-to-routed time is **3,457.26 s**. The same prior run
was 3,464.06 s, so this lifecycle is 6.80 s shorter on that metric.

| Archival stage | This run | Prior combined-policy run | Change |
| --- | ---: | ---: | ---: |
| Export | 498.22 s | 495.49 s | +2.73 s |
| Reconciliation | 1,644.17 s | 1,726.24 s | −82.07 s |
| Build/materialization/packing | 850.98 s | 814.35 s | +36.63 s |
| Publish/commit | 80.84 s | 76.72 s | +4.12 s |
| Fresh remote verification | 377.33 s | 345.54 s | +31.79 s |
| Pre-delete verification plus delete request | 1,215.18 s | 1,202.62 s | +12.56 s |

Inside reconciliation, normalization was 616.84 s versus 625.01 s and the fresh
comparison was 532.57 s versus 505.23 s. The extraction drop accounts for the
reconciliation change; the build and remote-verification increases consume it
in the archive-to-routed total. Normalization still reports threads 16, fan-in
16, concurrency 1, maximum fan-in 16, and 2 passes: partition 136.34 s,
sort/collapse 199.72 s, merge 279.96 s. Build stayed at 16 threads and sort 2,
with canonical reuse, and packed 140,578,674 rows into 15 files and 457,652,212
data bytes in 739.86 s.

Commit verification was 377.32 s, including 368.03 s of fresh comparison.
Pre-delete verification was 379.55 s, including 366.80 s of fresh comparison.
The Influx delete request was 835.60 s. Both worker journals record full
equality for the same 15 objects. The independent probe matched all
140,578,674 rows. API readiness passed in 2.57 s. Before and after deletion,
Arrow, NDJSON, and the client each returned one row for `uncorrected_inner`,
`W099`, node `000048b02dd3c51f`. `deletion_reverified` is true. No hourly
response was retained as a mismatch. Production deletion remains disabled
outside this isolated run. Nothing was committed or pushed.

##### Review: cached decoder accepted against real Influx responses (2026-11-01)

Reviewed the live/full-day manifests and saved acceptance reports, operation
profiles and build profile. The full-day frozen archiver hash matches its
manifest, and its decoder matches current source. Both runs passed, completed
and stopped. The live fixture confirms held grants block deletion and the old
snapshot remains readable after release. The full-day independent probe matched
140,578,674 source rows; reconciliation, commit and pre-delete exact proofs all
passed with fixed sixteen-thread fresh comparisons. Ordered pipeline behavior,
normalization 16/fan-in 16/one merge worker, build 16/sort 2 and canonical reuse
remain intact. API Arrow/NDJSON/client checks passed before and after deletion,
`deletion_reverified` is true, and final jobs/grants/late/deletion/quarantine
counts are zero. Retain the current cached decoder as the validated default.
No production decoder switch or further wiring is required.

| Metric | Prior combined-policy run | Cached decoder run | Observed change |
| --- | ---: | ---: | ---: |
| Influx extraction wall | 595.98 s | 494.75 s | −101.23 s / 17.0% |
| Extraction archiver self CPU | 567.13 s | 224.20 s | −342.93 s / 60.5% |
| Startup-inclusive archive-to-routed | 3,464.06 s | 3,457.26 s | −6.80 s / 0.2% |
| Sum of all worker stages, including deletion | 4,661.45 s | 4,667.03 s | +5.57 s / 0.1% |

The actual query-stage result corroborates the allocation-focused change on real
annotated responses: it saves substantial archiver CPU and about 1m41s in this
extraction run. It does **not** establish a material overall latency improvement
for this lifecycle pair. Reconciliation improved by 82.07 s after accounting for
other substeps, while export/build/publish/remote verification grew by roughly
75 s combined. All-worker stage time was effectively unchanged and slightly
higher. Do not substitute replay's 28.5% result for actual extraction savings or
call the CPU reduction an end-to-end wall-time reduction. These are separate
fresh runs with uncontrolled storage/cache/load conditions and different
physical replay files for the same logical day, not repeated identical-input
whole-pipeline trials.

Extraction now averages about 0.45 archiver CPU cores over its wall interval,
compared with 0.95 previously. This indicates less client computation but does
not identify the remaining wait as Influx execution alone: the stage also
includes HTTP reads, filesystem work and finalization. Subtracting self CPU from
wall time is not a precise breakdown of server/network/disk time, particularly
with Go runtime concurrency. No further parser or map-pool tuning is justified
solely by the remaining wall duration.

If optimization continues, inspect the largest remaining before-routing stage:
build/materialization/packing at 850.98 s (14m11s). The packing report alone
contains 267.33 s per-file exact proofs, 138.83 s whole-build proof, 192.97 s
batch sort/copy and 136.22 s actual packing. These substeps are included in the
739.86-second packing total; they are not extra stage costs. This breakdown
shows why making the Parquet writer faster alone cannot remove most build time.
Preserve all exact proofs while selecting an isolated retained-artifact candidate;
do not add another change before choosing a specific measured bottleneck.
The 835.60-second Influx delete request remains after routing and cannot improve
the archive-to-routed metric.

One sampler `CalledProcessError` affects resource-sample coverage, not the
separately successful equality/API/deletion gates. Exact cgroup-cap peaks are
not proof of memory headroom or an OOM; no OOM was recorded. Do not attribute
stage timing variation to that sampler event without additional evidence.
Production deletion stays disabled outside the isolated fixture.

This review updates documentation only. No service code/default, existing data,
saved artifact or remote object was changed/deleted, and nothing was committed
or pushed.

#### Finalized cached decoder and removed the old implementation (2026-11-01)

The accepted cached-column-plan/tag decoder remains the sole default in service
builds. Removed `influx_csv_baseline_test.go`, the diagnostic decoder dispatcher,
`SAGE_EXTRACTION_DECODER`, the `--decoder` CLI option and reversed A/B runner
branches/aggregation. No old decoder copy remains in active code or tests.
Existing saved runs, binary profiles and frozen historical source were not
modified or removed.

Replaced the old/new differential fixture with an explicit record oracle that
checks all fields for repeated tags, changed tag values, changing defaults, tag
removal and reordered typed schemas. Both generic and Parquet outputs are
checked against that oracle, including nanosecond timestamps and maximum uint64.
The retained-map ownership check and invalid transition/type/error-table gates
remain. Existing full typed/newline/Unicode fixtures continue to exercise the
same current implementation. This removes the baseline without weakening the
regression checks into agreement between two implementations.

`testing/extraction/run.py` now runs correctness gates followed by one profile
of the accepted implementation. It still reports decode/encode/full CPU and
allocation phases, exact source/output checks, unchanged source hashes and
required pprof artifacts. Reports identify `cached-column-plan-tags` rather
than a selectable decoder arm. New profile output lives at
`profile/results/profile.json`; historical `profile-baseline-*` and
`profile-candidate-*` paths describe saved past runs only. Current commands must
omit the removed `--decoder` option. The README reflects the single-path tool.

Local validation passed: Go race tests, service build, vet and 49 Python harness
tests (three extraction, four normalization, thirty capacity, twelve live).
The scan for removed baseline helpers/selectors found no remaining active
references. Production decoder logic and all normalization/build/fresh-proof
policies are unchanged by this cleanup. No new full-day VM run was performed;
the accepted real-response lifecycle above remains the performance/correctness
evidence. Nothing was committed or pushed, and no existing data or saved
artifact was deleted.

#### Started build optimization: retained source-window scan probe (2026-11-01)

The accepted 850.98-second build is not primarily final Parquet encoding.
Packing substeps include 267.33 s of per-file proofs, 138.83 s of whole-build
proof, 192.97 s batch sort/copy and 136.22 s actual packing. The whole 739.86 s
packing total is nested in build/materialization time. These nested figures
must not be added again to the 850.98 s stage.

A concrete candidate is avoiding unnecessary scans of `expected.parquet` when
selecting ten-million-row chronological batches. Current sort/copy and each
per-file proof select an ordinal range with `file_row_number`. Whether DuckDB
already prunes those reads must be measured; repeated SQL alone does not prove
repeated full-file I/O. Added the **test-only**
`testing/build_scan/run.py` probe and native Go tests rather than changing the
production build or weakening any exact proof.

For first/middle/final windows, each probe runs ordinal → time-pruned →
time-pruned → ordinal. Both variants keep identical ordinal predicates. The
candidate adds inclusive timestamp bounds from the conservative union of
min/max across every physical row group overlapping that ordinal range. It
never assumes global time order, unique timestamps or whole-row-group window
boundaries. UTC nanosecond logical type, non-null min/max statistics and complete
footer coverage are required. No guessed unit or missing-statistic fallback is
promoted as a faster service path.

The diagnostic measures full-column sort/copy at two threads and source
canonical partition staging at sixteen threads separately. Native DuckDB 2
profiles retain query/scan work, available row-group counts, byte counters and
buffer/spill peaks; operation wall and self/child CPU are recorded. Every sorted
result must have the expected count and pass a fresh fixed full exact comparison
against the **original unpruned ordinal window**. Canonical shards must match the
window count. Verification timing is separate from the operations under test.
This specifically detects unsafe filter/virtual-row-number interactions.

Native gates use boundary cuts, repeated nanosecond timestamps and deliberately
nonchronological row-group time ranges, including windows where time pruning can
skip physical groups. Pure Go tests cover conservative unions across group
boundaries, precise nanosecond predicate formatting and rejection of integer-time
or invalid windows. The candidate does not change Parquet architecture, final
packing, VARIANT/MAP fields, proof freshness, canonical reuse or source coverage.

Local read-only inspection of the retained expected file confirmed valid footer
statistics and these windows:

| Offset | Selected rows | Physical groups overlapping window | Total groups |
| ---: | ---: | ---: | ---: |
| 0 | 10,000,000 | 81 | 1,132 |
| 70,000,000 | 10,000,000 | 81 | 1,132 |
| 140,000,000 | 578,674 | 6 | 1,132 |

This is pruning eligibility, not measured engine pruning or a speedup. Run the
native probe on the Docker-capable VM from the workspace root:

```sh
python3 sage-data-rework/archive-exporter/testing/build_scan/run.py \
  --source sage-data-rework/archive-exporter/state/full-day-20250301-1793231701/coordinator/jobs/9982f31e-865e-49b9-82bc-138c397ba47e/expected.parquet
```

The default audited count is 140,578,674 rows, native memory 3GB, source/proof
threads sixteen and sort threads two; no container CPU/memory cap is imposed by
default. The runner freezes curated Go source and the Linux DuckDB binary,
requires fresh isolated scratch outside the retained source directory, and runs
with no network, credentials or writable input mounts. `--run-dir` can select a
new directory on a larger volume; `--prepare-only` freezes inputs without Docker.
No Influx startup, reseeding, Pelican read/write, publication, routing or deletion
is involved. No existing artifact is removed or resumed.

Return `run.json`, correctness/probe logs, `data/results/probe.json` and all
window `sort_copy.duckdb.json` / `canonical_source_copy.duckdb.json` profiles.
The manifest summarizes grouped DuckDB 2 query/system/I/O and scan metrics.
Compare paired operation times and actual scan work, including first/middle/
short-tail behavior. Raw byte counters are engine counters, not a complete
physical-I/O measurement, and operator work is not extra elapsed time. Hashing,
phase order and previous operations warm caches; reversed repetitions do not
establish cold conditions. Missing profiles, incomplete order, changed hashes,
wrong settings or failed exact proof prevent success.

If scan work/time improves enough, test integration with the complete retained
build and all layout/per-file/whole-build/reference proofs before changing
production. If the ordinal path already prunes effectively or the additional
predicate does not help, reject this candidate and use the profiles to choose
the next build substep. No speedup is assumed merely from the footer counts.

Local validation passed: Go race tests/vet, four scan-harness tests, seven packing
harness tests and retained-footer inspection. Native DuckDB profiling/equality
gates remain pending on the VM because the local CLI is Linux-only and Docker
access is unavailable. All production settings and code paths remain unchanged;
nothing was committed or pushed, and no existing data was deleted.

#### Linux build-window scan probe (2026-11-01)

`state/build-scan-1793570740` passed, `EXIT=0`, run id
`618132f5c624435f8b1aa0611007c612`. Correctness and probe containers exited 0
and were not OOM-killed. Source SHA stayed
`c91128eb5a35387bd50dfd4d316f2f918a7b0333e94604b0fc6df4d3c8161f6e`. DuckDB is
`f4c681bcf41a5c43dcf9a354703fe5bded9e2265282d1915d6d99637cd1e1f29`. Each of the
three windows ran ordinal, time-pruned, time-pruned, ordinal. Every sorted
window matched its count and passed a fresh fixed sixteen-thread comparison
against the original unpruned ordinal window. Canonical shard counts matched.
Memory was 3 GB, sort threads 2, and source/proof threads 16.

Both arms scan the same rows and the same physical groups. The ordinal predicate
already limits the scan to the footer overlap: 81 of 1,132 groups and
10,000,000 rows for the first and middle windows, and 6 of 1,132 groups and
578,674 rows for the tail. The added timestamp bounds do not skip another group
or row. `total_row_groups_to_scan` stays 1,132 because the file is still the
scan source; `total_row_groups_scanned` is the overlapping subset on every arm.

| Window | Order | Sort wall | Groups scanned | Rows scanned |
| --- | --- | ---: | ---: | ---: |
| Offset 0 | Ordinal | 27.893 s | 81 | 10,000,000 |
| Offset 0 | Time-pruned | 18.401 s | 81 | 10,000,000 |
| Offset 0 | Time-pruned | 21.532 s | 81 | 10,000,000 |
| Offset 0 | Ordinal | 14.776 s | 81 | 10,000,000 |
| Offset 70,000,000 | Ordinal | 15.960 s | 81 | 10,000,000 |
| Offset 70,000,000 | Time-pruned | 14.023 s | 81 | 10,000,000 |
| Offset 70,000,000 | Time-pruned | 14.648 s | 81 | 10,000,000 |
| Offset 70,000,000 | Ordinal | 13.725 s | 81 | 10,000,000 |
| Offset 140,000,000 | Ordinal | 0.582 s | 6 | 578,674 |
| Offset 140,000,000 | Time-pruned | 0.449 s | 6 | 578,674 |
| Offset 140,000,000 | Time-pruned | 0.528 s | 6 | 578,674 |
| Offset 140,000,000 | Ordinal | 0.449 s | 6 | 578,674 |

The reversed ordinal sort is the fastest trial in every window. The first
time-pruned trial beats only the first, slower ordinal trial. Cache temperature was not measured. Canonical copy
shows the same scan counts and no arm advantage: about 7.7–9.6 s and 32.3 MB
read on the ten-million-row windows, and about 1.1–1.3 s and 4.6 MB read on the
tail. Sort bytes read stay about 1.25–1.37 GB on those large windows for both
arms. Every ten-million-row sort hits a 3.0 GB buffer peak and spills about
0.94–0.99 GB. The tail does not spill. Separate verification took 20.5–30.2 s
on the large windows and 2.7–3.1 s on the tail.

**The time-pruned candidate is rejected.** The ordinal path already prunes to
the overlapping row groups, and the extra predicate does not reduce scan work.
The sort/copy time that remains is sorting, spilling, and writing that window.
In the accepted build, per-file proofs are still the larger packing piece at
267.33 s, ahead of the 192.97 s sort/copy. No production build path was changed,
no earlier artifact was deleted, and nothing was committed or pushed.


##### Review: reject time predicate; measure build memory next (2026-11-01)

Checked the manifest, frozen code/CLI hashes, all twelve window results and
native profiles. Both containers exited zero without OOM, source hashes match
before/after, and every window passed a fresh fixed sixteen-thread exact proof
against its original ordinal range. The frozen source and DuckDB executable
match their recorded digests. The screen is valid and gives a negative result
for the candidate, not a correctness failure.

Both sort and canonical-source copy report the same actual scan work in all
arms: 81 groups / 10,000,000 rows for first/middle windows and six groups /
578,674 rows for the tail. The 1,132-group `total_row_groups_to_scan` field is
not the number actually scanned. Original `file_row_number` filtering already
provides the pruning we were seeking. **Do not integrate footer-derived time
predicates into production packing.** They add schema/statistics handling and
validation complexity without demonstrated scan reduction. Production code
already uses the retained ordinal path, so no rollback or service change is
required. Keep saved profiles as evidence; the rejected test-only probe is not
a new architecture requirement.

Timing does not rescue this candidate: the final ordinal sort is fastest in
each window, and canonical-source copies have no consistent arm advantage.
The slow first ordinal copy must not be called cold-cache data: source hashing
already warms OS pages and cache state was not measured. This is an uncontrolled
ordered screen. Exact profile scan counts provide the stronger reason to reject
than the two-sample timing differences.

The next concrete experiment should vary **native build memory at 3GB versus
6GB**, holding build/comparison threads sixteen and batch-sort threads two.
Every full ten-million-row sort reaches about 3.0GB of native buffer memory and
spills about 0.94–0.99GB; the short tail does not spill. The hypothesis is that a
larger native budget reduces sort spill/work and elapsed time. It is not a claim
that every extra byte of buffer limit reduces runtime or that the entire build
is memory-bound. Canonical source staging is already scanning only its window
and reports no spill in these trials. Its approximately 32MB engine read counter
must not be confused with sort's roughly 1.3GB aggregate I/O counter, which is
not an isolated count of bytes read from the source Parquet window.

Use the complete retained-artifact build benchmark for this next screen, with
memory as the only varied setting and order 3GB → 6GB → 6GB → 3GB. Keep
materialization, sort/copy, final packing, layout audit, fresh per-file exact
proofs, canonical reuse/whole-build proof, binding hashes and independent
retained-reference equality. Record native spill/buffer metrics where possible,
phase CPU/wall and process/container peaks. Do not accidentally repeat the older
uniform-vs-two-sort-thread experiment while comparing memory: both memory arms
must use sort two. The existing packing runner needs a fixed-policy memory
comparison mode before this experiment; its current `--sort-threads` pairs
uniform/split arms and is not a clean memory-only screen.

No new Influx seed or Pelican interaction is needed for that retained build.
Run with adequate VM memory and equal optional container resources across arms;
do not use a 6GB container cap to represent a 6GB DuckDB buffer budget. Buffer
limit, process RSS and cgroup/page-cache peaks are different measurements.
Native memory remains 3GB until the experiment demonstrates a worthwhile total
build benefit and the combined lifecycle is validated. Per-file/whole-build
proofs still account for about 406 seconds of the accepted packing stage; no
proof should be removed to make the memory trial appear faster.

This review updates documentation only. No service policy, saved artifact or
remote data was changed/deleted, and nothing was committed or pushed.

#### Prepared fixed-policy 3GB/6GB retained-build comparison (2026-11-01)

Added `archive-exporter/testing/packing/memory.py` as a memory-only entry point
sharing the retained packing runner. Default order is 3GB → 6GB → 6GB → 3GB.
Materialization, sort/copy, packing and build proofs use the selected native
memory budget, with build/proof threads fixed at sixteen and sort/copy threads
fixed at two in both arms. No uniform-sort alternative or time-pruning predicate
is included. The production memory default remains 3GB; this is not a global
memory configuration change.

Every trial rematerializes typed expected rows from the same retained normalized
input and runs the complete production build: ordinal windows, plugin/vsn/time
sort, plugin-pure capped groups, layout audit, per-file fresh exact comparisons,
hash/inventory/coverage bindings and canonical-reuse whole-build proof. An
additional fresh exact comparison against the independently retained expected
file is fixed at sixteen threads/3GB, with profiling disabled, outside build
timing. Fixing that reference budget prevents it becoming a second varied
setting. The source hashes must match before/after and across all trials; both
input footers must equal the audited row count.

Added native profiling behind a private benchmark-only DuckDB field. It is not
set by the service or exposed through service flags/environment. Every native
invocation gets a distinct retained JSON path outside comparison scratch that
is ordinarily reclaimed. Multi-statement calls can overwrite that invocation's
profile with the last eligible statement, so the runner does not sum all raw
profiles into complete build CPU/I/O. It specifically requires one expected COPY
and every ordinal batch-sort COPY profile, with complete file-index coverage;
these are single-COPY observations. The manifest records their native query,
I/O, buffer and spill metrics without SQL. Raw profiles remain for inspection.
The small packing gate enables/checks JSON profiling at each requested budget
before large runs.

The native benchmark also records effective native settings, build self/child
CPU deltas, existing Go/child/cgroup peaks and sampled scratch. Build resource
metrics are captured before the extra reference proof. Native buffer limits,
process RSS, cgroup/page-cache peaks and sampled scratch are different measures;
none should be added as simultaneous peak memory. Profiling adds overhead in
both arms, and source hashing/order/cache conditions remain uncontrolled.

New acceptance checks require selected memory and fixed 16/2 threads in actual
profiles, CPU evidence, exact unchanged-input hashes, audited row/file coverage,
complete ordered materialization and per-file steps, all fresh/reused proof
boundaries and the fixed-budget independent reference proof. Missing material/
sort native profiles or incorrect proof settings fail the screen. Existing
thread/sort diagnostic behavior is preserved separately; `memory.py` does not
rely on its old paired uniform/split interpretation.

Run on the Docker-capable VM from the workspace root; no Influx startup,
full-day seed or Pelican interaction is required:

```sh
python3 sage-data-rework/archive-exporter/testing/packing/memory.py \
  --expected sage-data-rework/archive-exporter/state/full-day-20250301-1793231701/coordinator/jobs/9982f31e-865e-49b9-82bc-138c397ba47e/expected.parquet \
  --normalized sage-data-rework/archive-exporter/state/full-day-20250301-1793231701/coordinator/jobs/9982f31e-865e-49b9-82bc-138c397ba47e/export-dedup.parquet
```

Both retained inputs were confirmed present locally (462,138,695 and 774,832,276
bytes). Each new run freezes curated source and the architecture-matching Linux
DuckDB binary. Runtime uses no network or credentials and mounts inputs/binaries
read-only, with only new private scratch writable. Default container CPU/memory
caps are omitted; optional caps apply equally and a CPU cap must cover sixteen
threads. A 6GB native budget must not be confused with a 6GB container limit.
`--run-dir` can select a new isolated directory outside both source directories;
otherwise `state/packing-memory-<uuid>` is used. `--prepare-only` freezes source
without Docker. Old runs/data are neither resumed nor removed.

Return `run.json`, gate logs and each trial's `results/benchmark.json`,
`expected-profile.json`, `packing-profile.json` and `native-profiles/*.json`.
Compare total build medians first, then phase timings, native spill reductions
and actual process/container peaks. No speedup is presumed merely because the
sort spilled at 3GB. If 6GB is worthwhile, wire a build-specific session setting
and validate a full lifecycle before considering any wider memory change;
normalization and fresh remote/pre-delete proof budgets were not varied here.

Local validation passed: service build, Go race tests/vet and 45 Python tests
(eleven packing, four build-scan, thirty capacity). New harness tests cover the
3/6/6/3 order, fixed 16/2 runtime, rejected missing proofs/mutated inputs/wrong
budgets and required materialization/sort native profiles. Native profiling and
full retained builds remain pending on the VM; the local CLI is Linux-only and
Docker access is unavailable. Docs were updated. Nothing was committed or
pushed, and no existing artifact or remote data was deleted.

#### Linux retained-build memory screen, 3GB versus 6GB (2026-11-01)

The first attempt, `state/packing-memory-1793576507`, stopped in the 3GB
profile gate. A successful parquet `count` left an empty native profile, and
the gate rejected that file. The benchmark-only profiler now removes an empty
profile after a successful command and keeps it when the command fails. That
attempt was not resumed. No service flag or memory default changed.

The rerun passed in `state/packing-memory-1793576797`, `EXIT=0`. Order was
3GB, 6GB, 6GB, 3GB. All five gates and four trials exited 0 and were not
OOM-killed. Both inputs stayed unchanged:
expected `c91128eb5a35387bd50dfd4d316f2f918a7b0333e94604b0fc6df4d3c8161f6e`
and normalized `b486bf377fcfe9160df124ed62a7bd81799971283a698966080754f36171356e`.
Every trial built 15 files and 140,578,674 rows with threads 16, sort threads 2,
canonical reuse, fresh per-file proofs, and a reused whole-build proof. The
separate reference proof stayed fixed at 16 threads and 3GB, fresh, and outside
build time. DuckDB reported effective limits of 2.7 GiB and 5.5 GiB.

| Trial | Build | Materialize | Sort/copy | Pack | Per-file proofs | Whole-build proof | Peak sort spill | Peak sort buffer |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 3GB, first | 774.75 s | 65.93 s | 173.33 s | 135.21 s | 282.17 s | 114.03 s | 998 MB | 3,022 MB |
| 6GB, first | 752.64 s | 57.29 s | 189.85 s | 132.72 s | 278.53 s | 90.46 s | 897 MB | 5,587 MB |
| 6GB, second | 769.81 s | 63.44 s | 188.42 s | 139.59 s | 283.37 s | 90.98 s | 911 MB | 5,590 MB |
| 3GB, second | 749.02 s | 65.38 s | 153.79 s | 136.28 s | 286.03 s | 103.72 s | 1,024 MB | 3,023 MB |
| Median 3GB | 761.88 s | | | | | | | |
| Median 6GB | 761.23 s | | | | | | | |

The medians differ by 0.66 s. Six gigabytes does not win both orders: the
reversed 3GB trial is faster than both 6GB trials. The larger budget raises
peak sort buffers from 3.0 GB to 5.6 GB and cuts peak spill by about 0.10–0.13
GB. Every full window still spills. Sort/copy is slower at 6GB, by 173.33 s
to 189.85 s in the first pair and 153.79 s to 188.42 s in the reverse pair.
The whole-build proof is faster at 6GB in both orders, about 91 s versus
104–114 s, and that gain is offset inside the same build. Per-file proofs stay
about 279–286 s. Reference proofs, excluded from the build median, were
351.7–366.6 s.

Child RSS peaked at 3.41 GB on the 3GB arms and 6.18–6.22 GB on the 6GB arms.
Cgroup peaks were 7.74–7.78 GB and 9.61–9.73 GB. Go RSS stayed under 331 MB.
Sampled scratch peaked near 5.46 GB on every arm. No OOM event occurred.

**Do not raise the build memory budget.** Six gigabytes does not reduce total
build time, and it does not remove sort spill. Native build memory stays 3GB.
No lifecycle was started for this screen, no earlier artifact was deleted, and
nothing was committed or pushed.


##### Review: retain 3GB; target canonical projection CPU (2026-11-01)

Verified the successful rerun's manifest, frozen code/CLI hashes and all four
native trial reports. Five gates and four trials exited zero with no OOM. Both
input hashes are unchanged before/after and identical across arms. Each trial
has all fifteen per-file fresh fixed sixteen-thread exact proofs, one reused
whole-build proof and a fresh independent reference proof fixed at 16/3GB.
All 140,578,674 rows and fifteen files are accounted for, and native sort profile
coverage includes all fifteen windows. Effective memory differs as intended;
threads remain 16/2. This is a valid negative memory screen.

| Metric | 3GB median | 6GB median |
| --- | ---: | ---: |
| Complete measured build | 761.88 s | 761.23 s |
| Sort/copy | 163.56 s | 189.14 s |
| Whole-build exact proof | 108.88 s | 90.72 s |

The total median difference is **0.66 s / 0.09%**, much smaller than variation
between repetitions, and 6GB does not win both orders. Whole-build proof gains
are offset elsewhere, particularly sort/copy. More native memory raises process
and buffer peaks while every large sort still spills. **Keep the build and global
native memory budgets at 3GB.** Do not add a build-memory override or launch a
lifecycle run for 6GB based on this result. This only rejects the measured
3GB → 6GB proposal; it does not prove that memory can never matter on other
hardware or workloads. Eliminating spill alone is not the pipeline objective.

The first run remains failed and is not timing evidence. The user-supplied
benchmark fix is present: after a successful native statement, a newly created
empty profile placeholder can be removed when DuckDB emits no profile (for
example metadata-only count). Failed commands keep their placeholder for
diagnosis. Required materialization and every sort COPY profile are still
mandatory, so this does not accept a missing measured-operation profile. The
profiler remains disabled in service builds/configuration.

Existing raw profiles reveal a more promising target without another broad
screen. Inspected the fifteen captured canonical copies of packed files in
**each** of the four arms:

| Arm | Canonical copies | Projection share of recorded operator work | Copy wall sum | Peak native spill |
| --- | ---: | ---: | ---: | ---: |
| 3GB, first | 15 | 90.3% | 120.82 s | 0 |
| 6GB, first | 15 | 91.4% | 120.63 s | 0 |
| 6GB, second | 15 | 91.1% | 123.54 s | 0 |
| 3GB, second | 15 | 90.7% | 122.50 s | 0 |

These are **captured packed-file canonical COPY operations**, not 90% of build
wall time or pure OS CPU. The profiler captures only the final statement of a
multi-statement call; it does not provide the other side's canonical COPY here.
Operator timing describes work across threads and must not be added to wall
latency. The dominant projection computes nanosecond time, canonical complete
tags, VARIANT kind/value JSON and the hash partition key. Source scans and final
COPY work are much smaller in those observed operations, and none of these
canonical copies spills. Increasing their memory budget does not remove that
per-row projection work.

**Next recommendation:** isolate the canonical tag/value projection and test
whether canonical tag JSON can be computed once per distinct promoted-tag/meta
tuple and reused, instead of reconstructing/merging/sorting/serializing it for
every row on both sides of each per-file proof. Measure tuple cardinality first;
high-cardinality metadata or dictionary/join costs may negate reuse. Include
all dictionary construction and joins in the timed candidate. Preserve the
existing canonical output columns and exact tag JSON semantics, including nulls,
promoted-field overrides, metadata order, duplicates and row multiplicity.
Do not change warehouse schema or replace equality with hash-only checks.

Use a small native correctness gate and a retained canonicalization A/B at the
same 16 threads/3GB before touching production packing. Compare exact full
canonical outputs, elapsed/CPU/operator work and memory; if promising, then
integrate with a complete retained build retaining every per-file, whole-build
and independent reference proof. Avoid simultaneously tuning sort memory,
concurrency or file layout. A separate whole-proof-only memory experiment could
be considered later, but the current full-build screen does not establish that
policy and the larger per-file projection cost is the clearer next target.

This review updates documentation only. The accepted decoder, layout,
normalization 16/fan-in 16/one worker, build 16/sort 2, fixed fresh proofs and
canonical reuse remain unchanged. No existing artifacts/data were deleted, no
remote storage was contacted, and nothing was committed or pushed.

#### Removed rejected build-memory experiment code (2026-11-01)

Removed the active 3GB/6GB memory screen: `testing/packing/memory.py`, its
profile-summary/acceptance helpers and memory-specific tests, plus the memory
mode/order/aggregation branches in the shared retained packing runner. Removed
`SAGE_PACK_PROFILE_NATIVE`, the private per-command profiler field/setup and its
empty-placeholder cleanup from the native driver, and the associated native-gate
profiling branches. No rejected 6GB path or profiler hook remains in active
service/benchmark code. Existing generic memory configuration remains available
with its accepted 3GB default; no service setting was raised.

The retained packing diagnostic continues to run materialization, layout audit,
every per-file exact proof, canonical-reuse whole-build proof and independent
reference equality. Useful input before/after hashes, row admission, effective
settings and build CPU/resource reporting remain. Its reference proof inherits
that diagnostic's configured budget, as before the rejected memory screen; no
memory-mode special reference override remains. All accepted execution/layout/
proof policies are unchanged.

Updated current documentation to describe the rejected outcome without linking
to removed commands. Historical plan entries, saved results and frozen source
under `state/packing-memory-*` were left intact, including raw native profiles
that identify projection work as the next target. This cleanup did not delete
any saved data or interact with Pelican/NRDstor.

Local validation passed: service build, Go race tests/vet and 41 Python tests
(seven packing, four build-scan, thirty capacity). Active-code scans found no
remaining memory-mode selectors, memory helper imports or private profiling
hooks. Native lifecycle benchmarks were not rerun for this cleanup; the accepted
3GB build and decoder behavior remain unchanged. Nothing was committed or pushed.

#### Prepared per-file canonical tag dictionary screen (2026-11-01)

The next experiment applies canonical reuse to **verification SQL**, a different
stage from the accepted extraction cache. Typed expected/packed Parquet retains
promoted tag columns and `meta`; it does not retain extraction's original full
canonical `tags` JSON. Verification must reconstruct tags from each input's actual
columns. Passing extraction's tags through as packed-side proof data would skip
independent validation of that output and is not the proposed optimization.

Added a test-only candidate that constructs a distinct dictionary of all seven
promoted columns plus the complete metadata map, evaluates the existing
`IdentityTags()` expression on each dictionary entry, and joins back on every
tuple column with null-safe equality. Expected and packed inputs each build their
own dictionary. Canonical columns, tag JSON semantics, time/VARIANT serialization,
hash partitioning and complete bidirectional multiset comparison remain unchanged.
There is no production selector or change to service packing/canonicalization.

The offline runner is documented in
`archive-exporter/testing/canonicalization/README.md`. From the workspace root,
run on the VM:

```sh
python3 sage-data-rework/archive-exporter/testing/canonicalization/run.py \
  --build-dir sage-data-rework/archive-exporter/state/packing-memory-1793576797/trial-1-3GB/results
```

The runner freezes current Go source and the native CLI, builds a separate test
binary, and first runs a small native gate covering empty input, every promoted
tuple column, map ordering, null/empty maps, null values, promoted overrides,
escaped strings, repeated and unique tuples, nanosecond times and scalar VARIANT
kinds. Exact proof must also reject tag/time/kind mutations and lost duplicates.
Failure stops the larger screen.

The retained screen admits expected/all packed footer counts, then tests file
indices 0/7/14 on both sides: original expected ordinal windows and actual packed
files. The respective window sizes are 10,000,000 / 10,000,000 / 578,674 rows.
Each input runs baseline/dictionary/dictionary/baseline at **16 threads/3GB**,
for 24 sequential timed canonical COPY operations. Dictionary source scans,
distinct aggregation/materialization and null-safe joins are inside timing,
alongside unchanged time/value projection, hash routing and Parquet output.
Distinct tuple counts are recorded outside timing, but every measured COPY
rebuilds its own dictionary. Repeated processes do not share cached dictionaries.

Every output receives a fresh bounded 16-thread full canonical comparison against
that input through the original canonical SQL. These independent proofs are
outside COPY timing and include complete rows and duplicate multiplicity. No
whole-build proof is being replaced by this screen. Inputs are mounted read-only;
the measured expected/three packed files are hashed before/after. New output is
isolated, runtime networking is disabled, and existing artifacts remain intact.
No Influx, Pelican, routing, publication, API reads or deletion is involved.

`state/canonicalization-*/run.json` retains configuration, frozen code/CLI hashes,
container status, all trials and per-input median wall/CPU summaries.
`data/results/probe.json` retains exact proof evidence, cardinalities and unchanged
input hashes. Each COPY has its own raw DuckDB 2 grouped JSON profile covering
the full statement, including dictionary/join operators and buffer/spill metrics.
Operator work is across threads, not additive wall latency or pure OS CPU. OS
caches remain uncontrolled; reversed repeats are not cold-cache measurements.

Acceptance requires exact correctness everywhere and total COPY wall/CPU gains
after dictionary and join overhead on both expected and packed inputs, including
the tail. High tuple cardinality may negate reuse. A promising screen must then
pass a complete retained build with all per-file, whole-build canonical-reuse and
independent reference proofs before promotion. Sort/concurrency/layout/memory
policies are not being tuned simultaneously.

Local Go race tests/vet and the runner's isolation, evidence validation and
fail-fast tests pass. Native correctness/performance remain **pending on the VM**:
this workspace cannot access its Docker socket, and its DuckDB CLI is Linux-only.
No native speedup is claimed and no service policy changed. Nothing was deleted,
committed or pushed.

#### Linux canonical tag-dictionary screen (2026-11-02)

The first attempt, `state/canonicalization-1793584929`, stopped in the native
gate. This DuckDB cannot write the maximum uint64 into a VARIANT Parquet
column. The gate now compares that SQL fixture directly, where `to_json` can
represent the value. The retained screen still reads real Parquet. That
attempt was not resumed.

The rerun passed in `state/canonicalization-1793585126`, run id
`5636e29eccf742ad9a6227cb0e267ced`, `EXIT=0`. The gate and the probe exited 0
and were not OOM-killed. Threads were 16 and memory was 3GB. Expected and the
three packed files stayed unchanged. Every copy passed a fresh fixed
sixteen-thread canonical comparison against its own input. The retained
expected file contains only DOUBLE and VARCHAR variants, 140,474,618 and
104,056 values.

Distinct tag tuples are 15,677, 16,113, and 1,444 on the three windows. Each
dictionary trial was faster than both baseline trials on that same input,
including the tail and both expected and packed sides.

| Window | Side | Rows | Distinct tuples | Median baseline | Median dictionary | Wall reduction |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| File 0 | Expected | 10,000,000 | 15,677 | 8.836 s | 2.233 s | 74.7% |
| File 0 | Packed | 10,000,000 | 15,677 | 8.977 s | 1.303 s | 85.5% |
| File 7 | Expected | 10,000,000 | 16,113 | 8.769 s | 1.368 s | 84.4% |
| File 7 | Packed | 10,000,000 | 16,113 | 10.192 s | 1.892 s | 81.4% |
| File 14 | Expected | 578,674 | 1,444 | 1.225 s | 0.286 s | 76.6% |
| File 14 | Packed | 578,674 | 1,444 | 2.048 s | 0.247 s | 88.0% |

Child CPU follows the wall time. On the first expected window it falls from
120.6 s to 23.9 s. The dictionary scans the window twice, so scanned rows and
row groups double and engine bytes read rise from about 32 MB to about 105–128
MB on the ten-million-row windows. No trial spilled. The added operators are
the dictionary CTE, hash group, and hash join. Independent proof time stays
about 13–16 s on the large windows and about 1.6–2.4 s on the tail, and it is
outside the copy times above.

The copy saving survives dictionary and join overhead on both sides and on the
tail. That does not replace the per-file or whole-build proofs. A complete
retained build still has to measure those proofs before this candidate is
promoted. No production canonicalization changed, no earlier artifact was
deleted, and nothing was committed or pushed.

##### Review: advance tag dictionaries to complete retained-build validation

Audited `state/canonicalization-1793585126` against the current acceptance
validator and raw native profiles. The frozen Go-source and CLI hashes match the
manifest; both native containers exited zero without OOM. All 24 ordered trials
have fresh fixed sixteen-thread, non-reused complete canonical proofs. Recomputed
the four measured files' hashes from the retained input build and matched the
recorded before/after hashes. The source windows, packed sides, row counts and
dictionary cardinalities are complete and consistent. This is valid positive
screening evidence, not merely a faster projection with weaker validation.

| Sum across the six sampled input/window medians | Baseline | Dictionary | Reduction |
| --- | ---: | ---: | ---: |
| Canonical COPY wall time | 40.05 s | 7.33 s | 81.7% |
| Child process CPU | 500.40 CPU-s | 75.66 CPU-s | 84.9% |

These sums describe the six sampled canonicalization operations only. They are
not full-day build times, and their percentage must not be applied to the entire
archive-to-routed lifecycle. The first expected-side dictionary repeats vary
from 1.47 to 3.00 s, but both still beat both baseline repeats (8.47–9.21 s).
The same strict separation holds in every sampled group, including the tail.

The native operator tree confirms the intended mechanism. On the first expected
window, the dictionary tag projection emits **15,677 rows** after the distinct
aggregation, while the null-safe join emits all **10,000,000 rows**. Baseline
canonical projection performs about 108.24 operator-seconds of work in that
captured copy. The candidate's dictionary tag projection performs about 0.21,
the hash aggregation 1.63, the join 2.05, and the remaining canonical projection
1.98 operator-seconds. These timings measure work across threads, not elapsed
time; the measured COPY includes all these operators and both source scans.
The result supports repeated tag serialization as the bottleneck targeted here.

All 24 copies have zero native spill. Across ten-million-row copies, native peak
buffers are approximately 1.71–1.80 GB for baseline versus 1.39–1.56 GB for the
dictionary. The extra scan increases logical scan and engine-read counters, but
its cost is already included and does not erase the measured CPU/wall benefit.
That observation does not establish behavior for cold remote scans or unusually
high-cardinality metadata; this screen uses local retained build artifacts.

The gate adjustment is appropriate for this experiment: maximum uint64 remains
in the SQL comparison fixture without forcing DuckDB to write an unsupported
VARIANT Parquet value. That gate proves canonicalization equivalence for the
SQL value; it does not prove maximum-uint64 Parquet storage. The measured retained
warehouse contains DOUBLE/VARCHAR values as reported above. The failed first
attempt remains excluded from timings.

**Recommendation: advance to a complete retained-build comparison at fixed
16 build/proof threads, two sort threads and 3GB.** Compare baseline with the
dictionary in reversed repeats, including materialization, sorting, packing,
all fifteen per-file exact proofs and the canonical-reuse whole-build proof.
Keep a fresh independent reference proof explicitly on the original canonical
SQL, rather than changing both the implementation under test and its oracle.
Include dictionary construction/join overhead in actual build time, audit the
same fifteen files/layout/140,578,674 rows, and preserve source hashes and every
existing safety proof. Scope the first integration to build-local canonical
preparation; do not automatically enable it for remote/pre-delete verification.

The current screen justifies this next validation, not immediate promotion or
an uninterrupted lifecycle run. No production canonicalization or accepted
thread/memory/layout policy changed in this review. No existing data was deleted,
no remote storage was contacted, and nothing was committed or pushed.

#### Prepared complete retained-build tag dictionary comparison

Implemented `archive-exporter/testing/canonicalization/build.py` to follow the
positive 24-copy screen with actual full build costs. This is a separate runner
from the older thread/sort screen: both arms hold **16 build/proof threads, two
sort threads and 3GB** fixed. The default order is baseline → dictionary →
dictionary → baseline, with a separate process/container per build. Additional
repetitions reverse the order on even passes; at least two are required.

The candidate is connected through a private test-only projection hook consulted
only by `PackExpected` when preparing both canonical sides of a per-file proof.
Its SQL remains in `_test.go`. The production binary has no configuration or
environment selector to enable it, and the nil hook follows existing canonical
SQL. `Equal`, remote verification and pre-delete verification remain unchanged.
The native dictionary gate now checks that independent `Equal` ignores this
build hook. A small packing gate also requires a baseline reference proof.

Seven gates precede the full-day builds: dictionary correctness/independence,
bounded comparison, canonical reuse, and small packing/empty builds for both
arms. Each full build includes typed materialization, all fifteen file sorts and
packing operations, fsyncs, layout audits, fifteen fresh fixed sixteen-thread
full per-file proofs and a new whole-build proof from verified canonical shards.
Dictionary construction, scans and joins are inside measured build/proof time.
The accepted schema, file windows, plugin-pure capped groups and reuse ledger
are unchanged. Every build additionally receives a fresh sixteen-thread/3GB
reference comparison explicitly using **original canonical SQL** on the retained
typed reference versus new packed files, outside build timing.

Run from the workspace root on the VM:

```sh
python3 sage-data-rework/archive-exporter/testing/canonicalization/build.py \
  --expected sage-data-rework/archive-exporter/state/full-day-20250301-1793231701/coordinator/jobs/9982f31e-865e-49b9-82bc-138c397ba47e/expected.parquet \
  --normalized sage-data-rework/archive-exporter/state/full-day-20250301-1793231701/coordinator/jobs/9982f31e-865e-49b9-82bc-138c397ba47e/export-dedup.parquet
```

Both paths are retained inputs from the earlier packing screens. A new private
`state/packing-canonical-*` directory receives frozen source/CLI, logs, container
status and all build artifacts. Inputs are read-only and hashed before/after
each trial; hashes must match across arms. Runtime networking is disabled and
no runtime CPU/container memory cap is imposed by default. The runner checks
the requested arm, unchanged baseline oracle, effective native settings, total
140,578,674 rows, fifteen-file inventory, complete ordered steps, all per-file
proofs, reused whole-build proof, independent reference proof and layout audits.
Missing or failed evidence stops the run. Build-local canonical scratch must
have been reclaimed on return; older saved data is never removed.

Reports retain build wall/CPU, materialization timing, per-file/whole-build proof
substeps, layout audits and resource peaks. The outer manifest adds per-arm
median build/materialization/proof-stage totals and separate reference time.
Operator profiling is off, so this comparison measures the complete build with
normal proof settings. Native profiling from the prior isolated screen remains
the mechanism evidence. OS caches remain uncontrolled.

Local validation covers Go tests/race/vet and Python ordering, fixed-resource
mounts, source isolation, fail-fast gates, all four trial collection, and strict
rejection of missing proofs, wrong oracle, settings, counts, hashes and layout.
Native complete-build measurements are pending on the VM. No production default
has been promoted and no extraction, Pelican/NRDstor, publication, routing, API
or deletion operation is part of this experiment. Nothing was committed or pushed.

#### Linux complete retained-build tag-dictionary comparison (2026-11-02)

Passed in `state/packing-canonical-1793626534`, `EXIT=0`. Order was baseline,
dictionary, dictionary, baseline. All seven gates and four trials exited 0 and
were not OOM-killed. Both inputs stayed unchanged:
expected `c91128eb5a35387bd50dfd4d316f2f918a7b0333e94604b0fc6df4d3c8161f6e`
and normalized `b486bf377fcfe9160df124ed62a7bd81799971283a698966080754f36171356e`.
Every trial built 15 files and 140,578,674 rows at 16 threads, 2 sort threads,
and 3GB, with DuckDB reporting a 2.7 GiB limit. Canonical reuse stayed on.
Each build had fifteen fresh fixed per-file proofs, one reused whole-build
proof, and a fresh reference proof on the original canonical SQL outside build
time. The reference proof passed on every trial. Frozen code SHA
`b08d7c6e0b93420d1ab10bab193f2892eaa3fa4ae7755be5fcf6510f2ab8a4f1`. DuckDB SHA
`f4c681bcf41a5c43dcf9a354703fe5bded9e2265282d1915d6d99637cd1e1f29`.

| Trial | Build | Self CPU | Child CPU | Materialize | Sort/copy | Pack | Per-file proofs | Whole-build proof | Reference, excluded |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Baseline, first | 784.91 s | 274.63 s | 5,600.55 s | 58.06 s | 182.79 s | 133.49 s | 275.29 s | 131.31 s | 346.55 s |
| Dictionary, first | 579.12 s | 245.20 s | 3,114.93 s | 67.24 s | 158.76 s | 134.66 s | 95.87 s | 118.80 s | 399.83 s |
| Dictionary, second | 610.18 s | 247.21 s | 3,317.92 s | 90.26 s | 161.85 s | 137.92 s | 104.72 s | 111.35 s | 369.54 s |
| Baseline, second | 854.29 s | 302.01 s | 6,393.83 s | 68.76 s | 179.09 s | 142.38 s | 323.01 s | 137.09 s | 398.27 s |
| Median baseline | 819.60 s | 288.32 s | 5,997.19 s | 63.41 s | 180.94 s | 137.94 s | 299.15 s | 134.20 s | 372.41 s |
| Median dictionary | 594.65 s | 246.20 s | 3,216.42 s | 78.75 s | 160.30 s | 136.29 s | 100.29 s | 115.08 s | 384.69 s |

Median build wall falls from 819.60 s to 594.65 s, 224.95 s and 27.4%. The
slower dictionary trial, 610.18 s, is still faster than the faster baseline
trial, 784.91 s. The last trial is the slower baseline, so later-trial cache
warmth does not explain the gap. Median child CPU falls from 5,997.19 s to
3,216.42 s.

The reduction is concentrated in the fifteen fresh per-file proofs: median
299.15 s to 100.29 s. The whole-build proof also falls, 134.20 s to 115.08 s.
Pack stays about 136–138 s. Sort/copy is lower on the dictionary arm in both
orders, about 21–24 s. Materialization is higher on the dictionary arm in both
orders, 63.41 s to 78.75 s median, and that stage does not run the candidate.
The per-file proof reduction covers that increase. These build totals are the
measurement. The earlier six-window copy sum, 40.05 s to 7.33 s, remains
mechanism evidence and is not a fraction of this build.

The reference proof uses original canonical SQL on both arms. Its medians are
372.41 s baseline and 384.69 s dictionary, outside the build median. Adding
those reference times to the build medians still leaves the dictionary ahead
by about 213 s. That sum is not the acceptance metric.

Cgroup peaks were 7.60–7.77 GB. Child RSS peaked at 3.25–3.33 GB on baseline
and 3.37–3.39 GB on dictionary. Go RSS stayed under 322 MB. Sampled scratch
stayed near 5.46 GB. No OOM event occurred.

Production canonical SQL, remote verification, and pre-delete verification
remain unchanged. No lifecycle was started, no earlier artifact was deleted,
and nothing was committed or pushed.

##### Review: retain the dictionary and promote build-local preparation (2026-11-02)

Revalidated all four raw `benchmark.json` reports with the complete-build
acceptance validator. All seven gate containers and four trial containers exited
zero without OOM. Recomputed frozen source and CLI hashes and matched the
manifest; also recomputed both retained inputs' hashes and matched the recorded
before/after hashes across every arm. Effective DuckDB settings are identical:
sixteen threads and a reported 2.7 GiB native limit for the configured 3GB.

All four builds account for fifteen files and 140,578,674 rows, preserve two
sort threads and the accepted layout, and include all ordered packing steps.
The reports contain **60 fresh fixed sixteen-thread per-file proofs, four new
whole-build exact proofs reusing verified canonical shards, and four fresh
independent baseline reference proofs**. Every proof passed. Reference
canonicalization is explicitly baseline in both arms. Layout audits are complete
and build-local proof scratch was reclaimed on return. This is valid complete
retained-build acceptance evidence.

| Measured median | Baseline | Dictionary | Reduction |
| --- | ---: | ---: | ---: |
| Complete build | 819.60 s (13m40s) | 594.65 s (9m55s) | 224.95 s / 27.4% |
| Child CPU | 5,997.19 CPU-s | 3,216.42 CPU-s | 46.4% |
| Go/self CPU | 288.32 CPU-s | 246.20 CPU-s | 14.6% |
| Fifteen per-file proofs | 299.15 s | 100.29 s | 198.86 s / 66.5% |

The per-file proof saving is approximately 88% of the observed median build
saving and matches the mechanism established by the isolated native profiles.
Both dictionary builds (579.12–610.18 s) beat both baseline builds
(784.91–854.29 s), despite the dictionary's higher materialization times.
Only two repeats per arm and uncontrolled caches limit precision, but the
effect is substantially larger than the observed within-arm variation.

Do not attribute every stage difference to tag reuse. Materialization and sort
do not execute the candidate SQL; their differences reflect execution/cache
conditions or possible indirect effects. The whole-build proof consumes already
canonical shards and does not reconstruct tag JSON either. Its 19.12 s median
improvement is included in total build time but is not isolated evidence of a
separate dictionary optimization. The fresh baseline reference remains outside
build timing and is not a production step added by this candidate.

Resource behavior is acceptable in this screen: child RSS rises modestly to
3.37–3.39 GB from 3.25–3.33 GB, cgroup peaks remain 7.60–7.77 GB, scratch is
approximately 5.46 GB, and there are no OOM events. These process/cgroup figures
are not DuckDB buffer-limit measurements; native memory configuration stays
3GB. The measured workload has repeated tag tuples. High-cardinality days remain
an unmeasured performance boundary, not a correctness shortcut or a reason to
change the exact proof policy.

**Recommendation: retain this optimization and make the dictionary the default
for build-local per-file canonical preparation.** Keep original `Canonical`
SQL for independent reference proofs, remote verification and pre-delete
verification. Do not introduce a global replacement of canonicalization or
weaken any source/window binding, per-file, whole-build or reference proof.
When promoting, move the measured dictionary SQL out of `_test.go` into a
build-specific helper and remove the experimental build hook/selector from the
service path. Preserve the original SQL as the independent oracle and retain
saved benchmark artifacts.

After that integration, run one uninterrupted full-day lifecycle with the
accepted normalization/extraction/build policies through remote proof, routing,
API/client Arrow and NDJSON reads, fresh pre-delete reconciliation and guarded
hot deletion. This establishes the actual archive-to-routed effect and verifies
the promoted service path. Do not extrapolate the 27.4% build reduction to the
entire lifecycle or expand the dictionary to remote proofs without measurement.

This review updates analysis/documentation only; production remains unchanged
until that integration. No existing data was deleted, no Pelican/NRDstor path
was touched, and nothing was committed or pushed.

#### Promoted build-local tag dictionaries; prepared full-day lifecycle (2026-11-02)

Promoted the measured dictionary SQL into
`archive-exporter/internal/storage/build_canonical.go`. `PackExpected` now calls
the build-specific helper directly for both inputs of each per-file exact proof.
There is no alternate build projection, fallback, flag or selector. Removed the
private projection hook, `SAGE_PACK_CANONICALIZATION` test selector and obsolete
complete-build A/B runner/tests. Saved runs, frozen code and datasets remain
intact. The standalone read-only copy diagnostic remains available and now calls
the production build helper for its dictionary arm.

Checked the promoted helper against the accepted run's frozen source: its SQL
matches exactly after helper renaming. Original `Canonical` and `Equal` functions
are unchanged. Independent source/reference, remote and pre-delete proofs keep
the original SQL. Canonical columns, nanoseconds/VARIANT serialization, exact
multiset semantics, hash routing, source/output binding, per-file proofs and
canonical-reuse whole-build proof remain intact. Thread/memory/layout policy
stays normalization 16/fan-in 16/one merge worker, build/proofs 16, sort 2, fixed
partitioning, native memory 3GB and capped plugin-pure groups.

Actual packing profiles now record `canonicalization: tag_dictionary`. Native
small packing, empty and worker lifecycle gates assert this default. The retained
packing runner requires the promoted profile plus an independent baseline
reference, and runs the dictionary correctness gate before full trials.

Fresh live/capacity manifests record `build_canonicalization: tag_dictionary`.
Before deletion can be enabled in a full-day fixture, `routed` now checks the
actual packing profile: policy, row count, threads/memory/reuse, all fifteen
ordered per-file steps/proofs and the new reused whole-build exact proof. It also
requires completed materialization. Legacy/missing policy or proof evidence fails
before fixture deletion. Both build profiles are retained in `routed.json` and
`summary.json`, alongside existing operation timings and independent remote/read/
pre-delete evidence, even after normal confirmed-deletion staging cleanup.

Prepared a fresh never-started run, with all four frozen source hashes verified:

```text
archive-exporter/state/full-day-dictionary-20250301-49b3f0d9bd0a
```

From the workspace root, launch it on the Docker-capable host:

```sh
python3 sage-data-rework/archive-exporter/testing/capacity/run.py \
  --prepared sage-data-rework/archive-exporter/state/full-day-dictionary-20250301-49b3f0d9bd0a
```

This preparation pins the promoted code/policy for the audited March 1 source,
allocates a fresh managed destination prefix, and starts with deletion disabled.
The existing source warehouse is read-only; only the new private fixture's hot
data is eligible for the harness's guarded deletion after remote proof and API/
client checks. The run must complete uninterrupted through route, independent
full remote proof, Arrow/NDJSON/client reads, fresh pre-delete verification,
confirmed target-day removal and retained boundary sentinel, then post-delete
reads and drained-work checks. Return `run.json`, `reports/summary.json`,
`routed.json`, operation reports and both build profiles for lifecycle review.

Preparation performed no Docker startup, source-data scan or remote write.
Execution remains pending because this agent cannot access the local Docker
socket. No full-day/lifecycle speedup is claimed yet. Do not resume a prior
attempt or an older preparation; this directory is specifically frozen for the
promoted policy. If a run fails, retain it and prepare another new directory.

Local validation passed: Go service build, race tests/vet and 59 Python tests
(31 capacity, 12 live, seven packing, five canonicalization, four build-scan).
Active-code scans found no projection hook, build selector or retired A/B command.
No saved data/artifacts were deleted, no `/node-data` path was accessed, no remote
storage was changed, and nothing was committed or pushed.

#### Linux tag-dictionary full-day lifecycle (2026-11-02)

The prepared run passed in `state/full-day-dictionary-20250301-49b3f0d9bd0a`,
run id `0a0ea2a9077d4a2da436d1cca2bcb303`, execution started
2026-11-02T15:04:20Z, `EXIT=0`. `run.json` is `passed: true`, phase `complete`,
`services_stopped: true`. Policy is normalization 16, merge fan-in 16, merge
concurrency 1, fresh comparison 16, build 16, sort 2, canonical reuse, fixed
partitioning, and `build_canonicalization: tag_dictionary`. Prefix
`cold-storage-archive-live-0a0ea2a9077d4a2da436d1cca2bcb303` stays paired. Job
`47bbf516-fd5a-4697-a227-e4300358b60c` reached `COMPLETE` for 140,578,674 rows.
Seed took 5,750.85 s. The hot sentinel is `1740873600000000194`. Hot March 1 has
0 rows and the next-day sentinel remains. Pending deletions, jobs, readers,
writers, late work, and quarantine are 0. No OOM event and no resource-sampler
error are recorded. The archiver cgroup peak is 7,906,512,896 bytes, the verify
probe peak is exactly 4 GiB, Influx peaked at 3,130,585,088 bytes, and the API
at 333,942,784 bytes. Largest Go RSS was 405,852,160 bytes and largest child
RSS was 3,360,473,088 bytes.

Startup-inclusive archive-to-routed time is **3,466.72 s**. The accepted
cached-decoder full day `full-day-20250301-1793555812` was 3,457.26 s, so this
lifecycle is 9.46 s longer on that metric. The dictionary change is visible in
the build stage, and the other stages absorb it.

| Archival stage | This run | Cached-decoder full day | Change |
| --- | ---: | ---: | ---: |
| Export | 549.53 s | 498.22 s | +51.31 s |
| Reconciliation | 1,815.82 s | 1,644.17 s | +171.65 s |
| Build/materialization/packing | 612.68 s | 850.98 s | −238.30 s |
| Publish/commit | 76.68 s | 80.84 s | −4.16 s |
| Fresh remote verification | 404.58 s | 377.33 s | +27.25 s |
| Pre-delete verification plus delete request | 1,283.14 s | 1,215.18 s | +67.96 s |

Those six stages sum to 4,742.43 s versus 4,666.72 s on the prior run. The
238.30 s build reduction is 28.0% of that prior build stage. It is the measured
lifecycle change for tag dictionaries. It is not the offline 27.4% build-median
reduction applied to 3,457.26 s, and it does not shorten archive-to-routed.

Inside reconciliation, normalization was 711.52 s versus 616.84 s and the fresh
comparison was 624.78 s versus 532.57 s. Influx extraction was 479.51 s wall and
221.87 s self CPU, child CPU 0, versus 494.75 s and 224.20 s. Normalization
still reports threads 16, fan-in 16, concurrency 1, maximum fan-in 16, and 2
passes: partition 187.17 s, sort/collapse 215.51 s, merge 308.00 s.

The retained packing profile records `canonicalization: tag_dictionary`, 16
threads, sort 2, 3GB, canonical reuse, 15 files, and 140,578,674 rows.
Materialization passed in 87.82 s. Packing passed in 524.56 s, against 739.86 s
on the prior full day. Nested in that packing total: per-file proofs 96.17 s
versus 267.33 s, whole-build proof 115.66 s versus 138.83 s, batch sort/copy
174.16 s versus 192.97 s, and actual packing 134.58 s versus 136.22 s. All
fifteen per-file proofs are fresh, fixed, and sixteen-thread. The whole-build
proof reused the canonical shards. Data-file bytes are 457,600,012. Retained
canonical bytes are 1,911,144,969. Both profiles are in `routed.json` and
`summary.json`.

Commit verification was 404.58 s, including 397.53 s of fresh comparison.
Pre-delete verification was 423.51 s, including 413.90 s of fresh comparison.
The Influx delete request was 859.63 s. Both worker journals record full
equality for the same 15 objects, with the publish hint true on `COMMITTED` and
false on `DELETING`. The independent probe matched all 140,578,674 rows. API
readiness passed in 2.28 s. Before and after deletion, Arrow (1,240 bytes),
NDJSON (650 bytes), and the client each returned one row for
`uncorrected_inner`, `W099`, node `000048b02dd3c51f`. `deletion_reverified` is
true. Production deletion remains disabled outside this isolated run. Nothing
was committed or pushed.

##### Review: promoted policy passes uninterrupted full-day lifecycle (2026-11-02)

Audited the completed prepared run's manifest, all four frozen source hashes,
retained routed/summary build profiles, independent full-source comparison,
worker verification journals, both API reports, final hot counts/status and
resource diagnostics. Frozen source hashes match; the actual packing profile
passes the promoted-policy validator, not just the manifest's declaration.
`routed.json` and `summary.json` retain identical build profiles. All archival
states have one successful attempt and zero failed attempts. Services stopped
normally, resource sampling has no errors and no OOM events were recorded.

The complete **140,578,674-row, fifteen-file** day passed all fifteen fresh
fixed sixteen-thread per-file proofs and the new whole-build exact proof using
verified canonical shards. The independent source-to-published comparison
passed across 256 bounded partitions. COMMITTED and DELETING journals certify
the same fifteen object hashes, expected-input hash and metadata snapshot, with
full equality in both; only COMMITTED publishes the routing hint. These are
fresh remote proofs, not reuse of build-local comparisons.

Before and after confirmed fixture deletion, the bounded one-row sample passed
Arrow, NDJSON and default-Arrow Python-client checks. This verifies that sampled
read path, not every possible API query. The target hot day has zero rows, the
next-day sentinel remains, and pending deletion/jobs/readers/writers/late work/
quarantine are zero. The promoted build default is therefore validated through
the uninterrupted representative lifecycle, including its guarded deletion.
This is isolated-fixture evidence; production deletion settings did not change.

| Verified comparison with accepted cached-decoder full day | Previous | Dictionary lifecycle |
| --- | ---: | ---: |
| Startup-inclusive archive to routed | 3,457.26 s | 3,466.72 s |
| Build/materialization/packing stage | 850.98 s | 612.68 s |
| Fifteen per-file proofs within packing | 267.33 s | 96.17 s |

**Keep the dictionary as the build-local default.** Its 238.31 s / 28.0% build
improvement is consistent with the retained complete-build A/B, where both
dictionary repetitions beat both baseline repetitions. Per-file proof time
falls 64.0% in this lifecycle comparison. Keep the accepted 16 build/proof
threads, two sort threads, 3GB memory, canonical reuse and fixed partitioning.
Original canonical SQL remains the independent/remote/pre-delete representation.

There is **no measured overall routing-latency improvement in this run**:
archive-to-routed is 9.47 s / 0.27% longer. Before routing, slower export
(+51.31 s), reconciliation (+171.65 s) and fresh remote proof (+27.26 s) offset
the build saving, partly balanced by publish (-4.17 s). These are different
full-day runs under uncontrolled execution/cache conditions. Do not conclude
that the dictionary caused those unrelated stages to regress, or claim a
27–28% lifecycle improvement. The retained A/B is the controlled evidence for
the build optimization; this run adds service-path correctness and real stage
measurements.

The next performance target on the route path is **fresh reconciliation**, now
1,815.82 s: normalization 711.52 s, independent Influx extraction 479.51 s and
fresh exact comparison 624.78 s. Within that comparison, initial canonical
staging/routing is 432.38 s (69.2%), refinement 84.32 s and exact leaf comparison
105.78 s. These timings locate a promising next isolated investigation without
another thread/memory/layout change. Profile/benchmark those fresh canonical
inputs separately before extending dictionary-style work reduction beyond
build-local preparation. Each side must still be read independently and every
complete exact proof retained. Build shards cannot certify fresh Influx or
pre-delete content.

The 859.63 s Influx delete HTTP request is a separate large **post-routing**
cost. It affects completion, not archive-to-routed latency, and the client-side
request duration does not identify the server's internal bottleneck. Avoid
attributing it to exporter canonicalization or adding exporter threads to fix
it without server-side evidence. Remote verification/pre-delete optimizations
also remain separate measured candidates, not automatic consequences of this
successful build policy.

Updated active documentation to record this lifecycle acceptance. No new run,
service change, data deletion, remote access, commit or push was performed during
this review. Preserve the completed fixture, reports and paired remote prefix.

#### Removed obsolete dictionary experiment code (2026-11-02)

Kept the accepted dictionary as the sole build-local canonical preparation path.
Removed the remaining copy A/B runner `testing/canonicalization/run.py`, its
Python tests, and `TestNativeRetainedCanonicalization` with its
`SAGE_CANONICAL_*` settings, retained-input admission, benchmark orchestration and
profile collection. The earlier full-build A/B runner and build selector had
already been removed at promotion. No experiment selector or baseline build
fallback remains in active code.

Preserved the native dictionary correctness gate, moving it from
`canonicalization_probe_test.go` into `build_canonical_test.go`. It still checks
the promoted production helper against original canonical SQL, including
duplicates, nulls, metadata ordering/overrides, promoted keys, nanoseconds,
scalar kinds and rejection of mutated records or lost multiplicity. The
retained packing runner continues to invoke it before its trials.

Original `Canonical`/`Equal` are essential live code for fresh reconciliation,
independent reference, remote and pre-delete verification; they were not an
obsolete build fallback and remain unchanged. Confirmed the dictionary helper,
packing path, DuckDB driver, comparison/reuse code and worker match the accepted
lifecycle's frozen production sources byte-for-byte. This cleanup changes no
service SQL, execution/layout policy or exact proof.

Updated active documentation to remove retired commands and describe the
accepted default and retained regression coverage. Historical plan entries and
all saved `state` inputs, outputs, profiles, frozen source/CLI, completed fixture
and paired remote prefix remain intact. No data/artifact cleanup was performed.

Local Go race tests/vet and 54 Python tests passed (31 capacity, 12 live, seven
packing, four build-scan). Active-code scans found no retired probe, command or
selector references. No new native lifecycle was needed or run for this
test/tooling cleanup; the completed VM lifecycle remains the acceptance evidence.
No remote storage was accessed and nothing was committed or pushed.

#### Prepared isolated fresh reconciliation staging benchmark (2026-11-02)

Traced the remaining 432.38 s initial staging cost to `Worker.Reconcile`'s fresh
`Equal` call on two primitive files: normalized engine export and independently
queried Influx data. Each input stores full tag JSON plus scalar kind/value
text. Existing SQL parses tags into a map, splits promoted columns/metadata,
then canonicalization merges, sorts and serializes the tags per row. The
accepted build dictionary operates on typed warehouse columns and does not
remove this earlier primitive-input expansion.

Added a **test-only raw-tag dictionary** candidate. For each source independently,
it deduplicates the original tag JSON string before parsing it, applies the same
promoted-tag/metadata rules and `IdentityTags()` expression once per dictionary
entry, then null-safely joins canonical tags back to all rows. It does not trust
the original string as already canonical. Different ordering/whitespace can
have separate dictionary entries with identical canonical output. Null promoted
keys, non-promoted metadata values and duplicate row multiplicity preserve the
existing semantics. Time follows the original nanosecond conversion and scalar
kind/value conversion matches `Typed`/`Canonical`; invalid inputs still fail.

Candidate SQL and its selector exist only in Go test files. Production `Equal`,
`Canonical`, `Typed`, worker reconciliation, the accepted build helper and all
remote/pre-delete code are unchanged. No build-local shards or prior verified
results are reused to certify fresh inputs.

The new `archive-exporter/testing/reconciliation_staging/run.py` freezes source
and CLI, builds a separate Linux test binary, then runs bounded comparison,
typed fidelity and new candidate correctness gates before four full comparisons:
baseline → raw dictionary → raw dictionary → baseline, fixed **16 threads/3GB**.
The candidate gate writes real primitive Parquet with all scalar kinds including
maximum uint64 text, null values/maps/promoted keys, reordered and escaped JSON,
unique tags, duplicate records, empty input and nanosecond instants. It compares
against original SQL and rejects canonical field mutations, lost duplicates,
invalid scalar values/kinds and malformed tag JSON.

Run from the workspace root on the Docker-capable VM host:

```sh
reconciliation_input=sage-data-rework/archive-exporter/state/full-day-20250301-1793231701/coordinator/jobs/9982f31e-865e-49b9-82bc-138c397ba47e

python3 sage-data-rework/archive-exporter/testing/reconciliation_staging/run.py \
  --export "$reconciliation_input/export-dedup.parquet" \
  --influx "$reconciliation_input/influx.parquet"
```

These retained inputs are distinct normalized engine-export and independently
queried Influx files for the same audited 140,578,674-row day. They are not build
outputs. Both full row counts are admitted; source hashes are checked before/
after each trial and across arms. Raw tag cardinality is diagnostic outside
timing, but every actual query rebuilds its dictionary. The measured interval
includes all candidate scans, dictionary construction, joins, count queries,
initial canonical routing, bounded refinement and full exact leaf comparisons.

After each candidate trial, both full inputs separately receive a fresh exact
comparison between candidate and original canonical SQL. These two independent
baseline-oracle proofs are outside measured candidate wall/CPU and can add
substantial runtime. A shared candidate bug cannot pass simply because its two
sides agree. Gate/proof failure rejects the screen. The baseline trial continues
to exercise the actual original `Equal` path.

Results use a new `state/reconciliation-staging-*` directory. The manifest keeps
frozen source/CLI hashes, order, container status and median wall/CPU plus
count/initial-routing/refinement/exact timings. Per-trial `benchmark.json`
retains fresh comparison details, unchanged source hashes/cardinalities, resource
peaks and candidate input-oracle evidence. Resource peaks are sampled through
the main comparison; lifetime process/cgroup peaks can include admission work.
No operator profiler or new concurrent workers are enabled. OS caches remain
uncontrolled and repeated scans warm them; this is not a cold-cache benchmark.

Runtime networking is disabled and inputs/executables are read-only. No live
Influx read, normalization, Pelican/NRDstor access, publication, routing, API,
deletion or saved-data cleanup is performed. Keep all existing saved artifacts.
Acceptance requires reduced **total fresh comparison wall/CPU** after dictionary
overhead, all four full source comparisons and both original-SQL input oracles
on each candidate trial. High-cardinality tags may negate the benefit. A positive
result would justify separately integrating/validating reconciliation, not
automatically changing fresh remote or pre-delete proofs.

Local Go race tests/vet and ten Python tests passed (six existing comparison,
four new staging runner tests), covering source isolation, fixed settings,
reversed order, fail-fast gates, fresh proof/hash/oracle validation and all four
trial collection. Native correctness/performance are pending on the VM because
the local Docker socket is inaccessible. No speedup or service policy change is
claimed. Nothing was committed, pushed or deleted from saved data.

#### Linux fresh reconciliation raw-tag dictionary screen (2026-11-02)

Passed in `state/reconciliation-staging-1793646657`, `EXIT=0`. Order was
baseline, raw dictionary, raw dictionary, baseline. All three gates and four
trials exited 0 and were not OOM-killed. Both inputs stayed unchanged:
export `b486bf377fcfe9160df124ed62a7bd81799971283a698966080754f36171356e`
and influx `01a4f33382e0b9d56410de4a88c9ade33e3bb3da54a00ccdbaef82566f8f44c9`.
Every trial compared 140,578,674 rows at 16 threads and 3GB, with a fresh fixed
256-leaf proof. Each input has 206,907 distinct raw tag strings. Frozen code SHA
`9664a59decdcea5826139e5114ecb79e447afdd6d88db655195eb5d20669df23`. DuckDB SHA
`f4c681bcf41a5c43dcf9a354703fe5bded9e2265282d1915d6d99637cd1e1f29`.

| Trial | Comparison | Self CPU | Child CPU | Count | Initial routing | Refinement | Exact leaves |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Baseline, first | 485.65 s | 41.24 s | 6,527.64 s | 0.17 s | 340.24 s | 58.13 s | 85.40 s |
| Raw dictionary, first | 196.79 s | 31.86 s | 2,041.42 s | 7.62 s | 52.43 s | 66.04 s | 69.25 s |
| Raw dictionary, second | 144.88 s | 22.61 s | 1,549.97 s | 6.80 s | 33.58 s | 44.26 s | 58.80 s |
| Baseline, second | 475.18 s | 38.06 s | 6,467.12 s | 0.17 s | 335.00 s | 63.39 s | 74.67 s |
| Median baseline | 480.42 s | 39.65 s | 6,497.38 s | 0.17 s | 337.62 s | 60.76 s | 80.03 s |
| Median raw dictionary | 170.83 s | 27.24 s | 1,795.69 s | 7.21 s | 43.00 s | 55.15 s | 64.03 s |

Median comparison wall falls from 480.42 s to 170.83 s, 309.58 s and 64.4%.
Median child CPU falls from 6,497.38 s to 1,795.69 s. The slower dictionary
trial, 196.79 s, is still faster than the faster baseline trial, 475.18 s. The
last trial is the second baseline, so later-trial cache warmth does not explain
the gap. The drop is in initial routing, 337.62 s to 43.00 s. Count rises from
0.17 s to 7.21 s because each comparison rebuilds the dictionary. Refinement and
exact leaves also fall, by about 6 s and 16 s at the median.

Each dictionary trial then compared both full inputs, separately, against
original canonical SQL. Those proofs passed and are outside the times above:
export 316.95 s and 271.16 s, influx 295.61 s and 297.79 s. The Go test durations
of 820.76 s and 722.93 s include those oracles. They are not the comparison
result.

Cgroup peaks were 7.74–7.80 GB. Child RSS peaked at 2.71–2.93 GB. Go RSS stayed
under 85 MB. Sampled scratch stayed near 4.51 GB. No OOM event occurred.

This is the retained-file measurement of the fresh reconciliation comparison.
The 432.38 s initial-routing figure from the tag-dictionary full day is a
different execution of the same stage, and this 64.4% does not apply to that
run's 1,815.82 s reconciliation or to archive-to-routed. Production `Equal`,
remote verification, and pre-delete verification remain unchanged. No lifecycle
was started, no earlier artifact was deleted, and nothing was committed or
pushed.

##### Review: advance the raw-tag dictionary to reconciliation integration (2026-11-02)

Revalidated all four raw trial reports with the staging acceptance validator.
All three native gates actually executed and passed (no skips); all four trial
containers exited zero without OOM. Frozen Go-source and CLI hashes match the
manifest. Recomputed both retained input hashes and matched the recorded
before/after values across every arm. Each fresh comparison covers 140,578,674
rows at sixteen threads/3GB, with 256 exact leaves and no initial-stage reuse.
Both inputs consistently have 206,907 distinct raw tag strings.

The two dictionary trials each additionally passed two full independent
original-SQL input proofs, one for normalized export and one for queried Influx.
All four extra proofs are fresh fixed sixteen-thread, 256-leaf exact comparisons.
Their runtime is correctly outside measured candidate wall/CPU. This is valid
positive evidence, not a comparison where a shared projection error cancels out.

| Verified median | Original SQL | Raw-tag dictionary | Reduction |
| --- | ---: | ---: | ---: |
| Complete fresh comparison | 480.42 s | 170.83 s | 309.58 s / 64.4% |
| Initial canonical staging/routing | 337.62 s | 43.00 s | 294.61 s / 87.3% |
| Child CPU | 6,497.38 CPU-s | 1,795.69 CPU-s | 72.4% |

Initial-stage savings account for about 95% of the total median comparison
saving, consistent with reducing repeated tag parsing/expansion/serialization.
The dictionary has about 0.147% as many entries as input rows, roughly 679
observations per raw tag string. Count time rises from 0.17s to 7.21s, already
included in total comparison cost. Smaller refinement/exact-leaf improvements
must not all be attributed to parsing; these phases consume canonical shards
and their timings can depend on ordering/cache/execution conditions.

Both candidate repetitions (144.88–196.79s) beat both baseline repetitions
(475.18–485.65s). There is material candidate variation, but much less than the
observed gain. Reversed ordering strengthens the evidence without eliminating
uncontrolled caches; hash/cardinality/oracle scans warm caches. Resource peaks
have the documented admission/main-comparison scope, not pure DuckDB buffer or
oracle usage. No OOM occurred. High-cardinality days remain an unmeasured
performance boundary.

**Recommendation: retain and integrate the measured candidate specifically for
fresh primitive-file reconciliation.** Move its SQL into a reconciliation helper
used only by `Worker.Reconcile` on normalized export and independent query files.
Keep `Equal`/original `Canonical` for independent reference, remote and pre-delete
proofs, and keep the accepted typed build dictionary separate. Preserve fresh
reads, scalar validation, nanoseconds, canonical columns, counts, fixed routing/
refinement and every complete exact proof. Build-local shards and older proofs
cannot certify fresh hot contents. Remove the test-only selector at promotion
rather than introducing a production mode or fallback.

Then validate the actual worker path in a fresh uninterrupted full-day lifecycle
through remote proof/routing, bounded Arrow/NDJSON/client reads and guarded
deletion. Measure reconciliation and startup-inclusive archive-to-routed again.
The retained 309.58s comparison saving is not an already measured lifecycle gain;
its percentage does not apply to normalization, extraction or all reconciliation.

Updated active experiment notes. No production code/policy changed in this
review, no new run started, no saved artifact was deleted and no remote storage
was accessed. Nothing was committed or pushed.

#### Promoted primitive reconciliation; prepared lifecycle validation (2026-11-02)

Moved the measured raw-tag dictionary SQL unchanged into
`archive-exporter/internal/storage/reconciliation.go`. Confirmed its SQL helper
matches the accepted screen's frozen function exactly. `Worker.Reconcile` now
calls `DuckDB.EqualReconciliation` on the normalized engine-export and fresh
independent Influx-query files. This is the sole reconciliation path: no
selector, service flag, projection mode or original-SQL fallback remains.

The method requires distinct regular local Parquet files, rejects aliases/hard
links, symlinks and remote inputs, and uses the existing fresh `EqualCanonical`
machinery. Both inputs, dictionaries and canonical staging are freshly read/built
for every proof. Time/kind/value/tag semantics, counts, fixed routing/refinement,
bounded full bidirectional `EXCEPT ALL` and duplicate multiplicity remain
unchanged. It never uses verified build shards or an older proof as evidence of
fresh hot contents. The worker still fixes fresh proofs at sixteen threads.

Original `Canonical` SQL and `Equal` remain the independent reference/remote/
pre-delete proof path; the only `Equal` change records its baseline projection
in the comparison profile. The accepted typed build dictionary is separate and
unchanged. Base normalization 16/fan-in 16/one merge worker, build/proofs 16,
sort 2, native memory 3GB, mandatory build canonical reuse, fixed partitioning,
warehouse schema and layout policies remain intact.

Comparison profiles now explicitly identify `canonicalization:
raw_tag_dictionary` for primitive reconciliation and `baseline` for typed
independent/remote/pre-delete `Equal` calls. Fresh manifests record
`reconciliation_canonicalization: raw_tag_dictionary`. Full-day acceptance checks
actual successful operation profiles for the correct projection, freshness,
fixed mode and sixteen threads before fixture deletion can be enabled. Missing,
legacy or swapped projection evidence fails acceptance. Native worker lifecycle
tests also assert the actual reconciliation/remote policy boundary.

Kept the native parity/mutation/malformed-input gate and added coverage of the
production fresh-comparison method on real primitive Parquet, plus local-input
admission tests. Typed reconciliation gates use the production method and reject
nanosecond and signed/unsigned changes. The retained thread-comparison diagnostic
now exercises the promoted method in every arm and requires its actual profile.
Removed the accepted experiment's A/B runner/Python tests, test-only selector,
raw-cardinality diagnostic and per-trial extra-oracle orchestration. The original
SQL is retained as independent proof and regression oracle. Saved screen inputs,
reports, frozen source/CLI and completed fixtures remain intact.

Prepared a fresh never-started March 1 lifecycle, verifying all four frozen
source-tree hashes and the promoted worker/helper and harness policy checks:

```text
archive-exporter/state/full-day-reconciliation-20250301-effa67e8e83e
```

From the workspace root on the Docker-capable VM host:

```sh
python3 sage-data-rework/archive-exporter/testing/capacity/run.py \
  --prepared sage-data-rework/archive-exporter/state/full-day-reconciliation-20250301-effa67e8e83e
```

This freezes the integrated policy against the audited 140,578,674-row source,
allocates a fresh isolated destination and starts with fixture deletion disabled.
The uninterrupted run must pass normalization/source reconciliation, the accepted
fifteen-file build policy, publication, fresh baseline remote proof, routing,
independent full source-to-published equality, bounded Arrow/NDJSON/client reads,
fresh baseline pre-delete proof, confirmed hot-day removal with retained sentinel,
post-delete reads and drained-work/resource checks. Return `run.json`,
`reports/summary.json`, `routed.json`, operation reports and build profiles.
Measure actual reconciliation and archive-to-routed rather than extrapolating
the retained comparison percentage.

Preparation started no Docker service, source-data scan or remote operation.
Native integrated lifecycle execution is pending on the Docker-capable host;
this agent cannot access the local Docker socket. Use this new preparation,
not a completed/failed older fixture. Preserve a failed run and prepare a new
directory rather than resuming it.

Local Go build, race tests/vet and 61 Python tests passed (32 capacity, 12 live,
six comparison, seven packing, four build-scan). No saved data or artifacts were
deleted, no `/node-data` path was accessed, no remote storage was changed, and
nothing was committed or pushed.

#### Linux raw-tag reconciliation full-day lifecycle (2026-11-02)

The prepared run passed in `state/full-day-reconciliation-20250301-effa67e8e83e`,
run id `91e04283fde0423c9d192b2f1b47d86b`, execution started
2026-11-02T20:08:35Z, `EXIT=0`. `run.json` is `passed: true`, phase `complete`,
`services_stopped: true`. Policy is normalization 16, merge fan-in 16, merge
concurrency 1, fresh comparison 16, build 16, sort 2, canonical reuse, fixed
partitioning, `build_canonicalization: tag_dictionary`, and
`reconciliation_canonicalization: raw_tag_dictionary`. Prefix
`cold-storage-archive-live-91e04283fde0423c9d192b2f1b47d86b` stays paired. Job
`07b91fe8-27ac-43b4-87e1-e463e7c4934b` reached `COMPLETE` for 140,578,674 rows.
Seed took 5,570.93 s. The hot sentinel is `1740873600000000194`. Hot March 1 has
0 rows and the next-day sentinel remains. Pending deletions, jobs, readers,
writers, late work, and quarantine are 0. No OOM event and no resource-sampler
error are recorded. The archiver cgroup peak is 6,454,915,072 bytes, the verify
probe peak is exactly 4 GiB, Influx peaked at 3,147,546,624 bytes, and the API
at 329,322,496 bytes. Largest Go RSS was 418,275,328 bytes and largest child
RSS was 3,373,522,944 bytes.

Startup-inclusive archive-to-routed time is **3,011.40 s**. The tag-dictionary
full day `full-day-dictionary-20250301-49b3f0d9bd0a` was 3,466.72 s, so this
lifecycle is 455.32 s shorter on that metric.

| Archival stage | This run | Tag-dictionary full day | Change |
| --- | ---: | ---: | ---: |
| Export | 524.77 s | 549.53 s | −24.76 s |
| Reconciliation | 1,311.26 s | 1,815.82 s | −504.56 s |
| Build/materialization/packing | 708.12 s | 612.68 s | +95.44 s |
| Publish/commit | 74.53 s | 76.68 s | −2.15 s |
| Fresh remote verification | 384.26 s | 404.58 s | −20.32 s |
| Pre-delete verification plus delete request | 1,191.73 s | 1,283.14 s | −91.41 s |

Those six stages sum to 4,194.67 s versus 4,742.43 s. The reconciliation stage
accounts for the archive-to-routed reduction. Inside it, normalization was
653.78 s versus 711.52 s, Influx extraction was 475.56 s wall and 222.35 s self
CPU with child CPU 0 versus 479.51 s and 221.87 s, and the fresh comparison was
**181.91 s versus 624.78 s**. That comparison is the promoted path: its profile
records `canonicalization: raw_tag_dictionary`, fresh, fixed, and 16 threads.
Initial routing is 39.81 s versus 432.38 s. Count is 8.63 s, refinement 54.68 s,
and exact leaves 77.22 s. Child CPU for the comparison is 1,934.20 s versus
8,306.71 s. Normalization still reports threads 16, fan-in 16, concurrency 1,
maximum fan-in 16, and 2 passes: partition 146.00 s, sort/collapse 222.90 s,
merge 283.94 s. The 57.74 s normalization change is separate from the dictionary.

The retained-screen median, 480.42 s to 170.83 s, is a different execution of
this comparison. This live comparison is 181.91 s. The 64.4% screen figure is
not applied to 624.78 s or to 3,466.72 s.

The build stayed on the accepted tag dictionary: 15 files, 140,578,674 rows,
16 threads, sort 2, 3GB, canonical reuse, and `canonicalization: tag_dictionary`.
Materialization passed in 82.15 s. Packing passed in 625.64 s, versus 524.56 s
on the prior full day. Nested in that packing total: per-file proofs 126.92 s
versus 96.17 s, whole-build proof 126.13 s versus 115.66 s, batch sort/copy
221.60 s versus 174.16 s, and actual packing 146.63 s versus 134.58 s. All
fifteen per-file proofs are fresh, fixed, and sixteen-thread. The whole-build
proof reused the canonical shards. Data-file bytes are 457,580,635. Retained
canonical bytes are 1,909,884,300. The longer build is why archive-to-routed
fell by 455.32 s while the reconciliation stage fell by 504.56 s.

Commit verification was 384.26 s, including 374.76 s of fresh comparison, and
records `canonicalization: baseline`. Pre-delete verification was 367.83 s,
including 359.89 s of fresh comparison, also baseline. The Influx delete request
was 823.89 s. Both worker journals record full equality for the same 15 objects,
with the publish hint true on `COMMITTED` and false on `DELETING`. The
independent probe matched all 140,578,674 rows. API readiness passed in 2.56 s.
Before and after deletion, Arrow (1,240 bytes), NDJSON (650 bytes), and the
client each returned one row for `uncorrected_inner`, `W099`, node
`000048b02dd3c51f`. `deletion_reverified` is true. Production deletion remains
disabled outside this isolated run. Nothing was committed or pushed.

##### Review: integrated raw-tag reconciliation passes lifecycle acceptance (2026-11-02)

Audited `state/full-day-reconciliation-20250301-effa67e8e83e` against the current
build and operation-policy validators. All four frozen source-tree hashes match.
The actual reconciliation comparison records `raw_tag_dictionary`; COMMITTED
and DELETING comparisons record `baseline`. All are fresh fixed sixteen-thread
exact proofs with no initial-stage reuse. Actual build profiles preserve the
accepted typed tag dictionary, sixteen build/proof threads, two sort threads,
3GB, fifteen files, all per-file exact proofs and reused whole-build proof.
Routed and summary build profiles match. Every archival state has one successful
attempt and zero failures; services stopped normally with no resource-sampling
errors or OOM events.

The independent source-to-published proof covers **all 140,578,674 rows** across
256 bounded exact partitions. COMMITTED and DELETING journals certify identical
fifteen-object hashes, expected-input hash and metadata snapshot with full
equality, and only COMMITTED publishes the hint. Both pre-/post-deletion API
reports pass the bounded one-row sample through Arrow, NDJSON and default-Arrow
Python client. That sample validates the observed read path, not every possible
API query. The job is COMPLETE, the hot day has zero rows, its next-day sentinel
remains, and pending deletion/jobs/readers/writers/late work/quarantine are zero.

| Verified measured change versus accepted build-dictionary lifecycle | Previous | Integrated reconciliation | Reduction |
| --- | ---: | ---: | ---: |
| Startup-inclusive archive to routed | 3,466.72 s (57m47s) | 3,011.40 s (50m11s) | 455.32 s / 13.1% |
| Whole reconciliation stage | 1,815.82 s | 1,311.26 s | 504.56 s / 27.8% |
| Fresh reconciliation comparison | 624.78 s | 181.91 s | 442.88 s / 70.9% |
| Initial canonical staging/routing | 432.38 s | 39.81 s | 392.57 s / 90.8% |

**Keep both dictionary defaults:** raw-tag dictionaries only for primitive
reconciliation, and promoted-tag/meta dictionaries only for build-local per-file
preparation. The integrated comparison's roughly three-minute runtime is
consistent with the retained A/B's 170.83s candidate median. The observed
end-to-end routing gain is now measured in the full worker lifecycle; it is no
longer an extrapolation of the retained screen's percentage.

This remains one new full-day execution compared with a prior execution under
uncontrolled conditions, not a repeated controlled end-to-end A/B. Do not promise
a fixed 13.1% deployment speedup or attribute every stage difference to the SQL
change. The comparison saving itself is supported by the earlier reversed
retained A/B and its independent input proofs. Normalization (-57.74s), extraction
(-3.95s), export (-24.76s) and remote verification (-20.32s) also varied, while
build became 95.45s slower despite its unchanged accepted policy. Those effects
explain why the total routing change differs from the comparison-only saving.
The Influx delete request remains post-routing and does not explain route timing.

The promoted defaults have now passed representative uninterrupted full-day
acceptance through routing, independent remote proof, sampled reads, fresh
pre-delete proof and guarded deletion. Preserve original `Canonical`/`Equal` for
independent typed, remote and pre-delete proofs. Keep normalization/thread/memory/
layout/reuse policies unchanged; there is no basis here to remove a proof or
expand the dictionary to remote scans automatically. Fixture deletion acceptance
does not change production deletion configuration.

Updated active documentation with this result. This review starts no run,
changes no service code or policy, deletes no saved artifact, and accesses no
remote storage. Keep the completed fixture and its paired remote prefix intact.
Nothing was committed or pushed.

#### Final dictionary cleanup audit (2026-11-02)

Confirmed both accepted defaults remain the only service paths: typed tag/meta
dictionary preparation in `PackExpected`, and primitive raw-tag dictionary
comparison in `Worker.Reconcile`. Their older branches, projection hooks,
environment selectors, copy/full-build/reconciliation A/B runners and retained
copy probe were already removed during promotion and subsequent cleanup. Active
source/docs scans found no remaining retired selector, probe or command reference.
No additional obsolete dictionary code remains to delete.

All 24 production Go files (`main.go` and non-test `internal` sources) match the
successful integrated lifecycle's frozen sources byte-for-byte. Remaining
original `Canonical`/`Equal`, typed materialization helpers and native oracle
fixtures have live verification/materialization/regression callers; they are
not alternate dictionary defaults and must remain. No service code changed in
this audit, so no redundant native run was launched.

The completed lifecycle remains the acceptance evidence. Saved data, reports,
frozen runs and paired remote prefixes remain intact. No remote storage was
accessed and nothing was committed or pushed.

#### Prepared read-only remote canonical staging screen (2026-11-02)

The next target is fresh typed/remote verification, which still uses original
per-row tag reconstruction: latest COMMITTED comparison 374.76s with 252.94s
initial staging, and DELETING comparison 359.89s with 264.19s initial staging.
This investigation changes no production default or proof. It tests whether the
already accepted typed tag/meta dictionary can reduce either side of those
fresh comparisons without losing its gain to extra remote scan work.

Added `archive-exporter/testing/remote_staging/run.py` with three test-only arms:
original SQL on both inputs; dictionary on local expected only; and dictionaries
on local expected plus actual pinned Iceberg scan. All use **16 threads/3GB**,
same snapshot, fresh complete bounded exact proofs and unchanged routing/
refinement/writer policy. Default order is baseline → local → both → both →
local → baseline. Native thread/memory/I/O settings must match across arms.

Run from the workspace root on the Docker-capable VM host:

```sh
remote_input=sage-data-rework/archive-exporter/state/full-day-20250301-1793231701/coordinator/jobs/9982f31e-865e-49b9-82bc-138c397ba47e

python3 sage-data-rework/archive-exporter/testing/remote_staging/run.py \
  --expected "$remote_input/expected.parquet" \
  --verification "$remote_input/verification-COMMITTED.json"
```

This older isolated expected/journal pair is genuinely retained. Recomputed its
local expected SHA and matched the journal; its inventory has fifteen published
objects for the audited 140,578,674-row day. No remote request was made in local
preparation/validation. Do not substitute a reconstructed expected file for a
deleted lifecycle artifact and claim it has the journal's original SHA.

The runner reuses existing namespace/journal admission and guarded read-only
snapshot pinning. Three network-disabled native gates precede pinning: bounded
exact comparison, typed dictionary parity against original canonical SQL, and
all three projection arms on equal/empty/duplicate/mutated data. Pinning reads
the exact immutable v3 metadata/manifest tree and checks every data object hash;
unexpected files, delete manifests or namespace escapes fail. Each trial checks
that pin and all object hashes before comparison and after extra input oracles,
and rechecks expected-file hash/count. No head hint or mutable catalog selection
is used. Only the pin/trial runtime containers get outbound network.

Timed wall/CPU includes all setup, counts, dictionary construction, joins,
additional source scans, canonical COPY, bounded refinement and exact leaves.
The remote dictionary references its Iceberg source twice per canonical query;
all additional scan costs are included, without assuming twice the physical
network I/O. Column pruning/caches/optimizer behavior affect actual traffic.
Baseline→local isolates the local-side change; local→both adds remote-side work.
Fresh remote scans are never replaced with a local dataset or verified build
shards, and no previous proof result is reused.

Outside timing, every optimized full input receives a fresh original-SQL versus
dictionary proof. Local-only adds one complete local expected proof; both adds
local and actual remote-scan proofs. Across default repetitions this adds six
independent input proofs, guarding against a shared projection bug and adding
substantial overall runtime. Before/after remote integrity checks encompass
these extra reads too. Process/cgroup peaks are captured through the measured
comparison and can include admission, while later oracles are excluded from the
recorded resource scope. No operator profiler is enabled.

New private results go under `state/remote-staging-*`. The manifest retains
frozen source/CLI hashes, target/pin, order, container status and median wall/CPU/
count/staging/refinement/exact timings. Per-trial reports retain native settings,
fresh proof and original-SQL input-oracle details, unchanged expected/remote
evidence, separate readback/post-validation timing and resource peaks. OS/server
caches are uncontrolled; object SHA readback warms remote caches as in the
worker. This is not a cold-cache bandwidth measurement.

The issuer key is mounted read-only and excluded from frozen source/images/
reports. Tokens have read scope only for the selected isolated warehouse; TLS
verification stays enabled. No archiver, Influx, API, publication, commit, hint
update, routing, remote-directory creation, delete or remote cleanup is involved.
Only new isolated local scratch/reports are written. Existing datasets, snapshots,
completed fixtures, frozen runs and paired prefixes remain intact; `/node-data`
is prohibited by the existing input/namespace guards.

Acceptance requires every gate, pin/hash check, complete exact proof and
optimized-input oracle to pass, then a clear consistent improvement in **total
fresh comparison** wall/CPU after scan/join/count overhead. High cardinality or
extra remote reads can negate the gain. A promising arm still needs explicit
service integration and fresh lifecycle validation; neither remote nor pre-delete
production policy changes based on an unmeasured assumption.

Local Go race tests/vet and ten Python tests passed (six existing read-only remote
runner tests, four new projection-runner tests), covering isolation/key exclusion,
fixed budgets/order, proof/pin/hash/oracle validation, fail-fast gates/pinning and
all six trial collection. All 24 production Go files match the accepted lifecycle
source. Native gate/performance execution is pending on the VM because this
agent cannot access the Docker socket. No saved artifact was deleted and nothing
was committed or pushed.

#### Linux read-only remote canonical staging screen (2026-11-02)

Stopped in `state/remote-staging-1793664142`, `EXIT=1`. The run was not resumed.
Gates for bounded comparison, typed dictionary parity, and the three projection
arms passed. Snapshot pinning passed in 626.27 s and was not OOM-killed. Order
was baseline, local dictionary, both dictionaries, then the reverse. The first
baseline and the first local-dictionary trial exited 0. The first
both-dictionaries trial exited 1 after 34.14 s, before the reversed repetition.
No container was OOM-killed. Expected stayed
`c91128eb5a35387bd50dfd4d316f2f918a7b0333e94604b0fc6df4d3c8161f6e`. Frozen code
SHA `f846ceb465232dd4c5b4e5d5c5559ffaadb4a9d0c68b26b07c4f24cb1e7d3b32`. DuckDB
SHA `f4c681bcf41a5c43dcf9a354703fe5bded9e2265282d1915d6d99637cd1e1f29`.

The both-dictionaries comparison hit the existing 16 GiB native spill cap:
DuckDB reported `max_temp_directory_size` at 16.0 GiB/16.0 GiB while offloading
a block. That cap is the production staging budget. The trial record is
`passed: false` with 27.51 s elapsed and no accepted comparison. The remote
dictionary arm did not finish a fresh proof inside that budget.

The two completed trials compared 140,578,674 rows at 16 threads and 3GB, with
DuckDB reporting a 2.7 GiB limit. Both proofs were fresh, fixed, and 256-leaf,
and both recorded unchanged remote hashes.

| Trial | Comparison | Self CPU | Child CPU | Count | Initial routing | Refinement | Exact leaves |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Baseline, first | 342.31 s | 25.86 s | 4,567.41 s | 3.16 s | 237.20 s | 43.30 s | 57.37 s |
| Local dictionary, first | 249.76 s | 22.52 s | 3,200.61 s | 11.06 s | 145.84 s | 36.02 s | 55.52 s |

Those two times are one order only. The local trial's separate original-SQL
proof of the local expected input passed in 254.99 s and is outside the 249.76 s.
The Go test duration of 517.79 s includes that oracle. Cgroup peaks on the
completed trials were 6.75 GB and 6.80 GB. Child RSS peaked near 3.1 GB. Go RSS
stayed under 72 MB. Sampled scratch stayed near 4.54 GB. No OOM event occurred.

This screen does not choose an arm. The reversed local and baseline trials did
not run, and the remote-side dictionary did not complete. Production remote and
pre-delete proofs remain the original canonical SQL. No remote object was
deleted, no earlier artifact was removed, and nothing was committed or pushed.

##### Review: separate local-only timing from remote count-plan diagnosis (2026-11-02)

Verified frozen source/CLI hashes, successful gate/pin exits and both completed
raw reports with the acceptance validator. Each certifies 140,578,674 rows with
fresh fixed sixteen-thread, 256-leaf exact proofs and unchanged expected/pin/
object hashes. Local-only also passed its independent full expected-input
original-SQL proof. Native settings match: 16 threads, reported 2.7 GiB memory,
64 async threads/client pool and automatic read-ahead. No container OOM occurred.

| First-order observations only | Baseline | Local dictionary | Reduction |
| --- | ---: | ---: | ---: |
| Complete fresh comparison | 342.31 s | 249.76 s | 92.55 s / 27.0% |
| Initial staging/routing | 237.20 s | 145.84 s | 91.36 s / 38.5% |
| Child CPU | 4,567.41 CPU-s | 3,200.61 CPU-s | 29.9% |

The local-only gain includes dictionary/count/join overhead. Its 254.99s
original-SQL input proof is outside timing. These are single first-order
observations; reversed trials did not run. They are not medians or enough to
promote a production policy. Pin/readback timings are separate from comparison.

The failed report identifies the stage: count took 27.51s, initial fanout and
leaf count are zero, and initial-routing/refinement/exact times are zero.
The both-side query exhausted spill during **count**, before canonical COPY or
exact leaves. Sampled scratch reached 17,179,607,040 bytes, matching the roughly
16 GiB native spill-cap error. This is disk-spill exhaustion, not recorded
container OOM. Local expected remained unchanged, but the failed trial never
reached post-remote validation or input oracles and supplies no accepted proof.

This rejects the current remote-dictionary query shape under the current budget,
not Iceberg dictionaries in general. Whole-source CTE materialization or count/
dictionary plan expansion is plausible, not proven from these counters. Inspect
the failed count plan before repeating this arm or changing the 3GB/16GiB budgets.

**Recommendation:** run a fresh baseline/local/local/baseline screen at fixed
16/3GB, retaining fresh complete proofs, pin/object/local hashes and local
original-SQL input oracles. Exclude the failing both-side execution so local
reversed repetitions can finish. Separately obtain read-only EXPLAIN plans for
the baseline/local/both count queries on the same pin, checking materialized
CTEs, distinct aggregation and joins. Plan-only diagnosis neither executes the
full failing query nor certifies a corrected proof. Keep remote/pre-delete
production SQL unchanged and retain both successful dictionary defaults.

Updated active notes. No new run or remote access occurred in this review,
no saved artifact was deleted, and nothing was committed or pushed. Preserve
the failed directory and paired snapshot; do not resume it as a success.

#### Linux local-only remote screen and count plans (2026-11-03)

`state/remote-staging-1793667856` finished `EXIT=0`. It is a new directory.
`state/remote-staging-1793664142` was not resumed. Order was baseline, local
dictionary, local dictionary, baseline. The remote-dictionary arm was not
executed. Fixed 16 threads and 3GB. All four trials, the three gates, and the
pin exited 0. No container was OOM-killed. The pin is snapshot
`6916398151291571813`, 140,578,674 rows, 15 objects, and its metadata record
matches the failed run's pin. Expected stayed
`c91128eb5a35387bd50dfd4d316f2f918a7b0333e94604b0fc6df4d3c8161f6e`. Frozen code
SHA `e0ca20439d31f8b917d4d622d9a247228c1811c9ea350fe0ab1d856ce96ad4a9`. DuckDB
SHA `f4c681bcf41a5c43dcf9a354703fe5bded9e2265282d1915d6d99637cd1e1f29`. Native
settings matched on every trial: 16 threads, reported 2.7 GiB, 64 async
threads, 64 client-pool connections, automatic read-ahead.

| Trial | Comparison | Self CPU | Child CPU | Count | Initial routing | Refinement | Exact leaves |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Baseline, first | 336.92 s | 26.01 s | 4,495.21 s | 3.19 s | 228.49 s | 45.08 s | 58.80 s |
| Local dictionary, first | 270.41 s | 31.62 s | 3,346.39 s | 10.99 s | 138.31 s | 51.34 s | 67.54 s |
| Local dictionary, second | 233.18 s | 21.77 s | 2,962.32 s | 10.91 s | 132.22 s | 36.32 s | 52.40 s |
| Baseline, second | 303.03 s | 20.99 s | 4,102.00 s | 3.38 s | 211.11 s | 36.25 s | 51.04 s |
| Median baseline | 319.98 s | 23.50 s | 4,298.61 s | 3.28 s | 219.80 s | 40.66 s | 54.92 s |
| Median local dictionary | 251.79 s | 26.69 s | 3,154.36 s | 10.95 s | 135.27 s | 43.83 s | 59.97 s |

Median comparison is 68.19 s / 21.3% lower. Median child CPU is 1,144.25 s /
26.6% lower. Median initial routing is 84.53 s / 38.5% lower. Count is higher
because the local dictionary is built inside the timed comparison. The slower
local trial, 270.41 s, still beat the faster baseline, 303.03 s. The last trial
was that faster baseline. Each local trial's original-SQL proof of local
expected passed outside timing, at 243.62 s and 215.78 s. Proofs were fresh,
fixed, and 256-leaf. Remote hashes were unchanged. Cgroup peaks were 6.81–6.84
GB. Child RSS peaked near 3.1 GB. Go RSS stayed under 73 MB. Sampled scratch
stayed near 4.55 GB.

This confirms the local dictionary on both orders. It does not change production
remote or pre-delete SQL, and it does not raise the 3GB or 16 GiB budgets.

Count plans are in
`state/remote-count-plans-1793667856-plans/results/count-plans.json`. They are
`EXPLAIN` of the same count statement on that pin, not `EXPLAIN ANALYZE`. The
test passed in 1.83 s. The both-side count was not executed and did not fill
the spill budget. The captured trees are:

| Arm | Count shape |
| --- | --- |
| Baseline | Cross product of two ungrouped `count_star` aggregates. The remote side is one Iceberg scan with the March 1 time filter. No dictionary, hash aggregate, or join. |
| Local dictionary | Local side is a pipeline-dependent `tag_dictionary` CTE: hash group-by of the eight promoted tag/meta columns, then a hash join back to the Parquet scan. Remote side stays the ungrouped Iceberg count. |
| Both dictionaries | Local side as above, plus a pipeline-dependent remote `src` CTE and a second pipeline-dependent `tag_dictionary`. The remote source CTE is scanned twice. Its hash group-by and hash join sit under the remote count. |

The written helper asks for a materialized tag dictionary and a non-materialized
source. These count plans keep both as `PIPELINE_DEPENDENT`; no `MATERIALIZED`
operator remains. Distinct aggregation is the hash group-by. Its estimated
cardinality is not reduced: the local estimate stays 140,578,674 and the remote
estimate stays 28,115,734, which is the audited row count divided by five. The
Iceberg scan estimate is that same 28,115,734, not a measured remote count.
The join predicates are `IS NOT DISTINCT FROM` on the eight tag/meta columns.

So the failed count is a two-sided hash aggregate and hash join whose planner
does not see a small dictionary, with the remote source CTE consumed twice.
That is the query shape to reject under the current budget. It does not show a
corrected remote dictionary, and it does not justify a larger memory or spill
cap. Production remote and pre-delete proofs stay the original canonical SQL.
Build `tag_dictionary` and reconciliation `raw_tag_dictionary` stay in place.
No remote object was deleted, the failed directory was preserved, and nothing
was committed or pushed.

##### Review: accept local-side screen; keep Iceberg-side SQL unchanged (2026-11-03)

Revalidated all four raw reports with the acceptance validator. Frozen source/
CLI hashes match; the expected file's recomputed SHA matches its journal. All
three gates, pin and four trial containers exited zero without OOM. The pin
exactly matches the earlier failed screen, including metadata hashes, snapshot,
fifteen objects and 140,578,674 rows. Native thread/memory/I/O settings match.
Each trial passes a fresh fixed sixteen-thread, 256-leaf complete exact proof
and unchanged local/remote integrity checks. Both local-only trials additionally
pass full independent original-SQL expected-input proofs outside timing.

| Verified median | Original both sides | Local dictionary only | Reduction |
| --- | ---: | ---: | ---: |
| Complete fresh comparison | 319.98 s | 251.79 s | 68.19 s / 21.3% |
| Initial staging/routing | 219.80 s | 135.27 s | 84.53 s / 38.5% |
| Child CPU | 4,298.61 CPU-s | 3,154.36 CPU-s | 26.6% |

Both local-only trials (233.18–270.41s) beat both baselines (303.03–336.92s).
Savings survive higher count cost (3.28→10.95s), slower refinement/exact medians
and increased self CPU (23.50→26.69 CPU-s); total CPU falls because child CPU
dominates. Two repeats and uncontrolled caches limit precision, but the consistent
separation completes the previously missing reversed screen.

Reviewed the count-plan generator and trees: ordinary EXPLAIN, no ANALYZE,
`executed_count: false`, and matching pin identifiers. Both-side adds a remote
source CTE and dictionary hash group/join with multiple CTE consumers. Execution
mode is `PIPELINE_DEPENDENT`, with no `MATERIALIZED` operator. The earlier
materialization hypothesis is not proven. Estimated dictionary cardinalities
remain source-sized (140,578,674 local / 28,115,734 remote); these are estimates,
not measured distinct or remote row counts. EXPLAIN does not identify the exact
runtime operator that exhausted spill. The current both-side shape remains
rejected under 3GB/16GiB; no corrected remote arm or budget increase is justified.

**Recommendation: promote only the local expected-side dictionary for fresh
remote verification.** Scope it to `Worker.Verify`'s expected-Parquet versus
actual pinned Iceberg scan for both COMMITTED and DELETING. Keep original
`Canonical` on the remote side and original `Equal` as the independent reference/
regression oracle. Preserve fresh reads, object hashes, metadata/scope checks,
full exact proofs, hint ordering and fresh pre-delete verification. Do not reuse
build shards or enable both-side execution/fallback. Keep accepted build and
reconciliation dictionaries and all thread/memory/layout policies unchanged.

Then validate a new uninterrupted full-day lifecycle with actual local-only
verification-policy evidence, independent source-to-published proof, bounded
API/client reads and guarded fixture deletion. Measure COMMITTED, DELETING,
archive-to-routed and completion separately; the retained 68.19s comparison
saving is not an already measured lifecycle gain. Investigate a redesigned
remote-side query later without delaying the accepted local improvement.

Updated active notes. This review changes no production code/default, starts no
run, accesses no remote storage and deletes no saved artifact. Failed and
successful screens remain intact. Nothing was committed or pushed.

#### Promoted local-only verification; prepared lifecycle (2026-11-03)

Added `archive-exporter/internal/storage/remote_verification.go` with the fresh
`DuckDB.EqualRemote` path. `Worker.Verify` uses it for both COMMITTED and
DELETING: only regular local expected Parquet uses the typed tag/meta dictionary;
the actual `Worker.Scan(metadata,day)` remains original `Canonical` SQL. Local
expected admission rejects remote paths, directories and file symlinks. Every
proof reconstructs local canonical data and reads the real remote source afresh.
There is no selector, baseline fallback or both-side dictionary service path.

Renamed the shared typed helper/file to `typedDictionaryCanonical` in
`typed_dictionary.go`, also used by both local build inputs. Confirmed its SQL
matches the accepted screen exactly after helper renaming. Original `Canonical`
and independent `Equal` are unchanged, as is primitive reconciliation. Build
`tag_dictionary`, reconciliation `raw_tag_dictionary`, fixed sixteen-thread fresh
proofs, build 16/sort 2, native 3GB/16GiB spill, normalization/fan-in/one worker,
canonical reuse and warehouse schema/layout policies remain unchanged.

Full exact multiset/duplicate checks, counts, bounded routing/refinement, object
SHA readback, metadata scope, setup, hint ordering and verification journals stay
intact. No verified build stage, prior remote proof or local copy substitutes for
the actual remote read. Original `Equal` remains the independent reference path.
The typed dictionary optimization affects local expected only.

Successful verification comparison profiles record `canonicalization:
local_dictionary`. Fresh live/capacity manifests record
`verification_canonicalization: local_dictionary`; full-day acceptance requires
that actual policy for COMMITTED and DELETING, plus fresh fixed sixteen-thread
proofs, before fixture deletion can be enabled. Reconciliation must still report
`raw_tag_dictionary`. Missing/legacy/both-side policy evidence fails acceptance.

Kept and updated regression gates to exercise the production local-only method
on real typed Parquet with equal/empty/duplicate/mutated inputs and an independent
original-SQL check. Admission tests cover invalid expected inputs; worker native
lifecycle tests assert actual policy. The retained read-only remote thread
diagnostic now calls the promoted method and keeps its full independent local
input oracle outside timing and inside before/after remote integrity checks.
Removed the projection A/B runner/Python tests, selector, both-side execution and
EXPLAIN-only count-plan code. Saved failed/successful runs, count plans, inputs,
reports and frozen source/CLI remain intact. Active documentation is updated.

Prepared a fresh never-started March 1 lifecycle, verifying all four frozen
source-tree hashes, promoted helper/worker and actual harness policy checks:

```text
archive-exporter/state/full-day-local-verification-20250301-0ed2fc87cc6e
```

From the workspace root on the Docker-capable VM host:

```sh
python3 sage-data-rework/archive-exporter/testing/capacity/run.py \
  --prepared sage-data-rework/archive-exporter/state/full-day-local-verification-20250301-0ed2fc87cc6e
```

This freezes the integrated local-only verification policy against the audited
140,578,674-row source with a fresh isolated destination and fixture deletion
initially disabled. The uninterrupted run must pass source reconciliation,
fifteen-file build/layout and every proof, publication, local-only COMMITTED
verification, routing, independent full source-to-published equality, bounded
Arrow/NDJSON/client reads, fresh local-only pre-delete verification, confirmed
hot-day removal with retained boundary sentinel, post-delete reads and drained
work/resource checks. Return `run.json`, `reports/summary.json`, `routed.json`,
operation profiles and build profiles. Compare COMMITTED, DELETING, routing and
completion timings separately; the 21.3% retained screen gain is not an already
measured full-lifecycle improvement.

Preparation started no service, historical scan or remote request/write. Actual
integrated native lifecycle execution is pending on the Docker-capable host;
this agent cannot access the local Docker socket. Use this fresh preparation,
not a completed/failed older run; preserve failures and prepare a new directory.

Local Go build, race tests/vet and 67 Python tests passed (32 capacity, 12 live,
six read-only remote, seven packing, six comparison, four build-scan). No saved
data/artifact was deleted, no `/node-data` path was accessed, no remote storage
was changed, and nothing was committed or pushed. Production deletion settings
remain disabled outside the isolated fixture.

#### Linux local-only verification full-day lifecycle (2026-11-03)

The prepared run passed in `state/full-day-local-verification-20250301-0ed2fc87cc6e`,
run id `6d949f3a88424e5784ae4cd0eaaf6a5c`, execution started
2026-11-03T02:39:55Z, `EXIT=0`. `run.json` is `passed: true`, phase `complete`,
`services_stopped: true`. Policy is normalization 16, merge fan-in 16, merge
concurrency 1, fresh comparison 16, build 16, sort 2, canonical reuse, fixed
partitioning, `build_canonicalization: tag_dictionary`,
`reconciliation_canonicalization: raw_tag_dictionary`, and
`verification_canonicalization: local_dictionary`. Prefix
`cold-storage-archive-live-6d949f3a88424e5784ae4cd0eaaf6a5c` stays paired. Job
`90979d3c-ef64-406e-8c00-f44d8c1b1e66` reached `COMPLETE` for 140,578,674 rows.
Seed took 5,270.86 s. The hot sentinel is `1740873600000000194`. Hot March 1 has
0 rows and the next-day sentinel remains. Pending deletions, jobs, readers,
writers, late work, and quarantine are 0. No OOM event and no resource-sampler
error are recorded. The archiver cgroup peak is 7,895,080,960 bytes, the verify
probe peak is exactly 4 GiB, Influx peaked at 3,259,400,192 bytes, and the API
at 343,363,584 bytes. Largest Go RSS was 433,983,488 bytes and largest child
RSS was 3,260,825,600 bytes. Frozen archiver source SHA
`38bd6b5e5572032d99087473501b0167a5d69f083a5d134b5376326b26fe83a1`.

Startup-inclusive archive-to-routed time is **2,499.79 s**. The raw-tag
reconciliation full day `full-day-reconciliation-20250301-effa67e8e83e` was
3,011.40 s, so this lifecycle is 511.61 s shorter on that metric.

| Archival stage | This run | Raw-tag reconciliation full day | Change |
| --- | ---: | ---: | ---: |
| Export | 495.01 s | 524.77 s | −29.76 s |
| Reconciliation | 1,037.02 s | 1,311.26 s | −274.24 s |
| Build/materialization/packing | 609.28 s | 708.12 s | −98.84 s |
| Publish/commit | 89.35 s | 74.53 s | +14.82 s |
| Fresh remote verification | 263.38 s | 384.26 s | −120.88 s |
| Pre-delete verification plus delete request | 1,101.17 s | 1,191.73 s | −90.56 s |

Those six stages sum to 3,595.20 s versus 4,194.67 s. The archive-to-routed
drop is larger than the COMMITTED-stage drop because reconciliation and build
were also shorter on this execution. Inside reconciliation, which still records
`canonicalization: raw_tag_dictionary`, normalization was 515.83 s versus
653.78 s, Influx extraction was 425.61 s wall and 202.19 s self CPU versus
475.56 s and 222.35 s, and the fresh comparison was 95.56 s versus 181.91 s.
Initial routing there is 23.39 s versus 39.81 s. Those reconciliation changes
are the same promoted path on a different day-run, separate from local-only
verification. Normalization still reports threads 16, fan-in 16, concurrency 1,
maximum fan-in 16, and 2 passes: partition 89.51 s, sort/collapse 179.07 s,
merge 246.54 s.

COMMITTED verification was 263.38 s, including 254.42 s of fresh comparison,
and records `canonicalization: local_dictionary`. It is fresh, fixed, 16
threads, and 256-leaf, with no initial-stage reuse. Initial routing is 139.02 s
versus 252.94 s. Count is 9.67 s versus 3.59 s, refinement 45.39 s versus
33.33 s, and exact leaves 58.92 s versus 83.51 s. Child CPU is 3,213.76 s
versus 4,969.46 s. Object readback was 5.70 s and the publish hint 0.17 s.

DELETING verification was 259.43 s, including 245.49 s of fresh comparison,
also `local_dictionary`, fresh, fixed, 16 threads, and 256-leaf. Initial
routing is 136.16 s versus 264.19 s. Count is 10.73 s versus 3.17 s, refinement
33.67 s versus 30.75 s, and exact leaves 63.54 s versus 60.48 s. Child CPU is
3,134.00 s versus 4,923.07 s. The Influx delete request was 841.70 s versus
823.89 s, so the combined pre-delete stage fell by less than the proof itself.
`REMOTE_VERIFIED` and `ROUTED` state transitions are 0.002 s each; the routing
time above is the comparison's initial canonical staging.

The retained-screen median, 319.98 s to 251.79 s, is a different execution.
This live COMMITTED comparison is 254.42 s and this live DELETING comparison is
245.49 s. The 21.3% screen figure stays on that screen. It is not applied to
374.76 s, 359.89 s, or 3,011.40 s.

The build stayed on the accepted tag dictionary: 15 files, 140,578,674 rows,
16 threads, sort 2, 3GB, canonical reuse, and `canonicalization: tag_dictionary`.
Materialization passed in 53.82 s. Packing passed in 555.27 s, versus 625.64 s
on the prior full day. Nested in that packing total: per-file proofs 95.22 s
versus 126.92 s, whole-build proof 113.87 s versus 126.13 s, batch sort/copy
206.91 s versus 221.60 s, and actual packing 135.36 s versus 146.63 s. All
fifteen per-file proofs are fresh, fixed, and sixteen-thread. The whole-build
proof reused the canonical shards. Data-file bytes are 457,731,240. Retained
canonical bytes are 1,910,733,949.

Both worker journals record full equality for the same 15 objects, with the
publish hint true on `COMMITTED` and false on `DELETING`. The independent probe
matched all 140,578,674 rows. API readiness passed in 2.33 s. Before and after
deletion, Arrow (1,240 bytes), NDJSON (650 bytes), and the client each returned
one row for `uncorrected_inner`, `W099`, node `000048b02dd3c51f`.
`deletion_reverified` is true. Production deletion remains disabled outside this
isolated run. Nothing was committed or pushed.

##### Review: local-only verification passes integrated lifecycle acceptance (2026-11-03)

Audited the completed `full-day-local-verification-20250301-0ed2fc87cc6e` run.
All four frozen source-tree hashes match. Actual successful operation profiles
pass current policy validation: primitive reconciliation is `raw_tag_dictionary`,
COMMITTED and DELETING are `local_dictionary`, and all three are fresh fixed
sixteen-thread, 256-leaf complete exact proofs with no initial-stage reuse.
Actual build profiles preserve `tag_dictionary`, fifteen files, 140,578,674 rows,
16 build/proof threads, two sort threads, 3GB, every per-file proof and the new
whole-build proof using verified canonical shards. Routed/summary build profiles
match. Every archival state has one successful attempt and no failed attempts.
Services stopped normally with no OOM events or resource-sampler errors.

The independent source-to-published proof certifies all **140,578,674 rows**
across 256 bounded exact partitions. Both worker journals certify identical
fifteen-object hashes, expected-input hash and metadata snapshot with full
equality; only COMMITTED publishes the routing hint. Pre-/post-deletion Arrow,
NDJSON and default-Arrow client checks pass the bounded one-row sample. This
checks that read path, not all possible API requests. The job is COMPLETE,
the hot day is empty, the next-day sentinel remains, and pending deletions,
jobs, readers, writers, late work and quarantine are zero.

| Verified comparison with prior accepted reconciliation lifecycle | Previous | Local-only verification lifecycle |
| --- | ---: | ---: |
| Startup-inclusive archive to routed | 3,011.40 s (50m11s) | 2,499.79 s (41m40s) |
| COMMITTED fresh comparison | 374.76 s | 254.42 s |
| DELETING fresh comparison | 359.89 s | 245.49 s |
| COMMITTED full stage | 384.26 s | 263.38 s |

**Keep the promoted local-only verification default.** Its integrated comparison
times are consistent with the retained screen's 251.79s candidate median.
Keep typed dictionaries on local build inputs/local expected verification only,
raw-tag dictionaries on primitive reconciliation, and original canonical SQL
on actual Iceberg scans and in independent `Equal` references. Preserve every
fresh exact proof, object/scope check, hint/deletion gate and existing budget/
thread/layout policy. Do not enable the rejected remote-side dictionary.

Archive-to-routed is **511.61s / 8m32s / 17.0% shorter in this execution**.
This is measured successive-run behavior, not a controlled repeated lifecycle
A/B or a fixed deployment-speedup promise. The target COMMITTED stage saves
120.88s, about 24% of the total routing-time difference. Reconciliation also
saves 274.24s and build 98.84s despite using unchanged previously accepted paths;
export/publish and startup timing vary too. Do not attribute those savings to
local-only verification. The controlled retained screen establishes its isolated
gain; this lifecycle establishes correct worker integration and real timings.

Fresh pre-delete comparison also improves, but the delete HTTP request rises
823.89→841.70s, so the combined DELETING stage saves only 90.57s. That stage is
after routing and cannot explain the archive-to-routed change. Keep routing and
completion measurements separate when choosing a future target. The 21.3%
retained-screen figure remains scoped to that comparison, not the whole pipeline.

This completes representative uninterrupted lifecycle acceptance for all current
dictionary scopes, including independent full remote equality, sampled reads,
fresh pre-delete verification and guarded fixture deletion. Production deletion
configuration remains unchanged. Updated active documentation; this review
starts no run, changes no service code, deletes no saved artifact and accesses
no remote storage. Preserve the completed fixture and paired remote prefix.
Nothing was committed or pushed.

#### Final local-only verification cleanup audit (2026-11-03)

Confirmed `Worker.Verify` always uses the accepted local-only `EqualRemote`
path for COMMITTED and DELETING. The older verification branch, projection
selector, rejected both-side execution, A/B runner/tests and EXPLAIN helper were
already removed during promotion. No additional obsolete verification code
remains to delete. The only active `both_dictionary` mention is a negative
capacity-policy fixture proving that rejected policy cannot pass acceptance.

All 25 production Go files match the successful local-only lifecycle's frozen
source byte-for-byte. Original Iceberg-side `Canonical`, independent `Equal`,
full exact machinery and regression gates have live callers and remain required;
they are not an old verification fallback. Build/reconciliation defaults and
proof/budget/layout policies are unchanged. This audit changes documentation
only; no redundant native run was needed.

Saved datasets, reports, count plans, frozen runs, completed fixture and paired
remote prefixes remain intact. No remote storage was accessed and nothing was
committed or pushed.

## Future improvements — deferred backlog (2026-11-03)

The current archival pipeline is the accepted starting point. Further performance
work is deferred; the items below are candidates for later measurement, not
required changes to the initial architecture or instructions to start new runs.

### Starting baseline

Use `state/full-day-local-verification-20250301-0ed2fc87cc6e` as the latest
representative lifecycle reference: **140,578,674 measurements for one day**, fifteen
Parquet files totaling approximately 458 MB, **41m40s archive-to-routed**, and
approximately **59m55s of active worker stages including guarded hot deletion**.
The latter is a sum of worker-stage times, not total harness wall time; test-data
seeding, extra independent validation, API probes and waits are separate.

Retain the accepted scoped dictionaries: typed dictionaries for build inputs,
raw-tag dictionaries for primitive reconciliation, and typed dictionaries only
on local expected data during remote verification. Actual Iceberg scans retain
original canonical SQL. Keep the accepted 16-thread normalization/fresh-proof
policy, merge fan-in sixteen/one merge worker, build sixteen/sort two, 3GB native
memory, 16GiB spill guard, canonical reuse and published warehouse layout.
Current VM timings are a baseline, not production server sizing or an SLA.

### Performance candidates

| Priority/workstream | Candidate and current cost | First useful experiment |
| --- | --- | --- |
| 1 — routing | Overlap normalization (**516s**) and independent Influx extraction (**426s**), currently sequential inside reconciliation. | Compare sequential and concurrent scheduling against one isolated, fenced source. Normalize the captured export and query Influx into separate outputs; join both successful tasks before the unchanged exact comparison. Cancel/join work on failure and keep the job from advancing. Measure shared CPU, memory and scratch peaks; overlapping wall/CPU intervals must not be added as independent costs. |
| 2 — routing | Revisit normalization implementation, now **516s**: partition approximately 90s, sort/collapse 179s, Go merge 247s. | Profile retained-export CPU, allocation and I/O on the intended server. Thread counts and fan-in were already screened; identify remaining merge/encoding work before proposing another change. Measure complete normalization, including extra passes and output verification. |
| 3 — routing | Bounded build concurrency, now **609s** overall; batch sort/copy approximately 207s and packing 135s. | Compare sequential execution with two file-window workers using private scratch, an explicit combined resource budget and ordered result collection. Preserve chronological file-window assignment, plugin/vsn/time ordering, capped plugin-pure row groups, every per-file proof and the final coverage-bound whole-build proof. |
| Separate completion workstream | Investigate Influx deletion: the latest delete HTTP request is **842s**, after routing. | Profile the private Influx fixture/server rather than infer a cause from client wait time. Measure request acknowledgement, query-visible removal and physical storage reclamation separately. Preserve the deletion bounds, fresh pre-delete proof and retained boundary sentinel. This work does not improve archive-to-routed latency. |
| Lower priority — verification research | Redesign the rejected Iceberg-side dictionary, whose count query exhausted the **16GiB** spill guard. | Investigate a narrower query/count shape using EXPLAIN and bounded-scale execution before another full-day proof. The saved plans show pipeline-dependent CTE/group/join work, not a proven materialization root cause. Require full original-SQL input oracles and fresh remote integrity checks; keep original Iceberg-side SQL unless a corrected candidate passes within the declared budget. |
| Lower priority — routing | Bounded upload/readback concurrency; publish/commit currently approximately **89s**. | Measure two object transfers/readbacks before considering four. Keep a single catalog commit after all required object checks and preserve subsequent fresh verification. Do not remove repeated integrity checks merely because an earlier upload succeeded. |

For candidate 1, the latest timings give an idealized saving of at most roughly
**426s (7m06s)** if normalization and extraction overlap without slowing each
other: comparison still waits for both. This is a sensitivity bound, not a
forecast. Source/database/storage contention and shared Go CPU/allocations may
reduce the gain. It is distinct from increasing DuckDB threads or running
multiple archival jobs concurrently.

### Validation and operating coverage

Establish a repeatable baseline on the intended server before new tuning. Record
hardware, storage/network conditions and actual resource settings; compare arms
with equal declared budgets and reversed repetitions. Start with retained
artifacts or read-only pinned snapshots when possible, then validate a winning
integration through a fresh isolated full-day lifecycle. Report routing latency,
deletion/completion latency, stage wall/CPU and resource peaks separately.

Broaden coverage over time to multiple days, sparse and high-cardinality tags,
mixed scalar/string workloads, sustained ingestion/read traffic and reconnecting
edge-node bursts. Exercise restart/recovery and interrupted operations at the
existing durable boundaries, including late-data reconciliation and deletion.
Use these runs to validate backlog drainage, retry/idempotency behavior, staging
space and operational metrics such as archival lag, pending work and failed jobs.
This extends existing validation; it does not replace the implemented late-data
or recovery mechanisms.

Every candidate must preserve full record equality, nanoseconds/scalar types,
duplicates, fence/read-grant boundaries, snapshot/object checks and guarded
deletion. Keep the accepted warehouse schema/layout and configurable hot
retention unless a separate architecture change is explicitly chosen. Preserve
saved evidence and use new isolated directories/prefixes for experiments; data
cleanup remains a separate user-approved action. No new experiment, service
change, deployment, commit, push or storage cleanup is part of this backlog.

## Official service promotion: beehive-hot-to-cold-loader (2026-11-03)

Moved the maintained service from `sage-data-rework/archive-exporter` to the
workspace's [`beehive-hot-to-cold-loader`](https://github.com/waggle-sensor/beehive-hot-to-cold-loader/blob/main/README.md)
directory. The Go module/imports, built executable and Docker entrypoint are now
named `beehive-hot-to-cold-loader`. The destination contains `main.go`,
`internal/`, `go.mod`/`go.sum`, Docker/build-ignore configuration, operator docs,
Go tests and maintained `testing/` harnesses. `cmd/legacy-export` is excluded.
No repository initialization, commit, push or deployment was performed.

The research directory now holds a migration README plus historical `state/`,
frozen source/preparations, old binaries, the legacy exporter and its analysis/
data-product utilities. They were not moved or deleted. Its ignore rules remain
in place to protect saved state/build outputs. Current commands live under the
official service; explicit historical diagnostic inputs continue pointing to
the old state paths. The existing pinned DuckDB and matching Influx executables
were copied into the official service's gitignored `bin/` for local validation;
they were not committed or removed from the research directory. Old service/test
build outputs and the downloaded CLI archive were not promoted.

Updated operator/integration docs and API/Influx-loader guide links. Harnesses
derive workspace peers from the new service's parent rather than the former
extra research-directory depth, including the read-only signing-key default.
New fixture preparations default to the official service's own ignored `state/`;
old completed/failed/prepared evidence retains its original embedded paths and
hashes. Historical references to the original date-partition exporter now name
the retained `cmd/legacy-export/main.go`, not the official service.

The move preserves runtime flags/environment, coordination interfaces, retention
and deletion defaults, all accepted dictionary scopes, exact proof machinery,
thread/memory/spill budgets and warehouse schema/layout. Compared all 25
production Go files with the accepted local-only lifecycle's frozen source:
they match after normalizing only module import prefixes. `go.sum` is identical,
and copied native dependencies match the original hashes. The new module's
package inventory contains only the service and its three internal packages,
with no legacy exporter.

Validation from the new directory passed Go race tests, vet and service build,
plus all 74 Python tests (32 capacity, 12 live, six remote comparison, seven
packing, six local comparison, four scan, four normalization, three extraction).
The renamed executable's help works. A temporary real prepare-only smoke passed
sibling lookup, four frozen source hashes, new module/Docker entrypoint and
deletion-disabled defaults, without starting services, scanning historical data
or making remote requests. Local documentation links and changed-repository
whitespace checks pass. A Go package-list check initially used the sandbox's
unwritable default cache; rerunning with the same writable cache as the build
completed successfully. Docker/native integrated execution was not claimed for
this filesystem/module migration.

Future development and deployment should start from the official service guide.
The deferred backlog above still applies there. Saved datasets, warehouse
objects, snapshots, frozen runs and paired remote prefixes remain intact;
`/node-data` was not accessed.
