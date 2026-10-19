# Independent review of the pipeline experiments — 2026-10-14

**The statement “Iceberg cannot use a filesystem catalog because Pelican
does not allow it” is incorrect.** The existing work itself eventually
demonstrates a filesystem catalog on the NRDStor origin (§32–33). The
limitation is safe concurrent publication through the tested HTTP adapter,
not the ability to store or read an Iceberg table. Several earlier,
contradictory conclusions remain in the recommendation document.

This review inspected the current exporter, Beehive query implementation,
storage loader, experimental code, recorded results, and the installed
iceberg-go v0.7.0 source. It independently ran small local checks, not the
entire 194 GiB benchmark suite. A new live test compiled but stopped at DNS
resolution before creating any objects. Consequently, previous live results
are **reported evidence**, not independently reproduced measurements here.
No request targeted `/node-data`; no remote writes, commits, or pushes occurred.

## 1. Correct the catalog decision before using it to choose an architecture

The current recommendation successively says “database required,” “single
writer works,” and then “database genuinely required” again. Use this matrix:

| Design | What the evidence supports | Remaining condition |
| --- | --- | --- |
| Iceberg + SQL catalog + Pelican files | Existing experiments demonstrate tables and concurrent commits using SQLite | Production database deployment, recovery, retries, and multi-process tests remain |
| Iceberg filesystem catalog + Pelican adapter | Existing §32–33 demonstrate sequential commits and parallel producers feeding one committer | Adapter is explicitly unsafe under concurrent commits; crash/failover and scale are not certified |
| Custom manifest + Pelican | Existing experiments demonstrate publication and reads | Same single-committer/external-coordination requirement; more snapshot/retention/query semantics to own |

In `catalog/hadoop/hadoop.go`, `NewCatalog` rejects **every non-local URI
scheme** unless `allow-unsafe-commits=true`. It does not test Pelican or ask
the server whether a catalog is allowed. `CommitTable` depends on
`RenameNoReplace` for claiming and publishing the next metadata version.
The experimental `hadoopFS.RenameNoReplace` does a separate `Stat` and
`MOVE`, leaving a race. Prior probes report that this origin ignores
`Overwrite: F` and `If-None-Match: *`.

The appropriate conclusion is: **a filesystem catalog is a working
experimental single-committer option, but this adapter does not provide
Iceberg's concurrent-commit guarantee.** Multiple producers can encode and
upload in parallel; commit serialization is per table. A second committer
can cause corruption, but “every two-writer execution corrupts” is not an
established result. A database is one coordination solution, not a logical
requirement imposed by Pelican. A direct filesystem path with suitable
semantics would be a different deployment, not tested by these HTTP probes.

Do not equate one Kubernetes replica or one queue consumer with fencing:
failover, backfill, repair, compaction, and an old process resuming must all
respect the same exclusive commit ownership. Single-committer happy-path
tests do not establish crash durability or exactly-once ingestion.

The [Iceberg specification](https://iceberg.apache.org/spec/#file-system-tables)
also deprecates the filesystem commit scheme and schedules its removal from
format v4, which remains under development. This is a separate maintenance
consideration, not a reason to claim the existing v2/v3 implementation cannot run.

## 2. The version hint is not the commit point

The notes describe `version-hint.text` as an authoritative pointer and
suggest readers universally need just one extra GET. That is not how the
installed iceberg-go Hadoop catalog resolves tables: `findMetadataLocation`
scans metadata filenames; `writeVersionHint` is best effort **after** metadata
publication. DuckDB's discovery behavior is a different client concern.

**Independently reproduced:** after committing v2, a fresh local catalog
still loaded v2 with the hint set to `1`, set to `garbage`, or deleted.
See `experiments/icebergo/audit_hint_test.go` and
`experiments/results/audit_catalog_local.log`.

This distinction also matters for a SQL-catalog publication pointer. Its
update is a separate operation from the database commit; it needs ordered
publication, retry/reconciliation, and a freshness contract. Do not derive
the committed SQL-catalog head by picking the largest metadata filename:
failed commits can leave unpublished metadata files.

## 3. “An ETag would remove the database requirement” is insufficient

`4_catalog_recommendation.md` says either `Overwrite: F` support **or an ETag
on GET/HEAD** is enough. An ETag is only a validator. The server must also
atomically enforce a corresponding write precondition, and the catalog
adapter must use the resulting primitive correctly. An atomic no-replace
rename is the direct match for this Hadoop catalog. Conditional object
creation could support another commit implementation, but does not silently
repair the current adapter.

The finite probe suite establishes no usable conditional primitive among
those tested on this origin. It does not establish that all Pelican
deployments, future XRootD releases, or storage access methods lack one.
Likewise, 318 reads without a torn value are useful evidence, not proof of
crash durability. The note that `If-Unmodified-Since` is evaluated “against
the response header” is misleading: enforcement is server-side; the header
normally communicates the validator to the client.

## 4. The claimed Beehive query equivalence has reproducible gaps

`experiments/audit_local.py` creates two tiny synthetic Parquet records and
runs the existing backend with DuckDB 1.5.6 / PyArrow 25.0.1. Results are in
`experiments/results/audit_local.json`.

| Input | Actual result | Consequence |
| --- | --- | --- |
| Same name and tags, map entries in opposite order; `tail=1` | Two rows returned | `PARTITION BY name, meta` treats map order as series identity; canonicalize tags before grouping |
| `experimental_func=count` | Two raw observations returned | Unsupported aggregation is silently accepted; reject it until implemented |
| `bucket=_private` | The archive's ordinary rows returned | Bucket selection and the current API's private-bucket rejection are absent |
| Timestamp boundary ending in `.000000999Z` | Parsed as `.000000000Z` | Python datetime parsing loses sub-microsecond boundary precision |

The bucket result is a semantic/authorization-boundary gap in an experiment,
not evidence of deployed private-data exposure. `beehive-data-api` rejects
underscore-prefixed buckets and selects the requested public bucket; the
cold prototype accepts the field and ignores it.

The existing 12 checks establish a useful subset of behavior. They do not
justify a drop-in compatibility claim. The backend also discovers files by
directory listing rather than a pinned catalog snapshot, so the catalog
experiment and query experiment have not yet become one safe query path.

## 5. The Go storage adapter does not yet deliver selective remote reads

`experiments/icebergo/pelican_io.go:Open` calls `ReadFile` and wraps the entire
download in a `bytes.Reader`. Thus a successful Go scan can download each
selected Parquet file in full. It demonstrates functional interoperability,
not that row-group pruning saves network traffic through this adapter.

The separate Python range-reader measurements remain valuable. Their exact
150 ms latency and 32-worker speedup describe the measured route and workload,
not a universal Pelican constant or a production concurrency setting. Reading
the `plugin` column across every row group is not an end-to-end selective
Beehive query. Benchmark the intended query engine and FileIO together,
including bytes, requests, memory, and authenticated cache/origin behavior.

The Go filesystem adapter also needs implementation work beyond concurrency:
`statInfo.ModTime` always returns zero although upstream uses it for stale
claims, and `WalkDir` still infers collections from trailing slashes despite
the experiment documenting that the origin omits them. These are reasons to
treat the adapter as a probe, not production filesystem semantics.

## 6. Some pass counts claim more than the assertions test

In `fscatalog_parallel_test.go`, Q4 logs PASS unconditionally and discards
the hint-read error. Q3 checks total rows and number of distinct producers,
but not 250 rows per producer or the unique `(producer, seq)` pairs. It
reads the writer's returned table rather than independently reloading it.

`fscatalog_race_test.go` races **RenameNoReplace calls**, not Iceberg
transactions. It demonstrates an unsafe primitive, but describing its
winner count as measured lost Iceberg commits overstates the test. It also
counts all errors as `ErrExist`, does not fail on a final-read error, and
increments `unsafeRenames` concurrently without synchronization.

The new `audit_fscatalog_test.go` requires exact row identities through a
fresh catalog, exact hint values, recovery from missing/stale/malformed hints,
and explicit cleanup verification in a unique scoped storage directory.
It compiled, but its first PROPFIND failed with `no such host`. **No live
assertions passed in this review.** A real concurrent-transaction test and
failure-injection tests remain needed.

## 7. Reader independence is useful, but the ecosystem claim is broader than tested

The local catalog-free Go reader test passed again: schema, history, and
data are readable from a metadata location; a pinned reader stays on its
old version after a later commit. This supports Iceberg's publication model.
It does not demonstrate NDP, Spark, Trino, or cloud-warehouse access through
authenticated Pelican. Those clients need compatible FileIO, discovery,
and credentials. “No catalog service needed for this read” does not mean
“no SAGE-operated dependency”: storage routing and token issuance still matter.

The DuckLake exclusion also overstates impossibility. Existing §30 already
reports reading a database copy over HTTPS. It does not prove that a writer
could not repeatedly publish consistent, immutable database snapshots and
advance a discovery pointer, retaining their referenced data. That is an
**untested publication design**, with cost and freshness tradeoffs, not a
recommendation to adopt DuckLake. Iceberg's broader reader ecosystem remains
a reasonable preference; “DuckLake readers must always contact a live SAGE
database service” is not established by the experiments.

## 8. Preserve the timestamp and archive-scope qualifications

The precision finding is real for this archive's UTC-adjusted nanosecond
Parquet timestamps. It is not true for all DuckDB Parquet timestamps:

| Synthetic Parquet type | DuckDB 1.5.6 result for `1735689606915629791` |
| --- | --- |
| Nanoseconds, no timezone | `TIMESTAMP_NS`, exact integer preserved |
| Nanoseconds, UTC | `TIMESTAMP WITH TIME ZONE`, `1735689606915629000` |

This matches [DuckDB's timestamp documentation](https://duckdb.org/docs/current/sql/data_types/timestamp).
Keeping a microsecond timestamp plus an integer nanosecond value or remainder
is also an option if broad timestamp interoperability and exact preservation
are both wanted. A reader limitation alone is not evidence that scientific
precision is dispensable. The recorded eight-day collision sample is useful,
not a guarantee about future data.

The row-group diagnosis does hold: the exporter does not set
`MaxRowsPerRowGroup`, and an inspected real file has 10 million rows in one
group. Existing JSON supports the reported compaction results, but compares
queries over an already-exported archive. It does not establish a lossless
export from all of InfluxDB. The exporter intentionally skips observations
without `plugin`, uses only `FieldList()[0]`, and skips unsupported value
types. Define and verify the archived subset before changing retention.

## Recommended next step

Keep Iceberg as a reasonable candidate for standard metadata, snapshots,
and external consumption. Choose **catalog coordination separately**:
use a transactional catalog for independently committing writers; keep the
filesystem option as a scoped prototype if one committer is a real operational
constraint. The custom option does not remove coordination or retention work.

Before using these results to reduce Influx retention, validate one complete
path: source export → uploaded files → committed snapshot → fresh independent
query, including retries, interrupted commits, tag-order equivalence, and
concurrent compaction. Redaction also needs old snapshots/tags and cached
copies addressed; deleting rows from the latest snapshot alone is insufficient.

## Reproduction and artifacts

From `experiments/`:

```sh
/Users/franciscolozano/miniconda3/envs/sage_pipeline/bin/python -I audit_local.py
```

From `experiments/icebergo/`:

```sh
GOCACHE=/private/tmp/sage-audit-go-cache GOPROXY=off GOSUMDB=off \
  go test -tags reader -run '^(TestAuditVersionHintIsNotCommitPoint|TestReaderWithoutCatalog|TestVersionHintPointer)$' -count=1 -v .

# Requires network access and the existing local issuer-key configuration.
# Creates and removes only /sage/cold-storage-audit-<unique-number>/.
GOCACHE=/private/tmp/sage-audit-go-cache GOPROXY=off GOSUMDB=off \
  go test -tags pelican -run '^TestAuditFilesystemCatalog$' -count=1 -v .
```

Local catalog result: three test functions passed. Network result: DNS failure
before writes. Existing historical findings and production implementation
were preserved; the additions are audit fixtures, logs, this review, and
navigation notices on the older summaries.
