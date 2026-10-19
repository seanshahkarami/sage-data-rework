# Database catalog, with `version-hint.text` on Pelican

Decided 2026-10-14 from the catalog measurements and a live check of the
filesystem catalog on NRDStor. Revised the same day: hint publication moved
from each writer to a single reconciler, after the client behaviour below
was measured locally.

**Run end to end on NRDStor the same day: 12/12 (`exp34`).** Two concurrent
committers, one reconciler, one reader with no database. Every claim below
marked *measured* now has an origin result behind it, not a local fixture.

Writers commit through an Iceberg SQL catalog. A single reconciler reads the
committed head from the database and publishes `metadata/version-hint.text`
to Pelican. Consumers read that file and never open the database.

That split is the point. The database is what makes two committing services
safe. The hint is what lets a consumer find the current metadata JSON
without a Sage-hosted catalog.

**Many committers, one publisher.** The committers are the parallel part and
the database arbitrates them. Publishing the hint is one small serialized
step, and it sits *outside* the commit path, so it costs writers nothing and
cannot block them. This is the `exp33` shape — N parallel producers, one
serialized metadata step — with the serialized step moved off the critical
path.

## Why the failure mode is acceptable

This is the reason the design works, so it goes first rather than in a
caveats section.

| | filesystem catalog (`exp32`) | this design |
|---|---|---|
| two writers race | acknowledged commit **silently lost** | readers see an **older committed snapshot** |
| recoverable? | no — nothing on storage knows it should be there | yes — the database row is the true head |
| repair | none | rewrite the hint from `metadata_location` |

A lost race here is stale, not lossy, and it is self-healing. The database
never stops being correct; only what consumers *see* lags.

Measured on the origin (`exp34` Q1, Q5): two concurrent committers both
landed — one hit the database CAS, retried once, succeeded — for 100 rows
with 50 from each. Pointing the hint at an older metadata file then served
50 rows with no warning, and a single reconciler pass restored all 100.
Compare `exp32` on the same storage: 3 of 4 writers told they won, 2 commits
gone for good.

The staleness is **silent**. A client pointed at an older hint returns that
snapshot with no warning, while a newer metadata file sits beside it in the
same directory (measured below). Detection has to be active — the reconciler
comparing published hint against `metadata_location` — not incidental.

## What each side holds

The database row for a table is a directory entry:

| column | role |
|---|---|
| `catalog_name`, `table_namespace`, `table_name` | the name |
| `iceberg_type` | table or view |
| `metadata_location` | path of the current metadata file |
| `previous_metadata_location` | path of the one before it |

A second table, `iceberg_namespace_properties`, holds optional namespace
properties such as a location or a description.

Everything a reader uses to understand the data is in the metadata JSON on
Pelican, which the hint names:

- schema, column types, per-column docs, table properties (licence, contact)
- snapshot history and the metadata log
- the `refs` map: `main`, every other branch, and every tag, each pointing
  at a snapshot id
- manifests, which list the Parquet files in that snapshot

Branches and tags are not catalog rows. Creating or moving one is a writer
commit: a new metadata file, then a new hint. A consumer who opens the file
the hint names can resolve a tag such as `doi-v1` and read that snapshot.
The local tag test already showed a tag surviving later writes and snapshot
expiry (`experiments/icebergo/evolve_test.go`).

A consumer who knows the table's warehouse path misses the directory: the
list of namespaces and table names, and the namespace properties. A consumer
who wants "what tables exist?" still needs that listing, from a published
index or from the warehouse layout itself.

## The reconciler

The SQL catalog does not write `version-hint.text`. iceberg-go writes it
only from the filesystem catalog (`catalog/hadoop/hadoop.go`). pyiceberg
0.12.0 only reads one. The Pelican SQL-catalog run left several
`*.metadata.json` files and no hint (`exp19`). So publishing it is a step
someone has to own.

One process owns it, for every table:

```
for each table:
    head = SELECT metadata_location FROM iceberg_tables WHERE ...
    want = stem(head)                      # see format below
    if read(pelican, hint_path) != want:
        PUT hint_path  <- want             # no trailing newline
```

Properties that follow from one publisher rather than N:

- **It cannot race itself.** The hint-overwrite hazard disappears by
  construction instead of being repaired after the fact.
- **Correctness never depends on it.** The database is the CAS. The
  reconciler controls freshness only; if it stops, consumers hold the last
  published snapshot and nothing corrupts.
- **Steady state and repair are the same code.** The loop above *is* the
  recovery procedure, so the repair path is exercised continuously rather
  than being an emergency script nobody has run.
- **It is idempotent and restartable.** State lives in the database and on
  Pelican, not in the reconciler.

Run it on a timer, or wake it after commits; the loop is the same either
way. Freshness lag is bounded by its period, which is the one real cost of
moving publication off the commit path.

Publish the snapshot id alongside the hint — a sibling object, or the
reconciler's own log — so staleness is detectable without opening the
database.

Never publish a hint for a metadata file that is not the committed head: a
failed commit can leave a higher-numbered `*.metadata.json` on Pelican, and
the reconciler must take its answer from `metadata_location`, never from a
directory listing.

## Every committer uses the same catalog name

`catalog_name` is part of the primary key in `iceberg_tables`. Two services
that open the catalog under *different* names do not share a catalog at all
— each silently gets a private one over the same warehouse, and the second
committer fails with `NoSuchTableError` against a table that demonstrably
exists (`exp34`, found the hard way).

This is a deployment-config requirement, not a code one, and it fails in a
way that blames the table rather than the name. Pin the catalog name
alongside the database URI.

## Hint format

The file contains the **stem** of the object the SQL catalog wrote, for
example `00002-<uuid>`. Those files are named `00002-<uuid>.metadata.json`
(`table/locations.go`).

Two rules, both measured:

1. **No trailing newline.** Neither client trims. `echo`, most templating
   steps, and most editors add one, and the resulting error names a path
   that looks correct — a confusing way for a one-byte bug to present. The
   uploader should assert this.
2. **Not a bare number.** A hint containing `2` sends clients looking for
   `v2.metadata.json`, which this catalog does not create.

| client | how it resolves the hint |
|---|---|
| DuckDB 1.5.6 | reads `version-hint.text` and will not glob the directory; stem `00002-<uuid>` becomes `00002-<uuid>.metadata.json` |
| pyiceberg 0.12.0 `StaticTable.from_metadata` | a non-numeric hint is joined as `{stem}.metadata.json`; a bare number becomes `v{n}.metadata.json` |

DuckDB's format string in the 1.5.6 extension is
`v%s%s.metadata.json,%s%s.metadata.json`. The stem matches the second
pattern when metadata compression is `none`, which is iceberg-go's default.

### Measured (local fixture, pyiceberg 0.12.0 + DuckDB 1.5.6)

A SQL-catalog-shaped table (`00000`/`00001`/`00002-<uuid>.metadata.json`),
three commits, six rows at head:

| hint contents | DuckDB | pyiceberg |
|---|---|---|
| *(absent — what the SQL catalog leaves today)* | fail: "no version-hint could be found, globbing… disabled" | fail: no such file |
| `00002-<uuid>` — **this design** | **6 rows** | **6 rows** |
| `00001-<uuid>` (older, newer file present) | **3 rows, silently** | **3 rows, silently** |
| `2` | fail → looked for `v2.metadata.json` | fail → `v2.metadata.json` |
| `00002-<uuid>\n` | **fail** | **fail** |
| names a deleted file | fail | fail |

The third row is the staleness case: authoritative hint, no warning, newer
metadata ignored. The fifth is the newline rule.

## Which database

Measured in `exp35` (4/4), after an earlier draft recommended Postgres
without evidence:

| committing processes | rows | retries (max) | wall |
|---|---|---|---|
| 6 | 60/60 | 1 | 0.9 s |
| 12 | 120/120 | 2 | 2.5 s |
| 24 | **240/240** | **2** | 2.9 s |

Separate OS processes, not threads. No loss at any level. **There is no
concurrency-correctness argument against SQLite at this scale.**

The constraint is connectivity instead:

```
sqlite://db-host:5432/catalog.db  ->  ArgumentError: Invalid SQLite URL
```

A SQLite URI is a **file path**. Every committer must share a filesystem.

| deployment | catalog |
|---|---|
| one host / one pod | **SQLite** — simpler, nothing to operate, measured safe to 24 processes |
| multiple hosts | **Postgres** — not a reliability tradeoff; SQLite cannot be connected to at all |

So the question to settle is *where the committers run*, not *which
database is safer*. `SqlCatalog` takes either behind one interface, so this
is a URI change if the deployment later goes multi-host.

**Caveat:** measured on a local filesystem. SQLite over NFS has the locking
problems its own documentation warns about, so reaching multi-host
committers via a *shared volume* is the configuration to avoid — not
SQLite itself.

## Warehouse layout

pyiceberg's SqlCatalog lays a table out as `<warehouse>/<namespace>/<table>`
— **no** Hive-style `.db` suffix on the namespace directory. So the hint for
`sage.obs` lives at:

```
<warehouse>/sage/obs/metadata/version-hint.text
```

The reconciler should derive this from `metadata_location` rather than
construct it. `exp34` hardcoded `sage.db/obs` on the first attempt and the
reader got `FileNotFoundError` on a metadata file the catalog had certainly
written.

## iceberg-go will not follow this hint, by construction

`hadoop.Catalog` **does** read `version-hint.text`
(`catalog/hadoop/hadoop.go:403`), but parses it with `strconv.Atoi`. A stem
fails that parse, `readVersionHint` returns `0`, and the catalog falls back
to scanning metadata filenames (`findMetadataLocation`).

So the two formats are mutually exclusive: the numeric hint iceberg-go wants
is the one DuckDB and pyiceberg cannot resolve against SQL-catalog
filenames, and vice versa. **One file cannot satisfy both**, and no future
tightening will change that without an upstream change.

This is survivable because the consumers that matter are DuckDB and
pyiceberg, and iceberg-go is a *writer* here. But it means the fallback scan
is what any iceberg-go reader gets, and that scan recognizes **both** naming
styles — `v<N>.metadata.json` and `<seq>-<uuid>.metadata.json`
(`hadoop.go:102`, `uuidMetadataPattern`). An orphan `00003-*.metadata.json`
from a failed commit would therefore win that scan while the hint and the
database both still say `00002`. Consumers should follow the hint.

`experiments/icebergo/audit_hint_test.go` already shows the fallback: a
fresh hadoop catalog loads the correct metadata with the hint set to `1`, to
garbage, or deleted outright.

## Metadata expiry must stay pinned

A hint naming a deleted metadata file fails hard in both clients (table
above). Two pyiceberg properties decide whether that can happen:

| property | default | note |
|---|---|---|
| `write.metadata.delete-after-commit.enabled` | `False` | if enabled, old metadata is removed on commit |
| `write.metadata.previous-versions-max` | `100` | how many are kept when it is |

Latent today, not active. Pin both deliberately rather than inheriting them,
because turning deletion on later would break hints already published — and
any reader holding one.

### But that is not the whole story — `exp36`

Keeping deletion off bounds nothing. Measured at scale, two separate
mechanisms were being treated as one:

| knob | what it does | verdict |
|---|---|---|
| `write.metadata.delete-after-commit.enabled` | deletes old `*.metadata.json` files | **keep OFF** — deletes files published hints may still name |
| `table.maintenance.expire_snapshots()` | drops snapshot *entries* from the table's history | **turn ON**, on a retention policy |

The second is what actually bounds growth, and it is **safe for the hint**:
it writes a *new* metadata file and leaves the old one in place, so a hint
published before a pass still resolves to an older **valid** snapshot — the
same recoverable staleness this design already accepts (`exp36` Q6, 3/3).

Why it matters: every `metadata.json` embeds the **entire snapshot
history**, and the hint points consumers at exactly that file, so each
consumer downloads it on every read. Measured growth is ~0.7 KB per
snapshot, linear and unbounded:

| commit rate | snapshots | current `metadata.json` | `metadata/` if never expired |
|---|---|---|---|
| 1/day × 1y | 365 | 0.2 MB | 0.0 GB |
| 24/day × 1y | 8,760 | 5.9 MB | 25 GB |
| 24/day × 5y | 43,800 | **29.3 MB** | **627 GB** |
| 96/day × 5y | 175,200 | 117.2 MB | 10 TB |

The right-hand column is the cost of retaining every version (`n²/2`); the
middle column is what a reader pays per read even with the directory capped.
Hourly commits for five years make the per-read fetch 29 MB and the metadata
directory three times the size of the 209 GB data archive. Snapshot expiry
on a retention policy is not an optimization here, it is required.

## Writer and reader paths

```
committers (many)               Pelican                      consumers
  commit via SQL catalog  ──►   00002-<uuid>.metadata.json
  CAS in the database           manifests, Parquet
         │
         │ metadata_location
         ▼
  reconciler (one)        ──►   metadata/version-hint.text ──►  read hint
  DB head -> hint                                                open that JSON
                                                                 scan snapshot,
                                                                 branch, or tag
```

The database remains a Sage-operated writer dependency. Readers of a known
table do not need it.

## Status and remaining work

**Run end to end on NRDStor: `exp34`, 12/12, reproduced on a second clean
run.** Two concurrent committers arbitrated by the database with nothing
lost (Q1), the SQL catalog confirmed to publish no hint of its own (Q2), the
reconciler publishing the stem with no trailing newline (Q3), a reader with
no database seeing every committed row (Q4), a stale hint serving an older
*valid* snapshot silently and repairing on one pass (Q5), idempotence (Q6),
and an orphan metadata file failing to fool the reconciler (Q7).

The pieces had also been measured separately:

- the SQL catalog commits on Pelican and leaves no hint (`exp19`)
- the filesystem catalog writes a hint, and an independent reader loads the
  table from the warehouse alone (`exp32`, `TestAuditFilesystemCatalog`)
- DuckDB refuses a directory with no hint (`ndp_interop.py`)
- stem resolution, the newline rule, silent staleness and the numeric-hint
  failure (local fixture, measured above — no Pelican writes)

**Measured at the archive's real scale: `exp36`, 9/9** — 4,564 files, 608
day-partitions, 209 GB. The design holds; three things it assumed do not.
See `## Scale` below.

Remaining:

1. Decide the reconciler's period, which sets the freshness lag consumers
   see. Still owed, but no longer unconstrained, and the constraint is
   **not** the reconciler: a pass is 2–3 requests at any table size
   (`exp37` Q4), so it is affordable at any period. What bounds freshness
   is **commit** cost — ~1.3 s of local CPU at 4,564 files (`exp36` Q7)
   plus ~1 s of Pelican round trips per commit (`exp37`). The hint cannot
   be fresher than commits are, so pick the period from the commit cadence
   and treat it as free.
2. **Decide whether committers will ever run on separate hosts.** That,
   not reliability, is what picks the database — see below. Default to
   SQLite until the answer is yes.
3. Pin the two metadata-expiry properties **and set a snapshot-retention
   policy** — the second is the one that bounds growth, see above.
4. Publish the table index ("what tables exist?"), which the hint does not
   cover.
5. Promote `exp34`'s reconciler from a test function to a service: it is
   ~20 lines and already carries the no-newline assertion and the
   read-from-DB-not-from-listing rule.
6. **Fix `pelican_fileio.py` to stop HEADing every data file** — plumb
   `file_size_in_bytes` from the manifest into `PelicanInputFile`, or
   failing that cache sizes process-wide. 40% of reader requests,
   measured (`exp37` Q6c). This is the single largest cheap win in the
   whole design.
7. **Batch commits** rather than committing per file (10 min versus 1.2 h
   for the archive at day-batching; less still at monthly), and document
   unpruned full scans as a misuse of this deployment. **Commit cadence and
   partition granularity are separate decisions** — the steady-state plan
   is one *commit* per month, carrying ~30 *day-partitions*. Day-partitions
   are what make reads work (`exp36`: a one-week filter plans 7 files of
   608); partitioning by month to match the commit rhythm would undo that.

## Scale

`exp36`, against the real archive: 4,564 parquet files, 608 day-partitions,
209 GB (the 194 GiB `du` reports). Local timings — a tmpdir warehouse, the
archive read-only, nothing uploaded. **The scale question here is the
metadata and commit path, not throughput to Pelican**, which is still open.

**What holds.** Partition pruning works on the dimension that matters: a
one-week filter on a 608-partition table planned 7 files instead of 608,
6 ms instead of 218 ms. The read path scales.

**1. `add_files` cannot register the archive at all.** This is the finding
with the largest consequence and it was not visible at 100 rows.

The archive's `time` column is `timestamp[ns, tz=UTC]`, and pyiceberg 0.12
refuses it:

```
UnsupportedPyArrowTypeException: Column 'time' has an unsupported type:
timestamp[ns, tz=UTC]
```

Casting the *Iceberg* schema to microseconds does not help — `add_files`
reads the real footer, and the footer says ns. Neither escape hatch works
either: `downcast-ns-timestamp-to-us-on-write` governs `append` (which
rewrites bytes), and format-version 3, where Iceberg does have a native
`timestamp_ns`, raises `NotImplementedError: Writing V3 is not yet
supported` in pyiceberg.

So the cheap path — register 4,564 existing files in place, no byte
movement — **does not exist**. Ingest must rewrite to microseconds.
Measured on real files: **~26 MB/s**, about **2 hours of single-core CPU**
for the whole archive, one time, trivially parallel by day. The permanent
cost is on the steady-state path, where every incoming file is rewritten
rather than registered.

**2. The microsecond decision (§25) survives contact with real data.**
Worth checking rather than assuming, because 94% of sampled rows carry
sub-microsecond digits — truncation is discarding real bits. It discards
nothing that distinguishes a row: on 10M rows, duplicates of
`(time, name, node, plugin)` were **identical before and after**
truncation — zero new collisions — and the discarded remainder is uniform
across all 1000 values 0–999, the signature of clock granularity rather
than measurement. §25 stands, now for a measured reason.

**3. Commit cost is not flat.** Each commit rewrites manifest metadata
covering the files already in the table, so cost grows with accumulated
count:

| files in table | commit | scan planning |
|---|---|---|
| 1 | 3 ms | 1 ms |
| 300 | 63 ms | 112 ms |
| 600 | 155 ms | 200 ms |
| 1,200 | 333 ms | 405 ms |

Roughly 0.28 ms per accumulated file — ~1.3 s per commit at the archive's
4,564 files, locally, before any Pelican round trip. Nothing breaks, but it
sets a ceiling on commit frequency (hence the reconciler-period floor
above), and it is the argument for manifest compaction rather than an
ever-growing file list.

**4. Metadata growth** — see the expiry section above; the correction there
is the actionable one.

### Over Pelican (`exp37`, 12/12 on the real origin)

The numbers above are local. Re-measured against NRDStor, where the unit
that matters is **requests** at ~190 ms each, not milliseconds of CPU.

**The write path is flat.** Per-commit request count does not move as the
table grows — exactly 9 requests (`GET`×2, `PUT`×4, `HEAD`×3) from the
second commit on, whether the table holds 1 file or 40. The local
millisecond growth above does *not* become request growth. The bill is set
by the **number of commits**:

| | requests | wall |
|---|---|---|
| 4,564 single-file commits | ~40,800 | ~1.2 h |
| 608 day-batched commits | ~5,400 | ~10 min |
| **steady state: 12 monthly commits/yr** | **~108/yr** | **~0.3 min/yr** |

*(extrapolated from 40 measured commits)*. The first two rows are the
one-time backfill of the existing archive; **the third is the ongoing
cost, and it is nothing**. The commit path is not a constraint on this
design at the intended cadence — the monthly job's wall time is dominated
entirely by uploading ~10 GB of data (4–6 min at the 30–39 MB/s measured
in `exp38`) and by the ns→us rewrite, not by Iceberg.

**The reconciler is O(1)** — 2 requests to publish, 1 to confirm unchanged,
at any table size. It is not what constrains the period; commit cost is.

**The read path is the expensive side.** A cold reader pays ~5.1 requests
per data file, and that does not flatten. Iceberg's metadata layer is not
the cause — `metadata.json` and the manifest list are 4 requests total,
regardless of size. The cost is per data file, and **a third of it is a
defect in `pelican_fileio.py`**: it issues a HEAD per file to learn a size
the manifest already records in `file_size_in_bytes`. It caches size per
`InputFile` instance, but pyiceberg constructs a fresh instance per file
per scan, so the cache never survives. Measured with a process-wide cache:
**205 → 122 requests, 40% fewer.** The remaining 2 GETs per file are the
parquet footer then the row group — Iceberg's access pattern, correct.

Projected to 4,564 files *(extrapolated)*:

| read | requests | wall |
|---|---|---|
| full scan, as-is | ~23,400 | **~74 min** |
| full scan, sizes cached | ~13,900 | ~44 min |
| one-week partition filter | **~2,900** | ~9 min |

**Partition pruning holds over the network**, re-measured in requests
rather than inherited from the local file counts: 302 → **37 requests, 88%
fewer**. A full-table scan over HTTPS is the thing to design against, and
pruning — not a faster origin — is what makes consumers viable.

### Throughput, not just request count (`exp38`, 7/7, real bytes)

Everything above counts **requests**, because `exp37` registered a realistic
file *count* with tiny payloads to isolate the commit path. That left the
question consumers actually care about unanswered: how long does a real
query take? `exp38` uploaded real archive parquet and measured it.

**A sequential scan runs at ~5 MB/s. A concurrent one runs at 30–39 MB/s.**

| workers | throughput | one-week query *(extrapolated)* |
|---|---|---|
| 1 | 4.4–5.2 MB/s | ~9 min |
| 8 | ~30 MB/s | ~1.3 min |
| 16 | 34–39 MB/s | **~1 min** |

This needs **configuration, not code**: pyiceberg already maps scan tasks
onto a thread pool (`io/pyarrow.py:1847`), so `PYICEBERG_MAX_WORKERS=8..16`
buys 6.5–8.7× with no change to this design. The knee is at 8–16 workers,
consistent with §17's ~30 MB/s being a *per-stream* rather than a link
limit.

The pool is over **files**, which is a second argument for item 7: batch
size sets how many files a day-range query can parallelise across.

**Column projection is not the optimisation it looks like.** Selecting 1 of
13 columns moved 94% fewer bytes but took only 43% less time, and issued 20
*more* requests — a projected scan scatters small column reads instead of
streaming. Size queries by round trips, not bytes.

**A partial reprieve on row groups.** Archive files are 1 row group of 10M
rows, so pruning stops at the file. But ingest must rewrite every file
anyway (`add_files` cannot take ns timestamps), and pyiceberg emitted 2 row
groups per file unasked — so the exporter's `MaxRowsPerRowGroup` change is
additive rather than a prerequisite.

**Ingest is ~12 hours, one-time.** Real `tbl.append()` uploads ran at
3.9–5.2 MB/s (a raw PUT does 8.6; append also casts ns→us and re-encodes).

Two more items follow:

8. **Set `PYICEBERG_MAX_WORKERS` (8–16) wherever a consumer reads**, and
   say so in the consumer documentation. Nothing in the defaults hints
   that a reader is leaving 6–8× on the table.
9. **Document that queries are sized by round trips, not bytes**, next to
   the pruning guidance in item 7.

Not measured: concurrent *readers*, NRDStor under load, or a
well-connected host rather than a laptop link. The shape transfers — the
absolute rate does not.

### Storage overhead: the migration is net negative on bytes (`exp40`, 9/9)

Asked directly and not previously measured. Metadata had only ever been
counted in *requests* (§37: 4, O(1)) and as the growth of one file (§36 Q5:
~0.7 KB/snapshot). In bytes beside the archive:

| component | scales with | cost |
|---|---|---|
| manifests | **data files** | ~545 B/file → **2.5 MB for 4,564 files (0.0012%)** |
| `metadata.json` | **commits**, embeds all history | ~880 B/snapshot, retained forever |

**The rewrite this design already requires stores 83% of on-disk size
(§38), so 209 GB becomes ~173.5 GB.** Against a 0.5 MB metadata directory
at day-batched commits, adopting this design makes the archive **~35 GB
smaller**, not larger. That saving comes from re-encoding, not from
Iceberg — the same files through the same encoder with no catalog would
save the same bytes — but it does mean storage overhead is not an argument
against this design.

Two things this sharpens, both already in the list above:

- **Item 3 (retention) does not do what item 3 says.** Item 3 says snapshot
  expiry bounds metadata growth. **Measured, it does not.**
  `expire_snapshots()` rewrites history into a *new* metadata file and
  leaves every old one on disk — current file 34,318 → 9,531 B (−72%, the
  **read** cost, which is what §36 Q5 actually measured), retained total
  717,930 → 727,461 B (**+1%**). Two different costs; §36 Q5's conclusion
  was reused across them, and that was the error. The only knob that
  reclaims stored bytes is `write.metadata.delete-after-commit.enabled`
  with `previous-versions-max` (41 files/718 KB → 6 files/155 KB).

  **This no longer needs deciding.** The growth is quadratic in commits —
  163 MB at 608 day-commits, 34 GB/yr at hourly — but at the **monthly**
  cadence this design now targets it is **6.4 MB over ten years**, against
  roughly a terabyte of data. Keep `delete-after-commit` **off** so a
  published hint can never name a deleted file, and record the 6.4 MB as a
  known quantity rather than an unquantified risk. Item 3 should be
  rewritten to say *expiry is for the read cost*, which is real and worth
  having (−72% on the file every consumer downloads), and that storage
  retention is deliberately not used. §41 below measures the window and
  shows the defaults clear the safety requirement by ~8.4 years.
- **Expiry does not reclaim data files either**, which matters only later.
  Append-only, expiring 7 of 8 snapshots removed 0 data files and 0
  manifests. But after a `delete` (or the compaction `plans/4`
  contemplates), superseded files stay on storage unreferenced — measured,
  4 of 8 orphaned. pyiceberg 0.12 ships no orphan cleanup. Harmless for an
  append-only pipeline; a prerequisite before compaction.
- **Item 7 (day-batching) gains a third justification.** The same 32 files
  committed singly produced 765,779 B of metadata; batched, 23,746 B —
  **32× less**. Already justified on request count (§37) and read
  parallelism (§38); this is what keeps the quadratic above harmless.

### Sizing the retention window: it is arithmetic, not taste (exp41, 6/6)

The section above left `previous-versions-max` as a judgement call. It is
not — the window's behaviour is exact, and at this design's cadence the
defaults clear it by years.

**Retention is built into Iceberg.** Nothing custom to write: the two
properties in the table above are the whole mechanism. Deletion runs inline
on every commit (`table/update/__init__.py:735-752` trims `metadata_log`;
`catalog/__init__.py:858-870` deletes the difference).

**The window counts versions, not time.** 20 commits inside one second left
6 metadata files, not 20. pyiceberg 0.12 has **no age-based option for
metadata files** — `MAX_SNAPSHOT_AGE_MS` is a snapshot knob and, per the
section above, reclaims nothing. So the safe value tracks the **commit
rate**: "keep 5" is 5 hours at hourly commits, 5 days at daily ones, and
5 months at monthly.

**The hint failure is reproducible and exact.** With
`previous-versions-max=5`, a published hint's target was deleted after
commit #7, and a consumer following it got `FileNotFoundError` — not the
stale read this design accepts, but a hard failure until the reconciler
republishes. The break point is `previous-versions-max + 2` at every window
tested (2→#4, 5→#7, 10→#12), i.e. a published hint stays resolvable for
`previous-versions-max + 1` further commits.

So the constraint on item 3, in a form that can be checked:

> **`previous-versions-max` > commits per reconciler period + margin**

**At monthly commits the defaults already satisfy it, with room to spare:**

| cadence | margin at the default `previous-versions-max=100` |
|---|---|
| **monthly (this design)** | **~8.4 years** |
| daily | ~3.4 months |
| hourly | ~4 days |

**Recommendation: keep `delete-after-commit` off, and stop treating this as
an open item.** Not provisionally — the growth it would bound is 6.4 MB per
decade, which is less than one monthly manifest set, while turning it on
introduces a failure mode (a hint resolving to a deleted file) that this
design exists to avoid. Pin both properties explicitly rather than
inheriting them, per the table earlier in this document: an inherited
default that changes under a pyiceberg upgrade is exactly the kind of thing
that breaks quietly here.

**What would reopen it.** The window is count-based, so this conclusion is
a function of cadence, not of the design. If commits move to daily the
margin is still months; if they move to hourly or faster, both the growth
(34 GB/yr) and the hint risk become real, and the inequality above is how
to size the window — against a reconciler that is known to be running,
which is item 1's argument for promoting it from a cron job to a service.
