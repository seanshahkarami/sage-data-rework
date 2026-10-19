# Measured findings — cold storage research

> **Independent review (2026-10-14):** See [5_independent_review.md](5_independent_review.md)
> for corrections and independently reproduced limitations. In particular,
> the filesystem catalog works experimentally with one committer; the query
> prototype is not yet API-equivalent, and Go FileIO currently downloads
> entire selected files. Historical pass counts below have narrower coverage.

Work done 2026-10-08/09 on a local copy of the sage03 archive. Everything here
is measured, not estimated; the scripts are in `experiments/` and the raw JSON
in `experiments/results/`. `experiments/results/SUMMARY.md` has the full detail
and the numbers behind each claim.

Nothing is committed. `data/`, `scratch/` and `*.parquet` are gitignored.

## Setup

- conda env `sage_pipeline` (python 3.12, duckdb 1.5.6, pyarrow 25.0.1)
- `data/archive` — complete copy of `sage03:/media/local/archive-export-test/archive`,
  4,564 files / 194.3 GiB, verified byte-exact; 42,638,972,374 rows
- `data/iio-data`, `data/products` — the sibling derived products
- Bytes read are measured by serving Parquet over a local HTTP server that logs
  every Range request. That is the number that becomes Pelican traffic. Wall
  time on a local SSD hides it completely, which is why the local-disk numbers
  in earlier notes looked better than the design actually is.

## The headline

**The archive cannot prune inside a file, and one missing writer option is why.**

`archive-exporter/main.go` sets `SortingWriterConfig` but never
`MaxRowsPerRowGroup`, so each file is one row group of up to 10M rows. Page
indexes are written, but a reader still fetches whole column chunks.

Measured on `date=2025-01-01` over HTTP:

| query | rows returned | bytes read |
|---|---|---|
| `plugin = <rare>` | **1** | **49.0 MB** |
| `name = env.temperature` | 281,235 | 49.0 MB |
| `count(*)` | 59,496,597 | 458 KB |

One row costs 49 MB. On a 3-day slice (18 files, 910 MB) a rare plugin returns
12 rows for 146.6 MB.

Fixing row groups changes that to 0.45 MB for the same rare-plugin query on a
single file — about **120x less** — with identical rows returned.

## What each piece of the plan looks like after measuring

**`plans/1_draft.md` section 5 (folder vs column) is right about `plugin`
staying a column, for a reason the draft does not give.** The draft says
row-group statistics cannot isolate a plugin because min/max spans the batch.
That is only true of the *first* file of each day. Because `plugin` is the
leading sort key, files `data_1..N` hold the sorted tail and are often a single
plugin — 40 of 242 sampled files have plugin min == max. File-level statistics
therefore reject **232 of 242 files** for a rare plugin. The pruning problem is
not between files, it is *inside* the surviving ones.

**Section 7's manifest is worth building, and prunes better than expected.**
Over 30 days (242 files, 2.24 billion rows): a slim manifest carrying path,
rows, size and per-group `time`/`plugin` min-max is **80 KB**, read in under a
millisecond. Scaled to the full 4,564-file archive: **~1.5 MB in one request**
versus **281 MiB** of tail reads across 4,564 range requests. It also rejects
99% of files for a rare plugin and 86% for a measurement name. That weakens
the "we need Iceberg to do planning" argument at this scale — though not the
arguments about snapshots and safe rewrite.

The 281 MiB deserves a note, because an earlier version of this document said
~10 MB and both numbers came from our own measurements. The archive's Thrift
footers really are only ~2.2 KB each (~10 MB in total), but DuckDB never
requests a footer by itself — it pulls a 32-128 KiB tail chunk per file and
parses the footer out of it, so over 96% of what moves is slack. Verified by
logging every Range request (`experiments/probe_footer_cost.py`); see
`results/SUMMARY.md` §5. The manifest replaces the tail chunks, so it is worth
~30x more than the footer figure implies.

**The proposed first milestone passes.** Compacting 3 real days (178,902,746
rows) from 18 files to 9 and re-running four Beehive query shapes returns
identical records — compared by SHA-256 over the sorted record set, so a
reorder or dropped field would fail. Size drops to 83%, the rare-plugin query
gets 10x faster. What is *not* covered is the safety half: no manifest
publication, no reader-pinned catalog version, no deletion of superseded files.

**The query API can be served from Parquet without a new query language.**
`experiments/cold_backend.py` compiles the existing Beehive JSON query to
DuckDB SQL and emits `{timestamp, name, value, meta}`. 12 of 12 semantic checks
pass on real data, including the ones most likely to be got wrong:

- half-open `[start, end)` — the boundary instant appears only in the later half
- a range split in two is exactly the union, no gap, no duplicate
- `tail=N` is per *series*, not global (7 records for 7 series)
- the `influx_backend.go` quirk where `env.temperature` is an exact match with
  a literal `.` but `env.temp.*` is a real regex where `.` is any character

Aggregations (`experimental_func`/`experimental_window`, with their three
different timestamp rules) are not implemented yet.

**Late data: correction files work for appends only.** Writing a
`correction_0.parquet` into a published day is picked up, once, without
rewriting the day. But nothing distinguishes a correction from an original, so
there is no way to supersede a bad record or detect a duplicate re-delivery.
This is the strongest argument in these results for building the catalog.

**The archive carries no evidence about real data lateness.** Every footer's
`exported_at` falls in a single 29-hour window (2026-10-03/04), so the apparent
55–641 day "lag" is just `export date − day covered`, declining by one day for
each day later in the archive. This archive is one bulk backfill from October
2026, not a daily pipeline, and the spread measures its age rather than data
arrival. The open question "how long do late SAGE observations take to arrive?"
needs InfluxDB write timestamps and cannot be answered from these files.

## The concrete change to the exporter

Verified in Go, on real archive data, with the `parquet-go v0.32.0` the
exporter already uses:

```go
// 1. dictionary-encode the meta map's keys and values
Meta map[string]string `parquet:"meta,optional" parquet-key:",dict" parquet-value:",dict"`

// 2. real row groups
parquet.MaxRowsPerRowGroup(1_000_000),
```

| writer | vs today |
|---|---|
| today | — |
| + `MaxRowsPerRowGroup` only | 99.7% |
| + dict meta only | **74.0%** |
| both | **78.8%** |

Note the trap: `MaxRowsPerRowGroup` alone changes size by 0.3%, so anyone
testing it in isolation will conclude it does nothing. The size win is the
dictionary tag; the row groups are what make selective reads cheap. **They
should land together.**

A second condition, found only once this was measured over real Pelican
(`results/SUMMARY.md` §17): NRDStor costs **~150 ms per request** whatever
the size. Row groups turn one big read into many small ones, so a
*sequential* reader is slower with the fix than without it — 24.6s to pull
133 KB across 119 row groups. Issued 32-way concurrently the same reads take
0.65s. **The exporter change requires a concurrent reader to pay off**, and
row group size should be chosen against that 150 ms floor rather than
minimising bytes alone.

Across 8 days sampled over the whole 608-day span, rewriting with both fixes
gives **72.4%** aggregate (min 53.2%, median 68.1%, max 76.1%) — projecting
**194 GiB → 141 GiB, saving ~54 GiB**, at ~27s per day, parallel by day.

The spread is wider than the aggregate suggests and splits by era: 2025 days
(large, mixed-plugin) land at 74–76%, while 2026 days (small, ~5M rows) land
at 53–62%. The aggregate is byte-weighted and the sample's 2026 share (6.9%)
closely tracks the real archive's (7.2%), so the projection is sound — but
"stable" would be the wrong word for the per-day ratio.

A caution learned the hard way: the first attempt at this used pyarrow's
defaults and made most days *larger*. Go's `DELTA_BINARY_PACKED` on `time` beats
dictionary encoding by 12 MB on a single-plugin file, while dictionary encoding
beats plain strings by 20 MB on `meta`. Neither library's defaults win on both.
The encodings have to be set per column.

## Suggested next steps

1. Land the two-line exporter change and re-export a few days. Everything else
   depends on files that can be pruned.
2. Build the slim manifest as a real artifact and put planning behind it. It is
   80 KB per 30 days and already prunes well.
3. Extend `cold_backend.py` to aggregations, then diff it against live InfluxDB
   for a day — the one check this machine could not run.
4. ~~Test Pelican range GET against a re-exported file.~~ **Done — see
   `results/SUMMARY.md` §17.** NRDStor serves range reads, including the
   suffix read Parquet needs for its footer and multi-range. Real archive
   files were read over the real origin. But it surfaced a condition on
   item 1: **every request costs ~150 ms**, so the row-group fix only pays
   off if the reader issues ranges *concurrently* (32-way parallelism
   recovers 28x). A sequential reader over Pelican is slower with small row
   groups than without them.
5. Decide the catalog question with the late-data case as the test, since that
   is where the directory tree actually fails rather than merely being slow.
6. Settle the single-writer question before building either catalog. Both
   designs were run against the real origin: the custom catalog loses an
   update when two writers commit at once (3/3, silently — §18), Iceberg
   does not (3/3, no rows lost — §19), because its compare-and-swap lives
   in a database rather than on Pelican. Iceberg itself **runs** on
   NRDStor, so Pelican is no longer what rules it out — and as of
   2026-10-14 neither is the nanosecond timestamp loss: that was a
   pyiceberg limitation, not an Iceberg one. iceberg-go v0.7.0 writes v3
   `timestamptz_ns` bit-exact on real archive timestamps, both locally
   (§20, 7/7) and **onto the real NRDStor origin** (§21, 12/12).
   ~~**The single-writer question is now the whole argument.** If there will
   only ever be one writer, custom stays the cheaper answer — but that has
   to be an explicit decision, because it is the only thing the
   recommendation still rests on.~~

   **Answered 2026-10-14, and it settles the catalog question: use
   Iceberg.** The team's position is that cold storage must stay open to
   additional writers rather than being permanently locked to one. Since
   that was the only thing the "custom" recommendation still rested on, the
   recommendation moves to Iceberg. The decisive property is that the custom
   design's single-writer constraint **cannot be enforced by the storage** —
   the origin has no compare-and-swap, so a second writer produces a silent
   lost update (both sides report success, ~561 ms window) rather than an
   error. Fixing that requires a component that can do CAS, i.e. a database,
   at which point Iceberg's is already written. Accepted costs: a stateful
   catalog service, the ~190-line iceberg-go Pelican FileIO (§21), and ~5–9
   requests per commit. The Parquet-level findings in this document are
   unaffected — Iceberg indexes the files, it does not reformat them.
7. **Publishing to NDP is the reader path, and it needs no catalog.** A
   consumer given only a URL — with `nil` where the catalog goes — gets the
   schema with types, per-column documentation, licence, row/file/byte
   counts, history, predicate pushdown and the data, the first five without
   opening a single Parquet file (§23, 12/12). The SQLite database is the
   *writer's* component; publishing means handing over a URL. Two caveats:
   a bare URL does not see later commits (the fix is a 1-byte
   `version-hint.text` on storage, no database), and **DuckDB 1.5.6 cannot
   read `timestamptz_ns`** — it reads v2 + microseconds fine, and pyiceberg
   reads both, so the blocker is the nanosecond type rather than Iceberg or
   v3. That lands on the most likely NDP access path.

   **Settled 2026-10-14: store microseconds.** DuckDB support is a firm
   requirement, so the Iceberg table uses v2 + `timestamptz` and the
   sub-microsecond digits are not retained. This costs less than the 97.60%
   remainder figure suggests, because **DuckDB already truncates these
   timestamps today on plain Parquet with no Iceberg involved** (§25): its
   `TIMESTAMP` is microsecond precision and on 1.5.6 the loss happens at
   read time and cannot be recovered in SQL. Same file, same row — pyarrow
   reads `1735689606915629791`, DuckDB reads `1735689606915629000`. Two
   consequences worth carrying forward: the exporter should **keep writing
   `timestamp(nanosecond)` Parquet** so the precision survives at the source
   and the decision stays reversible, and the loss at the Iceberg boundary
   is permanent, so it is worth one confirmation that no SAGE measurement
   needs sub-microsecond resolution.

   One claim from §23 did **not** survive the real origin. iceberg-go also
   ships a filesystem catalog needing no database, which suggested Iceberg
   could run database-free. On Pelican it cannot (§24, 8/8): iceberg-go
   refuses that catalog on remote storage without an explicit
   `allow-unsafe-commits` opt-in, and the primitive its commit depends on is
   missing — `MOVE` with `Overwrite: F` returned 201 and **destroyed a
   committed metadata file**, as did `PUT` with `If-None-Match: *`. So the
   writer's database is genuinely required here. This removes a bonus rather
   than restoring an objection: the custom design has no database-free safe
   mode either, for the same missing compare-and-swap (§18). The reader
   result is untouched, and the `version-hint.text` pointer can simply be
   written by the writer — one extra PUT per commit.
8. Settle whether SAGE data will ever carry DOIs, because it is a second
   trigger and nothing here can measure it. Iceberg tags give the immutable,
   named, retrievable version a DOI must resolve to, and they survive
   aggressive snapshot expiry — 4 snapshots to 2, the untagged intermediate
   deleted, the tagged one kept and still readable (§22, 17/17). The same
   experiment settles the related question about schemas: a published schema
   **can** be changed later — columns added, columns renamed, no file
   rewritten, old rows still readable, because Iceberg resolves by field ID
   rather than by name. Widening is allowed, narrowing is refused. So the
   schema does not have to be perfect on day one, with the caveat that
   choosing too narrow a type still is permanent.
9. **Test whether the time-series table can also serve as the index to
   `/node-data`** (asked 2026-10-14). Iceberg cannot restructure object
   storage — it indexes rows in Parquet, and it has no notion of a JPEG;
   putting blobs *into* the table would defeat the range-read behaviour the
   whole design depends on (§5, §17). But the archive is **already**
   indexing the blobs informally, which is the part worth making
   first-class. Measured on `date=2025-01-01/data_0.parquet`: **43,353 rows
   with `name = 'upload'`, 1:1 with 43,353 distinct `value_str` URLs**
   pointing at `storage.sagecontinuum.org`, each carrying `filename` in
   `meta` alongside app keys. The blob is already a row with the same
   `(node, vsn, task, plugin, timestamp)` identity as every sensor reading.
   Declaring that index would make blob discovery a pruned query instead of
   a WebDAV listing, give `design_ideas.md`'s "single mechanism for batched
   Parquet files and BLOBs" a concrete form, and make orphan detection a
   join. **The load-bearing unknown is coverage**: whether every object in
   `/node-data` has an `upload` row, or only those a plugin chose to
   publish. If only some, the table is an incomplete index and cannot drive
   retention or integrity. That is an experiment, not a design question, and
   it is independent of the catalog choice. Full write-up in
   `4_catalog_recommendation.md`.
10. **Storage is already abstracted, for writing; moving an existing table is
    a migration** (§27, 15/15). `Data Pipeline Considerations.md` asks
    whether finalized long-term data could be routed to AWS or other
    platforms. iceberg-go resolves storage through a scheme registry and
    **already ships backends for S3 (plus S3-compatible: MinIO, Ceph,
    Alibaba OSS), GCS and Azure Blob — one import line each**, with
    `io.Register` exported for anything else, which is how our own Pelican
    IO attaches (§21, ~190 lines, no fork). So Pelican is already just
    another backend. Cost: importing all three backends took the probe
    module from 78 to 133 dependencies, so import only what is needed.
    **But a table written on one storage system cannot be moved to another
    by copying**: metadata records absolute locations (5 occurrences of the
    source root in one metadata file), and with the source deleted the copy
    opens its schema fine and then fails to read any data. Rewriting the
    JSON is insufficient because manifests are Avro, and iceberg-go v0.7.0
    has no rewrite helper. Prefer **re-exporting** to the destination, or
    writing there from the start, over moving. This is not an Iceberg
    advantage over the custom design on the second half — a custom manifest
    could use relative paths — but it is a clear one on the first.
    **The handover itself works**, which is the rest of that bullet
    (*"operational responsibility … would end"*): the partner needs only
    storage access and a `metadata.json` URL. They read with no SAGE
    database and get schema, column docs, licence and counts from the
    metadata alone (§23); if they want to keep appending — the nwf.org
    case — they adopt the table into their own catalog with the same one
    INSERT as item 11, after which it accepts commits (§28). No database
    export, no bespoke import tooling. That a URL-pinned reader does not
    see SAGE's later commits (§23) is correct behaviour for a handoff, not
    a gap; a partner wanting a live mirror needs replication instead.
    **Untested and worth checking before promising it:** adoption was
    measured on a table at the path its own metadata names, so
    copy-then-adopt should be assumed to hit the relocation failure above.
11. **A lost catalog is recoverable, and is not a data-loss event** (§28,
    15/15). Since §24 makes the catalog database mandatory on Pelican, it is
    a single point of failure worth testing. Deleting it entirely: the table
    still **reads** with no catalog at all (§23), so data is never
    unreachable — a lost catalog costs write availability, not data. Full
    recovery is **one INSERT** into `iceberg_tables` pointing at the newest
    `metadata.json`; the recovered table returns every row, keeps its
    snapshot history, and **accepts new commits**. Note the Spark
    `register_table` procedure is **not available on our catalog** — it
    exists on `rest`/`glue`/`hive`, not on `sql`, and is not on the
    `Catalog` interface — so the recovery step is ours to write, which is
    fine because it is one row. **The sharp edge is a stale pointer: a
    catalog restored one commit behind storage opens without error and
    silently serves older data** (measured: 200 rows where the newest
    snapshot has 300). Same failure shape as §18 and §27 — success status,
    wrong result. Mitigation is cheap: the newest metadata file is
    identifiable from storage alone, by `last-updated-ms` inside each file
    (cross-checked against filename order, which agrees because iceberg-go
    zero-pads). **Action:** write and rehearse a recovery runbook, back up
    catalog and storage together, and validate after restore by comparing
    the catalog's `metadata_location` against the newest file on storage.
    Untested: Postgres rather than SQLite, and whether snapshot expiry can
    delete metadata a stale catalog references (turning silent-stale into
    hard-fail).
12. **DuckLake was evaluated and rejected, on the Go client** (§29, 11/11 and
    4/4). DuckLake puts *all* metadata in the catalog database and only
    Parquet on storage, on the argument that Iceberg needs a database anyway
    and so gains nothing from also keeping metadata in files. Measured
    against our own constraints it wins three: **relocation works** — its
    catalog stores paths relative (`path_is_relative`), so the same
    destroy-the-source move test that breaks Iceberg (§27 Q2) reads 200/200
    rows and accepts new writes, and a mismatched data path is **refused
    with an error** rather than opening and failing later; **nanoseconds
    round-trip** with no format-version opt-in; and a **frozen lake served
    over HTTP** reads with no local catalog and no local data, read-only —
    the §23 publishing property. It loses on one thing that cannot be
    configured away: **there is no Go client.** One implementation (the
    DuckDB extension) plus a Rust DataFusion port; every Go project in the
    space drives DuckDB through cgo. Adopting it means embedding DuckDB in
    the exporter or writing a spec implementation, and it would undo §20/§21
    — the results that unblocked nanoseconds by moving to iceberg-go. Also:
    **no `timestamptz_ns`** (the archive's actual column type), and a 29-table
    catalog with a row per data file, so §28's one-INSERT recovery has no
    analogue and a lost catalog is far closer to real data loss (inferred
    from the schema, **not measured**). **Decision: stay with Iceberg.**
    Revisit if a native Go writer appears, or if partner handoff (§27 Q2)
    becomes a primary requirement rather than a bullet — DuckLake proves
    that weakness is not inherent to the model.

13. **That rejection was re-derived the same day, and now rests on reader
    reach alone** (§30). The Go-client requirement behind item 12 was
    withdrawn — Python is acceptable — so the decision was re-examined
    rather than kept with new reasoning substituted underneath it.
    Measured on the real origin: **DuckLake runs on Pelican.** It needs a
    custom fsspec filesystem to write there, but Iceberg needs the same
    kind of glue (`pelican_fileio.py` §19, `pelican_io.go` §21), so that is
    not a discriminator; with it, commits land, Parquet reaches the origin
    (PROPFIND 207) and nanoseconds survive. On the §18/§19 concurrency test
    DuckLake **loses no acknowledged row** — the losing writer is refused
    loudly, where the custom catalog lost 3/3 silently — matching Iceberg,
    and for the same reason Iceberg is safe: the guarantee lives in the
    catalog database, not in the object store. **Two of item 12's three
    objections do not survive.** The nanosecond one inverts — pyiceberg
    0.12.0 cannot write v3 at all and v2 rejects `timestamp[ns]`, so a
    *Python* Iceberg writer stores no nanoseconds on 97.60% of rows while
    DuckLake stores them — but that reversal is **moot under the
    2026-10-14 microsecond decision**, which keeps no nanoseconds in
    either format and explicitly unblocks a pyiceberg v2 writer. It is
    neutral unless that decision is reopened. What survives is
    **reader reach** (§23: NDP,
    Spark, Trino and the cloud warehouses read Iceberg; DuckLake has one
    implementation) and catalog-loss recovery (§28's one INSERT vs 29
    tables) — the latter **still inferred, not measured**, and now
    load-bearing, so it should be tested. **Decision: stay with Iceberg, on
    reader reach** — a narrower basis than item 12 claimed. **Action:**
    measure DuckLake catalog loss, and test Postgres as the catalog (the
    §30 concurrency result is SQLite, as §19's was). **[Superseded by item
    16: DuckLake is out, so the catalog-loss test is cancelled; Postgres
    remains open for Iceberg only.]** Revisit if the
    archive's consumers turn out to be DuckDB/Python rather than
    Spark/Trino shops. A methodological note worth carrying: four separate
    times the Pelican concurrency probe was about to record a DuckLake
    failure that had not happened — `database is locked` is also how
    DuckLake *correctly* refuses a losing commit, and a check that cannot
    tell a refusal from a loss manufactures findings (§19 had the same
    near-miss).

14. **The catalog database is NRDStor's requirement, not Iceberg's — and
    the search for a way around it is now complete (§31).** §24 concluded
    that a database-free filesystem catalog cannot be made safe on Pelican
    after testing two primitives (`MOVE` `Overwrite: F`, `PUT`
    `If-None-Match: *`). Because that is the *only* surviving objection to
    Iceberg and the sole reason SAGE must run a catalog database, it was
    re-tested against the other seven conditional primitives: `If-Match`,
    `If-Unmodified-Since`, the WebDAV `If:` header, `COPY` `Overwrite: F`,
    `LOCK`, and `MKCOL` on an existing collection. **None works, and the
    conclusion is unchanged** — but the reason §24 gave was wrong in a way
    that mattered. §24 said the origin "honors neither guard", implying it
    read the headers and ignored them. It does not read them: the origin is
    **XRootD 5.9.7's HTTP plugin**, advertising `DAV: 1` (no `LOCK` — that
    is class 2), returning **no `ETag` and no `Last-Modified` on any
    response** (so both standard validators are inexpressible, not ignored),
    and **acking a nonsense method with 200**. Its `Allow` list is wrong in
    both directions: `MOVE` is unadvertised but works atomically, `COPY` is
    advertised but silently copies nothing. **Consequence:** there is no
    eighth header to try, so "maybe we missed something" is now answered;
    and the requirement is correctly attributed — the same gap would defeat
    *any* format's database-free commit, DuckLake's included, so it is not a
    discriminator between them. **Action:** file a narrow feature request
    with the Pelican/NRDStor operators — the atomicity already exists at the
    POSIX layer (`MOVE` is atomic, §17), so **either a `MOVE` honouring
    `Overwrite: F` or an `ETag` on `GET`/`HEAD` would retire the catalog
    database entirely**. Methodological note: round 1's `COPY` and `LOCK`
    results would have been reported as "conditional ignored" without the
    round-2 controls, which is the right verdict for the wrong reason — when
    an origin acks everything, a negative result needs a positive control
    before it means anything.

15. **The filesystem catalog *does* run on Pelican — with exactly one
    writer (§32).** *(Read "writer" as "committer" — see item 17.)* §24 and §31 both concluded against the database-free
    catalog, and both did it by probing HTTP primitives; **neither ever
    built a table**. Implementing the eight interfaces
    `hadoop.HadoopCatalogFS` requires and running the catalog for real
    gives **6/6**: table created on Pelican, `version-hint.text` written
    and advanced `1`→`2`, the pointer naming a metadata file that exists,
    and — the one that matters — **an independent catalog instance loading
    the table from the warehouse URL alone, with no database anywhere in
    the picture**. That restores the property §28 found missing from the
    SQL-catalog layout, where a recipient must be *told* which of 11
    `*.metadata.json` files is current. **The catch, measured not
    asserted:** seven of the eight methods are honest, but
    `RenameNoReplace` cannot be — §31 proved the origin cannot express
    "fail if the destination exists" — so it degrades to check-then-move.
    Four concurrent claims onto one destination: **three writers told they
    won, one actually landed, two acknowledged commits silently lost.**
    **Consequence:** "no filesystem catalog on Pelican" is too strong a
    reading of §24. The accurate claim is *correct with one writer, silently
    lossy with two*, which converts a capability limit into an operational
    risk judgement — can SAGE guarantee a single writer? If so, the catalog
    database goes away entirely, along with the §28 recovery procedure and
    the reader's dependence on a SAGE endpoint. **Action:** decide the
    single-writer question deliberately; if it is plausible, test §32's
    three open items (realistic write volume, torn `version-hint.text`
    reads, enforceability of the single-writer lock). Methodological note:
    the single-writer test passed while carrying an *unverified* caveat
    about concurrency inherited from §18 — writing the race test turned an
    inherited argument into a measurement, and a caveat bolted onto a
    passing test is exactly the kind that gets skimmed past.

16. **DuckLake eliminated; Iceberg is the format (decided 2026-10-14,
    after §32).** The deciding constraint is **reader independence**: a
    reader must not need a SAGE-hosted endpoint to read the archive.
    DuckLake keeps *all* metadata in its catalog database, so reading one
    means reaching its `meta.db`. **Precision worth keeping, because an
    earlier framing got it wrong:** Pelican *can* host a `meta.db` — §30
    attached one over HTTPS with range reads — but only **read-only**,
    because DuckDB's httpfs cannot write. For an append-only-forever
    archive a read-only copy is a stale fork the moment the next commit
    lands, so a live DuckLake requires a database SAGE hosts and serves.
    **Why this is now decisive rather than a preference:** until §32 the
    comparison was Iceberg's *one mutable catalog row* against DuckLake's
    *whole database* — a difference of degree, and §29/§30 had shown the
    margin was narrow. §32 showed the Iceberg filesystem catalog runs on
    Pelican with an independent reader loading the table **from the
    warehouse URL alone, no database anywhere**. Iceberg's floor is zero
    SAGE-hosted components; DuckLake's is one, unchangeably. That is an
    asymmetry in kind. **This overrides real measurements against us** —
    §30 had DuckLake winning or tying on relocation, failing loudly,
    Python nanoseconds, and concurrency — and does so deliberately:
    reader independence is the one property that cannot be recovered later
    by changing a setting. **Cancelled as a consequence:** the DuckLake
    catalog-loss test (previously the top-ranked open measurement, item 13)
    — the decision no longer rests on it, so it cannot change the outcome;
    recorded as dropped, not forgotten. **Still open, for Iceberg only:**
    Postgres as the catalog, since §19's concurrency result is SQLite.
    **Reopen only if** the reader population turns out to be entirely
    DuckDB/Python *and* SAGE commits to hosting a metadata service
    indefinitely.

17. **"Single writer" constrains the commit, not the data path — parallel
    ingestion is unaffected (§33).** Item 15's phrasing reads as "SAGE must
    ingest the archive single-threaded", which would be a serious cost and
    is not what the measurement says. Two different things get called
    "writing": **producing data files** (encoding Parquet, uploading — the
    expensive part, and it touches the catalog not at all) and
    **committing** (appending them to the table and advancing
    `version-hint.text` — one small metadata operation, and the only part
    the missing CAS affects). The hadoop catalog needs the second
    serialized and says nothing about the first; iceberg-go's
    `AddFiles(ctx, filePaths []string, ...)` takes a *list* of
    already-written files, which is exactly the shape that lets N parallel
    producers feed one committer. Measured, **4/4**: four producers wrote
    Parquet to Pelican concurrently (250 rows each), one committer added
    all four files in a single commit, all 1000 rows present with none
    lost, and the pointer advanced **once** (`1`→`2`) rather than four
    times. Same storage, same adapter and same worker count as the race
    test that lost two commits — the only difference is whether the
    parallelism sits in the data path or the commit path.
    **Consequence:** the constraint costs **one serialized metadata call
    per batch**, not per file and not per row. Parallel production,
    parallel uploads and parallel readers are all unaffected, and "one
    process owns the commit" — a single ingest service, a queue with one
    consumer, a cron that cannot overlap itself — is a conventional
    architecture rather than a concession. What is genuinely ruled out is
    *two independent processes committing to the same table at once*: two
    replicas of the ingest loop, a backfill racing the live writer, an
    operator re-running a job by hand. **Action:** when item 15's
    single-writer question is decided, decide it about the **committer**;
    sizing the ingest pipeline is a separate question and this result does
    not constrain it. Methodological note: two harness bugs surfaced here
    and both failed misleadingly — the §32 adapter minted tokens against a
    hardcoded prefix (invisible while one test used it; presented as
    "namespace does not exist" when a second did), and `iceio.Register` is
    process-global and last-call-wins, so running all three tests together
    made §32 fail **with no output at all**. Both were found by running the
    tests in one invocation, which is how they should keep being run.

18. **The plans/6 design runs end to end on NRDStor — many committers, one
    reconciler, a reader with no database (§34, 12/12).** The design splits
    the two jobs the catalog was doing: writers commit through an Iceberg
    **SQL catalog**, so the database is the CAS that arbitrates them, and a
    **single reconciler** publishes `metadata/version-hint.text` to Pelican
    from the committed head, so consumers find the current metadata JSON
    with no SAGE endpoint. Every piece had been measured separately (§19,
    §32, `ndp_interop`, a local fixture) and the *combination* never had.
    **Measured on the real origin:** two concurrent committers both landed
    — one hit the database CAS, retried once, succeeded — 100 rows, 50 per
    committer, nothing lost; the SQL catalog confirmed to publish no hint of
    its own; the reconciler publishing the stem with no trailing newline; a
    reader with **no database** resolving the hint and seeing every
    committed row; idempotence on a second pass; and an orphan
    `09999-*.metadata.json` from a simulated failed commit failing to fool
    it. Reproduced on a second clean run.
    **The result that matters is the failure mode.** A stale hint served an
    older *valid* snapshot **silently** — 50 rows, no warning, while the
    newer metadata sat in the same directory and the database still named it
    as head — and one reconciler pass restored all 100. Compare §32 on the
    same storage: 3 of 4 writers told they won, 2 commits gone for good. So
    this design converts §32's **silent unrecoverable loss** into **silent
    recoverable staleness**, which is the entire argument for it. Two
    corollaries: the hint is authoritative, so clients never notice a newer
    file and detection must be *active* (the reconciler comparing published
    hint against `metadata_location`); and correctness never depends on the
    reconciler running, only freshness does.
    **Consequence:** this is the first design measured on this storage that
    gives **both** multi-writer safety and reader independence. Every prior
    option forced a choice between them — §32 had reader independence and
    lost commits, the SQL catalog alone had safety and a reader dependency
    on a SAGE endpoint. **Action:** (a) decide the reconciler's period,
    which sets the freshness lag and is the one number the design still
    owes; (b) ~~re-run Q1 against Postgres~~ **— withdrawn, see item 19**; (c) pin the two metadata-expiry properties; (d) the
    table index ("what tables exist?") is still uncovered.
    **Two deployment facts found by running it, both of which fail
    misleadingly:** `catalog_name` is part of the primary key in
    `iceberg_tables`, so two services opening the catalog under *different*
    names silently get private catalogs over one warehouse and the second
    committer fails with `NoSuchTableError` against a table that plainly
    exists — pin the catalog name in deployment config. And pyiceberg lays
    tables out as `<warehouse>/<ns>/<table>` with **no** Hive `.db` suffix,
    so the reconciler must derive the hint path from `metadata_location`
    rather than construct it. Neither bug could appear in a test of any
    single piece, which is the argument for end-to-end runs over more unit
    probes.

19. **SQLite is sufficient as the catalog; the constraint is connectivity,
    not concurrency (§35, 4/4). This corrects item 18.** Item 18 named
    "re-run Q1 against Postgres" as the highest-value open item. That came
    from SQLite's reputation, not from a measurement, and §34's Q1 was the
    easiest possible case — threads, one process, a local file. Measured
    properly with separate committing **processes**: **6 → 60/60 rows (1
    attempt), 12 → 120/120 (2 attempts), 24 → 240/240 (2 attempts)**, no
    loss and no exhausted retries at any level, wall time under 3 s
    throughout. Retry cost was measured on purpose, since a database that
    "works" but needs 30 attempts does not work; this is nowhere near that.
    **There is no concurrency-correctness argument against SQLite at this
    scale.** **The real limit:** `sqlite://db-host:5432/catalog.db` →
    `ArgumentError: Invalid SQLite URL`. A SQLite URI is a **file path**,
    with no network protocol, so every committer must share a filesystem.
    The question was never "is SQLite reliable enough" but **where do the
    committers run** — one host or pod, and SQLite is the simpler correct
    choice; separate hosts, and Postgres is needed because SQLite cannot be
    *connected to*, which is a deployment question rather than a
    reliability one. **Action:** default to SQLite; decide whether
    multi-host committers are in scope, since that is the only thing that
    forces Postgres. `SqlCatalog` takes both behind one interface, so the
    migration is a URI change if it ever comes. **Caveat:** measured on
    local APFS. SQLite over NFS has the locking problems its own docs warn
    about, so a *shared volume* used to get multi-host committers onto one
    SQLite file is the configuration to avoid — not SQLite itself.
    **Methodological note:** this is the second conclusion in this project
    to rest on reasoning rather than a result (§24 was the first), and both
    were caught by someone asking why. The pattern: §34's Q1 *passed*, and
    the unsupported recommendation was attached to it — the same position
    §32's unverified concurrency caveat held before the race test. A claim
    bolted onto a green result does not inherit its evidence.

20. **The design holds at the archive's real scale, but three things it
    assumed do not** (`exp36`, 9/9 — 4,564 files, 608 day-partitions,
    209 GB; local metadata/commit path only, nothing uploaded).

    a. **`add_files` cannot register the archive.** Its `time` column is
       `timestamp[ns]` and pyiceberg 0.12 refuses the footer; casting the
       Iceberg schema does not help, `downcast-ns-timestamp-to-us-on-write`
       only governs `append`, and v3 (which has native `timestamp_ns`) is
       not writable yet. **The cheap in-place path does not exist** —
       ingest must rewrite to microseconds. ~26 MB/s measured, ~2 CPU-hours
       for the archive once, parallel by day; a permanent per-file cost
       thereafter. *Action: treat rewrite-to-us as part of ingest, not a
       migration step.*

    b. **"Pin metadata expiry off" conflated two mechanisms.** Keep
       `write.metadata.delete-after-commit.enabled` **off** (it deletes
       files published hints may name) but **turn snapshot expiry on**
       (`table.maintenance.expire_snapshots()`, retention policy). Only the
       second bounds growth, and it is safe for the hint — it writes a new
       metadata file and leaves the old one readable (Q6, 3/3). Without it
       the file every consumer fetches per read grows ~0.7 KB per snapshot:
       **29 MB, and 627 GB of `metadata/`, at hourly commits over 5 years.**
       *Action: set a retention policy; correct plans/6 (done).*

    c. **Commit cost grows with accumulated file count** — ~0.28 ms per
       file, ~1.3 s per commit at 4,564 files, locally, before Pelican.
       *Action: this floors the reconciler period (open item 1 of plans/6)
       and argues for manifest compaction.*

    Holding up: partition pruning (608 → 7 files, 218 → 6 ms), and §25's
    microsecond decision, which now has evidence — 94% of rows carry
    sub-microsecond digits, but truncation produced **zero** new
    `(time,name,node,plugin)` collisions in 10M rows and the discarded
    remainder is uniform over 0–999, i.e. clock granularity.

    **Methodological note:** I first recorded `expire_snapshots` as absent
    from pyiceberg 0.12 on an `AttributeError` from an entry point I had
    guessed. It exists, at `table.maintenance.expire_snapshots()`. Grepping
    the installed package would have — and did — settle it immediately.
    A negative result about a library is worth one grep before it becomes a
    constraint in a design doc, particularly when it is the finding that
    makes a problem look unsolvable.

21. **Over Pelican, the write path is flat and cheap; the read path is
    neither — and 40% of its cost is ours to fix** (`exp37`, 12/12 against
    the real NRDStor origin; 11/11 in `sage_pipeline`, where Q7 skips).
    Closes the open item §36 left: the network cost of the commit path.

    a. **Per-commit request count does not grow with table size.** Exactly
       9 requests from the second commit on, at 1 file or 40. §36's local
       millisecond growth does **not** become request growth. So the bill
       is the number of COMMITS: 4,564 single-file commits ≈ 40,800
       requests / 1.2 h, versus 608 day-batched ≈ 5,400 / 10 min — both
       extrapolated. *Action: batch commits by day.*

    b. **The reconciler is O(1)** — 2 requests to publish, 1 to confirm
       unchanged, at any table size. It is not what bounds the reconciler
       period; commit cost is.

    c. **A cold reader pays ~5.1 requests per data file and that does not
       flatten.** Iceberg's metadata layer is innocent: `metadata.json`
       and the manifest list are 4 requests total regardless of size. The
       cost is per file — and **one third of it is a defect in
       `pelican_fileio.py`**, which HEADs each data file for a size the
       manifest already carries (`file_size_in_bytes`). It caches per
       InputFile instance, but pyiceberg builds a fresh one per file per
       scan. Measured fix: **205 → 122 requests, 40% fewer.**
       *Action: plumb `file_size_in_bytes` into `PelicanInputFile`.*

    d. **Partition pruning holds over the network**, re-measured in
       requests rather than inherited from §36's local file counts:
       302 → **37 requests, 88% fewer**, on a 60-partition table.

    Projected full scan of 4,564 files: ~23,400 requests / **74 min**
    as-is, ~13,900 with sizes cached, **~2,900 / 9 min** with a one-week
    filter. *Action: treat unpruned full scans over HTTPS as a misuse of
    this deployment and document it as such.* Partition pruning, not a
    faster origin, is what makes the read path viable.

22. **Querying over Pelican is viable, but only with a concurrent reader —
    and the default reader is not one.** (exp38, 7/7, real archive parquet
    on the real origin; §38.)

    §37 measured query *planning* and found it scales. It did not measure
    *execution*, because it used tiny payloads to isolate the commit path.
    This measures execution with real bytes.

    a. **A sequential Iceberg scan runs at ~5 MB/s; a concurrent one at
       30–39 MB/s.** Same query, same data, 6.5–8.7× apart. For a
       one-week query over the real archive that is **9 minutes versus
       ~1 minute.** pyiceberg already parallelises scan tasks over a
       thread pool (`io/pyarrow.py:1847`), so this needs **configuration,
       not code**: `PYICEBERG_MAX_WORKERS=8..16`.
       *Action: set it wherever a consumer reads, and document it — a
       reader that does not is 6–8× slower for no reason.*

    b. **The pool is over FILES**, so a query touching one file gets no
       parallelism. This is a second argument for day-batching commits
       (finding 21): batch size sets how many files a query can spread
       across.

    c. **Column projection saves bytes but not time.** 94% fewer bytes
       (141.7 → 8.6 MB) bought only 43% less time and cost 20 *more*
       requests, because a projected scan scatters small column reads
       instead of streaming. **Size queries by round trips, not bytes.**
       My own first projection scaled by bytes and was wrong by an order
       of magnitude (~5 s predicted vs ~35 s measured-and-scaled);
       corrected before publication.

    d. **The 1-row-group problem is partly self-fixing.** Archive files
       are 1 row group of 10M rows, so pruning stops at the file. But
       ingest must rewrite every file anyway (finding 20a: `add_files`
       cannot take ns timestamps), and pyiceberg emitted **2 row groups
       per file unasked**. The exporter's `MaxRowsPerRowGroup` change is
       therefore **additive, not a prerequisite** — a softening of the
       recommendation above, not a retraction.

    e. **Ingest costs ~12 hours, one-time.** Real uploads through
       `tbl.append()` ran at 3.9–5.2 MB/s (slower than a raw PUT's 8.6,
       because append also casts ns→us and re-encodes). 209 GB at that
       rate is ~12 h. Stored size was 83% of on-disk.

    Not measured: concurrent readers, NRDStor under load, or a
    well-connected host. The shape transfers; the absolute rate does not.

23. **"Iceberg is slower than plain Parquet" is unsupported — latency does
    not choose between the two designs.** (exp39, 5/5 over four runs on the
    real origin; §39.)

    Same files, same origin, same FileIO, same process, interleaved, with
    `catalog.py`'s `CatalogStore` ported to Pelican.

    | | Iceberg | custom | ratio |
    |---|---|---|---|
    | write 169 MB | 23.9 s | 25.2 s | 0.95× |
    | read all | 4.6 s | 7.1 s | 0.6× |
    | read one day | 2.9 s | 3.6 s | 0.8× |

    a. **The write penalty I predicted does not exist.** I expected the
       mandatory ns→us rewrite (finding 20a) to show up as latency. Both
       paths are network-bound at ~6 MB/s, and the rewrite sends **17%
       fewer bytes**, so it pays for its own CPU. Run-to-run spread
       (wall 0.86–1.05×) exceeds the gap between designs. *Recorded as a
       refuted hypothesis, not quietly re-aimed.*

    b. **Iceberg's metadata overhead is fixed, not per-file.** 12 requests
       vs the custom catalog's 2 — but neither grows with file count, and
       at 24 data files Iceberg is already at 0.50 requests/file and
       falling. Any query big enough to care amortises it.

    c. **The custom catalog scales worse in one specific way.** Its
       snapshot JSON embeds every live file: 274 B/file → **~1.3 MB at
       4,564 files, fetched in full on every read, unprunable.** Iceberg
       splits the same information across `metadata.json` and
       per-partition manifests, so a pruned reader fetches a subset. The
       mirror image of finding 20b: both have O(n) metadata; Iceberg's is
       partitioned.

    d. **Method note.** The first run showed Iceberg 3× *faster* on reads.
       That was an artefact — pyiceberg's scan is parallel and my custom
       reader was a `for` loop. Finding 22a (concurrency is worth 6–8×)
       is what made it recognisable. Pinned both to 8 workers and the gap
       vanished. *A comparison is only a comparison if what you are not
       studying is held equal.*

    *Consequence for `plans/4`:* its recommendation rests on operational
    surface and safety, which this does not disturb. But any argument that
    reaches for "Iceberg adds latency" should be struck — that claim is
    now measured and false.
