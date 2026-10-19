# Iceberg or a custom catalog?

> **Independent review (2026-10-14):** Read [5_independent_review.md](5_independent_review.md)
> before treating the historical conclusions below as a current decision.
> A filesystem catalog is a demonstrated single-committer prototype on this
> origin (§32–33); it is not prohibited by Pelican. The missing atomic
> no-replace operation makes this adapter unsafe for concurrent commits.
> Earlier claims that a database is always required remain below as history,
> but are superseded. The review also identifies query and test-coverage gaps.

> **Superseded (2026-10-14): the recommendation is now Iceberg.** The body
> below argues "custom, for now" and is kept because the reasoning is still
> the record of what was tested. But it rested on one surviving reason, and
> that reason was explicitly conditional: *"More than one writer needs to
> commit concurrently… If a second writer appears, 'custom' stops being the
> safer option."* The team has now stated that multi-writer must stay open —
> cold storage is to remain writable by more than one producer. That is not a
> new argument, it is the trigger this document named in advance. See
> **"Decision (2026-10-14): Iceberg"** at the end.

Short answer: ~~**custom, for now — but on one reason, not two, and it is the
weaker of the two originally given.**~~ **Iceberg** — see the box above; the
single reason for "custom" was conditional on a constraint that has been
withdrawn.

This document was first written on two independent reasons:

1. Iceberg cannot run on Pelican without standing up a separate stateful
   service, and what it needs from Pelican is a **superset** of what the
   custom catalog needs.
2. ~~**Iceberg cannot currently store the archive's timestamps without
   losing data.** Tested directly, not inferred.~~

**Reason 2 is withdrawn (2026-10-14).** It was called "the harder blocker,
and the one to check first" — so it was checked, and it does not hold.
Iceberg v3 has `timestamptz_ns`, and iceberg-go v0.7.0 writes it bit-exact
on real archive timestamps (`exp20`, 7/7 checks). The original finding came
from pyiceberg raising `NotImplementedError`, which is a limitation of that
*client*, not of Iceberg. Writing a client failure up as a format limit was
the mistake; it is recorded in full below rather than quietly fixed, because
it came close to settling the decision on its own.

**And reason 2 is now closed from the other end as well.** On 2026-10-14 the
team decided to store **microseconds**, because supporting DuckDB is a firm
requirement and DuckDB 1.5.6 cannot read `timestamptz_ns` (§23). So the
timestamp question no longer gates anything in either direction: the format
can carry nanoseconds, and we have chosen not to use that. Full rationale
under "Decision (2026-10-14)" below — including the measurement that makes
it cheap, which is that **DuckDB already truncates these timestamps today on
plain Parquet, with no Iceberg involved** (§25).

Reason 1 survives contact with the real storage, but **came out weaker than
written and needs restating too.** Pelican has now been measured
rather than assumed (`exp17`, §17): the custom catalog's one required
primitive — an atomic pointer swap — **exists** on NRDStor as `MOVE`,
verified under concurrent readers. The one thing the origin does not offer
is compare-and-swap.

That last gap was described here as constraining "both designs equally."
It does not. Measured head to head on the same origin:

| | two concurrent writers | why |
|---|---|---|
| custom catalog (`exp18`) | **lost an update 3/3, silently** | nothing enforces the check |
| Iceberg (`exp19`) | **0 rows lost, 3/3, conflict detected** | CAS in its database |

**Iceberg is safer here than the custom catalog, and Pelican does not rule
it out.** An Iceberg table runs on NRDStor — data and metadata — once given
a ~230-line FileIO (`experiments/pelican_fileio.py`); the "no FileIO for
this storage" objection is a packaging gap, not a capability limit. With
reason 2 withdrawn, what remains against Iceberg is the stateful service its
commit requires.

**That claim needs one correction too (2026-10-14).** It was written as
"every catalog pyiceberg ships needs a service or a database and no
filesystem catalog exists at all." True of pyiceberg; **false of
iceberg-go**, which ships a filesystem catalog that keeps the current-version
pointer on storage as a 1-byte `version-hint.text` and needs no database at
all (§23).

**And that correction needed a correction of its own — it does not survive
Pelican (§24).** The first version of this paragraph went on to say *a
database is required only if concurrent writers must be safe*, inferring
from a local-disk result. Tested on the real origin, it is wrong here:
iceberg-go **refuses** a filesystem catalog on remote storage without an
explicit `allow-unsafe-commits` opt-in ("this implementation is not atomic
and can lose data"), and the primitive its commit depends on is absent —
`MOVE` with `Overwrite: F` returned 201 and **destroyed a committed metadata
file**, as did `PUT` with `If-None-Match: *`. So on this storage the
filesystem catalog does not degrade to single-writer-but-correct; it
publishes over a committed version with a success status.

The accurate statement, scoped properly: *on local disk a database is
optional; on Pelican it is not.* This is a measurement about the origin, not
about Iceberg — and the custom design has no database-free safe mode either,
for exactly the same missing CAS (§18, 3/3 silent lost updates). It removes a
bonus for Iceberg rather than restoring an objection.

**Re-tested and closed (§31).** Because this is the only surviving objection
to Iceberg, it was worth more than §24's two probes, so the remaining seven
conditional primitives were swept: `If-Match`, `If-Unmodified-Since`, the
WebDAV `If:` header, `COPY` with `Overwrite: F`, `LOCK`, and `MKCOL` on an
existing collection. None works, and the controls explain why in a way §24
did not. The origin is **XRootD 5.9.7's HTTP plugin**: it advertises
`DAV: 1` (so no `LOCK`, which is class 2), returns **no `ETag` and no
`Last-Modified`** on any response (so the standard validators cannot be
expressed at all), and **acks a nonsense method with 200** — meaning a 2xx
here proves nothing was parsed, rather than that a condition was evaluated
and overridden. Its `Allow` list is wrong in both directions: `MOVE` is
absent but works, `COPY` is listed but silently copies nothing.

So the search is complete; there is no eighth header to try. Two things
follow. First, the requirement is correctly attributed — **the database is
NRDStor's requirement, not Iceberg's**, and the same gap would defeat any
format's database-free commit, DuckLake's included. Second, there is a
narrow, concrete ask for the Pelican/NRDStor operators: the atomicity
already exists at the POSIX layer (`MOVE` is atomic, §17), so **either a
`MOVE` that honours `Overwrite: F` or an `ETag` on `GET`/`HEAD` would be
enough to retire the catalog database entirely.** Worth filing, because the
underlying storage can already do it.

**And then the premise broke (§32).** §24 and §31 both probed *primitives*;
neither ever built a table. §32 did, implementing the eight interfaces
`hadoop.HadoopCatalogFS` requires, and **the filesystem catalog runs on
Pelican** — 6/6: table created, `version-hint.text` written and advanced
`1`→`2`, and an **independent catalog instance loaded the table from the
warehouse URL alone, with no database anywhere**. The missing CAS only bites
when two committers race, and `fscatalog_race_test.go` shows exactly how
hard it bites: four concurrent claims, **three told they won, one actually
landed, two acknowledged commits silently lost**.

So the correct statement is not "a filesystem catalog is impossible on
Pelican" but:

> It runs correctly with **exactly one committer**; with two it loses
> commits silently, and no header on this origin can prevent that.
> (*Committer*, not thread — §33 measured 4 parallel producers feeding one
> committer with no loss.)

That reframes the decision. The question is no longer a capability limit but
an operational one — *can SAGE guarantee a single writer?* If yes, the
filesystem catalog removes the catalog database outright, and with it both
the §28 recovery procedure and the reader's dependence on a SAGE-hosted
endpoint. If no — a backfill job, a manual repair, an accidentally-rerun
cron — the table corrupts with no error raised. Note the constraint is not
unique to Iceberg: §18 showed the custom design has no database-free safe
mode either, so single-writer-or-arbiter is already true of every option on
this storage.

**This does not by itself flip the recommendation**, which rests on reader
reach rather than on the database. But it removes the last *capability*
objection and converts it into a risk judgement, so it should be decided
deliberately rather than inherited from §24's wording. The open questions
that would settle it are in §32: realistic write volume, torn reads of
`version-hint.text`, and whether single-writer can actually be enforced.

**"One writer" means one *committer*, not single-threaded (§33).** The
phrasing above invites a reading it does not support — that SAGE would have
to ingest the archive serially — so it was measured rather than argued.
Two different things get called "writing": *producing data files* (encoding
Parquet, uploading — the expensive part, and it touches the catalog not at
all) and *committing* (appending them to the table and advancing
`version-hint.text` — one small metadata operation, and the only part the
missing CAS affects). The hadoop catalog needs the second serialized and
says nothing about the first. iceberg-go's `AddFiles(ctx, filePaths
[]string, ...)` takes a *list* of already-written files, which is exactly
the shape that lets N parallel producers feed one committer. §33 ran it:
**4/4** — four producers wrote Parquet to Pelican concurrently, one
committer added all four files in a single commit, all 1000 rows present
with 250 from each producer, and the pointer advanced **once** (`1`→`2`),
not four times. Same storage, same adapter, same worker count as the race
test that lost two commits; the only difference is whether the parallelism
sits in the data path or the commit path.

So the cost of the constraint is **one serialized metadata call per batch**
— not per file, not per row. Parallel production, parallel uploads and
parallel readers are all unaffected, and "one process owns the commit"
(a single ingest service, a queue with one consumer, a cron that cannot
overlap itself) is a conventional architecture rather than a concession.
What is genuinely ruled out is *two independent processes committing to the
same table at once*: two replicas of the ingest loop, a backfill racing the
live writer, an operator re-running a job by hand. Those corrupt the table
with no error raised, so the enforcement has to be real — a lock, a single
replica, or a queue — not a convention.

Five objections to Iceberg have now been tested. Four came back weaker than
written — the FileIO (packaging, not capability), the timestamps (one client,
not the format), the pointer swap (present, not missing), and external
readability (readers need no catalog at all, §23). **One came back intact:
the writer's database is genuinely required on Pelican (§24).** The
recommendation below still lands on custom, but it rests on that single
operational argument, and anyone revisiting it should know the margin is much
narrower than the original two-reason framing suggested.

The two halves have since been tested *together* (§21): iceberg-go writing
a format-version 3 table with `timestamptz_ns` onto the real NRDStor origin,
**12/12 checks, values bit-exact**, verified by fetching the metadata JSON
back over HTTPS. So "Iceberg on Pelican with the archive's timestamps" is
no longer a question with an assumed answer — it runs.

That service is narrower than it sounds, and the split is worth stating
plainly because it is easy to picture wrongly. Everything Iceberg writes as
a *file* goes to Pelican — 28 metadata objects and 8 data files in the
test. The database holds **one mutable row**: the current
`metadata_location`. **Writers need it; readers do not** — confirmed
directly in §23, where a reader opens the table by passing `nil` where the
catalog goes and still gets the schema, row counts and data. DuckDB queries
the data straight from Pelican with no catalog involved, which is the same
shape the custom design would use.

So the recommendation stands, but on a narrower base: **custom is the
cheaper single-writer design, not the safer one.** *(And that base has since
been removed — multi-writer must stay open, so the recommendation is now
Iceberg. See the box at the top and the decision section at the end.)*

Evidence in `experiments/results/SUMMARY.md` §12–13 and §17–27, and
`experiments/exp13..27`. This is a recommendation about the next 6–12
months, with an explicit condition for revisiting it — a condition that has
now been met.

## What Iceberg is, and what it is for

Background for anyone reading this cold. If you already know Iceberg, skip
to the next section.

### What it is

Apache Iceberg is a **table format**: a specification for a set of metadata
files that sit beside Parquet data files and describe which of them make up
a table *right now*. It is not a database, not a service, and not a storage
system. There is no Iceberg process to run and no Iceberg file format — the
data stays plain Parquet, readable by anything.

A table is three layers:

```
catalog   ──►  one mutable pointer: "the current metadata file is X"
                 │
metadata  ──►  schema, partitioning, and the list of snapshots
                 │   each snapshot ──► manifest list ──► manifests
                 │
data      ──►  the Parquet files themselves (plus delete files)
```

Only the top line is mutable. Everything below it is write-once.

Every write produces a **new** metadata file and leaves the old one intact,
then flips the catalog pointer to it. Nothing is ever edited in place. A
snapshot is therefore an immutable, complete description of the table at one
instant, and the pointer flip is the only moment anything changes.

That single design choice is where all the properties come from:

- **Atomic commits.** A writer either flips the pointer or it does not.
  Readers never see a half-written table.
- **Snapshot isolation.** A reader resolves the pointer once and reads that
  snapshot. A concurrent rewrite is invisible to it.
- **Time travel and rollback.** Old snapshots still exist; reading one is
  just reading an older metadata file.
- **Safe rewrite.** Compaction writes new files and commits a snapshot that
  references them. Readers on the old snapshot are unaffected until they
  refresh.
- **Row-level deletes.** A snapshot can reference *delete files* — in v3, a
  bitmap per data file — so a single record can be removed or superseded
  without rewriting the file that holds it.

The catalog pointer is the catch, and it is the whole operational cost: it
must support **compare-and-swap** ("set the pointer to X only if it is still
Y"), because that is what makes two simultaneous commits safe. The spec
deliberately does not standardise how — *"the atomic operation used to commit
metadata depends on how tables are tracked and is not standardized by this
spec."* In practice it is a database or a REST service. Pelican cannot do it
(§17–19), which is why Iceberg here means running something alongside.

### What it is for

Iceberg exists to make **changing a published dataset** safe. Not querying
it, not storing it — changing it, while people are reading.

The problems it was designed against are the ones that appear when a
directory of Parquet files is treated as a table by more than one party:

| problem | without a table format |
|---|---|
| rewrite a day while someone reads it | reader sees files vanish mid-query |
| two writers publish at once | last-writer-wins, silently |
| fix one bad record | rewrite the whole file, or live with it |
| "what did this look like last Tuesday?" | unanswerable |
| undo a bad publish | restore from backup |
| hand data to another team | they need your conventions and your code |

If none of those happen, Iceberg's machinery has nothing to do. **A
directory tree is a perfectly good table format for data that is written
once and never changes.** That is not a dismissal of Iceberg; it is the
condition under which it is not needed.

### What it is *not* for

Worth stating because it is the usual reason people reach for it first, and
it does not apply here.

**Iceberg is not a query accelerator.** Its manifests prune files by
min/max statistics — but a plain manifest does the same job: 80,434 bytes
for 30 days, under a millisecond to read, rejecting 240 of 242 files (99%)
for a rare plugin and 208 of 242 (86%) for a measurement name (§7). At
4,564 files that extrapolates to ~1.5 MB in one request. The planning
argument for Iceberg is dead at this scale.

**It also does not fix the archive's actual performance problem**, which is
that files cannot be pruned *internally* — one row group of up to 10M rows,
so a query returning one row reads 49 MB. That is a missing writer option in
the exporter (`plans/3_findings.md`), and Iceberg over unprunable files just
plans faster toward reading 49 MB.

### Two things it does give you that are easy to miss

**The schema does not have to be right on day one.** A table was created
with a deliberately incomplete schema, rows written, and then two columns
added and one renamed — on the published table, with no file rewritten.
The rows written under the old schema still read back correctly, and the
renamed column still returns its pre-rename values, because **Iceberg
resolves columns by field ID, not by name** (§22, 17/17). A
directory-of-Parquet design resolves by name, so a rename there means
rewriting every file or carrying the old name forever.

The guardrail matters as much as the capability: widening `float` → `double`
is allowed, narrowing is refused (`cannot promote double to float`). So the
honest version of the claim is not "Iceberg lets you be careless early" but
**"the additive and renaming kinds of mistake stop being expensive"**.
Choosing too narrow a type is still permanent.

**One version can be named and kept.** An Iceberg *tag* is an immutable
named pointer to a snapshot. Tested the way a citation would actually be
used, it survives later writes, returns the data as of *then* rather than
as of now, costs no duplicated data, and — the check that decides whether
it is safe to cite — **survives aggressive snapshot expiry**: 4 snapshots
dropped to 2, the untagged intermediate deleted, the tagged one kept and
still readable (§22).

That is the hard half of a DOI, and only the hard half: an immutable,
retrievable, named state of a dataset that keeps changing. Iceberg mints no
identifier, registers nothing with DataCite, and provides no landing page or
citation metadata. A table property could hold a DOI string; nothing in
Iceberg validates it.

And "data organization" is the weakest way to argue for Iceberg here —
organizing files is what the directory tree and the 80 KB manifest already
do well. What it adds is *versioned* organization: being able to say which
exact set of files was the dataset at a named moment, and still answer that
after the table has moved on. **If nothing is ever republished or corrected,
a `date=YYYY-MM-DD/` tree plus a frozen manifest copy is already a citable
artifact.** Which is the same conditional the rest of this document keeps
arriving at.

### Publishing to a third party (NDP)

NDP is an ecosystem and enabler rather than a data host: it reads, never
writes. That makes the reader path the one that decides whether Iceberg
helps, and it had never been tested until §23 — every earlier experiment
went through the writer's catalog.

**A reader needs no catalog.** Given a metadata URL and read access, with
`nil` passed where the catalog goes, a consumer gets the full schema with
types, per-column documentation, dataset licence and contact, row/file/byte
counts, snapshot history, the data itself, and predicate pushdown (§23,
12/12). The first five come **without reading a single Parquet file** —
which matters on Pelican, where a listing that had to scan data would cost
~150 ms per request.

This is the opposite of what the SQLite dependency suggests. The database is
the *writer's* private component; publishing to NDP means handing over a
URL, not database access.

**On the premise, though — one correction.** Iceberg writes two different
files and the word "manifest" names the wrong one:

- **`*.metadata.json`** carries the schema, types, column docs, partitioning,
  history and table properties. Plain JSON — §23 confirms a consumer can
  read the whole structure with nothing but Python's stdlib `json`.
- **`*.avro` manifests** list data files with per-column bounds. Binary;
  machine-readable only.

So the self-describing claim is right and it holds up — but it is the
metadata JSON doing that work, not the manifest. And it is not unique to
Iceberg: the custom manifest (§7) could carry the same fields. The real
difference is that Iceberg's is a **published specification an integration
targets once**, versus a bespoke JSON shape that must be documented,
versioned and explained to every consumer. For a single partner that is a
small difference; for an ecosystem that intends to federate many providers,
it is most of the point.

**Two caveats worth knowing before promising NDP anything.**

*A bare URL is a pointer, not a subscription.* A reader given one metadata
file does not see later commits — tested: the writer committed more rows and
the reader still returned the old count, with `Refresh` failing outright.
The fix is a 1-byte `version-hint.text` on storage naming the current
metadata file; a reader resolves the current table with one extra GET
(§23). DuckDB looks for exactly this file and refuses to guess without it.

One qualification on how that pointer gets written. Iceberg's filesystem
catalog publishes it automatically, but **that catalog cannot be used on
Pelican** — iceberg-go refuses it on remote storage, and the origin cannot
back its commit protocol (§24). So with a SQL catalog the writer publishes
the hint itself: one extra PUT after each commit, no database involvement,
and the reader side is unchanged. It is a line of code, not a design
problem — but it is ours to write rather than something that happens for
free.

*DuckDB cannot read v3 nanoseconds today.* `iceberg_scan` fails with
`Unrecognized primitive type: timestamptz_ns` on DuckDB 1.5.6, while the
same client reads a v2 + microsecond table fine, and pyiceberg 0.12.0 reads
*both*. The control is what makes this diagnostic: the blocker is the
nanosecond type, not Iceberg, not v3, not the Go writer — the same
client-gap shape as the original nanosecond mistake. But it lands on the
most likely NDP access path, a notebook user running DuckDB.

**Settled 2026-10-14: store microseconds.** DuckDB support is a firm
requirement, so the Iceberg table uses v2 + `timestamptz`, which every
reader tested can open. The rationale, and why this costs less than the
97.60%-remainder figure implies — DuckDB already truncates these
timestamps today on plain Parquet — is recorded under "Decision
(2026-10-14)" below.

### What it would be for *here*

Concretely, against this archive, Iceberg earns its cost in four places,
the first three examined with measurements in **"Where the custom catalog
genuinely loses"** below:

1. **Corrections.** `exp11` found correction files work for appends only
   (§10) — nothing distinguishes a correction from an original, so a bad
   record cannot be superseded and a duplicate re-delivery cannot be
   detected. This is the strongest argument in these results for a real
   catalog.
2. **The safety half of compaction.** `exp10` passed the rewrite milestone
   on correctness (§9: 178,902,746 rows, 18 files → 9, identical records by
   order-independent digest) but covers no manifest publication, no
   reader-pinned version, and no deletion of superseded files. A reader
   mid-query during a rewrite is currently unprotected.
3. **Handoff.** "Here is an Iceberg table" is self-describing; "here is our
   JSON manifest" is not.
4. **Citable versions.** If SAGE data is ever to carry DOIs, a tag gives the
   immutable, named, expiry-proof snapshot a DOI must resolve to, at no
   storage cost (§22). The custom equivalent is freezing and retaining a
   manifest copy per citation — buildable, but it is the same mechanism
   rebuilt, and the expiry-safety property has to be remembered rather than
   enforced.
5. **Third-party readers (NDP).** A consumer given only a URL gets the
   schema, types, per-column documentation, row counts and licence without
   reading a single Parquet file, and **without the writer's catalog** —
   `nil` is a supported value where the catalog goes (§23, 12/12). The
   custom design can publish the same facts, but as a bespoke JSON shape
   each consumer must be taught; Iceberg's is a published spec an
   integration targets once.

And one it has already won on measurement rather than argument:
**concurrent writers** — 0 rows lost in 3/3 trials, against the custom
catalog's 3/3 silent lost updates (§18–19).

### So the real question

Not "Iceberg or custom", but **how much of Iceberg does SAGE need?**

If the answer is corrections, safe compaction, reader pinning and citable
versions, then a custom catalog converges on being a worse Iceberg — those
*are* most of Iceberg, and each is subtle in the way that produces silent
data loss when got slightly wrong. There is already one measured instance of
exactly that failure in this work.

If the answer is "index the files, plan queries, one writer, append-only",
Iceberg is a large amount of machinery for a problem an 80 KB manifest
already solves.

**That question reduces to two facts nobody has yet. First: will published
SAGE data ever need to change after it is written?** Corrections,
retractions, re-delivered duplicates, policy redaction. `exp11` could not answer it — the
archive is a single bulk backfill (every `exported_at` inside one 29-hour
window, 2026-10-03/04), so the apparent 55–641 day lag measures the age of
the archive, not data arrival, and it carries no evidence about how often
late or corrected observations arrive (§10). Answering it needs InfluxDB
write timestamps.

**Second: will SAGE data be cited — DOIs, papers, reproducible datasets?**
That is a programme decision rather than a measurement, and it points the
same way: a citation is a promise that a named version stays retrievable and
unchanged, which is exactly what the tag mechanism enforces and what a
directory tree leaves to discipline.

**Third: how many third parties, and how federated?** For NDP alone, a
documented custom manifest is a perfectly reasonable answer — §23 shows
Iceberg's advantage there is a published spec rather than a capability the
custom design lacks. For several consumers, or a federation that expects to
point standard tooling at a registered dataset, the bespoke shape becomes a
support obligation that recurs with each one.

**Those three missing inputs matter more to this decision than anything
still open about Iceberg itself.** Note that two of them — citation and
third-party publication — are the kind of requirement that tends to arrive
after the storage design is fixed, which is an argument for deciding them
deliberately now rather than discovering them later.

## The nanosecond blocker is a Python-client limit, not an Iceberg limit

**Corrected 2026-10-14.** Earlier versions of this section were headed
"Iceberg cannot represent `time` today". That was wrong, and the error is
worth naming because it nearly decided the recommendation: a failure in one
*client* was written up as a limit of the *format*. See `exp20`.

The exporter writes `time` as nanoseconds
(`parquet:"time,timestamp(nanosecond),delta"`). Iceberg v2 has no nanosecond
type, but **v3 does** — `timestamp_ns` and `timestamptz_ns` — and the Iceberg
spec has supported them since v3 landed. What differs is which client can
write them:

| client | v3 + `timestamptz_ns` | |
|---|---|---|
| pyiceberg 0.12.0 | **cannot write** | `NotImplementedError` at `table/metadata.py:588`, unconditional (apache/iceberg-python#1551, still open) |
| pyiceberg 0.12.0 | **can read** | loads a v3 table, scans 500 rows, values exact |
| iceberg-go v0.7.0 | **writes it** | `supportedTableFormatVersion = 3`, no guard |

Measured end to end (`exp20`, 7/7 checks, on 500 real archive timestamps each
carrying a non-zero ns remainder): iceberg-go creates the v3 table, appends,
and the values round-trip **bit-exact**. On-disk metadata reads
`"format-version": 3` with type `timestamptz_ns`; the Parquet holds
`timestamp[ns, tz=UTC]`. v2 rejects the type outright, which confirms the
column really is exercising the v3 path rather than silently downgrading.

Repeated against the **real NRDStor origin** (`exp21`, 12/12): same result,
same exactness, with the metadata JSON fetched back over HTTPS to confirm
`"format-version":3` and `timestamptz_ns` actually landed there. iceberg-go
needs its own Pelican IO (~190 lines, `experiments/icebergo/pelican_io.go`)
for the same reason pyiceberg did — no HTTPS IO ships with it — and Pelican
accepts a Go-minted ES256 SciToken.

pyiceberg then *reads* that Go-written table correctly — same 500 values —
and fails only on write: `ValueError: Cannot write manifest for table
version: 3`. So the boundary is precisely **write support in one client**,
not representability.

### What choosing microseconds costs: no `add_files` (`exp36`)

§25 chose v2 + microseconds over the v3 route. That choice has a
consequence measured later and worth stating here, because it changes
ingest rather than just storage: **the existing archive cannot be
registered in place.**

`add_files` reads each file's real footer, and the archive's footers say
`timestamp[ns, tz=UTC]`, so pyiceberg refuses them regardless of the
Iceberg schema the table was created with. The two apparent escape hatches
do not apply: `downcast-ns-timestamp-to-us-on-write` governs `append`,
which rewrites bytes, and v3 is unwritable per the table above. So the
metadata-only path — point Iceberg at 4,564 files that already exist, move
no bytes — is unavailable under this decision.

Ingest must rewrite instead: ~26 MB/s measured on real files, about two
single-core CPU-hours for the 209 GB archive once, parallel by day, and a
permanent per-file cost on the steady-state path. That is affordable, and
§25's reasoning still holds (v3 would strand DuckDB, per the section
above) — but "rewrite everything on the way in" is a different ingest
design from "register what is already written", and it should be chosen
knowingly rather than discovered during implementation.

The upside, also measured (`exp36` Q2): truncating to microseconds costs
nothing that distinguishes a row. 94% of sampled rows carry sub-microsecond
digits, yet truncation produced **zero** new `(time,name,node,plugin)`
collisions across 10M rows, and the discarded remainder is uniform over
0–999 — clock granularity, not measurement.

The 97.60% measurement is unaffected and still matters: on 593,388,314 rows
across 8 days, that many carry a non-zero nanosecond remainder (96–99.9% on
2025 days, 56–60% on 2026 days), and pyarrow refuses the microsecond cast
unless forced:

```
ArrowInvalid: Casting from timestamp[ns, tz=UTC] to timestamp[us, tz=UTC]
would lose data: 1735689606915629791
```

What changes is the conclusion drawn from it. Adopting Iceberg does **not**
require truncating timestamps or waiting for the spec. It requires writing
through a client that supports v3 — which, for a Go exporter, already exists.
A Python writer would still have to truncate or wait, so the constraint is
real but lands on the language choice, not on Iceberg.

(An earlier draft also said "55.00% of 5,000,000 rows". That came from one row
group of one day and had no code behind it; `experiments/probe_ns_remainder.py`
now measures it — see `results/SUMMARY.md` §16, which also records why this
cannot be measured through DuckDB.)

If the team concludes microsecond precision is scientifically sufficient,
that is still a legitimate answer — but it is now an optimisation, not a
precondition, and should be an explicit recorded decision either way.

### Decision (2026-10-14): microseconds, for DuckDB support

**Recorded as asked for above.** Supporting DuckDB is a firm requirement, so
the table stores `timestamptz` at microsecond precision and the
sub-microsecond digits are not retained. This closes the question that §13,
§20, §21 and §23 kept open.

Three things make this cheaper than the 97.60% remainder figure suggests,
and one makes it sharper:

1. **DuckDB already truncates these timestamps today, on plain Parquet,
   with no Iceberg anywhere** (§25). Its `TIMESTAMP` is microsecond
   precision, and on 1.5.6 the loss happens at read time and cannot be
   recovered in SQL — `CAST(time AS BIGINT)` raises, and `epoch_ns(time)`
   returns a ns-scaled number whose last three digits are already zero.
   Same file, same row: pyarrow reads `1735689606915629791`, DuckDB reads
   `1735689606915629000`. So for anyone querying through DuckDB this
   decision changes nothing — it makes the storage match what that reader
   was always going to see.
2. It removes the one interop failure in §23. v2 + microseconds reads in
   **every** client tested: DuckDB, pyiceberg, stdlib `json`, pyarrow.
3. It also removes the v3 dependency, which widens the client field again —
   pyiceberg can *write* v2, so a Python writer stops being blocked (§20).
   That does not change the Go exporter plan, but it is one less constraint.

The sharper point: **the loss is permanent and happens at write time.** This
is the one Iceberg mistake that schema evolution cannot undo (§22 — widening
is allowed, narrowing is refused, and the data is already gone regardless).
So it is worth confirming once with whoever owns the science that no SAGE
measurement needs sub-microsecond resolution, rather than inferring it from
the fact that DuckDB cannot show it.

**Do not change the exporter's Parquet.** Keep `time` as
`timestamp(nanosecond)` in the archive files. The truncation belongs in the
Iceberg write path only, so the full-precision value survives in the
underlying Parquet and the decision stays reversible if v3 support lands
everywhere later. Writing microseconds at the exporter would destroy the
data at the source and make this irreversible for no gain.

## Why Iceberg is also harder operationally *here*

Not a verdict on Iceberg generally — it is a verdict on Iceberg **on Pelican**.

**Iceberg's commit requires a compare-and-swap that this storage does not
provide.** The spec standardises the table format but explicitly does not
standardise the commit: *"the atomic operation used to commit metadata depends
on how tables are tracked and is not standardized by this spec."* Two schemes
exist. The filesystem one (write, then rename) is deprecated with a warning
naming exactly our situation: *"unsafe in object stores and local file
systems"*, removed in spec v4. The other needs *"a pointer in a metastore or
database that is updated with a check-and-put operation."*

Pelican offers authenticated PUT/GET with bearer tokens. Measurement
(`exp17`) refined this: NRDStor **does** provide `MOVE`, and it is atomic
enough that 318 concurrent reads across 20 swaps never saw a torn or missing
pointer — so the "write, then rename" scheme is mechanically available. What
it does **not** provide is the check-and-put the other scheme needs:
`If-None-Match: *` and `Overwrite: F` are accepted and ignored. Since the
rename scheme is deprecated and removed in spec v4, Iceberg here still means
running a **REST or JDBC catalog service plus its database** — a stateful
component, with its own backup, failover and upgrade story, alongside an
archive whose entire appeal was being plain files on storage we already have.
("In front of" would overstate it: §19 measured that queries do not pass
through the service — see below.)

(The custom catalog is unbothered by the deprecation, because it is not
bound by the Iceberg spec: it can use `MOVE` as its commit and accept the
single-writer constraint, which Iceberg's spec-compliant path cannot.)

**`exp19` confirmed this by running it.** Every catalog pyiceberg 0.12.0
ships — REST, Hive, Glue, DynamoDB, SQL, BigQuery, InMemory — requires a
service or a database; there is no filesystem/hadoop catalog in pyiceberg
at all, so the deprecated rename scheme is not merely unwise here, it is
unreachable from Python. The commit is one SQL statement
(`pyiceberg/catalog/sql.py:520-536`):

```sql
UPDATE iceberg_tables SET metadata_location = <new>
 WHERE metadata_location = <old>   -- rowcount < 1 -> CommitFailedException
```

Pelican holds only immutable files and never participates in the commit.

Two things this measurement changes, both uncomfortable for the argument
above. First, **the database is why Iceberg is safe**, and the comparison
is therefore not like-for-like: given the same database, the custom
catalog's `commit(expected_parent=...)` is exactly where an equivalent
conditional UPDATE would go, and it would be equally safe. Second, the
cost gap is smaller than "stateful service" suggests — 9 requests per
commit against the custom catalog's 5, ~0.9s against ~0.7s. The real
operational surface is metadata growth: **28 metadata objects for 8 data
files** after 8 appends, since Iceberg never mutates metadata. That needs
snapshot expiry and metadata rewrite as routine maintenance.

And the service is narrower than it sounds, because **readers never touch
it**. Everything Iceberg writes as a file lives on Pelican; the database
holds one mutable row, the current `metadata_location`. DuckDB reads the
data straight from Pelican with no catalog in the path — measured three
ways (§19): `read_parquet` on a data-file URL with either the `?authz=`
parameter or an `Authorization` header via `CREATE SECRET`, and
`iceberg_scan` pointed at a bare metadata URL, which resolves the manifests
over HTTPS itself and returned all 306,000 rows. So this is the same
DuckDB-straight-to-Pelican shape the custom design would use.

That makes the database a **write-side** cost, not a whole-system one: one
exporter needs it, every consumer does not. The caveat is consistency
rather than access — without the catalog a reader gets whichever snapshot
it was handed, since no `version-hint.text` marks the current metadata
file. The custom catalog's `CURRENT` pointer is genuinely simpler there.

The database also **cannot be moved onto Pelican** to avoid the service, and
the failure mode is the bad kind: `PUT` with `Content-Range` returns 201 and
is *silently ignored* (a 1000-byte file became 10 bytes), `PATCH` returns
501, and `OPTIONS` advertises `DAV: 1` — no class 2, no locking. Hosting
SQLite there would appear to work and corrupt the file. This is the same
shape as the ignored `If-None-Match` above, and it is also the reason the
custom catalog can skip a database at all: its pointer is a *file*, swapped
by `MOVE`.

**`iceberg-go` cannot address this storage today, though less badly than
expected.** v0.7.0 (2026-10-01) has a genuinely capable write path — append,
rewrite files, rewrite manifests, positional and equality deletes all marked
supported — and its delete-file read path looks like the most mature part of
the library (deletion vectors, positional and equality deletes, with
dedicated regression tests). Its FileSystem table lists only S3, GCS, Azure
Blob and local, so a Pelican FileIO has to be written. But there is a clean
public extension point: `io.Register(scheme, factory)`, against an `IO`
interface whose required surface is just `Open` and `Remove`. Writing a
bearer-token HTTP FileIO is tractable.

That solves *data access*. It does not touch the commit problem — and the
library's own source comments warn that rename "is not atomic in other
implementations like blob stores", i.e. it tells you not to build a commit on
it. It is also 0.x with no API-stability promise, and `beehive-data-api` is
Go, so this is the client that matters.

So the binding constraint is not Iceberg's maturity. It is that **Iceberg
needs more from Pelican than the custom catalog does, not less.**

## What the custom catalog actually costs

`experiments/catalog.py` is **211 lines** and passes **10/10** safety checks on
real archive data (`exp13`), including the ticket's stated bar — *"a failed
attempt leaves the archive frontier unchanged unless the published snapshot
actually contains the interval."*

| property | result |
|---|---|
| plan from statistics, no file opens | 2 of 12 files survive, 0.01 ms |
| reader pinned across a mid-query compaction | unaffected |
| compaction swaps files atomically | no overlap, no gap |
| snapshot written but never committed | invisible, frontier unchanged |
| superseded files | identifiable, retained, not auto-deleted |
| commit against a stale parent | rejected *(sequentially — see below)* |

All ten hold on Pelican too (`exp18`, 11/12). The exception is the last
row: it is a **sequential** test, and it passes on Pelican for the same
reason it passes on POSIX — it never had two real writers. With two,
Pelican loses an update every time. On POSIX the window is microseconds;
on Pelican it is ~561 ms. The check is honest about what it tests; it just
cannot stand in for concurrency on storage without compare-and-swap.

It needs **one** primitive: an atomic pointer swap, isolated in a single class
so the Pelican port is one file. It does not need atomic rename of data files,
or listing. The port is real and it is one file —
`experiments/pelican_store.py`, 140 lines, with `Catalog` and `Snapshot`
unchanged above it.

The "it does not need conditional writes" clause needs qualifying now that it
has been measured. It is true only of a **single writer**. Pelican offers no
compare-and-swap — `If-None-Match: *` and `MOVE` with `Overwrite: F` are both
accepted and silently ignored (§17) — so `commit(expected_parent=...)` degrades
from an enforced check to read-then-compare-then-swap across the network. exp18
ran two writers against it and **both committed, in 3 of 3 trials, with no
error on either side**: one snapshot simply disappears from the chain. The
window is ~561 ms wide, not the microseconds POSIX gives you. So: conditional
writes are not needed to *build* this, but their absence is a hard
single-writer constraint on *operating* it, and it has to be enforced outside
the storage.

And it stays small. A naive snapshot projects to 39 MB at today's 4,564 files,
which would be a real argument for Iceberg's split-manifest design — but
measured on 558 real files, the shape dominates:

| snapshot form | projected to 4,564 files | gzipped |
|---|---|---|
| all 9 columns, per row group | 2.9 MB | 127 KB |
| time + plugin, per **file** | **1.5 MB** | **86 KB** |

The cheap form prunes identically: 480 of 558 files rejected for a real
plugin, the same as full per-row-group statistics. DuckDB also accepts a
4,059-path planned file list (366 KB of SQL) and counts 38.9 billion rows in
0.85s, so planning to an explicit list is viable at full scale.

## Where the custom catalog genuinely loses

Three things, stated plainly:

1. **Row-level corrections and redaction.** The custom catalog supersedes
   whole *files*. Fixing or removing one record means rewriting its file.
   Iceberg expresses this natively with deletion vectors — a bitmap per data
   file, applied cheaply at read time.

   `plans/Data Pipeline Considerations.md` raises exactly this as the
   **redaction** workflow: data published by mistake that must be removed
   after the fact, by "searching the Parquet files for matches, and then
   reconstructing and building a replacement Parquet file". That is
   file-rewrite redaction, which is what the custom catalog does natively —
   and the same doc judges the case rare ("If (policy-based) redaction of
   existing data is very rare, Parquet files would be very durable"). Rare
   redaction is the regime where whole-file replacement is fine and deletion
   vectors are not worth a catalog service. If redaction turns out to be
   routine, that reverses.

   Worth noting for the longer term: Iceberg v3 replaces position delete
   files with deletion vectors and v4 removes equality deletes entirely, so
   anything built now should not depend on equality deletes.
2. **Everything is ours to operate.** Snapshot expiry, orphan cleanup,
   retention of superseded files past live readers — Iceberg ships these; we
   would write and maintain them.
3. **No ecosystem.** Spark, Trino, Snowflake, Athena and DuckDB can all read
   an Iceberg table. They cannot read our snapshot format.

   This is the gap worth taking most seriously, because
   `plans/Data Pipeline Considerations.md` asks three questions that all
   point at it: pushing finalised Parquet to resources Sage does not govern
   (a partner's AWS), whether partners could own their own long-term path,
   and whether search could be hosted at cached sites like UCSD rather than
   on Sage servers. An Iceberg table is a far better handoff artifact for all
   three than a bespoke snapshot format — "here is an Iceberg table" is
   self-describing in a way "here is our JSON manifest" is not.

   Partial mitigation, now **tested rather than assumed** (`exp16`): a real
   Iceberg table was built from archive data, its catalog database deleted,
   the table directory moved, and DuckDB still read all 2,000,000 rows from
   the bare metadata path. The table is genuinely self-describing.

   `exp19` repeated this **on Pelican itself**: catalog database deleted,
   all 306,000 rows read back from a bare metadata URL on NRDStor —
   including from **DuckDB** via `iceberg_scan`, which resolved the
   manifests over HTTPS with no catalog in the path. A partner needs a URL
   and a token, not access to our service. So the handoff argument holds on
   the storage we would actually use, and holds for the engine a recipient
   is most likely to reach for.

   One gap to plan around — there is no `version-hint.text`, so 11
   `*.metadata.json` files sat in the directory with nothing on the storage
   marking which was current. A recipient must be *told* the metadata path.
   Fine for a deliberate handoff; useless as disaster recovery.

   One gotcha worth writing into any handoff instructions: the token must be
   scoped to the **whole table prefix**, not just the data files, because
   `iceberg_scan` also fetches the metadata JSON and the Avro manifests. A
   data-only scope returns `403` and looks like a capability limit.

   So if external readability becomes the driver, emitting Iceberg metadata
   as an *export* format alongside the custom catalog is a real option — it
   keeps the commit problem out of it entirely, since an export is written
   once and never concurrently updated. The nanosecond issue above no longer
   blocks this: an export written by iceberg-go can carry v3 `timestamptz_ns`
   with no precision loss (`exp20`), so the export path is now unobstructed.

## Recommendation

**Build the custom catalog, and keep the table format Iceberg-compatible in
the one way that is cheap: the data files.**

The files are already plain Parquet in a Hive-style `date=` layout. Iceberg
tables are plain Parquet in a Hive-style layout plus metadata. If we never
paint ourselves into a corner on the *data* files, adopting Iceberg later is
largely a metadata import, not a re-export of 194 GiB.

This now matters more than when it was written. With the timestamp blocker
withdrawn, "adopt Iceberg later" is a genuinely open path rather than one
gated on an upstream fix — and keeping `time` as `timestamp[ns]` in the
exported Parquet is exactly what preserves it, since v3 `timestamptz_ns`
maps onto those files without a rewrite.

Order of work:

1. **Land the exporter fix first** (`plans/3_findings.md`). Nothing else
   matters if files cannot be pruned; a catalog over unprunable files just
   plans faster toward reading 49 MB to return one row.
2. ~~**Measure Pelican before building either.**~~ **Done** — `exp17`,
   `results/SUMMARY.md` §17, against the real NRDStor origin with real
   SciTokens. All three gating tests pass:

   - **HTTP Range reads and read-after-write consistency** — both work,
     including the suffix read Parquet uses to find its footer, and
     multi-range. An object is readable immediately after `201 Created`.
   - **Atomic pointer swap** — `MOVE` onto an existing path, verified under
     4 concurrent readers across 20 swaps: 318 reads, 0 torn, 0 errors,
     never observed going backwards. `catalog.py`'s `os.replace` maps onto
     this directly.
   - **DELETE** works on files and directories.

   So the custom catalog works as designed, with **one qualification**:
   there is no compare-and-swap. `If-None-Match: *` and `MOVE` with
   `Overwrite: F` are both *ignored* — they return 201 and overwrite.

   `exp18` then ran the catalog itself on Pelican — `catalog.py` unchanged,
   `CatalogStore` swapped for `PelicanCatalogStore`, snapshots and pointer
   as real NRDStor objects. **11 of 12 checks pass.** The one failure is
   the qualification above, now measured rather than predicted: two
   concurrent writers **lost an update in 3 of 3 trials**, silently. The
   check-to-swap window is **~561 ms** (a GET then a PUT+MOVE), against
   microseconds on POSIX, and nothing at the storage layer arbitrates it.

   So single-writer is a **hard constraint, not a preference**, and it must
   be *enforced* — a lease, a lock, or a singleton job — rather than assumed
   because only the exporter writes today. It binds the **commit**, not the
   data path: §33 measured 4 producers writing Parquet to Pelican in
   parallel behind one committer with nothing lost. It constrains Iceberg identically
   (its spec-compliant commit needs the same check-and-put), so it does not
   favour either option; it makes "more than one writer" the trigger to
   watch most closely.
3. **Then build the catalog**, reusing `catalog.py` as the shape.

## Revisit Iceberg when any of these becomes true

- ~~**v3 write support lands** in the client we would use, removing the
  nanosecond blocker — or the team decides microsecond precision is enough.
  This is the precondition for all the others.~~
  **Settled 2026-10-14 by decision, not by waiting: microseconds.** DuckDB
  support is required, so the table is v2 + `timestamptz` and v3 write
  support is no longer on the critical path at all. See "Decision
  (2026-10-14)" above. The note below remains accurate about what iceberg-go
  can do, and matters again only if the precision decision is revisited.
  **Settled for Go: it already has.** iceberg-go v0.7.0 writes v3 with
  `timestamptz_ns`, bit-exact on real archive timestamps — on local disk
  (`exp20`, 7/7) and on the real NRDStor origin (`exp21`, 12/12). This
  precondition is met for a Go writer and remains open only for a Python
  one (pyiceberg 0.12.0 reads v3 but cannot write it). With both this and
  the pointer-swap item below now settled, **the two findings that most
  strongly favoured custom have each turned out narrower than written** —
  what remains against Iceberg is the stateful service its commit needs,
  not the format and not the storage.
- ~~Pelican cannot provide an atomic pointer swap, so a stateful service is
  required regardless — the main cost argument for custom disappears.~~
  **Settled: it can.** MOVE onto an existing path is atomic — 318 concurrent
  reads during live swaps, 0 torn, never went backwards (§17).
- Row-level corrections or redaction turn out to be routine rather than rare.
- ~~External engines need to read the archive directly, or data has to be
  handed to partners who will operate it themselves.~~
  **This one has now happened — see NDP above.** It is no longer a
  hypothetical trigger but a stated requirement, and §23 measured the cost
  of meeting it: readers need **no catalog at all**, so the database stays
  the writer's private component. Unlike the other triggers this does not
  force Iceberg, because an Iceberg *export* alongside the custom catalog
  also satisfies it (see below). What it does do is move the question from
  "is the ecosystem worth anything to us?" to "do we want to maintain two
  metadata formats or one?"
- ~~More than one writer needs to commit concurrently.~~ **This trigger has
  now fired (2026-10-14).** The team's position is that cold storage must stay
  open to additional writers rather than being locked to one forever. It is
  therefore no longer a future condition to watch, and it is what moves the
  recommendation to Iceberg. Original text follows, since the measurement
  behind it is what makes the trigger decisive:
  This is now the *live*
  trigger rather than a hypothetical one: exp18 showed two concurrent writers
  losing an update in 3 of 3 trials, silently, because the origin has no
  compare-and-swap. A second writer is not a performance problem here, it is a
  correctness one, and it cannot be fixed inside `pelican_store.py`.
  **This trigger now points at Iceberg specifically**, because `exp19` ran
  the same race against Iceberg on the same origin and lost nothing, 3/3.
  If a second writer appears, "custom" stops being the safer option and the
  choice becomes which database to run — at which point Iceberg's is
  already written.

- **A table format with a native Go writer beats Iceberg on relocation or
  on failing loudly.** Added 2026-10-14 after evaluating DuckLake (§29),
  which beats Iceberg on both but has no Go client. The trigger is the Go
  client, not the format: if one appears — or if the exporter ends up
  embedding DuckDB for other reasons — the comparison reopens. See
  "DuckLake instead of Iceberg?" below.

Each is a concrete trigger rather than a matter of taste, which is the point:
the decision is reversible, and the data files are what make it reversible.

## Caveats

- The Iceberg *spec and client* findings come from reading the spec, Java
  1.12.0 source and iceberg-go v0.7.0 release notes directly; web search was
  unavailable during that pass, so an in-flight conditional-write proposal
  could exist that it did not surface. Worth a check before acting. The
  timestamp and interop findings (§13) are different in kind — those were
  executed locally against real archive data.
- Versions pinned at 2026-10-09: Iceberg Java 1.12.0, iceberg-go v0.7.0,
  pyiceberg 0.12.0. The v3 write gap is a moving target and already moved:
  checked again 2026-10-14, iceberg-go writes v3 and pyiceberg still does
  not. Re-check per client rather than treating "Iceberg" as one thing —
  that conflation is exactly what produced the original wrong finding.
- Every catalog number was computed on a POSIX filesystem. **Pelican has now
  been measured** (`experiments/exp17` and `exp18`, `results/SUMMARY.md`
  §17–18) and the primitive the custom catalog needs is present; the remaining
  gap is compare-and-swap, which affects concurrent writers only — but there
  it is fatal rather than slow (3/3 silent lost updates). The origin is itself
  `storageType: posix`, which is why MOVE behaves as well as it does; an S3 or
  other object-store origin would need this re-measured from scratch.
- `exp19` tested Iceberg through **pyiceberg**, not iceberg-go. The commit
  model is the spec's and applies to both, but the exporter is Go, and
  iceberg-go's FileIO gap would have to be closed separately. The Python
  FileIO took ~230 lines, so this is a sizing data point, not a guarantee.
- `exp19`'s concurrency result covers **appends**, which commute. Two
  concurrent *rewrites* of the same files would conflict genuinely, and
  Iceberg would reject one rather than merge them. That is the correct
  behaviour, but it is a different test and it was not run.

---

# Decision (2026-10-14): Iceberg

Two statements from the team settle this, and neither is a new argument —
both are conditions this document had already named as decisive.

## 1. Multi-writer must stay possible

> *"we don't want to forever drop that option since we want to keep our cold
> storage open for more writers."*

The recommendation above is "custom, for now" on exactly **one** surviving
reason, and that reason was stated conditionally from the start: it holds
only while there is precisely one writer, forever. The revisit trigger said
so in advance — *"If a second writer appears, 'custom' stops being the safer
option."*

Keeping the option open is enough to fire it. The reason is that the custom
design's single-writer constraint is **not enforceable by the storage**:

| | two concurrent writers | failure mode |
|---|---|---|
| custom catalog (`exp18`) | lost an update **3/3** | silent — both writers report success |
| Iceberg (`exp19`) | **0 rows lost, 3/3** | conflict detected and raised |

The custom catalog's commit degrades to read-compare-swap across the network
with a **~561 ms** window and nothing policing it. A second writer does not
cause an error or a slowdown; it causes a snapshot to vanish from the chain
with both sides reporting success. That is the worst available failure shape:
undetectable at write time, and discoverable only later as missing data.

Critically, **this cannot be fixed inside `pelican_store.py`** — the origin
offers no compare-and-swap (`If-None-Match: *` and `MOVE` with
`Overwrite: F` are both accepted and ignored, §17, re-confirmed §24). The
only fix is a component that *can* do CAS, i.e. a database. At that point the
question stops being "Iceberg or custom" and becomes "whose database" — and
Iceberg's is already written, specified and tested.

So the honest framing: the custom option was never "simpler forever." It was
"simpler **if** we accept a permanent, externally-enforced, single-writer
constraint." Declining that constraint is a legitimate architectural choice,
and it is the one that was made.

## 2. The ecosystem argument, restated as the user put it

> *"iceberg … is a well known and utilized Open Table format that a lot of
> tools support. If we build our own custom cold storage just using parquet
> files, I'm scared that we will slowly go towards the direction of building
> something complex that users and tools will have a harder time to analyze."*

This matches what the measurements show, and the document had already
half-conceded it ("a custom catalog converges on being a worse Iceberg").
Of five tested objections to Iceberg, four collapsed:

| objection | outcome |
|---|---|
| no FileIO for Pelican | packaging gap; ~230 lines Python / ~190 Go (§19, §21) |
| cannot store ns timestamps | false — v3 + iceberg-go is bit-exact (§20); moot anyway, we chose µs (§25) |
| needs a service even to *read* | false — readers need no catalog at all (§23, 12/12) |
| cannot be handed to third parties | false — self-describing from a URL (§23) |
| **needs a database to write safely on Pelican** | **held (§24, §31)** — but so does the custom design (§18) |

The one that held is not a discriminator: *neither* option has a
database-free safe mode on this storage. It removes a bonus for Iceberg
rather than restoring an objection.

§31 re-tested it against every conditional primitive rather than §24's two,
and it still holds — but it is now attributable. The origin (XRootD 5.9.7)
implements no conditional HTTP at all: no `ETag`, no `Last-Modified`,
`DAV: 1`, and 2xx even for methods it does not support. **The database is
the origin's requirement, not the format's**, and it would be imposed on
any format we picked.

## What this costs, stated plainly

Choosing Iceberg is not free, and the following are now accepted costs
rather than open questions:

1. **A stateful catalog service** must be run and backed up alongside the
   exporter. This is the real price. Note it buys the CAS that makes
   multi-writer safe — it is not pure overhead.
2. **iceberg-go's FileIO for Pelican** must be maintained (~190 lines,
   written and tested in §21). It is a supported extension point
   (`iceio.Register`), not a fork.
3. **~5–9 requests per commit** at ~150 ms each (§21). Fine at the current
   batch cadence; it is a reason to keep commits coarse, not frequent.
4. **Microsecond timestamps** (§25) — decided, with zero measured identity
   collisions across 593M rows.

## What does *not* change

The Parquet data files. Every finding in `3_findings.md` about row-group
sizing, pruning and compaction applies identically — Iceberg indexes the
files, it does not reformat them. The exporter fix is still the first
milestone and is independent of this decision. That is also what keeps the
decision reversible: the data files, not the metadata, are the asset.

---

# Does Iceberg also help organise `/node-data`?

Asked 2026-10-14: *"We also host our object storage on Pelican in the folder
/node-data so I'm wondering if by using Iceberg we can structure our object
storage better as well."*

**Partly — but not in the way the question implies, and the useful half is
already half-built.**

## What Iceberg cannot do

Iceberg is a table format. It indexes **rows in Parquet files**. It has no
concept of a JPEG, a WAV, or any opaque blob, and there is no Iceberg layout
that will reorganise `/node-data`'s objects. Putting image bytes *into* a
table is possible but a bad fit: blobs bloat the files that Parquet's
columnar pruning depends on, and they defeat the range-read behaviour that
§5/§17 showed is the whole reason this storage works for us. So:
**Iceberg will not restructure your object storage.**

## What it can do — and the archive is already doing it informally

The valuable thing is not storing the blobs in Iceberg but **indexing them
there**. And this already exists in the data, unplanned. Measured on
`date=2025-01-01/data_0.parquet`:

| | |
|---|---|
| rows with `name = 'upload'` | 43,353 |
| distinct `value_str` URLs | 43,353 (1:1, no duplicates) |
| `meta` carries | `filename`, plus app keys e.g. `thres_otsu` |

A row looks like:

```
value_str = https://storage.sagecontinuum.org/api/v1/data/
            cloud-motion-v1-368/sage-cloud-motion-v1-1.24.11/
            000048b02d15bc6d/1735709333134117735-img2_...jpg
meta      = {filename: img2_1735709333134117735.jpg, thres_otsu: 26.0}
```

So the blob is already a **row in the time-series table**, carrying the same
`(node, vsn, task, plugin, timestamp)` identity as every sensor reading, with
the object URL as its value. The blobs live in object storage; the *index*
to them lives in the table.

That is the right architecture, and it means the answer to the question is:
Iceberg improves `/node-data` **by making that existing index a first-class,
queryable, schema-tracked thing** rather than a string convention nobody
declared. Concretely it would give you:

- **Blob discovery as a normal query.** "Every image from vsn W023 between
  two timestamps where `thres_otsu > 50`" becomes a predicate over the table,
  pruned by statistics, returning URLs — with no listing of `/node-data` at
  all. Listing a WebDAV collection to find objects is the slow path we are
  otherwise stuck with (recall DELETE won't even recurse, §21).
- **One mechanism for both**, which is the stated goal in `design_ideas.md`:
  *"further simplify the bulk path to use a single mechanism for the batched
  Parquet files and BLOBs."* A table that indexes blobs is that mechanism.
- **Referential integrity becomes checkable.** Today nothing verifies that
  43,353 URLs resolve. With the index declared, an orphan/dangling scan is a
  join, and retention on blobs can be driven by the table rather than guessed
  from paths.
- **Permissions get a seam.** `Data Pipeline Considerations.md` raises
  restricted species data. Withholding *rows* from an index is tractable;
  retrofitting policy onto a flat object namespace is not.

## The honest caveat

This is a **design observation grounded in one measured day**, not an
experiment. What it rests on (43,353 upload rows, 1:1 with URLs, `filename`
in `meta`) is measured; what it would cost is not. In particular none of the
following has been tested, and all should be before committing to it:

- whether every blob in `/node-data` has a corresponding `upload` row, or
  only those a plugin chose to publish — **this is the load-bearing
  assumption**, and if the answer is "only some," the table is an incomplete
  index and cannot drive retention or integrity checks;
- whether the Hive-style layout in `design_ideas.md`
  (`/run-id=/app-id=/data-schema-id=/node-id=/date=`) should also govern the
  blob paths, which is a naming decision independent of Iceberg;
- how the index stays consistent when an upload succeeds and the table commit
  fails, or vice versa — a two-system write with no transaction across them.

Worth a dedicated experiment (an `exp26`) rather than folding it into the
catalog decision. It does not change the Iceberg choice either way: the
multi-writer requirement already settles that on its own.

---

# Can storage be abstracted? (asked 2026-10-14, measured as §27)

The question: *"can we abstract Pelican as a storage to allow for other
storage locations? Can Iceberg also do this? Is it already implemented?"* —
against the `Data Pipeline Considerations.md` bullet about pushing finalized
long-term data to resources SAGE does not govern (AWS, a partner's
platform, the nwf.org case).

The framing in the question is the right one: *"with the custom option we
can do it since anything can be built — but if Iceberg already makes this
easy then that's another value added."* So the only useful answer is what
**already exists**, not what is possible. `exp27` (15/15) measured it, and
it splits in two with different answers.

## Yes, and it is already implemented — for *writing* elsewhere

iceberg-go resolves storage through a scheme registry. Each backend's
`init()` registers its schemes, so **the integration step is an import
line**. Listed from the live registry:

| storage | schemes | step |
|---|---|---|
| local filesystem | `file` | none |
| **S3, and S3-compatible** (MinIO, Ceph, Alibaba OSS) | `s3 s3a s3n oss` | one import |
| Google Cloud Storage | `gs` | one import |
| Azure Blob | `abfs abfss wasb wasbs` | one import |

That answers the bullet's literal question — AWS is supported today — and
the S3-compatible coverage matters more than S3 itself, since it reaches
most of what a partner institution would run on-premise.

For anything else, `io.Register(scheme, factory)` is exported public API.
**This is not theoretical: it is how our own Pelican support works** (§21,
~190 lines, no fork). So Pelican is already "just another backend" in this
architecture, which is precisely the abstraction the question asks for —
and it was never Pelican-specific to begin with.

This is a genuine value-add over the custom design. Not because the custom
design *couldn't* abstract its store — `pelican_store.py` already does, in
140 lines — but because S3, GCS and Azure implementations **already exist,
are maintained upstream, and cost one import each**. Under the custom
design each is a port we write and keep working.

**Cost, stated:** importing all three took the probe module from **78 to 133
dependencies**. Import only what is needed; Pelican's own IO adds none.

## No — not for *moving* a table that already exists

This is the half the bullet's use case actually needs, and it fails.

Iceberg metadata records **absolute** locations: 5 occurrences of the
source root in a single metadata file. Copy the table elsewhere and the
copy still points home. Tested with the source directory **deleted** first,
so the result cannot come from silently reading the original:

| | |
|---|---|
| moved table's metadata opens (schema, docs, properties) | **yes** |
| moved table's data reads | **no** — `no such file or directory` |

That failure mode is worse than a clean one: the table opens and describes
itself correctly, and only breaks when someone reads rows. And the obvious
fix is not enough — rewriting every `*.json` leaves data paths stale,
because **manifests are Avro**. iceberg-go v0.7.0 ships no rewrite helper.

Three ways to live with it, in order of preference:

1. **Re-export to the destination** instead of moving. Satisfies "operational
   responsibility ends," and is simpler than path surgery.
2. **Write the long-term path to the destination from the start**, which Q1
   makes a one-line change.
3. **Write a rewrite step.** Last resort; it is the Avro manifests that make
   it real work.

Worth knowing before promising a partner a handoff, which is why it is
recorded here rather than discovered during one. Note this is *not* a point
for Iceberg over the custom design — a custom manifest could store relative
paths and be more portable. Iceberg wins the first question outright and
loses the second.

## And yes — the handover itself works (§23 + §28)

Both questions above are about bytes. The bullet's other half is about
people: *"after the successful push, operational responsibility, including
search and redaction, would end."* That is the part a storage abstraction
alone does not deliver, and it is where Iceberg is strongest.

| what the partner needs | answer | from |
|---|---|---|
| read the data with no SAGE database | yes — hand over a URL | §23 |
| schema, column docs, licence, contact, counts | yes, from metadata JSON alone | §23 |
| see data SAGE commits later | no — a URL is a snapshot pointer | §23 |
| **take over writing** to the table | yes — one catalog row | §28 |

The handover is: grant storage access, give them the `metadata.json` URL.
Nothing else. They read immediately with no catalog; if they want to keep
appending — the nwf.org case, where they bought the node and want to go on
collecting — they insert one row into their own catalog pointing at that
metadata and the table accepts commits (§28 measured exactly this while
recovering from a deleted catalog: 200 → 300 rows after the insert).

That the third row is a "no" is a feature here, not a gap: after handover
SAGE *should* stop appending. It only matters if a partner expects a live
mirror rather than a handoff, which is a different requirement and would
need replication, not this.

**The caveat to check before promising it.** §28 recovered a table sitting
at the path its own metadata names. Adoption plus **relocation** collides
with the Q2 failure above — the inserted row would reference metadata whose
data paths are dead. Copy-then-adopt should be assumed broken until tested;
it is an inference from two results, not a measurement. Re-export avoids it,
which is the third reason on this page to prefer re-export over moving.

---

# Disaster recovery: what happens if the catalog is lost? (§28)

Raised 2026-10-14 from a disaster-recovery article on Iceberg. Worth
answering here because the Iceberg decision **introduces a component the
custom design also needs but that nothing on Pelican can rebuild**: §24
established the catalog database is mandatory on this storage, so it is a
single point of failure. `exp28` (15/15) tested it by deleting the catalog
outright.

## The reassuring part

**A lost catalog is not a data-loss event.** With the database deleted, the
table still opens from its metadata path with `nil` where the catalog goes
(§23) and every row reads back. The catalog holds a *pointer*, not the
data. What a catalog loss costs is **write availability**, not data — and
that is the first thing to confirm in an outage.

Full recovery is **one SQL row**. The SQL catalog's entire per-table state
is `(catalog_name, table_namespace, table_name, metadata_location,
iceberg_type)`, so recovery is an INSERT pointing at the newest
`metadata.json`. Measured: the recovered table returns all rows, retains its
snapshot history, and **accepts new commits** — a full recovery, not a
read-only rescue.

## Two things the article assumes that are not true for us

The article's recommended tools are **Spark procedures**. Our writer is Go:

- **`register_table`** — exists in iceberg-go on `rest`, `glue` and `hive`
  catalogs. **Not on `sql`** (ours), and not on the `Catalog` interface.
  Not a blocker, since the recovery it automates is the one-row INSERT
  above, but the step is ours to write and rehearse.
- **`rewrite_table_path`** — confirmed to exist in Spark, and confirmed
  **absent from iceberg-go v0.7.0**. This resolves the unverified lead left
  in §27: the capability is real, it just is not in our client. A handoff
  that needs path rewriting means standing up Spark for the migration —
  acceptable for a one-off, which is why re-exporting remains the simpler
  option.

## The failure mode to design against

A catalog backup that lags the storage backup by one commit **opens without
error and silently serves the older snapshot** (measured: 200 rows where the
newest has 300). This is the same shape as §18's lost update and §27's moved
table — a success status over a wrong result, which is the category of
failure this whole investigation keeps surfacing on this storage.

The mitigation does not require trusting the catalog backup at all: the
newest metadata file is identifiable **from storage alone**, by
`last-updated-ms` inside each metadata file (cross-checked against filename
ordering, which agrees because iceberg-go zero-pads its sequence prefix).

## Operational requirements this adds

Not objections to Iceberg — conditions on operating it, and they belong in
the implementation plan:

1. A **recovery runbook**, dry-run before it is needed. The step is small;
   the time to discover that is not during an outage.
2. **Back up catalog and storage together**, and record the metadata
   pointer with each backup.
3. **Validate after restore**: compare the catalog's `metadata_location`
   against the newest metadata file on storage. This single check catches
   the silent-stale case.
4. **Untested and worth knowing**: recovery against Postgres rather than
   SQLite, and whether snapshot expiry (§22) can delete metadata that a
   stale catalog still references — which would convert silent-stale into a
   hard failure. The second is arguably the more useful outcome, but it has
   not been measured.

---

# DuckLake instead of Iceberg? (asked 2026-10-14, measured as §29)

> **CLOSED 2026-10-14 — DuckLake is eliminated.** See "Closed: DuckLake is
> out" at the end of this document. The deciding argument is not in this
> section or in §30: it is reader independence. DuckLake needs a live,
> SAGE-hosted `meta.db` (Pelican can host a read-only copy, not a writable
> one), while §32 showed Iceberg can be read from a warehouse URL with **no
> database anywhere**. Everything below remains accurate as measurement —
> including the parts where DuckLake wins — and is kept because the margin
> was genuinely narrow and the reasoning should stay auditable.

Asked after the decision above had already landed on Iceberg, which is the
right time to ask it — the alternative is being judged against constraints
that are now written down and measured rather than against a feature list.

**What DuckLake is.** A 2025 table format from the DuckDB team. Its premise
is a direct critique of Iceberg: Iceberg scatters metadata across files on
object storage *and still needs a catalog database* for the final pointer,
so you run a database either way and get none of its transactional power.
DuckLake puts **all** metadata in the database and only Parquet on storage.
Given §24 — where we proved the database is mandatory on Pelican — that
critique lands squarely on our deployment, so it deserved a real test.

## It wins three of our four constraints

| constraint | Iceberg | DuckLake |
|---|---|---|
| nanoseconds | v3 + `timestamptz_ns` (§20) | `timestamp_ns`, no v3 opt-in |
| publish with no live DB | URL only (§23) | frozen lake over HTTP, read-only |
| **relocation** | **fails** (§27 Q2) | **works** — relative paths |
| **Go writer** | **first-party, in use** | **none** |

The relocation result is the one that stings, because §27 Q2 is our one
unfixed weakness. DuckLake's catalog has a `path_is_relative` column and
stores data-file paths relative. The same destroy-the-source move test that
leaves an Iceberg table opening-but-unreadable gives DuckLake 200/200 rows
and accepts new writes afterwards. And with a mismatched data path it
**refuses with an error** instead of guessing — the first thing in these
experiments that fails loudly rather than succeeding wrongly, against
§18/§24/§27/§28 which all did the opposite.

So the honest summary is that DuckLake is not the weaker design. On the two
things this storage keeps punishing us for — absolute paths and silent wrong
answers — it is better.

## It loses on the one thing that cannot be configured away

**There is no Go client.** One implementation (the DuckDB extension), plus a
Rust DataFusion port. Every Go project in the space drives DuckDB through
cgo. Adopting DuckLake therefore means either embedding DuckDB in the
exporter — cgo, a C++ dependency, no pure-Go build — or writing a Go writer
against the spec, which is precisely the "anything can be built" answer the
§27 framing already dismissed as uninteresting.

That is disqualifying *here specifically*, because §20 and §21 are the
results that unblocked nanoseconds by moving the writer to Go. Choosing
DuckLake would undo them.

Two smaller problems, for completeness:

- **No `timestamptz_ns`.** The spec has `timestamp_ns` and a
  microsecond-only `timestamptz`, and nothing that is both. The archive's
  column is `timestamptz_ns`, so DuckLake forces a choice between the zone
  and the precision.
- **Catalog loss is worse.** Iceberg's per-table state is one row, which is
  why §28 recovery is one INSERT and why §23 readers need no catalog at all.
  DuckLake's catalog is 29 tables with a row per data file; the Parquet
  survives a catalog loss but the table does not. **Inferred from the
  schema, not measured** — no catalog-loss test was run against DuckLake.

## Decision

**Stay with Iceberg**, on the Go client alone. Everything else about the
comparison is closer than expected, and that is worth recording rather than
flattening into "we picked the standard one."

**Revisit if** a native Go writer appears, if the exporter embeds DuckDB for
other reasons, or if partner handoff becomes a primary requirement rather
than a bullet in `Data Pipeline Considerations.md`. DuckLake is the proof
that §27 Q2 is a choice Iceberg made, not a law of the model.

**Not tested:** DuckLake on Pelican. Its catalog is a database file rather
than a CAS pointer swap, so §24 does not transfer directly and the question
is genuinely open — it was not run because the Go finding decides the
matter before it matters. Also untested: concurrent writers, catalog loss,
Postgres as catalog, and any realistic data volume.

## Re-decided the same day: the Go requirement was withdrawn (§30)

The decision above has exactly one load-bearing premise, and it was removed
hours later: *"it is fine that there is no Go client, we are not tied to Go,
we can also use Python."* So it does not stand as written, and the honest
thing is to re-derive it rather than keep the conclusion and swap the
reasoning underneath it.

**What §30 measured.** DuckLake runs on Pelican. It needs a custom fsspec
filesystem to get there — but so does Iceberg (`pelican_fileio.py` in §19,
`pelican_io.go` in §21), so "DuckDB cannot write to `https://`" is a glue
requirement both formats have, not a discriminator. With that glue it
commits, the Parquet lands on the origin, and nanoseconds survive. On the
§18/§19 concurrency test it loses no acknowledged row: the losing writer is
refused with an error, where the custom catalog lost 3/3 silently. It
matches Iceberg.

**What inverted — and then cancelled.** §29 listed "no `timestamptz_ns`"
against DuckLake, which is true against *iceberg-go*. Against **pyiceberg**
it reverses: pyiceberg 0.12.0 cannot write v3 at all and v2 rejects
`timestamp[ns]` (§13/§20), so a Python Iceberg writer stores no nanoseconds
whatsoever, on 97.60% of archive rows.

**That reversal does not actually help DuckLake here, because of the
microsecond decision recorded above.** We already chose to store
`timestamptz` at microsecond precision for DuckDB support, and that
decision explicitly notes pyiceberg can write v2, so a Python writer is
not blocked. Nanoseconds are not being kept by either format under the
current plan. The honest position: **this axis is neutral unless the
microsecond decision is reopened** — which §25 says should be confirmed
with whoever owns the science. An earlier draft of this section treated
the reversal as a live advantage for DuckLake; that was an error of not
reading our own decision back.

**What survives.** Two things, and only two:

1. **Reader reach** (§23). The binding constraint on this archive is who can
   read it. Iceberg is read by NDP, Spark, Trino and the cloud warehouses;
   DuckLake has one implementation and a Rust port. For an archive whose
   point is to be handed to people we do not control, this is the strongest
   remaining argument and it does not depend on language choice at all.
2. **Catalog loss** (§28). Iceberg recovers with one INSERT; DuckLake's
   29-table catalog has no analogue. But this is **inferred from the schema
   and still not measured**, and it is now doing real work in the decision,
   which is more weight than an untested claim should carry.

**Decision: stay with Iceberg, on reader reach.** (Superseded the same day by
the closure below, which rests on the stronger and more precise *reader
independence* — no SAGE endpoint at all, §32 — rather than on how many
engines can parse the format.) That is a weaker and more
specific basis than §29's, and it should be recorded as such — the
comparison is closer than §29 made it look, and two of §29's three
objections do not survive the Python allowance.

**Before relying on point 2, measure it.** A DuckLake catalog-loss test is
cheap, needs no credentials, and is the obvious gap. **Also still open:**
Postgres as the catalog, which DuckLake's docs name as the multi-writer
backend — the §30 concurrency result is SQLite, as §19's Iceberg result
was.

> **Superseded.** The catalog-loss test was **cancelled** by the closure
> below: the decision no longer rests on point 2, so measuring it cannot
> change the outcome. Point 1 (reader reach) was the weaker form of what
> became the deciding argument — reader *independence*, sharpened by §32.
> Postgres-as-catalog stays open for **Iceberg** only.

**Revisit if** reader reach stops being the constraint — if the consumers
turn out to be DuckDB/Python shops rather than Spark/Trino ones, the case
for Iceberg largely dissolves, because §30 shows DuckLake wins or ties on
every operational axis measured so far: relocation (§27 Q2), failing loudly,
nanoseconds in Python, and concurrency.

## Closed: DuckLake is out (decided 2026-10-14, after §32)

**Decision: DuckLake is eliminated. Iceberg is the format.** The basis is
reader independence: *a reader must not need a SAGE-hosted endpoint to read
the archive.* DuckLake cannot satisfy that; Iceberg now can, completely.

### The argument

DuckLake keeps **all** metadata in the catalog database and only Parquet on
storage. So reading a DuckLake table means reaching its `meta.db`.

One precision, because the earlier framing of this was slightly wrong and
the record should not repeat it: **Pelican *can* host `meta.db`** — §30
attached one over HTTPS and read it with range requests. What it cannot do
is host a **writable** one, because DuckDB's httpfs cannot write, and the
archive is append-only-forever, not a frozen snapshot. A read-only copy on
Pelican is a stale fork the moment the next commit lands.

So a live DuckLake archive requires a `meta.db` that SAGE hosts and serves.
Every external reader then depends on a SAGE endpoint being up, reachable,
and maintained — for a dataset whose entire purpose is to be handed to
people we do not control, and to outlive the team operating it.

### Why §32 makes this decisive rather than merely preferable

Until today the comparison was *Iceberg's one mutable catalog row* against
*DuckLake's whole database*. That is a difference of degree, and §29/§30
had already shown the margin was narrower than first written.

§32 changed the shape of it. The Iceberg **filesystem catalog runs on
Pelican**, and an independent catalog instance loaded the table **from the
warehouse URL alone, with no database anywhere in the picture**. Iceberg can
therefore reach *zero* SAGE-hosted components for a reader. DuckLake's floor
is one hosted database, and no configuration changes that — it is what the
format is.

That is an asymmetry in kind, not in degree, and it lands exactly on the
constraint that matters most here.

| | Iceberg | DuckLake |
|---|---|---|
| what a reader needs from SAGE | **nothing** (§23, §32) | a live `meta.db` endpoint |
| metadata location | on Pelican, self-describing | in the database |
| database-free operation | **yes, single-writer (§32)** | not possible |

### What this costs, honestly

§30 measured DuckLake winning or tying on several operational axes —
relocation (§27 Q2), failing loudly on a broken path, nanoseconds under a
Python writer, and concurrency. Those were real results and this decision
overrides them, deliberately: reader independence is worth more to this
archive than any of them, and it is the one property that cannot be
recovered later by changing a setting.

Two other things this closes:

- **The DuckLake catalog-loss test is cancelled.** It was the top-ranked
  open measurement in §30 and findings item 13, on the grounds that
  Iceberg's recovery advantage was *inferred from the schema, not
  measured*. That was the right objection while the decision rested on it.
  It no longer does — reader independence decides this without reference to
  catalog loss — so measuring it would not change the outcome. Recorded as
  **deliberately dropped**, not forgotten.
- **Postgres-as-catalog**: *(largely closed by §35 — SQLite measured safe
  to 24 concurrent committing processes; Postgres is required only if
  committers run on separate hosts, since a SQLite URI is a file path.
  A deployment question, not a reliability one.)* Original text:
  remains open for *Iceberg*, where §19's
  concurrency result is still SQLite-only. It is no longer a DuckLake
  question.

### What would reopen it

A reader population that is in practice all DuckDB/Python **and** a decision
that SAGE will host a metadata service indefinitely. If SAGE is committed to
running an endpoint anyway, the central objection weakens considerably and
§30's operational wins come back into play. Absent both, this stays closed.

## Latency is not a reason to prefer either design (`exp39`, 5/5)

This document compares the two designs on safety, operations and
capability, and quotes commit cost (9 requests vs 5, ~0.9 s vs ~0.7 s). It
never measured the two things a sceptic would ask about: Iceberg's metadata
layer sits between a consumer and the bytes, and its write path re-encodes
every file. `exp39` measured both, like-for-like — same files, same origin,
same FileIO, same process, interleaved, with `catalog.py`'s `CatalogStore`
ported to Pelican.

| | Iceberg | custom | ratio |
|---|---|---|---|
| write 169 MB | 23.9 s | 25.2 s | 0.95× |
| read all (8 workers) | 4.6 s | 7.1 s | 0.6× |
| read one day | 2.9 s | 3.6 s | 0.8× |
| metadata requests | 12 | 2 | — |

**The write penalty does not exist.** The ns→us rewrite that §36 showed is
mandatory was expected to cost wall time; it does not. Both paths are
network-bound at ~6 MB/s and the rewrite sends **17% fewer bytes**, so it
pays for its own CPU. Run-to-run spread (0.86–1.05×) exceeds the difference
between the designs.

**Iceberg's metadata overhead is fixed.** 12 requests against 2 is a 6×
ratio that does not grow: at 24 data files Iceberg is already at 0.50
metadata requests per data file, and falling.

**The custom catalog scales worse in exactly one place.** Its snapshot JSON
embeds every live file — 274 B/file, so **~1.3 MB at 4,564 files, fetched in
full on every read and unprunable**. Iceberg splits the same information
across `metadata.json` and per-partition manifests, so a pruned reader
fetches a subset. Both designs have O(n) metadata; Iceberg's is partitioned
and the custom catalog's is not. If the custom catalog is built, this is the
thing to design for — and it is solvable (per-day snapshot shards), just not
free.

**What this changes in the recommendation above: nothing, directly.** The
case for the custom catalog rests on operational surface and the
ns-timestamp freedom, neither of which this touches. But it removes one
argument from circulation: **any claim that "Iceberg adds latency" is now
measured and false**, and should not be used to support either option.
