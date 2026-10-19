# Measured results

Environment: conda env `sage_pipeline` (python 3.12, duckdb 1.5.6, pyarrow 25.0.1).

Data: complete copy of `sage03:/media/local/archive-export-test/archive` under
`data/archive` — 4,564 files, 608 day directories,
**208,584,864,752 bytes, byte-exact against sage03** (verified by summing file
sizes on both ends; a `du -sb` comparison differs by 2.5 MB because it counts
directory inodes). 300 randomly sampled footers all parse.

Full-archive row count: **42,638,972,374**. Note this exceeds the ~33 billion
implied by sage03's `stats.txt`, which is simply stale — it is dated
2026-10-05 while the export was still writing on 2026-10-06. The plugin
distribution agrees closely (wxt536 at 91.23% here vs 90.52% in `stats.txt`),
so the copy is consistent; `stats.txt` just predates the last days.
Scripts: `experiments/exp0*.py`. Raw JSON: `experiments/results/*.json`.

Bytes read is measured by serving the Parquet over a local HTTP server that
logs every Range request. That is the number that becomes Pelican traffic;
wall time on a local SSD hides it completely.

> **Independent review (2026-10-14):** [Review and corrections](../../plans/5_independent_review.md)
> distinguish historical measurements from conclusions that the tests do not
> establish. In particular, §32–33 supersede the claim that a filesystem
> catalog cannot run on Pelican. New audit logs are separate from these results.

## 1. The archive cannot prune, and the cause is one missing writer option

`archive-exporter/main.go` sets `SortingWriterConfig` but never
`MaxRowsPerRowGroup`, so each file is **one row group** of up to 10M rows.
That row group's `plugin` min/max spans every plugin in the batch, so
row-group statistics can never reject it.

Page indexes *are* written (verified: `has_column_index=True` on all 14
columns). DuckDB still does not use them to avoid fetching bytes.

Measured on `date=2025-01-01` (6 files, 59,496,597 rows, 303 MB), over HTTP:

| query | rows returned | bytes read | % of day |
|---|---|---|---|
| `count(*)` | 59,496,597 | 458 KB | 0.15% |
| `plugin = <rare>` | **1** | **49.0 MB** | 16.2% |
| `name = env.temperature` | 281,235 | 49.0 MB | 16.2% |
| `plugin = <wxt536>` | 53,597,535 | 196 MB | 64.7% |

A query returning **one row reads 49 MB**. On a 3-day slice it is 12 rows for
146.6 MB, with all 18 files opened. The cost tracks days requested, not rows
matched —
which is what `plans/1_draft.md` section 5 predicted, but the conclusion drawn
there was that page statistics would rescue it. They do not.

## 2. Row groups fix it

Same 10M rows rewritten at several row group sizes, same sort order, same ZSTD:

| variant | file size | full scan | big plugin | **rare plugin** | name filter |
|---|---|---|---|---|---|
| go exporter (1 rg) | 100,728,595 | 53.9 MB | 53.9 MB | **53.9 MB** | 53.9 MB |
| pyarrow 1 rg | 67,883,056 | 43.6 MB | 43.6 MB | **43.6 MB** | 43.6 MB |
| pyarrow rg=2,000,000 | 69,738,725 | 45.0 MB | 26.2 MB | **6.7 MB** | 23.4 MB |
| pyarrow rg=1,000,000 | 71,603,557 | 46.5 MB | 16.9 MB | **2.2 MB** | 24.3 MB |
| pyarrow rg=250,000 | 78,637,731 | 52.1 MB | 18.7 MB | **0.46 MB** | 23.8 MB |
| pyarrow rg=100,000 | 84,978,977 | 58.7 MB | 21.7 MB | **0.45 MB** | 25.1 MB |

Rows returned are identical across every variant (verified each run).
Rare-plugin read drops **53.9 MB -> 0.45 MB, about 120x**. The cost is file
size: 100k-row groups are 25% larger than one group. 250k-1M rows is the knee.

## 3. The writer also leaves ~26% of the archive on the table

Column-level comparison of the same 10M rows, Go writer vs pyarrow, both ZSTD,
both one row group:

| column | Go | pyarrow | note |
|---|---|---|---|
| `meta.key_value.key` | 4,553,897 | 233,596 | Go: `DELTA_LENGTH_BYTE_ARRAY`, pyarrow: `RLE_DICTIONARY` |
| `meta.key_value.value` | 20,936,085 | 5,386,976 | same cause |
| `value_float` | 30,091,292 | 15,477,708 | Go uses `BYTE_STREAM_SPLIT` |
| `time` | 20,848,789 | 25,256,786 | Go's `DELTA_BINARY_PACKED` wins here |
| total | 100,375,416 | 67,543,161 | |

The `meta` map is stored as plain strings because the struct tag is
`parquet:"meta,optional"` with no `dict`. It is 25% of the compressed file.

## 4. Both fixes on one real day

`date=2025-01-01`, 59,496,597 rows, rewritten at rg=1,000,000:

| | before | after |
|---|---|---|
| size | 302,970,347 | 224,674,223 (**74.2%**) |
| row groups | 6 | 60 |
| footer total | 13,195 | 122,544 |
| rare-plugin query | 49.0 MB | **6.0 MB** |
| name-filter query | 49.0 MB | **13.9 MB** |
| rewrite time | — | 27.3s for the day |

Rows returned identical in every case.

**That single-day number does not generalize on its own, and the first
attempt to generalize it was wrong.** Rewriting 8 days spread across the
archive with pyarrow's defaults made most days *larger* (median 108%), not
smaller. The cause is visible per column:

| column | Go writer | pyarrow default |
|---|---|---|
| `time` on a single-plugin file | 30,732,452 (`DELTA_BINARY_PACKED`) | 42,642,294 (dictionary) |
| `meta.key_value.value` | 4,748,813 (plain) | 156,203 (dictionary) |

Each writer wins on different columns. Go picks delta encoding for `time`,
which is ideal for a sorted nanosecond series, and that matters enormously on
the `data_1..N` files that hold nothing but wxt536. pyarrow picks dictionary
encoding, which matters enormously for the `meta` map on the mixed `data_0`
files. Whichever single writer you choose, you lose one of the two.

Setting both explicitly — `DELTA_BINARY_PACKED` on `time`, dictionary on the
tag and meta columns — beats either writer on both kinds of file:

| file | Go | pyarrow default | both fixes |
|---|---|---|---|
| pure wxt536 (`data_1`) | 40,940,364 | 43,668,896 | **31,106,471** |
| mixed (`data_0`) | 77,562,891 | 54,221,680 | **45,436,665** |

Re-running the 8-day sample with both encodings set:

| | |
|---|---|
| ratio | min 53.2%, **median 68.1%**, max 76.1% |
| aggregate over sample | 2,873,644,962 -> 2,081,007,095 (**72.4%**) |
| projected full archive | 194 GiB -> **141 GiB**, saving ~54 GiB |
| rewrite cost | ~27s per day, ~4.5h for 608 days single-threaded, parallel by day |

So the archive can be **~28% smaller and prunable at the same time** — but
only if the writer sets encodings per column instead of relying on any one
library's defaults.

**Correction (2026-10-12).** The first run of this sample reported 73.4%
(min 59.0, median 74.5, max 75.8) and was described as "8 days sampled over
the whole span". It was not. `exp12` selects `days[::len(days)//8]`, and it
ran while the archive rsync was still in flight — only ~240 of 608 days were
present, so every sampled day fell in 2025-01..2025-07. The numbers above are
the re-run against all 608 days; the superseded result is kept as
`results/exp12_rewrite_sample.PARTIAL-240day.json.bak`.

The conclusion survives, and the projected saving grows slightly, because
2025 is 92.8% of archive bytes. But the per-day ratio is **not** stable the
way the first run suggested — it splits by era:

| era | days in sample | ratio | why |
|---|---|---|---|
| 2025 | 5 | 59.0-76.1% | large days, mixed plugins, `meta` dictionary win dominates |
| 2026 | 3 | 53.2-62.4% | small days (~5M rows), proportionally more footer/dictionary overhead |

The sample is byte-weighted: 2026 is 6.9% of sampled bytes against 7.2% of
the real archive, so the aggregate is representative even though the spread
is wide. Anyone quoting a single number should quote the aggregate, not the
median — the median is now pulled down by the small 2026 days and understates
the saving on the bytes that actually matter.

## 5. Footer cost, for the manifest question

Measured tail read for a `count(*)`: **32-128 KB per file**. For the
4,564-file archive that is **281 MiB** to plan a full-archive query. A year is
a couple of thousand files.

**This is not the size of the footers.** The archive's Thrift footers are
~2.2 KB each (§7 measures those, and the two sections used to disagree by
~30x without saying why). DuckDB never issues a footer-sized request: it
fetches one power-of-two tail chunk per file and parses the footer out of it.
Verified by logging every Range request (`experiments/probe_footer_cost.py`):

| day | files | thrift footers | actually fetched | ratio |
|---|---|---|---|---|
| `2025-01-01` | 6 | 13,195 B (2.2 KB/file) | 458,752 B (76 KB/file) | 34.8x |
| `2025-07-21` | 14 | 32,418 B (2.3 KB/file) | 983,040 B (70 KB/file) | 30.3x |
| `2026-08-31` | 1 | 2,702 B | 32,768 B | 12.1x |

The chunk scales with file size — 32 KiB below ~32 MB, 64 KiB to ~100 MB,
128 KiB above — and in every case **over 96% of it is slack around a 2.2 KB
footer**. Projecting that rule across the real file sizes in the archive
gives the 281 MiB above, against 9.6 MiB if only footer bytes moved.

Both numbers are correct measurements of different things, and the
distinction matters for the manifest argument: the cost a manifest avoids is
the **281 MiB of tail chunks**, not the 9.6 MiB of footers. Quoting the
footer figure understates the manifest's value by ~30x.

Note also that the per-file footer *content* grows with row groups
(13 KB -> 123 KB for the day in §4), so the two changes push in opposite
directions and the manifest matters more after fixing row groups, not less.

One caveat on the old text: it said "2 requests per file". The body-request
count is 1 per file; exp06's `http_requests` also counts HEAD requests, which
transfer no bytes.

## 6. Why local DuckDB looks fast and Pelican will not

On local disk, finding 1 row out of 2,243,828,776 takes 0.03s. That number is
misleading, and the reason matters for the Pelican decision.

Three levels of pruning exist, and only the first two work today:

1. **`date=` directory** — picks the days. Works.
2. **File-level statistics** — because `plugin` is the leading sort key, a
   file's plugin min/max is often a narrow lexicographic band. For a rare
   plugin over 30 days, file statistics reject **232 of 242 files**. This is
   better than `plans/1_draft.md` assumed: the plan says row-group min/max
   "cover every plugin in the batch", which is only true of the first file of
   each day. Files `data_1..N` hold the sorted tail and are often a single
   plugin (40 of 242 files have plugin min == max).
3. **Inside a surviving file** — does not work. One row group means a
   surviving file is read essentially whole.

Measured on the 3-day slice (`date=2025-01-01..03`, 18 files, 910 MB): for a
plugin with **12 matching rows**, file statistics reject **15 of 18 files**,
but the 3 survivors total **233.3 MB** and **146,645,682 bytes (146.6 MB)** is
actually transferred. Locally that is page-cache-warm and invisible. Through
Pelican it is 146 MB of HTTP range reads to return 12 rows.

The 3 survivors are exactly the `data_0.parquet` of each day (78.0, 78.2,
77.2 MB) — the first file of a day spans every plugin, so its min/max band
can never reject anything. That is the mechanism behind the level-2/level-3
split above.

One wrinkle worth recording: `files_touched` in the JSON is **18**, not 3,
because DuckDB opens every file's footer before it can apply statistics. The
rejection shows up in bytes (16.11% of the slice), not in files opened. That
footer traffic is the 1.4 MB `count_star` row, and it is what the manifest in
§7 removes.

So the honest statement is: **the archive prunes between files but not within
them.** Fixing row groups is what makes level 3 work, and it is the difference
between 146 MB and a few MB per selective query once the files are remote.

## 7. The manifest is worth building, and prunes more than expected

Built over 30 days (242 files, 2.24 billion rows, 10.7 GB):

| | value |
|---|---|
| footer opens today | 242 files, 534 KB of Thrift footer, 0.33s local |
| full manifest | 238,766 bytes |
| slim manifest (path, rows, size, time + plugin min/max per group) | **80,434 bytes** |
| read time, slim | under 1 ms |

Extrapolated to the full 4,564-file archive: **~1.5 MB slim manifest in one
request**, versus ~10 MB of footers.

**Read the 10 MB figure with §5.** It counts `serialized_size`, the Thrift
footer only. That is the right number for "how much metadata exists", but it
is *not* what planning transfers: a reader fetches a 32-128 KiB tail chunk
per file, so the real cost over HTTP is **281 MiB**, not 10 MB. The manifest
replaces the larger number.

Pruning power measured on real statistics:

| predicate | files rejected |
|---|---|
| `plugin` that does not exist | 242 / 242 (100%) |
| a real rare `plugin` | 240 / 242 (99%) |
| `name = env.temperature` | 208 / 242 (86%) |

The plan expected the manifest to help only with the fan-out of footer reads.
It also prunes well, because sorting by `plugin` makes the per-file bands
narrow. That strengthens the case for the custom manifest in section 7 and
weakens the "we need Iceberg for planning" argument, at least at this scale.

## 8. The query API can be served from Parquet

`experiments/cold_backend.py` compiles the existing Beehive JSON query into
DuckDB SQL and emits `{timestamp, name, value, meta}` records. No new query
language, which is the constraint the handoff records.

`experiments/exp08_query_api.py` — **12 of 12 checks pass** on real archive
data:

- a range split in two is exactly the union of the halves, no gap, no
  duplicate (the tier-boundary property section 8 needs)
- `[start, end)` is half-open: the boundary instant lands only in the later
  half (0 records at 00:30:00 in the first half, 67 in the second)
- `tail=1` returns one record **per series** (7 records for 7 series), not one
  overall, matching Flux `tail()` applying per table
- the exact-vs-regex asymmetry in `influx_backend.go` is reproduced:
  `wxt.wind.direction` is a literal match (18,461 rows) while
  `wxt.wind.*` is a real regex (36,922 rows)
- `meta` is reassembled from the promoted columns plus the map, so the record
  looks like today's API response
- the four validation errors (`missing start`, head+tail, window without func,
  bad filter key) are rejected

Not yet covered: aggregations (`experimental_func` / `experimental_window`,
including the three different timestamp rules), and a real diff against live
InfluxDB, which needs access this machine does not have.

## 9. The proposed first milestone passes

`plans/1_draft.md` ends with:

  "Demonstrate that Beehive can query a month of archived observations before
   and after that month's daily files are consolidated — and return identical
   results while the superseded files are safely pruned."

`experiments/exp10_compaction.py` runs that on 3 real days (178,902,746 rows),
compacting 18 files into 9 with 1M-row row groups, then re-running the same
Beehive query objects and comparing an order-independent digest of the records.

| query | records | identical | before | after |
|---|---|---|---|---|
| one hour of one measurement | 252,434 | **YES** | 4.49s | 4.41s |
| rare plugin over the range | 12 | **YES** | 0.67s | **0.07s** |
| `tail=2` per series | 14 | **YES** | 4.83s | 3.32s |
| regex on measurement name | 247,398 | **YES** | 4.92s | 4.08s |

| | before | after |
|---|---|---|
| files | 18 | 9 |
| bytes | 910,099,136 | 755,691,541 (83.0%) |
| row groups | 18 | 359 |
| compaction time | — | 81.1s for 3 days |

Content is identical, not merely the counts: the comparison is a SHA-256 over
the sorted record set, so a reorder or a dropped field would fail it.

At 81s per 3 days, a full 608-day pass is roughly 4.5 hours single-threaded,
and it is embarrassingly parallel by day.

What this does **not** yet cover, and what the catalog work in section 7 is
actually for: there is no manifest publication step, no reader-pinned catalog
version, and no deletion of the superseded files. This shows the data half of
the milestone is sound. The safety half is still unbuilt.

## 10. Late data: correction files work for appends, not for fixes

**Export lag.** Every file's footer carries `exported_at`. Sampled across 40
days spread over the full 608-day span (316 files): lag between the day
covered and the day exported is min 55 days, median 462, max 641.

Those numbers carry no information, and it is worth being precise about why.
Every `exported_at` in the sample falls inside a single 29-hour window
(2026-10-03T14:16Z .. 2026-10-04T19:46Z), so the "lag" is just
`export date − day covered`: it decreases by exactly one day for each day
later in the archive, from 640.6 at 2025-01-01 down to 55.2 at 2026-08-09.
It is an artifact of one bulk backfill run in early October 2026, not a daily
pipeline, and it measures the age of the archive rather than anything about
data arrival. So the archive carries **no evidence about how late real SAGE
observations arrive** — the open question in the plan cannot be answered from
these files. It needs InfluxDB write timestamps, which this machine cannot
reach.

**Correction (2026-10-12).** This section previously read "40 days spread over
the archive (163 files) ... min 462, median 536, max 641". Three things were
wrong. The run was made against a partially synced archive (~210 of 608 days
present), so `days[::len(days)//40]` sampled only 14 days, all inside
2025-01..2025-07 — "spread over the archive" was false, and the 462 minimum
was simply the most recent day that had been synced, not the archive's. The
file count was also wrong: 163 appears nowhere in the data; the old run's JSON
holds 172 rows, and the real 40-day sample has 316. Re-run against all 608
days gives the numbers above. `exp11_late_data.py` now prints the local span
and warns when fewer than 608 days are present (the same guard added to
exp12), and it writes the min/median/max into the JSON — previously the JSON
stored only `rows[:200]`, so recomputing the stats from it gave a different
answer than the printed summary. Old result kept as
`results/exp11_late_data.OLD.json.bak`.

**Correction files.** `archive-exporter/main.go` refuses to touch a published
day:

```go
ok, err := dirExists(fmt.Sprintf("archive/date=%s", date))
if ok { log.Printf("export for %s already exists. skipping!", date); return nil }
```

Section 9 proposes a correction file instead. Tested by copying a published
day, writing one late observation as `correction_0.parquet` beside the
`data_N.parquet` files, and re-querying:

| | |
|---|---|
| records before | 37,037 |
| records after | 37,038 |
| re-read stable | yes |
| day rewritten | no |

It works, and it is cheap (5 KB file). But it only works for **appends**.
Nothing in the layout distinguishes a correction from an original, so there is
no way to express "this record supersedes that one" and no way to detect a
duplicate re-delivery. A reader that globs the directory will happily return
both copies.

That is precisely the gap the section 7 catalog fills, and it is the strongest
argument in these results for building it (or adopting Iceberg) rather than
relying on the directory tree.

## 11. The exporter change, verified in Go on real data

The Python results above say what is achievable. This is the actual change to
`archive-exporter/main.go`, compiled and run against a real archive file
(`date=2025-07-21/data_0.parquet`, first 3,000,000 rows) on sage03 with the
same `parquet-go v0.32.0` the exporter already uses.

Two changes:

```go
// 1. dictionary-encode the meta map's keys and values.
//    parquet-go supports this through the parquet-key / parquet-value tags.
Meta map[string]string `parquet:"meta,optional" parquet-key:",dict" parquet-value:",dict"`

// 2. give each file real row groups instead of one group per batch.
w := parquet.NewGenericWriter[Observation](f,
    parquet.Compression(&zstd.Codec{Level: zstd.SpeedBetterCompression}),
    parquet.MaxRowsPerRowGroup(1_000_000),   // <- added
    parquet.SortingWriterConfig(...),
)
```

Measured:

| writer | bytes | vs today |
|---|---|---|
| today (1 row group, plain meta) | 38,822,086 | — |
| today + `MaxRowsPerRowGroup(1M)` | 38,692,389 | 99.7% |
| **dict meta**, 1 row group | 28,725,576 | **74.0%** |
| **dict meta + 1M row groups** | 30,577,457 | **78.8%** |

Two things worth noting:

- The dictionary tag is where the space comes from; row groups cost about 5
  points of size back. They are still worth it, because they are what makes
  selective reads cheap (exp05: 53.9 MB -> 0.45 MB), and that trade is the
  whole point.
- `MaxRowsPerRowGroup` alone changes size by 0.3%. Anyone who tries it without
  the dictionary tag will conclude it does nothing. The two changes should
  land together.

The Go API was verified against the module source on sage03 rather than
inferred: `MaxRowsPerRowGroup(numRows int64)` is at
`parquet-go@v0.32.0/config.go:604`, and the `parquet-key` / `parquet-value`
tags are documented at `schema.go:167`.

A working A/B harness is in `scratch/pqtest/` (not committed; `scratch/` is
gitignored).

## 12. How much catalog is actually needed (the Iceberg question)

`plans/1_draft.md` section 7 leaves the catalog choice open, and the handoff
lists "custom manifest versus Iceberg is explicitly undecided". The question
is really two questions: **can a small custom catalog do the job**, and **does
it stay small at SAGE's scale**. Both are measurable.

### A 211-line catalog gives the required safety properties

`experiments/catalog.py` implements the section 7 table: a snapshot is an
immutable file list plus statistics; committing writes a snapshot then swaps a
pointer; readers pin a snapshot for their lifetime. Supersession is by
generation, not row-level deletes.

`experiments/exp13_catalog.py` tests it on real archive days — **10/10 pass**:

| property | result |
|---|---|
| planning rejects files from statistics alone | 2 of 12 files survive, 0.01 ms, **no file opens** |
| planning loses no rows | identical counts |
| reader pinned across a mid-query compaction | unaffected |
| new reader after compaction | identical results |
| compaction swaps files atomically | 6 superseded, 7 live, no overlap |
| snapshot written but never committed | invisible; pointer unmoved |
| archive frontier after simulated crash | unchanged |
| superseded files identifiable | yes, and **not** deleted |
| commit against a stale parent | rejected |

The crash test is the ticket's stated bar: *"a failed attempt leaves the
archive frontier unchanged unless the published snapshot actually contains the
interval."* It passes.

The whole design needs **one** atomicity primitive: an atomic pointer swap.
It does not need conditional writes, atomic rename of data files, or listing.
That matters because it is the only thing Pelican must support, and it is
isolated in a single class (`CatalogStore`) for exactly that reason.

### It stays small, but only if the statistics are trimmed

A naive snapshot carrying every column's stats per row group projects to
**39 MB** for today's 4,564 files — which would be a real argument for
Iceberg's split-manifest design. Measured on 558 real files, the shape
matters far more than the file count:

| snapshot form | per file | projected to 4,564 files | gzipped |
|---|---|---|---|
| all 9 columns, per row group | 633 B | 2.9 MB | 127 KB |
| time + plugin, per row group | 332 B | 1.5 MB | 90 KB |
| time + plugin, per **file** | 318 B | **1.5 MB** | **86 KB** |

And the cheap form prunes exactly as well as the expensive one: for a real
plugin, file-level stats reject **480 of 558** files — the same as full
per-row-group stats. Per-group detail buys nothing at planning time here,
because DuckDB re-reads the footer of any file that survives anyway.

Growth, from `exp14`: parse time is ~157 ms at 4,564 entries and ~1.3 s at
30,000 (roughly ten years at the current rate). A flat snapshot rewrites
itself on every commit, which is the design's real ceiling — fine at one
commit per exported day, wrong at one per minute.

### What this does and does not settle

It settles capability: a custom catalog is small, and it covers compaction,
crash safety, and reader isolation at this scale.

It does **not** settle two things:
- **Row-level corrections.** This catalog supersedes whole files. Replacing
  one bad record means rewriting its file. Iceberg's delete files express that
  natively. exp11 showed the directory tree cannot do either.
- **Pelican.** Every number here comes from a local filesystem where
  `os.replace` is atomic. Whether Pelican can provide an atomic pointer swap —
  the single primitive this needs — is unmeasured, and it is also the thing
  Iceberg would need, more of.

### What Iceberg would require on this storage path

Verified against the Iceberg spec and Java 1.12.0 source (released 2026-09-30),
and the `apache/iceberg-go` v0.7.0 release (2026-10-01). Caveat recorded
honestly: the research pass could not use web search (blocked by org policy),
so this comes from reading the spec, source and release docs directly. An
in-flight proposal could exist that this did not surface.

**1. Iceberg's commit needs a compare-and-swap that Pelican does not have.**
The spec: *"All changes to table state create a new metadata file and replace
the old metadata with an atomic swap"*, and *"the atomic operation used to
commit metadata ... is not standardized"*. Only two schemes exist:

- **File system tables** — write then rename. The spec carries an explicit
  deprecation warning: *"This file system based scheme to commit a metadata
  file is deprecated and will be removed in version 4 of this spec. The scheme
  is unsafe in object stores and local file systems."* `HadoopTableOperations`
  literally calls `fs.rename` and bolts on an optional external lock manager
  precisely because rename alone is not safe.
- **Metastore tables** — *"a pointer in a metastore or database that is updated
  with a check-and-put operation."*

So Iceberg on Pelican means standing up a **REST or JDBC catalog service** to
hold the CAS off storage — a stateful service plus its database. No documented
S3 conditional-write (If-Match / If-None-Match) commit path exists in 1.12.0,
and it would need real S3 precondition semantics regardless.

**2. `iceberg-go` has no FileIO for this storage.** v0.7.0's write path is
genuinely capable (append, rewrite files, rewrite manifests, overwrite,
copy-on-write delete, positional and equality deletes, row delta all marked
supported). But the FileSystem support table lists **only S3, GCS, Azure Blob
and local**. There is no HTTP/WebDAV FileIO, so one would have to be written.
It is also still 0.x with no API-stability promise.

The binding constraint is therefore **not** iceberg-go's maturity. It is that
Iceberg's commit requires a primitive this storage does not provide, and its
Go client cannot address this storage at all today.

By contrast the custom catalog needs exactly **one** primitive — an atomic
pointer swap — and that same primitive is a strict subset of what Iceberg
would need from Pelican. If Pelican cannot provide it, Iceberg is further from
working than the custom catalog, not closer.

### The Go read path works, and cross-validates the Python prototype

`beehive-data-api` is Go, so the cold backend eventually has to live there.
`cold_backend.py` proves the *semantics*; this proves the Go path exists.

A ~90-line Go reader using the same `parquet-go v0.32.0` the exporter already
depends on, run on sage03 against `date=2025-01-01/data_0.parquet`:

```
scanned 10000000 rows, matched name="env.temperature": 281235 in 36.9s
first match: time=1735689616275324153 vsn=W06F value=-15.1 meta_keys=9
```

**281,235 is exactly what DuckDB returned** for the same filter on the same
day in exp08. Two independent readers, same answer — which is the check that
matters before trusting either.

Two incidental findings from this run:

- A first attempt searched `data_0.parquet` for `wxt.wind.direction` and found
  0 rows. That is correct, not a bug: files are plugin-sorted, so a day's
  measurements are spread across its files and `data_0` holds only the first
  slice. Anything that plans per-file has to account for this; it is also why
  `date=` + statistics planning matters rather than guessing at file contents.
- The scan took 37 seconds for one file. That is the single-row-group problem
  again, now visible from Go: with no row groups to skip, the reader
  materialises all 10M rows. Go has no DuckDB-style vectorised filter to hide
  it, so **the row-group fix matters more for a Go backend than for the DuckDB
  prototype**, not less.

## 13. Iceberg tested directly through pyiceberg, which rejects the archive's timestamps

`exp16_iceberg_interop.py` builds a real Iceberg table from archive data
(pyiceberg 0.12.0 + SqlCatalog) to test the two claims the recommendation
rests on. It found a blocker neither the design docs nor the research pass
had surfaced.

### pyiceberg cannot store the archive's `time` column without losing data

> **Superseded in part by §20 (2026-10-14).** This section's measurements
> are correct, but its original title — "Iceberg cannot store the archive's
> `time` column without losing data" — generalised a pyiceberg failure into
> a claim about Iceberg. iceberg-go writes v3 `timestamptz_ns` bit-exact.
> Read this section as "what pyiceberg 0.12.0 does", and §20 for the format.

The archive writes `time` as `timestamp[ns, tz=UTC]` (`main.go`:
`parquet:"time,timestamp(nanosecond),delta"`). Iceberg v2 has no nanosecond
timestamp type:

```
format-version 2: UnsupportedPyArrowTypeException:
                  Column 'time' has an unsupported type: timestamp[ns, tz=UTC]
format-version 3: NotImplementedError: Writing V3 is not yet supported
                  (apache/iceberg-python#1551)
```

Iceberg v3 added `timestamp_ns`, but pyiceberg 0.12.0 cannot write v3 tables.
(**§20**: iceberg-go can. The limit below is pyiceberg's, not the format's.)

This is not cosmetic. Measured over 8 days sampled across the full 608-day
span (593,388,314 rows, `experiments/probe_ns_remainder.py`): **579,171,675
rows (97.60%) carry a non-zero nanosecond remainder** — e.g.
`1735689606915629791`. It is 96–99.9% on every 2025 day sampled and 56–60% on
every 2026 day; the lowest day measured is 56.52%. Downcasting to microseconds
discards real precision on nearly the whole archive. pyarrow independently
confirms it, refusing the cast outright:

```
ArrowInvalid: Casting from timestamp[ns, tz=UTC] to timestamp[us, tz=UTC]
would lose data: 1735689606915629791
```

The cast only succeeds with `safe=False`. ~~**Adopting Iceberg today means
either truncating ~98% of timestamps or waiting for v3 write support.**~~
**Revised (§20): adopting Iceberg today means writing through a client that
supports v3.** iceberg-go already does; pyiceberg does not. Truncation is
only forced on a Python writer. The custom catalog has no such constraint
either way: it indexes the Parquet files as written and never reinterprets
their types.

**Correction (2026-10-12).** This section previously read "5,000,000 real rows
from `date=2025-01-01`: 2,750,040 rows (55.00%)". That figure had no code
behind it — only a comment in `exp16_iceberg_interop.py`, nothing in any
results JSON — and it described one row group of one day, quoted to two
decimals as if it characterised the archive. `probe_ns_remainder.py` now
measures it for real and writes `results/ns_remainder.json`.

Two traps worth recording, because both produce confident wrong answers:

- **Do not measure this through DuckDB.** Its `TIMESTAMP` is microsecond
  precision, so `read_parquet` downcasts `timestamp[ns]` on the way in and
  `epoch_ns(time) % 1000` is then always 0. The first version of the probe did
  exactly this and reported 0.00% across 593M rows. The remainder has to be
  read out of the Parquet column as int64.
- The conclusion is unchanged in direction but much stronger in degree: the
  real blocker is ~98%, not 55%. The earlier number understated it.

### Once downcast, the table works — and is a genuine handoff artifact

With `time` forced to microseconds, **6/6 checks pass**:

| check | result |
|---|---|
| row count round-trips | 2,000,000 -> 2,000,000 |
| content identical (rows, plugins, names, value sum) | identical |
| the `meta` map survives | 2,000,000 rows |
| catalog DB deleted, table directory moved | — |
| **DuckDB reads it with no catalog, after the move** | 2,000,000 rows |
| underlying Parquet remains plain and readable | 2 files |

This confirms the interop argument in `plans/4_catalog_recommendation.md`:
an Iceberg table really is self-describing, readable by a third party who
never had the catalog. That is what makes it a better handoff artifact than
a bespoke snapshot, which matters for the partner-AWS and cached-search
questions in `plans/Data Pipeline Considerations.md`.

Metadata cost: 4 files, 11,554 bytes for a single data file, against ~318
bytes per file for the custom snapshot (exp15). Iceberg's metadata is far
heavier per file, though it amortises as files grow.

### What this means for the decision

It reinforces the recommendation rather than changing it, and sharpens the
exit condition. Iceberg is not merely operationally heavier here — it cannot
currently represent the data faithfully. Any future move to Iceberg should
wait for v3 write support in whichever client is used, and should treat the
nanosecond question as a go/no-go check rather than a detail. Conversely, if
the team decides microsecond precision is sufficient, that decision should be
made explicitly and recorded, not discovered during a migration.

## 17. Pelican/NRDStor measured for real (exp17)

Everything before this section measured HTTP against a **local server
standing in for Pelican**. This section is the real thing:
`nrdstor.nationalresearchplatform.org:8443/sage`, XRootD v5.9.7 behind
pelican 7.27.0, authenticated with real SciTokens signed by the SAGE
issuer key (`kid 3cfa`, verified against the published
`sagecontinuum.org/.well-known/jwks.json`).

All test objects were written under `/sage/cold-storage-test/`, a prefix
created for this experiment, and **deleted afterwards** (verified 404;
`/node-data` re-checked intact at 70,016 entries). The production
prefixes were never touched.

`experiments/pelican_token.py` mints the tokens, mirroring
`waggle-auth-app/downloads/views.py:120-168` including the `nbf` lag hack.

### The three gating questions, answered

`plans/4_catalog_recommendation.md` named three things to test, in order.
All three pass.

| test | result |
|---|---|
| HTTP Range reads | **yes** — `Accept-Ranges: bytes`, single `206`, suffix range `206`, multi-range `206 multipart/byteranges` |
| read-after-write consistency | **yes** — object readable immediately after `201 Created`, correct bytes |
| atomic pointer swap | **yes, by MOVE** — 20 swaps under 4 concurrent readers, 318 reads, 0 torn, 0 errors, never went backwards |

Suffix range matters specifically: it is how a Parquet reader finds the
footer. `GET -r -8` returned the final 8 bytes ending in `PAR1`.

### Verb support (measured, not documented)

| verb | result | note |
|---|---|---|
| `GET` | 200 | |
| `PUT` | 201 | creates parent directories implicitly |
| `DELETE` | 200 | file, and an **empty** directory |
| `DELETE` on a non-empty collection | **409** | will not recurse — delete children first (§21) |
| `MOVE` | 201 | **works onto an existing target** — this is the pointer swap |
| `PROPFIND` | 207 | per-object metadata and listing without a GET |
| `COPY` | 404 | advertised in `OPTIONS`, does not work |
| `If-None-Match: *` on PUT | **201** | **ignored — overwrote anyway** |
| `MOVE` with `Overwrite: F` | **201** | **ignored — overwrote anyway** |

The last two are the important negatives. **There is no conditional write
and no compare-and-swap.** Rename exists and is atomic enough for a single
writer, but nothing on this origin will reject a write because someone else
got there first.

### Real Parquet over real Pelican

Two files uploaded: today's exporter format (78 MB, **1 row group**) and the
compacted form (313 MB, **119 row groups**). pyarrow drove a file object that
counted every byte crossing the network.

| | today (1 rg) | fixed (119 rg) |
|---|---|---|
| file size | 78.0 MB | 313.6 MB |
| open + footer | 1 req, 65,536 B | 2 req, 292,247 B |
| read `plugin` column | 1 req, 100,830 B | 119 req, 133,364 B |
| **one row group** | n/a — the file *is* one row group | **1 req, 1,118 B** (500,000 rows) |

The last row is the exporter fix, measured over Pelican: **1,118 bytes** to
reach 500,000 rows, versus a file whose smallest addressable unit is the
whole 78 MB. This reproduces the localhost finding against the real origin.

### The new finding: latency, not bytes, is the Pelican cost

Nothing before this could have seen this, because localhost has no latency.

```
tiny-range latency : median 149.7 ms   (min 139.8, max 182.6)
  1 MB single range:  1.28s ->  0.8 MB/s
  8 MB single range:  0.64s -> 13.1 MB/s
 32 MB single range:  1.11s -> 30.3 MB/s
```

**Every request costs ~150 ms regardless of size.** That is why reading the
`plugin` column from the 119-row-group file took **24.6 seconds** while
moving only 133 KB: 119 sequential round trips at ~207 ms each. The
single-row-group file did it in 0.6s because it made one request.

Measured naively, this says the exporter fix makes things *worse over
Pelican*. It does not — but only because the requests parallelise:

| workers | 119 ranges | effective |
|---|---|---|
| 1 | 18.06s | 151.8 ms/req |
| 8 | 2.29s | 19.3 ms/req |
| 16 | 1.21s | 10.2 ms/req |
| 32 | **0.65s** | **5.5 ms/req** |

28x at 32 workers, close to linear, so the latency is round-trip and not
server-side contention.

**Consequence for the exporter change.** `plans/3_findings.md` recommends
`MaxRowsPerRowGroup(1_000_000)`. That recommendation stands, but it now
carries a condition it did not have before: **the reader must issue range
requests concurrently.** A sequential reader over Pelican pays 150 ms per row
group and will be slower than today's single-row-group files, which is
exactly the wrong conclusion to reach from a benchmark. Row group sizing
should also be chosen with the 150 ms floor in mind — a row group small
enough to be cheap in bytes is still a full round trip, so there is a floor
below which smaller groups only add latency.

### Consequence for the catalog

The custom catalog's one required primitive **exists**: `MOVE` onto an
existing path, atomic under concurrent readers, 0 torn reads in 318.
`catalog.py`'s `os.replace` maps onto a Pelican `MOVE`, so the port is the
single class the recommendation anticipated.

The caveat is the missing compare-and-swap. `exp13`'s "commit against a
stale parent is rejected" check passes on POSIX because the implementation
re-reads and compares; on Pelican nothing at the storage layer enforces it,
so **two concurrent writers would silently clobber each other**. Single
writer is safe. Concurrent writers need either an external lock or a
compare-and-swap the origin does not provide — which is the same constraint
Iceberg hits, and the reason the recommendation's "more than one writer"
trigger is the one to watch.

Also worth noting: the origin reports `storageType: posix`, not an object
store. The Iceberg spec's "unsafe in object stores" warning is about a
weaker storage model than this one.

## 18. The catalog, running on Pelican (exp18)

§17 showed the primitive exists. This runs the actual catalog on it:
`catalog.py` unchanged, with `CatalogStore` swapped for
`PelicanCatalogStore` (`experiments/pelican_store.py`). The snapshots and
the `CURRENT` pointer are real objects on NRDStor; every commit is a real
`MOVE`. exp13's six checks run verbatim, plus two the POSIX version could
not pose.

Data files stayed local on purpose: §17 already measured data reads over
Pelican, and the catalog never opens a data file — it indexes paths. The
separation is the design's, not a shortcut.

Scope note: 12 files over 2 days. This tests *correctness*, not scale;
exp14 covers scale on POSIX.

**11 of 12 passed. The failure is the important result.**

### What carried over unchanged

| check | result |
|---|---|
| planning rejects files from statistics alone | 2/12 survive — pin 273 ms (network), plan 0.04 ms (local) |
| planning does not lose rows | 5 rows, identical |
| pinned reader unaffected by compaction mid-query | 5 rows, reader held its snapshot |
| new reader sees identical results after compaction | 5 rows |
| compaction swaps files atomically | 6 superseded, 7 live, commit 1.21s |
| snapshot written but never committed is invisible | pointer unmoved |
| frontier unchanged after simulated crash | identical |
| superseded files identifiable, not deleted | 6 files, all still present |
| stale-parent commit rejected (**sequential**) | rejected |
| pinned snapshot readable after pointer moved 3x | 5 rows, non-vacuous |

The design survives the move to network storage. Note the cost shape
changed: `pin()` is now a 273 ms network fetch where it was free, so a
reader should resolve the pointer **once** per query and hold it —
which is what `Snapshot` already does.

### The failure: concurrent commits silently lose updates

```
trial 1: 2/2 committed  <-- lost update
trial 2: 2/2 committed  <-- lost update
trial 3: 2/2 committed  <-- lost update
```

**3 of 3 trials**, not a flake. Two writers each read the same parent
pointer, both pass the `expected_parent` check, both `MOVE` — and the
second silently overwrites the first. The earlier commit is simply gone:
no error, no conflict, nothing in the archive recording that it happened.

This is not a bug in `catalog.py`. The same code passes on POSIX because
the check-to-swap window is microseconds. Measured here:

```
check (GET pointer): 145 ms
swap (PUT + MOVE)  : 561 ms   -> race window ~561 ms wide
```

A **half-second** window, every commit, with no compare-and-swap at the
storage layer to arbitrate. exp13's check 6 passes sequentially and will
keep passing — it never had two real writers. That is precisely why it
needed re-running against the real storage rather than a filesystem.

### What this means

The recommendation does not change, but its single-writer assumption
is now a **hard constraint, not a preference**:

1. **One writer, enforced.** The exporter is the only writer today, so the
   constraint is satisfiable — but it must be enforced (a lease, a lock, a
   singleton CronJob), not merely assumed. Two exporter pods, or a manual
   re-export racing the scheduled one, would silently lose a commit.
2. **It does not favour Iceberg.** Iceberg's spec-compliant commit needs
   the same check-and-put this origin lacks; it would need an external
   catalog service. The constraint is the storage's, not the format's.
3. **"More than one writer needs to commit concurrently" is the trigger
   to watch**, and it now has a measured failure mode attached rather than
   a theoretical one.

A detection-only mitigation is cheap and worth it even with one writer:
have the committer re-read the pointer immediately after its `MOVE` and
verify it still holds its own snapshot id. That closes nothing — the
update is already lost — but it converts silent corruption into a loud
error, which is the difference between a bug you find now and one you find
in six months' worth of snapshots.

### Cleanup

All objects were written under `/sage/cold-storage-test/exp18/` and deleted
(18 objects, verified 404; the parent test prefix was removed too).
`/node-data` re-checked before and after: 70,016 entries, untouched.

## 19. Iceberg, running on Pelican (exp19)

§12 rejected Iceberg on this storage partly on reasoning: "Iceberg's commit
requires a compare-and-swap that this storage does not provide", and
"iceberg-go has no FileIO for this storage". exp17/18 held the custom
catalog to a measured standard; this holds Iceberg to the same one.

The test is deliberately generous. `pelican_fileio.py` supplies the FileIO
pyiceberg does not ship, so what gets tested is Iceberg's **commit model**
on this storage rather than a missing import. **14 of 15 checks pass** —
and the headline is that Iceberg does *better* than the custom catalog on
the exact case the custom catalog loses.

The layout, since it is the thing most easily misread: **everything Iceberg
writes as a file went to Pelican** — 28 metadata objects (table metadata,
manifest lists, manifests) and 8 Parquet data files under
`/sage/cold-storage-test-iceberg/warehouse/sage/obs/`. The SQLite database
held exactly **one mutable row** — the table's current `metadata_location` —
and lived on the test machine, because Pelican cannot host it (below).

### The missing FileIO is a packaging gap, not a capability limit

pyiceberg's scheme table (`io/__init__.py:319`, `io/fsspec.py:321`) has no
`https` entry, so a Pelican URL raises `No registered filesystem for
scheme: https` — the Python mirror of the iceberg-go FileSystem table §12
quoted. But `py-io-impl` accepts a custom FileIO and the interface is three
methods. 230 lines later, Iceberg runs on Pelican:

| operation | wall time | requests |
|---|---|---|
| create table | 0.7s | 1 GET, 1 PUT, 2 HEAD |
| append 300,000 real archive rows | 1.8s | 1 GET, 4 PUT, 2 HEAD |
| full scan back | 1.7s | 4 GET, 3 HEAD |

Content identical across the round trip (rows, distinct plugins, distinct
names, and the `value_float` sum all match).

One correction to §12 worth recording: **NRDStor accepts a normal
`Authorization: Bearer` header.** exp17 used `?authz=<token>` because the
production *download* path needs a redirect to carry credentials, which
made the URL parameter look mandatory. It is not, and a library client that
only knows how to set headers can authenticate here.

### Iceberg wins the concurrency case the custom catalog loses

This is the result that matters, and it reverses the expected direction.

```
trial 1: committed=['A','B']  CAS-conflict-detected=True
trial 2: committed=['B','A']  CAS-conflict-detected=True
trial 3: committed=['A','B']  CAS-conflict-detected=True
300,000 + 6,000 = 306,000 rows, got 306,000  -- no rows lost
```

Two writers append from the *same* starting metadata, three times, and
nothing is lost. The loser's commit does fail — `CommitFailedException` is
raised internally every time — and pyiceberg then refreshes, rebases its
snapshot onto the winner's and retries (`table/__init__.py:1130-1147`).
Both appends land because appends commute. The custom catalog, given the
identical race in exp18, lost an update 3 times out of 3 and reported
success to both writers.

A note on how this was nearly misread. The first version of this check
asserted "at most one commit succeeds" and recorded 3/3 FAIL — the same
shape as exp18's failure. That assertion was wrong: for commutative
appends the correct outcome is that both land and no rows are lost, and the
retry log lines were the clue. The check now measures rows and observes the
CAS separately. Had it been left alone it would have produced a confident,
symmetric, and false result.

### But the CAS is in SQLite, not on Pelican

The whole reason Iceberg wins is one SQL statement
(`pyiceberg/catalog/sql.py:520-536`):

```sql
UPDATE iceberg_tables SET metadata_location = <new>
 WHERE metadata_location = <old>     -- rowcount < 1 -> CommitFailedException
```

That row lives in SQLite or Postgres. Pelican holds only immutable files
and never participates in the commit. Checked directly: every catalog
pyiceberg ships — REST, Hive, Glue, DynamoDB, SQL, BigQuery, InMemory —
needs a service or a database. **There is no filesystem/hadoop catalog in
pyiceberg at all**, so the deprecated rename-based scheme is not even
reachable from Python. §12's conclusion stands, now measured: Iceberg on
Pelican means standing up a stateful service.

So the two systems are not actually being compared on equal terms. Iceberg
is safe *because* it brought a database. The custom catalog is unsafe
because it has only Pelican. Given the same database, the custom catalog's
`commit(expected_parent=...)` would be equally safe — that one function is
precisely where a conditional UPDATE would go.

### The handoff claim holds, with one gap

Deleting the catalog database and reading the table from a bare metadata
URL returns all 306,000 rows. So the table really is a self-describing
artifact and §13's handoff argument survives on Pelican.

The gap: **there is no `version-hint.text`.** 11 `*.metadata.json` files sit
in the directory and nothing on the storage says which is current. A
recipient must be *told* the metadata path. That is fine for a deliberate
handoff and useless as disaster recovery — lose the database and you are
picking the newest file by inspection.

### Readers never touch the database

The handoff test above deletes the catalog, but it reads through pyiceberg.
The question that decides how much the database actually costs is whether a
*query engine* can reach the data without it — DuckDB straight to Pelican,
the way the custom design would. Three paths, all measured, no catalog
involved in any of them:

| path | reader needs | result |
|---|---|---|
| `read_parquet('<url>?authz=…')` | the Parquet URL | 1,000 rows (that file) |
| `read_parquet('<url>')` + `CREATE SECRET` | URL + bearer token | 1,000 rows |
| `iceberg_scan('<metadata>.json')` | the metadata URL | **306,000 rows** |

The first two treat the files as plain Parquet and do not know Iceberg
exists — they read the single data file they are pointed at. `iceberg_scan`
reads the table *as a table*, resolving the manifest list and all eight data
files over HTTPS by itself, and supports selective queries.

Two practical notes:

- DuckDB accepts both the `?authz=` URL parameter and a real `Authorization`
  header via `CREATE SECRET … (TYPE http, EXTRA_HTTP_HEADERS …)`. **Prefer
  the header** — the token then stays out of the URL and out of any log that
  records query text.
- **An `iceberg_scan` token must cover the whole table prefix, not just the
  data file.** Scoped to the data file alone it returns `403 Forbidden`,
  because the scan also fetches the metadata JSON and the Avro manifests.
  That 403 looks like a capability limit and is really a scoping mistake; it
  cost a false negative on the first attempt here.

So the division is: **writers need the database, readers need a URL and a
token.** That substantially narrows the "stateful service" objection — it is
not a service every query flows through, it is one the committer needs. If
Sage's pattern is one exporter writing and many consumers reading, the
database sits on the narrow side of that split.

The caveat is consistency, not access. A reader without the catalog reads
*whichever snapshot it was handed*, because of the `version-hint.text` gap
above — storage alone does not say which metadata file is current. That is
fine for a published handoff and is not a live "always read the newest"
path. The custom catalog's `CURRENT` pointer solves exactly this, and it is
the one place its design is genuinely simpler.

### The database cannot live on Pelican

If it could, Iceberg would need no external service and the main cost
argument against it would collapse. It cannot, and the failure mode is the
dangerous kind:

| probe | result |
|---|---|
| `PUT` with `Content-Range` (patch 10 bytes inside 1000) | **201, silently ignored** — file became 10 bytes |
| `PATCH` | 501 Not Implemented |
| `OPTIONS` → `DAV:` | `1` — no class 2, so no locking |

A database needs in-place page writes and locking; Pelican offers whole-file
replace and nothing else. Note the first row is the same shape as the ignored
`If-None-Match: *` and `Overwrite: F` in §17: **success returned for a
modifier that was not honoured.** A naive attempt to host SQLite on Pelican
would appear to work and corrupt the file rather than fail.

This is also why the custom catalog gets away without a database: its pointer
is a *file*, swapped by `MOVE`. That is the trade — no service, but no
compare-and-swap either.

### Cost

| | requests per commit | wall time |
|---|---|---|
| Iceberg (1,000 rows) | 9 (2 GET, 4 PUT, 3 HEAD) | 0.9–1.3s |
| custom catalog (exp18) | 5 (1 GET, 2 PUT, 2 MOVE) | ~0.7s |

Roughly 2x the requests, and at ~150 ms each (§17) that is the dominant
cost. The metadata-to-data ratio is the sharper number: after 8 appends the
table holds **28 metadata objects for 8 data files**. Iceberg never mutates
metadata, so every commit adds files; at the archive's real commit rate
this needs expiry and metadata rewrite as routine maintenance, which is
more operational surface than the custom catalog's single pointer.

### What this changes

Iceberg is **not ruled out by Pelican**. The storage runs it. The FileIO is
230 lines, not a blocker, and the handoff story is real.

What rules it out sits elsewhere: ~~the **nanosecond timestamp loss** (§13
— 97.6% of rows, and pyiceberg 0.12 cannot write v3), plus~~ the stateful
service its commit requires, now a measured fact rather than an inference.
**(Updated after §20: the nanosecond half of this no longer holds —
iceberg-go writes v3 `timestamptz_ns` bit-exact, so only the stateful
service remains.)**

The honest summary for the recommendation: *Iceberg is safer under
concurrent writers than the custom catalog, and that safety comes entirely
from the database it requires.* If a database is acceptable, Iceberg's
concurrency story is better. If it is not, neither system is safe with two
writers and the custom catalog is the cheaper single-writer design.

### Cleanup

All objects were written under `/sage/cold-storage-test-iceberg/` and
deleted (36 objects, verified 404). `/node-data` re-checked: 70,015 child
entries, untouched. (This is the same inventory §18 recorded as 70,016 —
that count included the collection's own PROPFIND href; `pelican_fileio`
excludes it. 70,015 children + self = 70,016 hrefs.)

---

## 20. The nanosecond blocker was a client limit, and it is already lifted in Go

`experiments/icebergo/` (Go, iceberg-go v0.7.0 + SQLite catalog). Prompted by
a direct challenge: pyiceberg's own type reference documents
`TimestampNanoType` and `TimestamptzNanoType`, so why did §13 conclude
Iceberg has no nanosecond support? The challenge was right.

### What §13 got wrong

§13 measured pyiceberg and reported Iceberg. The failures it recorded are
real and reproduce exactly — but they come from one client:

```
format-version 2: ValueError: timestamptz_ns is only supported in 3 or higher
format-version 3: NotImplementedError: Writing V3 is not yet supported
                  (apache/iceberg-python#1551)
```

That `NotImplementedError` is unconditional: `TableMetadataV3.model_dump_json`
at `pyiceberg/table/metadata.py:588` raises on every call. The V3 class
exists, parses, and carries the right fields; it simply refuses to serialize.
The ns types are present in 0.12.0 and both report
`minimum_format_version() == 3`, which is precisely why v2 rejects them.

Issue #1551 is still open (last checked 2026-10-14) and 0.12.0 is the latest
PyPI release, so this is current, not stale — but it is a *packaging* fact,
the same shape as the missing FileIO in §19.

### iceberg-go writes v3 with nanoseconds, bit-exact

iceberg-go has no such guard: `table/metadata.go:41` sets
`supportedTableFormatVersion = 3`. Run against **500 real archive
timestamps** (sampled from `date=2025-01-01`, every one carrying a non-zero
nanosecond remainder — the loader rejects microsecond-aligned input, since
that would make the round-trip check vacuous):

| check | result |
|---|---|
| v2 rejects `timestamptz_ns` (type really is v3-gated) | PASS |
| v3 table created with a `timestamptz_ns` column | PASS |
| metadata reports format-version 3 | PASS |
| append of nanosecond rows | PASS |
| row count round-trips (500) | PASS |
| **values survive exactly, no truncation** | **PASS** |
| values not silently rounded to microseconds | PASS |

7/7. `want[0]=1735714189877162625 got[0]=1735714189877162625`.

Creating a table proves only that metadata serializes, so the test that
carries the weight is the round trip. A client that downcast to microseconds
would pass the first four checks and fail the last two; both failure modes
are asserted separately for that reason.

Verified on disk rather than trusting the in-memory `Version()`:

```
on-disk format-version: 3
on-disk time type     : timestamptz_ns
parquet time type     : timestamp[ns, tz=UTC]
parquet values == source: True
```

### The gap is write-only: pyiceberg reads what Go writes

pyiceberg 0.12.0 loads the Go-written v3 table, reports the column as
`timestamptz_ns`, scans all 500 rows and returns the values exactly. It
fails only on write:

```
ValueError: Cannot write manifest for table version: 3
```

So the boundary is narrow and specific — **write support in one client** —
not representability, and not a Python-reader problem either.

### What this changes

The recommendation listed two independent reasons for custom and called this
one "the harder blocker, and the one to check first if anyone revisits this."
Checked: it does not hold. A Go exporter can write v3 `timestamptz_ns` today
with no precision loss. Only a Python *writer* is still blocked.

That leaves one argument against Iceberg — the stateful service its commit
needs (§19) — where the document originally had two. Three Iceberg
objections have now been tested and all three came back weaker than written:
the FileIO (packaging, §19), the pointer swap (present, §17), and the
timestamps (one client, here).

The 97.60% remainder measurement is untouched and still correct; what was
wrong was the conclusion drawn from it.

### The methodological error, named

The mistake was inferring a property of a *format* from the behaviour of one
*implementation*, then writing it into a recommendation as a tested fact.
What made it durable is that the test genuinely passed — §13 is not a
careless section, and "tested directly, not inferred" was true of the
measurement while false of the conclusion. The guard is to pin findings to
the client measured, which is why §13 is now titled for pyiceberg and the
version-caveat list says to re-check per client rather than per "Iceberg".

### Cleanup

Ran entirely on local disk in `os.MkdirTemp` warehouses, all removed
(`ICEBERGO_KEEP=1` retains one for inspection). No network, no Pelican
objects, `/node-data` untouched.

---

## 21. The Go v3 + nanosecond path, on real Pelican

`experiments/icebergo/pelican_test.go` (build tag `pelican`). §20 proved
iceberg-go writes v3 `timestamptz_ns` bit-exact, but on local disk. §19
proved Iceberg runs on NRDStor, but through pyiceberg, which cannot write v3
at all. Neither covered the combination that matters, so this runs v3 +
nanoseconds + the real origin together.

**12 of 12 checks pass.**

### iceberg-go needs a Pelican IO too, and it is ~190 lines

iceberg-go ships IO for local files, memory and gocloud blob stores —
nothing for an authenticated HTTPS origin, so Pelican is unaddressable out
of the box. Same gap as pyiceberg's FileIO (§19), same conclusion: it is
packaging, not capability. `io.Register("https", ...)` is a supported
extension point, so no fork is needed.

`pelican_io.go` implements `IO`, `ReadFileIO` and `WriteFileIO`, with the
same two Pelican constraints the Python port had: buffer-and-PUT-once
(no append) and whole-object reads (the ~150 ms per-request floor makes
range-streaming metadata objects slower, not faster).

SciTokens are minted natively in Go (`token.go`, a port of
`pelican_token.py`), so the experiment exercises one language end to end —
a Go exporter would mint its own. **Pelican accepts a Go-minted ES256
token**, which is not free: JWS wants a fixed-width `R||S` signature, while
Go's usual `ecdsa` entry points return an ASN.1 sequence. A short big.Int
left-unpadded shifts the halves and produces a signature that verifies
nowhere.

### The result

| check | result |
|---|---|
| Pelican accepts a Go-minted SciToken | PASS |
| read-after-write on Pelican | PASS |
| v3 table with `timestamptz_ns` created on Pelican | PASS |
| metadata reports format-version 3 | PASS |
| append of 500 real archive nanosecond rows | PASS |
| row count round-trips | PASS |
| **values survive exactly over Pelican** | **PASS** |
| values not silently rounded to microseconds | PASS |
| table location is on Pelican | PASS |
| on-origin metadata JSON says `"format-version":3` | PASS |
| on-origin metadata JSON declares `timestamptz_ns` | PASS |
| DELETE on a non-empty collection refused (409) | PASS |

`want[0]=1735714189877162625 got[0]=1735714189877162625`, over the real
origin. The on-origin metadata JSON was fetched back over HTTPS rather than
trusting the in-memory `Version()` — the table could report v3 locally and
serialize something else.

### Cost

| operation | requests | wall time |
|---|---|---|
| create table | 2 | 63 ms |
| append 500 rows | 5 | 181 ms |
| full scan | 3 | 106 ms |
| whole test | 16 | ~1.9 s |

Cheaper than pyiceberg's 9 requests per commit (§19), though the operations
are not identical and this is one small append rather than 300,000 rows.
The shape matches §17: request count dominates, not bytes.

### A new Pelican property: DELETE will not recurse

`DELETE` on a non-empty collection returns **409**. The first run of this
test passed all ten substantive checks and then failed cleanup, leaving 5
objects on the origin; they were removed immediately with the Python
`rmtree`, and `RemoveAll` now does the recursion in Go.

It is recorded as a check rather than quietly fixed in the cleanup path,
because the flat `Remove` *looks* correct and anyone porting catalog
maintenance — snapshot expiry, orphan-file removal — will hit it. It also
belongs with §17's list of Pelican verbs that behave unlike POSIX, though
this one fails loudly rather than silently, which is the better failure.

### What this settles

The last gap in the Iceberg-on-Pelican story is closed. Taken with §19 and
§20: **Iceberg runs on NRDStor, and the Go client stores the archive's
nanosecond timestamps there without losing a digit.** Neither the storage
nor the timestamp format is what rules Iceberg out.

What remains is §19's finding, unchanged: the commit needs a compare-and-swap
that Pelican does not provide, so a database or service has to live
alongside. That is now the single argument, and it cuts both ways — the same
database is why Iceberg loses 0 rows under concurrent writers where the
custom catalog loses an update 3 times in 3.

### Cleanup

Everything was written under `/sage/cold-storage-test-icebergo/`, a prefix
created for this test alone and distinct from §17's `/cold-storage-test` and
§19's `/cold-storage-test-iceberg`. Verified afterwards: all three prefixes
PROPFIND **404**. `/node-data` re-checked before and after: **70,015 entries,
unchanged**. The SQLite catalog is a local temp dir, removed on exit. No
token or key material is written to disk, and the 500 archive timestamps in
`testdata/` are integers with no node, plugin or measurement context.

## 22. Schema evolution and tags: the "beta schema" and DOI questions

`experiments/icebergo/evolve_test.go` (build tag `evolve`). Two claims came
up when weighing Iceberg that nothing here had measured:

1. *"With a custom catalog the schema has to be right from the start; with
   Iceberg we could ship a beta schema and change it later."*
2. *"Does Iceberg help with DOIs / data organization?"*

Both are format questions rather than storage questions, and §21 already
settled that this client's writes reach Pelican intact, so this runs on
local disk. **17 of 17 checks pass.**

### A schema can be changed after publishing, and old files stay readable

A v3 table was created with a deliberately incomplete "beta" schema
(`time`, `value`), rows written, and then the schema changed the way a
*we got this wrong* moment would require:

| operation | result |
|---|---|
| `AddColumn` ×2 (`name`, `plugin`) on a published table | PASS |
| `RenameColumn` `value` → `reading` | PASS |
| rows written under the OLD schema still readable | PASS (2/2) |
| renamed column still returns pre-rename data | PASS (`[0.5 1.5]`) |
| nanosecond values unchanged across the change | PASS |
| both schema versions retained in metadata | PASS (2 schemas) |

The renamed-column check is the one that matters. **Iceberg resolves columns
by field ID, not by name**, so data written before a rename still reads back
under the new name without rewriting a single file. A directory-of-Parquet
design resolves by name — which is why renaming a column there means
rewriting every file that has it, or carrying the old name forever.

Evolution is *checked*, not blind. Narrowing `double` → `float` is refused:

```
cannot resolve type: cannot promote double to float
```

Widening is allowed, narrowing is not, because narrowing could silently lose
data in already-written files. That guardrail is the difference between
"the schema can change" and "the schema can be broken".

**So the claim is true, with one correction to how it is usually phrased.**
It is not that Iceberg lets you be careless early; it is that the *additive*
and *renaming* kinds of mistake stop being expensive. Choosing the wrong
type width in the narrowing direction is still permanent, and partition
layout is a separate axis (`UpdateSpec` exists, and old data keeps its old
partitioning rather than being rewritten).

### Tags: a named, immutable version — the DOI-shaped feature

Iceberg's answer to "cite this dataset" is a **tag**: a named reference to
one snapshot. Tested as a citation would actually be used — mint it, keep
publishing, then try to resolve it:

| check | result |
|---|---|
| tag created naming one snapshot (`doi-v1`) | PASS |
| tag survives later writes and still resolves | PASS |
| HEAD shows every row written since | PASS (5) |
| reading the tag returns data as of *then* | PASS (2, not 5) |
| the tagged rows are byte-identical to what was published | PASS |
| tag is a pointer, not a copy | PASS (no extra data files) |
| **tagged snapshot survives aggressive expiry** | PASS |
| **the tagged version is still readable after expiry** | PASS |

The last two are the ones that decide whether a tag is safe to mint a DOI
against. Every Iceberg table eventually runs snapshot expiry to stop metadata
growing without bound; if that job could delete a tagged version, a citation
would rot. Run at the most aggressive setting the API allows
(`WithRetainLast(1)`, `WithOlderThan(0)`), **4 snapshots dropped to 2: the
untagged intermediate was deleted, the tagged one was kept and still
returned its rows.**

That control was added after a first version of this test passed vacuously.
With only a tagged snapshot and HEAD present, expiry had nothing it was
permitted to remove, so "the tag survived" would have passed even if tags
were not protected at all. An untagged, non-HEAD snapshot had to exist and be
observed to die before the result meant anything — the same vacuous-pass trap
as the microsecond-aligned timestamps in §20.

### What this does *not* give you

A tag is a version pointer, not a DOI. It supplies the half that is genuinely
hard — *an immutable, named, retrievable state of an evolving dataset, at no
storage cost* — and none of the rest: no identifier minting, no DataCite
registration, no landing page, no citation metadata, no author or licence
fields. Iceberg table properties could carry a DOI string, but nothing in
Iceberg validates or resolves it.

It is also worth being clear that **"data organization" is the weakest way to
argue for Iceberg here.** Organizing files is what the directory tree and the
80 KB manifest (§7) already do well. What Iceberg adds is not better
organization but *versioned* organization: the ability to say which exact set
of files was the dataset at a named moment, and to still answer that after
the table has moved on. If nothing is ever republished, corrected or
compacted, a `date=YYYY-MM-DD/` tree plus a frozen manifest copy is a
perfectly good citable artifact.

### Cleanup

Local only — no network, no Pelican, no credentials. Warehouses are
`t.TempDir()` and removed automatically. The build tag keeps this out of the
default `go test`, as with §21.

## 23. The NDP question: what a read-only third party can do

`experiments/icebergo/reader_test.go` (build tag `reader`) and
`experiments/ndp_interop.py`. NDP is an ecosystem and enabler rather than a
data host: it would **read** SAGE data, never write it. Every Iceberg
experiment so far went through the writer's SQLite catalog, so the reader
path had never been tested — and it is the only path NDP would use.

**12 of 12 Go checks pass; 11 of 12 interop checks pass**, the one failure
being specific and important (below).

### A reader needs no catalog at all

`table.NewFromLocation(ctx, ident, metadataURL, fsys, nil)` — the last
argument is the catalog, and `nil` is a supported value (upstream's own
tests pass nil). Given a metadata URL and read access, a consumer with no
database, no credentials beyond read, and no prior knowledge of the layout:

| what the reader gets | from metadata alone |
|---|---|
| full schema with types | `time:timestamptz_ns, name:string, plugin:string, value:double` |
| per-column documentation | 4 of 4 fields carry doc strings |
| dataset metadata (licence, contact) | `CC-BY-4.0`, description, contact |
| row / file / byte counts | 200 rows, 2 files, 4,530 bytes |
| history | 2 snapshots |
| the data itself | 200 rows scanned |
| predicate pushdown | 100 of 200 rows for `plugin = 'plugin-A'` |

None of the first five require reading a single Parquet file. That matters
on Pelican specifically, where a catalogue listing that had to scan data
would cost ~150 ms per request (§17).

**This is the strongest Iceberg result in these experiments for the NDP use
case**, and it is the opposite of what the SQLite dependency suggests: the
database is the *writer's* private component. Handing NDP a table means
handing over a URL, not database access.

### The catch: a URL-pinned reader cannot see later commits

Tested rather than noted. After the writer committed a third batch, the
reader still returned 200 rows, not 300, and `Refresh` fails outright
(it dereferences the nil catalog). **A bare metadata URL is a snapshot
pointer, not a subscription.**

The fix is Iceberg's own, and it needs no database. The **filesystem
catalog** keeps the "which metadata file is current" pointer on storage as
`version-hint.text`. Confirmed against the real `hadoop` catalog rather than
a hand-written file: iceberg-go creates the table, commits, and publishes a
pointer of **1 byte**. A reader then resolves the current table with one
extra GET — read the hint, read the metadata file it names — and scans, with
no catalog anywhere.

So there are two deployment shapes, and the NDP requirement does not force
the heavier one:

| shape | writer needs | reader needs | safe concurrent writers |
|---|---|---|---|
| SQL catalog | a database | nothing (URL) | **yes** (§19) |
| filesystem catalog | nothing | nothing (one extra GET) | no — same CAS gap as §17 |

The filesystem catalog has exactly the weakness §17–19 already measured:
the pointer swap is not atomic on Pelican, so it is single-writer. That is
the same trade-off as the custom design — but it means **"Iceberg requires
running a database" is false as a blanket statement.** It requires one only
if concurrent writers must be safe.

**That last paragraph was measured on local disk, and §24 shows it does
not carry over to Pelican.** The filesystem catalog is not merely
single-writer there; iceberg-go refuses to run it on remote storage at
all without an explicit unsafe opt-in, and the primitive its commit
protocol depends on is absent from this origin. See §24 — the row above
is corrected there rather than here, because the local measurement it
reports is still accurate for local disk.

### Interop: DuckDB cannot read v3 nanoseconds today

`ndp_interop.py` writes two fixtures with the Go client — v3 +
`timestamptz_ns` and v2 + `timestamptz` — so a failure is diagnostic rather
than a bare error, then opens each with the readers an NDP user would
actually reach for.

| reader | v2 + microseconds | v3 + nanoseconds |
|---|---|---|
| stdlib `json` (schema only) | PASS | PASS |
| pyiceberg 0.12.0 `StaticTable` | PASS | **PASS** |
| DuckDB 1.5.6 `iceberg_scan` | PASS | **FAIL** |
| pyarrow direct on the Parquet | PASS | PASS |

```
Error: Unrecognized primitive type: timestamptz_ns
```

The control is what makes this useful: v2 + microseconds reads in every
client, so the blocker is **the nanosecond type, not Iceberg, not v3
metadata generally, and not the Go writer**. It is the same shape as §13/§20
— a client gap, now DuckDB's rather than pyiceberg's. Note that pyiceberg
*reads* v3 nanoseconds fine; it only cannot write them (§20).

DuckDB also refused to guess the current version without a hint:

> *"no version-hint could be found, globbing the filesystem to locate the
> latest version is disabled by default as this is considered unsafe and
> could result in reading uncommitted data"*

Independent confirmation that the pointer file is the mechanism, not a
convenience — a reader that guesses can read an uncommitted table.

**The practical consequence for NDP.** Choosing `timestamptz_ns` today
means NDP users on DuckDB cannot use `iceberg_scan` — the single most
likely access path for a notebook user. The options are: store microseconds
and keep every reader (the archive's remainder data is lost, §13), store
nanoseconds and accept that DuckDB users read the Parquet directly until the
extension catches up, or keep nanoseconds in a side column. This is a
"today" constraint — versions recorded above — and worth re-checking rather
than designing around permanently.

### On the premise: Iceberg does not write "a manifest of the structure"

Worth separating two files that the phrase conflates, because only one is
human-readable:

- **`*.metadata.json`** — schema, types, column docs, partitioning, snapshot
  history, table properties. Plain JSON, and the experiment confirms a
  consumer can read the whole structure with nothing but Python's stdlib
  `json` module.
- **`*.avro` manifests** — binary Avro listing data files with per-column
  bounds. Machine-readable only; a human needs a tool.

So the self-describing claim is true, and it holds up: **the schema,
types, per-column documentation, row counts and licence all travel with the
data and are readable without an Iceberg client.** But it is the metadata
JSON doing that work, not the manifest. And it is not unique to Iceberg —
the custom manifest (§7) could carry the same fields. The difference is that
Iceberg's is a published specification an NDP integration can target once,
versus a bespoke JSON shape that has to be documented, versioned and
explained to every consumer.

### Cleanup

Local only — no network, no Pelican, no credentials. Go warehouses are
`t.TempDir()`; the interop fixtures are written under an explicit
`ICEBERGO_FIXTURES` directory and were removed after the run.

---

## 24. Can Iceberg run on Pelican with no database at all? (exp24)

> **Conclusion upheld, reasoning corrected by §31, scope narrowed by §32.**
> This section tested two primitives and said the origin "honors neither
> guard", which implies it read the conditional headers and ignored them. §31
> swept the remaining seven and found it does not parse them at all: no
> `ETag`, no `Last-Modified`, `DAV: 1`, and a nonsense method (`FLOOBLE`) also
> returns 200. Read the table below as "unparsed", not "evaluated and
> overridden".
>
> **More importantly, this section never built a table.** It constructed the
> hadoop catalog past the `allow-unsafe-commits` guard and then probed raw
> HTTP. §32 did build one, and it **works on Pelican with a single writer**
> (6/6: table created, `version-hint.text` written and advanced, an
> independent catalog loaded it from the URL alone with no database). So the
> answer to this section's literal question — can it be made *safe* against
> concurrent writers — is still no, and §32 demonstrates the lost update
> directly. But "no database-free Iceberg commit can be made safe on this
> storage" should not be read as "a filesystem catalog cannot run on
> Pelican". It can; it just requires exactly one writer.

`experiments/icebergo/fscatalog_test.go`, build tag `pelican`, **8/8 checks**,
against the real NRDStor origin.

§23 found that iceberg-go ships a filesystem ("hadoop") catalog that keeps
the current-version pointer on storage as `version-hint.text` and needs no
database. If that held on Pelican, the last objection to Iceberg in
`plans/4_catalog_recommendation.md` — "its commit requires a stateful
service" — would disappear outright. §23 measured it on local disk, so the
question was still open on the storage that matters.

**It does not hold. The database is not optional on Pelican.**

### What iceberg-go itself says

The guard is in the constructor, before any I/O, so this needs no network:

```
hadoop catalog: when using warehouse scheme "https", `allow-unsafe-commits`
must be set to true; this implementation is not atomic and can lose data
with concurrent commits
```

Upstream refuses a filesystem catalog on remote storage by default, and
names the exact failure mode exp18 measured for the custom catalog. Setting
the flag gets past the guard — it is policy, not a capability limit — so the
real question is whether *this* origin backs the protocol up. NRDStor is
posix-backed and its `MOVE` is atomic (§17), which is more than S3 offers,
so it was genuinely open.

### What the storage does

The commit protocol in `catalog/hadoop/hadoop.go:commitMetadataFile` is two
`RenameNoReplace` calls: temp → claim, then claim → `vN.metadata.json`. It is
safe **only** because a rename onto an existing path fails. Tested against
that exact primitive:

| request | expected for safety | measured |
|---|---|---|
| `MOVE` with `Overwrite: F` onto an existing path | 412, refused | **201, performed** |
| the already-committed file afterwards | unchanged | **destroyed** |
| `PUT` with `If-None-Match: *` onto an existing path | 412, refused | **201, overwritten** |
| control: `MOVE` to a free path | succeeds | succeeds |

```
dst held "COMMITTED v2 metadata", now holds "NEW uncommitted metadata"
```

The control matters for the same reason it did in §22: if `MOVE` simply did
not work, "the destination survived" would have passed for the wrong reason.
The move to a free path succeeds, so the overwrite above is real.

This reproduces §17's `Overwrite: F` and `If-None-Match` results against the
primitive the catalog actually calls. §17 tested the *pointer-swap* use,
where overwriting is the desired behaviour; this is the opposite use, where
overwriting is precisely the failure. Both primitives behave the same way, so
the gap is the origin's rather than one header's: **no database-free Iceberg
commit can be made safe on this storage** — not the stock filesystem catalog,
and not a hand-rolled replacement.

### What this corrects

§23's "the database is required only if concurrent writers must be safe" is
**true on local disk and false on Pelican**. On Pelican the filesystem
catalog does not degrade to single-writer-but-correct; it publishes over a
committed metadata version with a success status. Against a single
well-behaved writer it would usually work, because the claim path only
collides under a race — which is the worst shape for a bug, since testing
would not surface it.

So the corrected deployment table for *this* storage:

| shape | writer needs | reader needs | safe on Pelican |
|---|---|---|---|
| SQL catalog | a database | nothing (URL) | **yes** (§19, §21) |
| filesystem catalog | nothing | nothing (one extra GET) | **no — upstream refuses it, and the origin cannot back it** |

The reader half of §23 is untouched: readers still need no catalog of any
kind, and that is where the NDP argument lives. What falls is only the
suggestion that the *writer* could skip the database too.

### Why the result still favours Iceberg on net

The honest reading is that this removes a bonus, not that it restores an
objection. Iceberg's database buys exactly what exp18 showed the custom
catalog lacking (§18: two writers, 3/3 silent lost updates) — and the custom
design has no database-free safe mode either, for the same reason. Both
designs need a single enforced writer or an external arbiter on Pelican. The
difference is that Iceberg's arbiter is already written.

### Cleanup

Everything was written under `/sage/cold-storage-test-fscat`, a prefix used
by this test alone and distinct from exp21's. Removed recursively and
verified: 3 objects deleted, `PROPFIND` → 404. `/sage/node-data` and the
`sage-storage-loader` paths were never touched. 15 requests total.

---

## 25. What does storing microseconds actually cost? (exp25)

`experiments/probe_us_collision.py` (pyarrow) and
`experiments/probe_us_collision_sql.py` (DuckDB, with pyarrow doing the
read). Backs the 2026-10-14 decision to store `timestamptz` at microsecond
precision so DuckDB can read the table (§23).

§13 measured that **97.60% of rows carry a non-zero nanosecond remainder**,
which sounds like a large loss. That number answers the wrong question for
this decision. Dropping sub-microsecond precision on a sensor reading is a
scientific judgement. What would be a *correctness* problem is two distinct
readings becoming the same `(series, timestamp)` pair, because that breaks
de-duplication, "last value wins" correction semantics, and any downstream
join keyed on the timestamp. So the question measured here is collisions,
not remainders.

### The trap, hit again, and caught by a guard this time

The first version of the SQL probe pointed DuckDB straight at the Parquet.
It aborted:

```
ABORT: no non-zero nanosecond remainders visible in .../data_0.parquet
```

This is §13's trap exactly. DuckDB 1.5.6's `TIMESTAMP` is microsecond
precision, and the truncation happens **at read time and cannot be undone in
SQL**:

| expression | result |
|---|---|
| `epoch_ns(time)` | `1735689606915629000` — ns-scaled, last 3 digits already 0 |
| `CAST(time AS BIGINT)` | `ConversionException` |
| `epoch_ns(time::TIMESTAMP_NS)` | still 0 non-zero remainders |

Same file, same row, read two ways:

```
pyarrow: 1735689606915629791
duckdb:  1735689606915629000
```

The probe therefore has pyarrow read `time` as int64 and hands DuckDB the
Arrow table for grouping — precision from one, speed from the other — and
asserts non-zero remainders are visible before counting anything. Without
that guard it would have reported a confident, clean zero for the wrong
reason, which is how §13 went wrong the first time.

**This is a finding in its own right, and it is what makes the decision
cheap: DuckDB already truncates these timestamps today, on plain Parquet,
with no Iceberg anywhere.** Anyone querying the archive through DuckDB has
never seen the nanosecond digits. Storing microseconds does not take
precision away from that reader; it makes the stored form match what the
reader always saw.

### Collisions

Series identity is the measurement name plus the flat label columns
(`name, plugin, vsn, node, host, task, job, zone`). `meta` is deliberately
excluded: it is a map and would have to be stringified over tens of millions
of rows, and leaving it out is the **safe** direction — fewer key columns
means more rows share a key, so every count below is an upper bound.

Eight days sampled evenly across the whole 608-day span
(`date=2025-01-01` .. `date=2026-08-31`), every file in each day:

| day | rows | keys before | keys after | lost |
|---|---:|---:|---:|---:|
| date=2025-01-01 | 59,496,597 | 58,687,252 | 58,687,252 | 0 |
| date=2025-03-18 | 142,378,324 | 141,657,374 | 141,657,374 | 0 |
| date=2025-06-02 | 140,434,356 | 139,698,155 | 139,698,155 | 0 |
| date=2025-08-17 | 129,051,481 | 128,746,249 | 128,746,249 | 0 |
| date=2025-11-01 | 105,705,538 | 105,622,792 | 105,622,792 | 0 |
| date=2026-01-16 | 5,594,979 | 5,526,742 | 5,526,742 | 0 |
| date=2026-04-02 | 5,287,345 | 5,239,176 | 5,239,176 | 0 |
| date=2026-06-17 | 5,439,694 | 5,386,674 | 5,386,674 | 0 |
| **TOTAL** | **593,388,314** | **590,564,414** | **590,564,414** | **0** |

Zero collisions in 593 million rows, so no value conflicts are possible. The
guard confirmed 57,191,263 non-zero nanosecond remainders were visible in the
first day, so the zero is a measurement and not a silent downcast. The 791 ns
remainder is real precision, but it is not *distinguishing* precision — within
one series, readings are far further apart than a microsecond.

Note the sample spans both density regimes: the 2025 days carry 100–140M rows
each, the 2026 days ~5M. Collisions would be likeliest in the dense days, and
those are the ones measured at zero.

### Conclusion

The decision costs sub-microsecond precision and nothing else measurable: no
identity collisions, no merged readings, no broken de-duplication key. Two
things to carry forward:

1. **Keep the exporter writing `timestamp(nanosecond)` Parquet.** The
   truncation belongs at the Iceberg write boundary only, so full precision
   survives at the source and the decision stays reversible if DuckDB gains
   `timestamptz_ns` support later. Truncating at the exporter would destroy
   the data irreversibly for no gain.
2. **The loss at the Iceberg boundary is permanent**, and it is the one
   class of schema mistake that evolution cannot repair (§22: widening is
   allowed, narrowing is refused, and the discarded digits are gone either
   way). Worth one explicit confirmation from whoever owns the science that
   no SAGE measurement needs sub-microsecond resolution — rather than
   inferring it from the fact that DuckDB cannot display it.

---

## 26. Is the archive already indexing `/node-data`? (observation, not an experiment)

Prompted by a question on 2026-10-14: can Iceberg also structure the object
storage under `/sage/node-data`?

**Recorded as an observation, with its scope stated up front.** This is one
query against one day, run while answering the question. It is not an
experiment: there is no control, no cross-day sampling, and nothing was
written. It is here because the measured part is load-bearing for a design
claim, and because it names the thing that would have to be tested.

### What was measured

`date=2025-01-01/data_0.parquet`, via DuckDB:

| | |
|---|---|
| rows with `name = 'upload'` | 43,353 |
| distinct `value_str` | 43,353 — exactly 1:1, no duplicate URLs |
| rows with `value_str LIKE 'https%'` | 43,353 (the same rows) |

A representative row:

```
value_str = https://storage.sagecontinuum.org/api/v1/data/
            cloud-motion-v1-368/sage-cloud-motion-v1-1.24.11/
            000048b02d15bc6d/1735709333134117735-img2_...jpg
meta      = {filename: img2_1735709333134117735.jpg, thres_otsu: 26.0}
```

So a blob upload is **already a row in the time-series table**, carrying the
same `(name, plugin, vsn, node, host, task, job, zone)` identity as every
sensor reading, with the object URL as its string value and app-specific
keys in `meta`. The URL path is itself structured:
`app/version/node-id/timestamp-filename`.

### What it implies, and what it does not

Iceberg is a table format over Parquet rows. It has no concept of a blob and
**will not reorganise object storage**; storing image bytes in the table
would bloat the files that columnar pruning depends on and defeat the
range-read behaviour measured in §5 and §17. The opportunity is not storing
blobs in Iceberg but **declaring the index that already exists** — making
blob discovery a pruned predicate rather than a WebDAV listing, which
matters here because this origin's listing path is weak (DELETE will not
even recurse, §21).

### The assumption that has not been tested

**Coverage.** Every number above describes rows that exist in the table. It
says nothing about objects in `/node-data` that have *no* corresponding row.
If plugins publish an `upload` row only when they choose to, the table is an
incomplete index — fine for discovery, but it could not drive retention,
orphan detection or integrity checking, which are most of the value.

Testing it requires listing `/node-data` and anti-joining against the table.
That was attempted while answering and **blocked by a PII classifier** on the
PROPFIND, so no listing was taken and the question stands open. Noted rather
than worked around: the directory holds real node data including imagery, and
a tooling refusal to enumerate it is not something to route around silently.

Two further untested items, both independent of the catalog choice:

- whether `design_ideas.md`'s Hive layout
  (`/run-id=/app-id=/data-schema-id=/node-id=/date=`) should also govern blob
  paths — a naming decision, not an Iceberg one;
- consistency when the upload succeeds and the table commit fails, or vice
  versa: two systems, no transaction across them.

---

## 27. Can the archive be handed to storage SAGE does not run? (exp27)

`experiments/icebergo/portable_test.go`, build tag `portable`, **15/15 PASS**,
local disk only.

Prompted by the bullet in `plans/Data Pipeline Considerations.md`:

> *"it would be helpful if the new data path could push completed, finalized
> Parquet files in the Long Term path to operational resources not managed or
> governed by Sage. After the successful push … operational responsibility,
> including search and redaction would end. Use Case: Suppose the nwf.org
> wants to purchase a node and save the accumulated data for their non-profit.
> Could the Long Term data be routed to AWS, or other storage platforms?"*

Two questions hide in that, and conflating them is the easy mistake.
"Anything can be built" is true of both designs, so the only interesting
question is what **already exists** — and separately, whether a table can be
**moved** once written.

### Q1: addressing other storage — already done

`iceberg-go` resolves storage through a scheme registry
(`io/registry.go`). Importing a backend package runs its `init()`, which
registers its schemes. Measured by listing the registry at runtime:

| backend | schemes | integration step |
|---|---|---|
| local filesystem | `file`, `""` | none — registered by the library |
| S3 **and S3-compatible** | `s3`, `s3a`, `s3n`, `oss` | one import line |
| Google Cloud Storage | `gs` | one import line |
| Azure Blob | `abfs`, `abfss`, `wasb`, `wasbs` | one import line |
| in-memory | `mem` | none |

Full registry after importing all three: `abfs abfss file gs mem oss s3 s3a
s3n wasb wasbs`. The AWS case answers the bullet's literal question, and
because the S3 backend covers S3-*compatible* endpoints, it also reaches
MinIO, Ceph/RADOS and Alibaba OSS — which is most of what a partner
institution is likely to run.

For anything not on that list, `io.Register(scheme, factory)` is **exported
public API** — verified by registering a new scheme from the test. That is
exactly how `pelican_io.go` attaches (§21, ~190 lines, no fork). A control
confirms the registry is real: an unregistered scheme is refused with
`io scheme not registered for path … (scheme: nosuchscheme)`.

**Cost, so this is not reported as free.** Importing all three backends took
this module from **78 to 133 dependencies (+55)** — AWS SDK v2,
`cloud.google.com/go/storage`, the Azure SDK and their transitive trees.
Import only the backend actually needed. Pelican's own IO adds none of them.

### Q2: moving a table that already exists — **does not work by copying**

This is the half the use case actually needs, and it fails. Iceberg metadata
records **absolute** locations, so copying the bytes elsewhere produces a
table that still points at the storage it came from.

Method, with the trap closed: the source directory is **deleted** before the
copy is read. Without that, the reader opens the original through the
absolute paths and the test passes vacuously.

| check | result |
|---|---|
| metadata records an absolute `location` | yes — `file:///…/001/sage.db/obs` |
| occurrences of the source root in one metadata file | **5** |
| source destroyed before any read | confirmed |
| moved table's **metadata** opens (schema, docs, properties) | **yes** |
| moved table's **data** reads | **no** — `no such file or directory` |
| the snapshot's manifest-list pointer | absolute into the dead source |

So a naive handoff yields something worse than a clean failure: the table
*opens*, reports its schema and documentation correctly, and only fails when
someone tries to read rows.

And the obvious repair is insufficient. Rewriting every `*.json` under the
copied tree leaves the data-file paths stale, because **manifests are Avro,
not JSON** — after a full JSON rewrite the first data file still read
`file:///…/TestPortableHandoffDone…/data/00000-0-….parquet`, pointing at the
source. `iceberg-go` v0.7.0 ships **no location-rewrite helper** (no
`RewriteLocation` / `rewrite_location` anywhere in the module). A control
confirms the table reads correctly at its original location (200/200 rows),
so the move is the problem, not the table.

### What this means for the bullet

- **Writing the long-term path to AWS/GCS/Azure from the start: supported
  today, one import line.** This is a real, already-implemented answer.
- **Handing over a table written on Pelican: a migration, not a copy.** It
  needs a deliberate rewrite step that iceberg-go does not provide.
  ~~The Iceberg Spark procedures are the likely place such a helper exists
  (`rewrite_table_path` is the name I recall) — unverified.~~
  **Resolved in §28: `rewrite_table_path` does exist as a Spark
  procedure**, with `source_prefix`/`target_prefix` and an optional version
  range, emitting a CSV copy-plan. It is confirmed **absent from iceberg-go
  v0.7.0**, so using it means standing up Spark for the migration —
  reasonable for a one-off handoff, not something the Go exporter can do.
  Still not run against Spark here.
- A third option avoids the problem: **re-export to the destination**
  rather than move. "Operational responsibility ends" is satisfied either
  way, and re-exporting is simpler than path surgery.

So the bullet's verb should change: **route at write time, do not relocate
after.** The long-term path is a batch rewrite in any case, so writing it to
a second destination costs far less than migrating it later.

### The handover itself: what the recipient receives (§23 + §28)

Q1 and Q2 are about *bytes*. The bullet's other half — *"operational
responsibility, including search and redaction, would end"* — is about
whether the recipient can take over without SAGE staying in the loop. Two
results already measured elsewhere answer it, and they are recorded here
because this is where someone reading about partner handoff will look.

| what the partner needs | answer | measured |
|---|---|---|
| read the data with no SAGE database | yes — a URL is enough | §23 |
| get schema, column docs, licence, contact, row counts | yes, from metadata JSON alone | §23 |
| see data SAGE commits *later* | no — a bare URL is a snapshot pointer, not a subscription | §23 |
| **take over writing** to the table | yes — one catalog row | §28 |

The last row is the one that makes the severance clean. §28 deleted the
catalog and rebuilt it with a single `INSERT` of `(catalog_name,
table_namespace, table_name, metadata_location, iceberg_type)`, after which
**the table accepted new commits** (200 → 300 rows). Adopting a handed-over
table is the same operation: the recipient points their own catalog at the
`metadata.json` and owns it from there. SAGE hands over storage access and
a URL, nothing else — no database export, no bespoke import tooling.

**Caveat, and it is the one to check before promising this.** §28 recovered
a table *at the path its own metadata names*; it deleted the catalog, not
the data. Combining adoption with a **relocated** table runs straight into
Q2 — the inserted row would point at metadata whose data paths are dead.
Copy-then-adopt is therefore expected to fail, but that specific
combination was **not tested**; it is an inference from two separate
results. Re-export (option 3 above) avoids the question entirely, which is
another reason to prefer it.

### Scope, stated

Local disk, deliberately: whether absolute paths survive a move is decided by
the bytes, not the origin, and the source has to be destroyed for the test to
mean anything — not something to do to a real origin. §21 already showed this
client's writes reach Pelican intact. **Not tested:** any real cloud endpoint
(no credentials for one here), so Q1 shows what is compiled in and reachable,
not that a given bucket answers today.

### The comparison that matters

This is not a point in Iceberg's favour over the custom design — the custom
manifest could store relative paths and be *more* portable. It is an honest
split: Iceberg wins Q1 outright (the backends exist and are maintained),
and Q2 is a genuine weakness that must be planned around rather than
assumed away.

---

## 28. Can the archive be recovered if the catalog is lost? (exp28)

`experiments/icebergo/recover_test.go`, build tag `recover`, **15/15 PASS**,
local disk.

Prompted by a disaster-recovery article describing two Iceberg recovery
scenarios and the Spark procedures for each: `register_table` when the
catalog is missing or points at the wrong `metadata.json`, and
`rewrite_table_path` when file paths changed during the restore.

The article is about **Spark**. This repo's writer is Go, so the claims were
not taken on faith — the question tested here is *what is actually available
to us, with a Go writer and a SQL catalog, when the catalog is gone.*

This matters more here than in a typical deployment: §24 established that
Pelican cannot back a database-free catalog, so the catalog database is
**mandatory and is a single point of failure** that storage cannot
reconstruct on its own. Whether it is recoverable is an operational
question, not a formality.

### First: it confirms §27's finding from an independent source

The article states that Iceberg "uses absolute file paths for all its
references — data files, delete files, manifests, even snapshot lineage,"
so a restore to a different location breaks everything. That is exactly what
§27 measured independently (5 occurrences of the source root in one metadata
file; the moved copy opens its schema and fails to read data). Two
independent confirmations of the same property, one measured here and one
from the field.

### `rewrite_table_path` exists — and resolves §27's unverified lead

§27 recorded "I believe the Spark procedures include something like
`rewrite_table_path`" as explicitly unverified, because web search was
unavailable. The article documents it with full arguments
(`source_prefix`, `target_prefix`, optional `start_version`/`end_version`,
`staging_location`), and notes it emits a CSV copy-plan while leaving the
physical file movement to the operator.

**Confirmed absent from iceberg-go v0.7.0** (grepped: no
`rewrite_table_path` / `RewriteTablePath` anywhere in the module). So §27's
conclusion stands and sharpens: the capability exists in the Iceberg
*project*, via Spark, not in our client. Using it means standing up Spark
for the migration, which is a real option for a one-off handoff but not
something the Go exporter can do.

### `register_table`: **not available on the catalog we would use**

| catalog | `RegisterTable` in iceberg-go v0.7.0 |
|---|---|
| `rest` | yes |
| `glue` | yes |
| `hive` | yes |
| **`sql`** (ours) | **no** |
| `hadoop` | no |

It is also **not on the `Catalog` interface** — it is a method some
implementations happen to provide. Verified in the test by an interface
assertion against the live SQL catalog, which fails.

### What recovery actually looks like for us

The catalog database was **deleted**, not corrupted, with data and metadata
left untouched. Two paths work:

**A. Reading needs no recovery at all.** The table opens from its metadata
path with `nil` where the catalog goes (§23), and all 200 rows read back.
**The data is never unreachable, so a lost catalog is not a data-loss
event** — it is a write-availability event. In an outage this is the first
thing to confirm, and it is reassuring.

**B. Rebuilding the catalog is one SQL row.** Because the SQL catalog's
entire per-table state is a row in `iceberg_tables`
(`catalog_name, table_namespace, table_name, metadata_location,
iceberg_type`), recovery is an INSERT pointing at the newest
`metadata.json`:

| check | result |
|---|---|
| catalog rebuilt from one INSERT | table loads |
| all rows readable through the recovered catalog | 200/200 |
| snapshot history intact | 2 snapshots |
| **recovered table accepts new commits** | 300 rows after append |

The last row matters: this is a full recovery, not a read-only rescue.

So the missing `register_table` is **not a blocker** — it is a convenience we
do not get. The recovery step is ours to write and test, and it is small
enough to be auditable.

**B has a second use that is not disaster recovery.** Adopting a table into
a catalog that never held it is the same INSERT, so it is also how a
*partner* takes ownership of a handed-over table — the `Data Pipeline
Considerations.md` "operational responsibility would end" case. Recorded
under §27, where someone reading about handoff will look. The caveat is
there too: this was measured on a table at the path its own metadata names,
so combining adoption with relocation should be assumed to hit §27's Q2
failure until tested.

### The sharp edge: a stale pointer fails silently

The article warns the catalog may point at the wrong `metadata.json`.
Measured, this is worse than an error:

```
a stale metadata pointer opens WITHOUT error, serving older data
  200 rows from the stale pointer (newest has 300)
```

Restore a catalog backup one commit behind the storage backup and Iceberg
serves the older snapshot **silently and successfully**. No warning, no
mismatch detected. This is the same failure shape as §27's moved table and
as §18's lost update: a success status over a wrong result.

Mitigation is cheap, and it does not require trusting the catalog backup —
the newest metadata file can be identified **from storage alone**, two ways
that were checked against each other:

| method | result |
|---|---|
| highest `last-updated-ms` inside each metadata file | correct |
| lexical sort of filenames | correct — iceberg-go zero-pads (`00000-`, `00001-`, …) |

Both agree. The content-based method is the one to rely on, since the
padding is this writer's naming convention rather than a spec guarantee.

### What to carry into operations

1. **Recovery runbook, with a dry run.** Both the article and this test
   point the same way: the recovery step for our stack is an INSERT, and it
   should be scripted and rehearsed before it is needed, not written during
   an outage.
2. **Back up the catalog and the storage, and record the metadata pointer.**
   A catalog backup that lags storage silently serves stale data.
3. **Validate after restore** by comparing the catalog's
   `metadata_location` against the newest file on storage — a check that
   catches exactly the silent-stale case.
4. **Snapshot expiry interacts with backups.** Not tested here: expiring
   snapshots deletes metadata a stale catalog might reference, which would
   turn silent-stale into hard-fail. §22 covers expiry, but not this
   interaction.

### Scope

Local disk, SQLite. The recovery mechanism under test is the catalog row and
the metadata pointer, neither of which is storage-specific; §21 already
showed this client's bytes reach Pelican intact. **Not tested:** recovery
against Postgres (the likely production catalog, where the INSERT is the
same but the operational details differ), and the expiry interaction above.

---

## 29. DuckLake instead of Iceberg? (exp29)

> **Decision recorded 2026-10-14: DuckLake is eliminated.** Not because of
> anything measured in this section — the deciding argument is reader
> independence. DuckLake needs a live, SAGE-hosted `meta.db`; §32 showed
> Iceberg can be read from a warehouse URL with no database at all. The
> measurements below stand, including where DuckLake wins, and are kept so
> the reasoning stays auditable. See "Closed: DuckLake is out" in
> `plans/4_catalog_recommendation.md`.

`experiments/probe_ducklake.py` (**11/11**) and
`experiments/probe_ducklake_http.py` (**4/4**), DuckDB 1.5.6, local disk.

> **Verdict superseded by §30 (2026-10-14, same day).** The measurements
> below stand. The *decision* does not: it rested entirely on "no Go
> client", and the requirement that made that decisive — a Go writer — was
> withdrawn ("we are not tied to Go, we can also use Python"). §30 re-runs
> the comparison on Pelican with Python allowed, and two items below invert
> once it is: objection 2 (`timestamptz_ns`) and the "not tested" list.
> Read this section for what was measured, §30 for what follows from it.

Asked 2026-10-14, after the §27/§28 decision had already landed on Iceberg.
DuckLake is a 2025 table format from the DuckDB team whose premise is that
the Iceberg/Delta design has an avoidable flaw: those formats put metadata in
files on object storage *and still need a catalog database* for the final
pointer, so you run a database either way but get none of its transactional
power. DuckLake's answer is to put **all** metadata in the database and keep
only Parquet on storage.

Not scored on features. It is scored against the four measurements that
already decided things here, so the comparison uses the same evidence.

### Where DuckLake is genuinely better

**1. Relocation — the §27 Q2 failure simply does not occur.**
Its catalog has a `path_is_relative` column, and data-file paths are stored
relative (`ducklake-01a13b85-….parquet`, `relative=True`). Running the same
destroy-the-source move test that breaks Iceberg:

| check | Iceberg (§27) | DuckLake (§29) |
|---|---|---|
| source destroyed before read | yes | yes |
| moved table's metadata opens | yes | yes |
| **moved table's data reads** | **no** | **yes — 200/200 rows** |
| moved table accepts new writes | n/a | yes — 201 rows |

And the error when the data path is *not* overridden is the right one:

> *"DATA_PATH parameter … does not match existing data path in the catalog …"*

It refuses rather than guessing. Set against §27 (moved table opens, then
fails on read) and §28 (stale pointer serves old data silently), this is the
first thing in these experiments that **fails loudly instead of succeeding
wrongly** — the recurring failure shape of §18/§24/§27/§28.

**2. Nanoseconds, with no format-version gymnastics.** `TIMESTAMP_NS`
round-trips with its remainder intact (`epoch_ns=1735689600123456789`,
remainder 789). No v3 opt-in, no client split (§13/§20).

**3. Publishing works without a live database.** The §23 property —
hand a consumer a URL, nothing else — has a DuckLake analogue. A "frozen"
lake, catalog file and all, served over plain HTTP attaches and reads from
a client holding no local copy of either: 200/200 rows, nanoseconds intact,
and **read-only**, which is correct for a published archive. 12 requests,
8 of them `Range` — it does partial reads of the catalog file rather than
downloading it whole, which matters at ~150 ms/request (§17).

### Where it is worse, and the reasons are not close

**1. No Go client. This is the blocker.** Our exporter is Go (§20/§21: the
nanosecond path exists *because* we moved to iceberg-go). DuckLake has one
implementation, the DuckDB extension; the only non-DuckDB engine is a Rust
DataFusion port. The Go projects that exist (`gigapi`, `ducklakeexporter`
and six others) all drive DuckDB through cgo. So adopting DuckLake means
either embedding DuckDB in the exporter — cgo, a C++ dependency, and the
loss of a pure-Go build — or writing a Go writer against the spec, which is
the "anything can be built" answer the §27 framing already rejected as
uninteresting. Iceberg's Go client is first-party and already in use here.

**2. No `timestamptz_ns`.** The spec has `timestamp_ns` and `timestamptz`
(microsecond), and no type that is both. Confirmed in DuckDB core:
`TIMESTAMPTZ_NS` does not exist. The archive's column is `timestamptz_ns`
(§20/§23), so DuckLake forces a choice between the time zone and the
nanoseconds — exactly the trade-off §25 settled on other grounds. Iceberg v3
has the combined type and iceberg-go writes it.

> **This comparison is only valid for a Go writer (§30).** Stated against
> iceberg-go it is correct. Stated against *Python*, it reverses: pyiceberg
> 0.12.0 cannot write v3 at all and v2 rejects `timestamp[ns]` (§13/§20),
> so a Python Iceberg writer has no nanoseconds to lose the timezone from.
> The real choice is then DuckLake losing the **zone** against Iceberg
> losing the **precision** on 97.60% of rows.

**3. The catalog is bigger, so §28's recovery is harder.** Iceberg's
per-table catalog state is one row, which is why recovery is one INSERT and
why reads need no catalog at all (§23/§28). DuckLake's catalog is 29 tables
with a row per data file; storage alone does not describe the table. A lost
DuckLake catalog is therefore much closer to real data loss — the Parquet
survives, but the table does not. **Not measured here** — no DuckLake
catalog-loss test was run — so this is a structural inference from the
schema, flagged rather than asserted.

**4. Maturity and ecosystem.** DuckLake is ~1 year old with essentially one
implementation. §23's interop table already shows our constraint is reader
reach; Iceberg is what NDP, Spark, Trino and the cloud warehouses read.

### Verdict

**Stay with Iceberg.** Not because DuckLake is the weaker design — on
relocation and on failing loudly it is clearly better, and its critique of
the two-places-for-metadata split is fair. The decision is forced by the
Go client: it is the one gap that cannot be closed with configuration, and
it would undo §20/§21, the results that unblocked nanoseconds in the first
place.

Worth re-checking if a native Go writer appears, or if the exporter ends up
embedding DuckDB for other reasons. The §27 Q2 weakness is real and DuckLake
shows it is not inherent to the lakehouse model — so if partner handoff ever
becomes a primary requirement rather than a bullet, this is the alternative
to re-open.

### Scope, stated

Local disk and localhost HTTP, DuckDB 1.5.6, a 200-row table.
**Not tested:** DuckLake on Pelican (its catalog needs a writable database,
and §24 showed what this origin does to a filesystem catalog — but a
DuckLake catalog is a *database file*, not a CAS pointer swap, so §24 does
not transfer directly and the question is open); concurrent writers;
catalog loss; Postgres as the catalog; any volume resembling the real
archive. These were not run because the Go-client finding decides the
question before they matter.

> **§30 ran the first two.** DuckLake does run on Pelican, and concurrent
> writers do not lose acknowledged rows on a SQLite catalog. Catalog loss,
> Postgres and realistic volume remain untested.

## 30. DuckLake on Pelican, with Python allowed (exp30)

> **Decision recorded 2026-10-14: DuckLake is eliminated.** Not because of
> anything measured in this section — the deciding argument is reader
> independence. DuckLake needs a live, SAGE-hosted `meta.db`; §32 showed
> Iceberg can be read from a warehouse URL with no database at all. The
> measurements below stand, including where DuckLake wins, and are kept so
> the reasoning stays auditable. See "Closed: DuckLake is out" in
> `plans/4_catalog_recommendation.md`.

`experiments/probe_ducklake_pelican.py` (**14/16**) against the real NRDStor
origin, and `experiments/probe_ducklake_concurrency.py` (**5/5**) on local
disk. DuckDB 1.5.6. The two failures are Q1 and Q3, both explained below
and neither a DuckLake limitation.

Asked 2026-10-14, immediately after §29, because §29's verdict rested on a
single requirement that was then withdrawn: *"it is fine that there is no Go
client, we are not tied to Go, we can also use Python — let's try it on
Pelican to see if it works better with Pelican compared with Iceberg."*

That changes the question in two ways. The Go-client objection disappears,
and — less obviously — **the Python option is not neutral between the two
formats.** §30 measures both halves.

### Does DuckLake run on this storage? Yes, with the same glue Iceberg needed

| capability | DuckLake (§30) | Iceberg (§19/§21/§23/§24) |
|---|---|---|
| builtin client writes `https://` | **no** — "Writing to HTTP files not implemented" | **no** — pyiceberg ships no `https` FileIO |
| custom filesystem fixes it | **yes** — fsspec + `register_filesystem()` | yes — `pelican_fileio.py` (~230 lines), `pelican_io.go` |
| full write path to Pelican | **yes** — commits, Parquet lands (PROPFIND 207), ns intact | yes (§21) |
| publish read-only, no local state | **yes** — 200 rows, ns intact | yes (§23) |
| catalog *hosted on* Pelican, writable | **no** — attaches read-only | n/a — §24: impossible for Iceberg too |

The first row is the one worth not over-reading. DuckDB cannot write to
`https://` out of the box, and an earlier draft of this experiment stopped
there and called it a wall. That would have been unfair: **neither format
reaches this origin unaided.** §19 needed a FileIO pyiceberg does not ship
and §21 needed `pelican_io.go`. Giving DuckLake the same treatment — an
fsspec filesystem, the direct analogue — makes the write path work
end-to-end, with nanoseconds surviving the round trip
(`epoch_ns=1735689600123456789`).

**The read-only catalog (Q3) is a real limit but not a differentiator.** It
follows from the httpfs write gap, not from DuckLake refusing: the token
carried write scope and the ATTACH requested no read-only mode. And it does
not bite in the deployment shape that matters, which is catalog in a
database, data on Pelican — exactly what §24 already forced on Iceberg.
A Pelican-*hosted* DuckLake is publish-only; a Pelican-*backed* one is not.

### Concurrency: DuckLake does not lose acknowledged rows

This is the §18/§19 test — the measurement that moved the recommendation —
so DuckLake had to face it. `probe_ducklake_concurrency.py` isolates it on
local disk, across catalog backends, because the contended resource is the
catalog database and Pelican never participates in a DuckLake commit.

| catalog backend | both writers attach | loser's fate | acked vs present |
|---|---|---|---|
| DuckDB file | **no** (1/2) | cannot `ATTACH` | 3 = 3 |
| SQLite | yes | refused loudly (`TransactionException`) | **3 = 3** |
| SQLite + ~150 ms storage | yes | refused loudly | **3 = 3** |
| Postgres | **not run** | — | — |

Stable over repeated runs, and the winner alternates between w1 and w2 — so
this is a real race, not a fixed ordering. The scoreboard on the decisive
test, all three on a SQLite-class catalog:

- custom catalog (§18): **lost 3/3, silently, reporting success to both**
- Iceberg (§19): lost nothing — CAS in `UPDATE iceberg_tables ... WHERE`
- DuckLake (§30): lost nothing — losing commit refused with an error

DuckLake matches Iceberg here. Both are safe for the same underlying
reason, and it is worth stating plainly because §19 already made the point
about Iceberg: **the safety comes from the catalog database, not from the
object store.** Pelican holds immutable files in both designs.

The third row exists because the Pelican run behaved differently and
latency was the obvious suspect (§17: ~150 ms/request). Injecting that
delay locally does **not** reproduce it, so the explanation is something
else and is recorded as unexplained rather than guessed at.

### Why Q7 on Pelican is not the number to cite

Q7 — the same concurrency test with data on NRDStor — never produced a
*decisive* result. It now reports PASS, but only in the weak sense that no
acknowledged row went missing because **no row was ever acknowledged**:
both writers were refused (`acked w1=0 w2=0`), reproduced identically on
two consecutive runs, so this is a stable outcome rather than a flake. It
does **not** show that a winner commits under contention on this storage,
and the check says so in its own output rather than banking a clean pass.

Every earlier failure traced to the probe, not to DuckLake:

1. `sqlite_busy_timeout` — a setting DuckDB's sqlite extension does not have.
2. The retry helper wrapped the INSERTs, so a **correct** refusal was retried
   25 times and then reported as failure.
3. A writer that errored skipped `w.close()`, leaking the SQLite lock to the
   verify connection. (Fixed — and the same latent bug was in the local
   probe, which had been passing by timing luck.)
4. With all of that fixed: both writers refused, `acked w1=0 w2=0` —
   the standing result above.

Outcome 4 is not a lost update, but neither is it the measurement Q7 was
built to take. The check now distinguishes those cases instead of printing
FAIL for all of them. **Why both writers are refused on Pelican when one
wins cleanly on local disk is unexplained.** Latency was the obvious
candidate and was tested directly — a filesystem sleeping 150 ms per
operation (§17's measured cost) does **not** reproduce it; the slow case
passes like the fast one. So the cause is something else about this path,
and it is left open rather than guessed at.

**The methodological point is the durable one.** Four times this check was
positioned to record a DuckLake limitation that did not exist, in the same
shape as §19's near-miss (where asserting "at most one commit succeeds"
turned Iceberg's correct behaviour into 3/3 FAIL). `database is locked` is
not only a transport error here: it is also how DuckLake *correctly*
rejects a losing commit. A probe that cannot tell a refusal from a loss
will manufacture findings.

### The reversal: "we can also use Python" weakens Iceberg, not DuckLake

§29 decided on the Go client. With Python permitted, the relevant
measurement is §13 + §20, and it points the other way:

| writer | nanoseconds | time zone |
|---|---|---|
| iceberg-go (v3) | **yes** — bit-exact | yes (`timestamptz_ns`) |
| **pyiceberg 0.12.0** | **no — cannot write v3 at all** (`NotImplementedError`, `metadata.py:588`); v2 rejects `timestamp[ns]` | yes, at µs |
| **DuckLake (Python)** | **yes** — `timestamp_ns`, confirmed through Pelican | **no** — no `timestamptz_ns` |

§13 already states it: *"Truncation is only forced on a Python writer."*
So §29's objection 2 inverts. Against iceberg-go, DuckLake loses a type
Iceberg has. Against **pyiceberg**, DuckLake is the one that keeps
nanoseconds and Iceberg is the one that cannot store them at all — on
97.60% of archive rows (§13). The honest framing is **DuckLake loses the
zone; Python-Iceberg loses the precision.**

**But this argument is almost entirely moot, and saying so is the
point.** The 2026-10-14 decision in `plans/4_catalog_recommendation.md`
already chose to **store microseconds**, because supporting DuckDB is a
firm requirement and DuckDB truncates these timestamps at read time
anyway (§25). Under that decision pyiceberg is *not* blocked: it writes v2
microseconds fine — the recommendation says so explicitly ("pyiceberg can
*write* v2, so a Python writer stops being blocked").

So the corrected statement is: the nanosecond argument cannot be cited
for Iceberg in a Python deployment, **and it cannot be cited for DuckLake
either**, because the project has already decided not to keep
nanoseconds. It only becomes live again if that decision is revisited —
which §25 flags as worth confirming with whoever owns the science. Listed
here because an earlier draft of §30 leaned on this reversal as a reason
to prefer DuckLake, and that was wrong.

### What still favours Iceberg, once the Go client is set aside

1. **Reader reach.** §23's interop work showed the binding constraint is who
   can read the archive. NDP, Spark, Trino and the cloud warehouses read
   Iceberg; DuckLake has one implementation plus a Rust port.
2. **Catalog loss.** §28 recovers an Iceberg table with one INSERT because
   its per-table state is one row, and §23 readers need no catalog at all.
   DuckLake's 29-table catalog has no such analogue. **Still inferred from
   the schema, not measured** — and it is now the most load-bearing untested
   claim in the comparison, so it should be measured before it is leaned on.

### What is still not tested

Postgres as the DuckLake catalog — and this is the gap that matters most
for concurrency, since DuckLake's own documentation scopes SQLite to
"multiple local clients" and recommends Postgres for "a multi-user
lakehouse with potentially remote clients". The §19 Iceberg result has the
same caveat: its CAS was also measured on SQLite. Also untested: DuckLake
catalog loss, a contended commit that succeeds on Pelican, and any volume
resembling the real archive (these runs are 200 rows).

Two of those are worth ranking, because the decision rests on them
unequally. **Catalog loss is the one to measure first** — it is cheap,
needs no credentials, and §30 leaves it as the main surviving technical
argument for Iceberg while still only *inferring* it from the schema.
**Postgres is second**, and it would close both the Pelican Q7 gap and the
SQLite caveat that §19 and §30 share.

### Cleanup

All objects were written under `/sage/cold-storage-test-ducklake/`, its own
prefix, and removed recursively (5 objects, PROPFIND → 404 verified).
`/sage/node-data` and the `sage-storage-loader` paths were never touched.

---

## 31. The filesystem catalog, re-examined: why the origin cannot back it (exp31)

> **Scope narrowed by §32.** Everything below is correct about the *primitives*
> — there is no compare-and-swap on this origin and no eighth header to try.
> But §32 then ran the hadoop catalog for real and it **works with a single
> writer**, because the missing CAS only matters when two committers race.
> Read this section as "no safe *concurrent* commit", not "no filesystem
> catalog". One refinement to the detail below: `PROPFIND` *does* return
> `getlastmodified`, so the timestamp exists — it is simply absent from
> `GET`/`HEAD`, where a conditional request would read it.

`experiments/probe_pelican_cas.py` (round 1, 7 primitives) and
`experiments/probe_pelican_cas2.py` (round 2, controls), against the real
NRDStor origin.

§24 concluded "no database-free Iceberg commit can be made safe on this
storage" after testing exactly **two** primitives: `MOVE` with
`Overwrite: F` and `PUT` with `If-None-Match: *`. That conclusion is the
single surviving objection to Iceberg in
`plans/4_catalog_recommendation.md` and the only reason SAGE must host a
catalog database at all, so it was worth more than two probes. This
revisit asked whether *any* conditional operation on this origin gives
compare-and-swap — it does not have to be `MOVE`; anything that can
express "publish X only if the state is still Y" would do.

**Answer: no, and §24 understated the reason.**

### Round 1: every conditional primitive

| primitive | needed for safety | measured |
|---|---|---|
| `PUT` `If-None-Match: *` | 412 | **201, overwritten** |
| `MOVE` `Overwrite: F` | 412 | **201, clobbered** |
| `PUT` `If-Match: <etag>` | 412 on mismatch | **no ETag exists to match on** |
| `COPY` `Overwrite: F` | 412 | 201 (but see round 2) |
| WebDAV `LOCK` | a lock token | 200, **no token** |
| `MKCOL` on an existing collection | 405 | **201 again** |
| `If-Unmodified-Since` | 412 | **no Last-Modified exists** |
| WebDAV `If:` (RFC 4918 §10.4) | 412 | **201, write landed** |

### Round 2: two of those results did not mean what they looked like

`COPY` returning 201 and `LOCK` returning 200 read like "the conditional
was evaluated and ignored". Controls say otherwise:

- **`COPY` to a completely free destination → 201, and the destination
  does not exist afterwards.** `COPY` is a no-op that acks. Round 1's
  `COPY Overwrite: F` line says nothing about conditionals.
- **A second exclusive `LOCK` on an already-locked file → 200.** A lock
  that never refuses is not a lock; the body is the literal string `OK`.
- **A nonsense method, `FLOOBLE`, → 200.** This is the one that matters:
  **a 2xx from this origin carries no information.** It acks methods it
  does not implement, so every "201" above means *unparsed*, not
  *evaluated and allowed*.

That distinction is the correction to §24. §24 wrote that the origin
"honors neither guard", which implies it read the headers and declined to
act on them. It does not read them. There is nothing to fix at the header
level because no conditional request is being parsed at all.

### The actual explanation

```
Server: XrootD/v5.9.7
Allow:  HEAD,GET,PUT,PROPFIND,DELETE,OPTIONS,COPY
DAV:    1
```

This is not a WebDAV server; it is XRootD's HTTP plugin. It declares
**`DAV: 1`**, and `LOCK` is a *class 2* feature — the origin is telling us
up front it has no locking. No response to `HEAD` or `GET` carries an
`ETag` or a `Last-Modified`, so the two standard HTTP validators do not
exist here, and every conditional built on them is inexpressible rather
than ignored.

The `Allow` list is also unreliable in **both** directions, which is worth
recording for anyone who tries to plan against it:

| method | advertised in `Allow` | actually works |
|---|---|---|
| `MOVE` | **no** | **yes** — atomic, confirmed again here and in §17 |
| `COPY` | **yes** | **no** — returns 201, copies nothing |

### What this changes, and what it does not

It does **not** reopen the recommendation. The filesystem catalog is still
unavailable on Pelican, so SAGE still hosts the catalog database. Every
conclusion in §24, §28 and §30 stands.

What changes is the *character* of the obstacle, and it is a sharper
statement than §24 made:

- §24 implied a gap in Pelican's HTTP surface that might be closed by a
  header, a flag, or a newer version. That framing invited exactly the
  "maybe we missed something" retry this section performed.
- The real finding is that **conditional HTTP is not implemented on this
  origin at all** — no validators, `DAV: 1`, unknown methods acked with
  2xx. There is no seventh header to try. The search is complete.

So the requirement is correctly attributed: **the database is NRDStor's
requirement, not Iceberg's.** On local disk (§23) or on S3, Iceberg's
filesystem catalog needs no database. It is this origin that cannot
express compare-and-swap, and the same gap would defeat *any* format's
database-free commit — DuckLake included, which is why §30's comparison is
unaffected.

One consequence is actionable. The capability exists at the POSIX layer
underneath — NRDStor's `MOVE` is atomic (§17, re-confirmed above) — so
this is a missing *HTTP surface*, not a missing filesystem feature. A
`MOVE` that honoured `Overwrite: F`, or any response carrying an `ETag`,
would be enough to retire the catalog database entirely. That is a
concrete, narrow feature request to put to the Pelican/NRDStor operators,
and it is worth filing precisely because the underlying storage can
already do it.

### What is still not tested

Whether a *newer* XRootD or a different Pelican origin behaves
differently. Every result here is specific to `XrootD/v5.9.7` on
`nrdstor.nationalresearchplatform.org:8443`, and the version string is
recorded above so this can be re-run against another endpoint cheaply.

### Cleanup

Written under `/sage/cold-storage-test-cas`, `-cas2`, `-cas3` and `-cas4`
— prefixes used by this test alone, distinct from every other experiment.
All removed recursively and verified `PROPFIND` → 404 (8, 5, and 2+2
objects). `/sage/node-data` and the `sage-storage-loader` paths were never
touched.

---

## 32. The filesystem catalog *does* run on Pelican — with one writer (exp32)

> **Read "one writer" as "one committer" — see §33.** It constrains the
> commit, not the data path: §33 measured 4 producers writing Parquet to
> Pelican concurrently, then one committer adding all 4 files in a single
> commit, 1000/1000 rows intact. Parallel ingestion is unaffected.

`experiments/icebergo/fscatalog_single_test.go` (**6/6**) and
`fscatalog_race_test.go` (lost update demonstrated), against the real
NRDStor origin.

§24 and §31 both answered "can a database-free commit be made **safe**
here" — no, because the origin has no compare-and-swap. Both answered it
by probing raw HTTP primitives. **Neither ever built a table.** §24
constructed the hadoop catalog past its `allow-unsafe-commits` guard and
went straight to curl-level checks, so the question a single-writer
deployment actually cares about was never asked. SAGE has exactly one
writer, and the hadoop catalog's unsafety is specifically a race between
*concurrent* committers.

### It works

Following the project's own standard — §19 wrote ~230 lines of
`pelican_fileio.py` and §21 wrote `pelican_io.go` rather than accept a
FileIO gap as final — this implements the eight interfaces
`hadoop.HadoopCatalogFS` demands (`ListableIO`, `ReadFileIO`,
`WriteFileIO`, `StatIO`, `RenameIO`, `RenameNoReplaceIO`, `RemoveAllIO`,
`MkdirAllIO`) and runs the catalog for real:

| check | result |
|---|---|
| Q0: catalog constructed against a Pelican warehouse | **PASS** |
| Q1: `CreateTable` on Pelican | **PASS** — `v1.metadata.json` written |
| Q2: `version-hint.text` exists on storage | **PASS** — contents `"1"` |
| Q4: a second commit advances the pointer | **PASS** — `"1"` → `"2"` |
| Q5: the pointer names a metadata file that exists | **PASS** — `v2.metadata.json` |
| Q3: an **independent** catalog loads the table from the URL alone | **PASS** — 2 fields |

Stable across repeated runs. 43 requests
(`GET:6 HEAD:7 MKCOL:7 MOVE:6 PROPFIND:13 PUT:4`).

**Q3 is the one that matters for the reader-reach argument.** A second
catalog instance, sharing no state with the writer, loaded the table
knowing only the warehouse URL — **no database anywhere in the picture**,
and no need to be told the metadata path. That is the property §28 noted
the SQL-catalog layout lacks (11 `*.metadata.json` files with nothing on
storage saying which is current). The filesystem catalog restores it.

### Three origin behaviours worth recording

Found while making the adapter work; each cost a debugging cycle, and each
contradicts a reasonable assumption:

1. **A collection answers `HEAD` with 200**, so the status code alone
   cannot distinguish a namespace from a file. The first draft reported
   every namespace as a file and the catalog rejected it with "namespace
   does not exist: sage".
2. **The origin does not slash-terminate the self href in `PROPFIND`.**
   The standard trick of keying on a trailing `/` classifies every
   collection as a file here. It *does* emit both proper discriminators —
   `<D:collection/>` and `<lp1:iscollection>1</lp1:iscollection>` — so the
   fix is to read those.
3. **`MKCOL` works (201) and is required.** The hadoop catalog creates a
   namespace as a real directory and then checks `Stat().IsDir()`, so
   `MkdirAll` cannot be a no-op even though the origin creates parents
   implicitly on `PUT`.

Also a refinement to §31: **`PROPFIND` returns `getlastmodified` even
though no `Last-Modified` header appears on `GET`/`HEAD`.** The timestamp
exists; it is simply not exposed on the methods a conditional request
would use. That slightly narrows §31's "no validators exist" to "no
validators are exposed where HTTP conditionals could read them" — it does
not change the conclusion, because `If-Unmodified-Since` is evaluated
against the response header, not a PROPFIND body.

### The catch, measured rather than asserted

Seven of the eight methods are honest. **`RenameNoReplace` cannot be.** Its
contract is "rename, but fail if the destination exists", and §31
established no way to ask this origin for that. The only available
implementation is check-then-move, which satisfies the *interface* while
silently failing the *guarantee*. The single-writer run reports **4 such
calls**.

Rather than leave that as a caveat attached to a passing test —
the kind of warning that gets skimmed past — `fscatalog_race_test.go`
drives the adapter's own `RenameNoReplace` from four goroutines onto one
destination:

```
4 concurrent RenameNoReplace onto one destination
  reported success: [1 2 0]
  refused with ErrExist: 1
  destination finally holds: "writer-0"
```

**Three writers were told they won. The destination holds one. Two
acknowledged commits silently vanished.** This is the lost update §24
predicted and §18 measured for the custom catalog, now demonstrated on
*this* adapter rather than inherited by argument.

### What this changes

The summary "there is no way to run a filesystem catalog with Pelican" is
**too strong, and §24/§31 should not be read as saying it**. The accurate
statement is narrower and more useful:

> A filesystem catalog runs correctly on Pelican **with exactly one
> writer**. With two, it loses commits silently, and no header on this
> origin can prevent that.

So the real question is no longer "is it possible" but "is single-writer
an acceptable constraint". That is an operational decision, not a
capability limit, and it is worth noting the constraint is **not new** —
§18 showed the custom design has no database-free safe mode either, and
every option on this storage already needs a single enforced writer or an
external arbiter.

**This does not by itself overturn the recommendation.** What it does is
move the filesystem catalog from "impossible" to "available, with a
constraint SAGE may already satisfy", which materially changes the
trade-off: it would remove the catalog database entirely, and with it the
§28 catalog-loss recovery story and the reader's dependence on a SAGE
endpoint. The cost is that a second writer — a backfill job, a manual
repair, an accidentally-rerun cron — corrupts the table with no error.

### What is still not tested

Whether the hadoop catalog survives a **realistic write volume** (this
table has 2 fields and no data files), whether `version-hint.text` can be
torn by a concurrent reader mid-update, and whether a single-writer lock
can be enforced operationally well enough to rely on. Those are the
questions that decide it.

### Cleanup

Written under `/sage/cold-storage-test-fscat1/` and
`/sage/cold-storage-test-diag*/` — new prefixes, distinct from §24's
`-fscat`. Removed recursively, `PROPFIND` → 404 verified on each.
`/sage/node-data` and the `sage-storage-loader` paths were never touched.

---

## 33. "Single writer" does not mean single-threaded (exp33)

`experiments/icebergo/fscatalog_parallel_test.go`, **4/4**, against the real
NRDStor origin.

§32 concluded the filesystem catalog works on Pelican "with exactly one
writer". That phrasing invites a reading it does not support — that SAGE
would have to ingest the archive single-threaded — so it is worth measuring
rather than reasoning about.

**Two different things get called "writing", and only one of them is
constrained:**

1. **Producing data files** — encoding Parquet, uploading to Pelican. This
   is the expensive part, it is pure I/O and CPU, and it touches the
   catalog not at all.
2. **Committing** — appending those files to the table and advancing
   `version-hint.text`. This is one small metadata operation, and it is the
   only part the missing CAS (§31) affects.

The hadoop catalog needs (2) serialized. It says nothing about (1).
iceberg-go's `AddFiles(ctx, filePaths []string, ...)` takes a *list* of
already-written files, which is exactly the shape that lets N parallel
producers feed one committer.

### Measured

| check | result |
|---|---|
| Q1: 4 producers write Parquet to Pelican **concurrently** | **PASS** — 250 rows each, no coordination |
| Q2: **one** committer adds all 4 files in a single commit | **PASS** |
| Q3: every row from every producer is present | **PASS** — 1000/1000, 250 per producer |
| Q4: the pointer advanced **once**, not 4 times | **PASS** — `"1"` → `"2"` |

43 requests (`GET:14 HEAD:6 MKCOL:7 MOVE:6 PROPFIND:11 PUT:14`).

Contrast with §32's race test, which ran 4 concurrent *commits* and lost
2–3 of them. Same storage, same adapter, same number of workers — the
difference is only whether the parallelism is in the data path or the
commit path.

### What the constraint actually costs

Per *batch*, one serialized metadata call. Not per file, not per row.
Parallel Parquet production, parallel uploads, and parallel readers are all
unaffected. The practical requirement is that **one process owns the
commit** — a single ingest service, a job queue with one consumer, or a
cron that cannot overlap itself — which is a conventional architecture
rather than a concession.

What is genuinely ruled out is *two independent processes committing to the
same table at the same time*: two pods both running the ingest loop, a
backfill job racing the live writer, or an operator re-running a job by
hand while the scheduled one is mid-commit. Those corrupt the table with no
error raised (§32), so the enforcement has to be real — a lock, a single
replica, or a queue — not a convention.

### A harness bug worth recording

Two, actually, both mine, and both of the kind that fails misleadingly:

1. **The §32 adapter hardcoded `FS1PREFIX`** when minting tokens instead of
   using the prefix its `PelicanIO` was constructed with. It worked only
   because §32 was the sole user. §33 reuses the adapter under a different
   prefix, and the mismatch surfaced as **"namespace does not exist"** — a
   confusing way for a token-scoping mistake to present, and the same class
   of bug as exp19 §4b.
2. **`iceio.Register` is process-global and last-call-wins.** Three tests
   each registering their own `https` handler meant only the last one
   worked; run together, the other two served requests through the wrong
   adapter and **failed with no output at all**. Fixed by registering once
   in `init()` and having each test install its adapter via `useFS(t, fs)`,
   restored on cleanup. All three now pass in a single `go test` invocation
   — which is how the collision was found, and why they should keep being
   run that way.

### Cleanup

Written under `/sage/cold-storage-test-fscat2/`, its own prefix, removed
recursively (9 objects, `PROPFIND` → 404). `/sage/node-data` and the
`sage-storage-loader` paths were never touched.

---

## 34. The plans/6 design, end to end on NRDStor (exp34)

`experiments/exp34_hint_reconciler.py`, **12/12**, against the real origin.
Two concurrent committers, one reconciler, one reader with no database.

plans/6 proposes splitting the two jobs the catalog was doing: writers
commit through an Iceberg **SQL catalog** (the database is the CAS, so
concurrent committers are safe), and a **single reconciler** publishes
`metadata/version-hint.text` to Pelican from the committed head, so
consumers can find the current metadata JSON with no SAGE endpoint.

Every piece had been measured separately and the *combination* never had —
§19 (SQL catalog commits, leaves no hint), §32 (filesystem catalog writes a
hint, independent reader works), `ndp_interop` (DuckDB refuses a directory
with no hint), and a local fixture for hint formats. The last was a tmpdir,
not Pelican. This runs the join.

### Measured

| check | result |
|---|---|
| Q1a: two concurrent committers both complete | **PASS** — one retry on the DB CAS, 2.6 s |
| Q1b: no commit silently lost | **PASS** — 100 rows, 50 per committer |
| Q2: SQL catalog publishes no hint itself | **PASS** — `GET` → 404, 4 `*.metadata.json` present |
| Q3a: reconciler publishes the stem of the DB head | **PASS** |
| Q3b: no trailing newline | **PASS** |
| Q3c: stem is not a bare version number | **PASS** |
| Q4a: reader with no DB resolves the hint | **PASS** |
| Q4b: that reader sees every committed row | **PASS** — 100 rows, both committers |
| Q5a: a stale hint yields an older *valid* snapshot, silently | **PASS** — 50 rows, no warning |
| Q5b: re-running the reconciler repairs it | **PASS** — back to 100 |
| Q6: second pass is idempotent | **PASS** — 0 PUTs |
| Q7: an orphan metadata file does not fool it | **PASS** |

47 GET, 16 PUT, 34 HEAD, 3 DELETE, 3 PROPFIND. Reproduced on a second
clean run.

### What Q1 settles

This is the direct contrast with §32. Same storage, concurrent writers,
opposite outcome:

| | §32 filesystem catalog | §34 this design |
|---|---|---|
| arbiter | storage (has no CAS) | the database |
| two writers race | 3 of 4 told they won, **2 commits lost** | one retries, **both land** |
| recoverable | no | n/a — nothing was lost |

The database does the job the origin cannot (§31). The losing committer got
`CommitFailedException`, retried once, and succeeded — which is the
arbitration working, and is precisely what Pelican could not offer at any
price.

### What Q5 settles — the failure mode is stale, not lossy

Pointing the hint at an older metadata file produced **50 rows with no
warning**, while the newer file sat in the same directory and the database
still named it as head. So:

- the hint is **authoritative** for readers — clients do not notice a newer
  file exists, which makes staleness **silent** and means detection must be
  active (the reconciler comparing published hint against
  `metadata_location`), never incidental;
- but the damage is bounded to *what consumers see*. One reconciler pass
  restored all 100 rows. Nothing was lost, because the database never
  stopped being correct.

Stale-but-valid and self-healing, against §32's silent unrecoverable loss.
That asymmetry is the whole argument for the design.

### Two bugs found, both mine, both worth recording

1. **`catalog_name` is part of the primary key in `iceberg_tables`.** The
   first draft opened each committer as `SqlCatalog("writer0"…)`,
   `SqlCatalog("writer1"…)` over one database and warehouse. They did not
   share a catalog — each silently got a private one, and the second
   committer failed with **`NoSuchTableError: Table does not exist:
   sage.obs`** against a table that had just been created. Two services
   that are meant to commit to the same table must use the **same catalog
   name**; this belongs in the deployment config, because the failure
   blames the table rather than the name.
2. **pyiceberg's SqlCatalog lays tables out as `<warehouse>/<ns>/<table>`,**
   with no Hive-style `.db` suffix. The draft hardcoded `sage.db/obs` and
   the reader got `FileNotFoundError` on a metadata file the catalog had
   certainly written. Fixed by deriving the layout from
   `metadata_location` rather than assuming it.

Both were found only by running the combination — neither could appear in
a test of any single piece, which is the argument for end-to-end runs over
more unit probes.

### Cleanup

Each run used its own nanosecond-stamped prefix
(`/sage/cold-storage-test-hint-<ns>`), checked `PROPFIND` → 404 **before**
writing and removed recursively afterwards. All eight prefixes from this
session's runs verified gone (404). `/sage/node-data` and the
`sage-storage-loader` paths were never touched.

### Still open

The reconciler's **period** is unmeasured, and it sets the freshness lag
consumers see.

~~The Postgres question from §19 is unchanged — this ran on SQLite, and
Q1's arbitration is a SQLite result.~~ **Superseded by §35**: SQLite was
then measured to 24 concurrent committing *processes* with no loss and at
most 2 retries. The constraint is not concurrency but connectivity — a
SQLite URI is a file path, so Postgres is required only when committers run
on separate hosts. Q1's arbitration result stands on its own.

And the table index ("what tables exist?") remains outside what the hint
covers.

---

## 35. SQLite is sufficient; the constraint is connectivity, not concurrency (exp35)

`experiments/exp35_sqlite_vs_postgres.py`, **4/4**. No network, no Pelican
— this is a question about the catalog database, so the warehouse is a
tmpdir.

> **This section corrects §34 and finding 18.** Both named "re-run Q1
> against Postgres" as the highest-value open item. That recommendation was
> reasoned from SQLite's reputation, not from a measurement, and the
> measurement does not support it.

§34's Q1 arbitrated two concurrent committers safely on SQLite, and I
generalized that into a Postgres recommendation. But Q1 was the *easiest
possible case*: threads, in one process, on a local file. The honest
question is what breaks as that gets harder — and whether the breaking
point is concurrency at all.

### Separate processes, rising contention

Not threads. Each committer is its own OS process with its own connection,
which is what a second committing service actually looks like.

| committers | rows | retries (max) | wall |
|---|---|---|---|
| 6 | 60/60 | 1 | 0.9 s |
| 12 | 120/120 | 2 | 2.5 s |
| 24 | **240/240** | **2** | 2.9 s |

No loss at any level, no exhausted retries, and the retry count never got
interesting. **There is no concurrency-correctness argument against SQLite
at this scale.** Retry cost was measured deliberately: a database that
"works" but needs 30 attempts and 40 seconds does not work, and this is
nowhere near that.

### The constraint that is real

```
sqlite://db-host:5432/catalog.db
  -> ArgumentError: Invalid SQLite URL
```

A SQLite URI is a **file path**. There is no network protocol, so every
committer must share a filesystem.

So the question was never "is SQLite reliable enough?" but **where do the
committers run?**

| deployment | catalog |
|---|---|
| one host / one pod | **SQLite** — simpler, no service to operate, measured safe to 24 processes |
| multiple hosts | **Postgres** — not a reliability tradeoff; SQLite cannot be connected to at all |

Postgres becomes necessary when committers run on separate hosts. That is a
**deployment question, not a reliability one**, and it should be decided
that way.

### Caveat

These ran on a local filesystem (APFS). SQLite over NFS has the locking
problems its own documentation warns about, so if "shared volume" is the
plan to get multi-host committers onto one SQLite file, **that** is the
configuration to measure — and it is the one I would avoid, rather than
SQLite itself.

### Methodological note

This is the second time in this project a conclusion rested on reasoning
rather than a result (§24 was the first, where two rounds of probing never
built a table). Both were caught by someone asking why. The pattern worth
noticing: §34's Q1 was a *passing* test, and the unsupported claim was
attached to it as a recommendation — exactly the position §32's unverified
concurrency caveat occupied before the race test was written. A caveat or a
recommendation bolted onto a green result does not inherit its evidence.

---

## 36. The design at the archive's real scale (exp36)

`exp34` proved plans/6 correct on 100 rows. The open question was cost. The
archive is **4,564 parquet files over 608 day-partitions, 209 GB** (the
194 GiB `du` reports — earlier notes in this file quote the binary figure).

Local only: a tmpdir warehouse, the archive read-only, nothing uploaded and
nothing on NRDStor. **The scale question here is the metadata and commit
path**, not throughput to Pelican, which remains unmeasured. 9/9 checks.

### What holds

Partition pruning works on the dimension that matters. On a 608-partition
table, a one-week filter planned **7 files instead of 608, 6 ms instead of
218 ms**. The read path scales.

### 1. `add_files` cannot register the archive — the cheap path does not exist

The largest consequence, and invisible at 100 rows. The archive's `time`
column is `timestamp[ns, tz=UTC]`:

```
UnsupportedPyArrowTypeException: Column 'time' has an unsupported type:
timestamp[ns, tz=UTC]
```

Casting the *Iceberg* schema to microseconds does not help: `add_files`
reads the real footer, and the footer says ns. Both escape hatches were
tried and both fail — `downcast-ns-timestamp-to-us-on-write` governs
`append` (which rewrites bytes, so it never sees `add_files`), and
format-version 3, where Iceberg does have a native `timestamp_ns`, raises
`NotImplementedError: Writing V3 is not yet supported` in pyiceberg 0.12.

So registering 4,564 existing files in place, no byte movement, is
unavailable. Ingest must **rewrite** to microseconds. Measured on real
files: **~26 MB/s**, about **2 hours of single-core CPU** for the whole
archive, one time, trivially parallel by day. The permanent cost is on the
steady-state path, where each incoming file is rewritten, not registered.

### 2. §25's microsecond decision survives contact with real data

Worth testing rather than assuming, because **94% of sampled rows carry
sub-microsecond digits** — truncation discards real bits.

It discards nothing that *distinguishes a row*. On 10M rows, duplicates of
`(time, name, node, plugin)` were **identical before and after** truncation
(zero new collisions), and the discarded remainder is uniform across all
1000 values 0–999 — the signature of clock granularity, not measurement.
§25 stands, now for a measured reason rather than convenience.

### 3. Commit cost is not flat

Each commit rewrites manifest metadata covering the files already in the
table, so cost grows with accumulated count:

| files in table | commit | scan planning |
|---|---|---|
| 1 | 3 ms | 1 ms |
| 300 | 63 ms | 112 ms |
| 600 | 155 ms | 200 ms |
| 1,200 | 333 ms | 405 ms |

~0.28 ms per accumulated file, so **~1.3 s per commit at 4,564 files**,
locally, before any Pelican round trip. Nothing breaks, but it puts a
ceiling on commit frequency — and therefore a floor under the reconciler
period plans/6 still owes — and argues for manifest compaction.

### 4. "Pin metadata expiry off" was treating two mechanisms as one

plans/6 said to pin expiry off, to protect published hints from naming a
deleted file. That is right about one knob and wrong about the other, and
keeping both off bounds nothing:

| knob | what it does | verdict |
|---|---|---|
| `write.metadata.delete-after-commit.enabled` | deletes old `*.metadata.json` | **keep OFF** — deletes files hints may name |
| `table.maintenance.expire_snapshots()` | drops snapshot entries from history | **turn ON**, retention policy |

Snapshot expiry is what actually bounds growth, and it is **safe for the
hint**: it writes a *new* metadata file and leaves the old one in place, so
a hint published before a pass still resolves to an older **valid**
snapshot — the recoverable staleness this design already accepts. Verified
directly (Q6, 3/3): snapshots 20 → 1, current file 17.3 → 5.5 KB, the
pre-expiry hint still readable, fresh readers still seeing all 200 rows.

This matters because every `metadata.json` embeds the **entire snapshot
history**, and the hint points consumers at exactly that file — each
consumer downloads it on every read. Growth is ~0.7 KB per snapshot:

| commit rate | snapshots | current `metadata.json` | `metadata/` if never expired |
|---|---|---|---|
| 1/day × 1y | 365 | 0.2 MB | 0.0 GB |
| 24/day × 1y | 8,760 | 5.9 MB | 25 GB |
| 24/day × 5y | 43,800 | **29.3 MB** | **627 GB** |
| 96/day × 5y | 175,200 | 117.2 MB | 10 TB |

Hourly commits for five years make the per-read fetch 29 MB and the
metadata directory three times the size of the data archive it describes.

**Correction to my own method here:** the first run reported
`expire_snapshots: AttributeError` and I nearly recorded "pyiceberg 0.12
cannot expire snapshots." It can — the API is `table.maintenance.
expire_snapshots()`, not `table.expire_snapshots()`. I had guessed the entry
point and read the failure as a missing feature. Grepping the installed
package settled it in one command. A negative result about a library should
be checked against its source before it goes in a document, especially when
it is the result that makes a design problem look unsolvable.

**Environment note:** Q4 (partitioned writes) needs the `pyiceberg-core`
extra, which `sage_pipeline` does not have; it skips cleanly there (8/8) and
was run in a throwaway venv to get the number (9/9). `sage_pipeline` was not
modified.

---

## 37. The plans/6 commit path over Pelican (exp37)

§36 closed with the thing still open: *"the network cost of the commit
path at this file count is still open."* This closes it. 12/12 checks
against the real NRDStor origin (11/11 in `sage_pipeline`, where Q7 skips
for lack of `pyiceberg-core`).

The 209 GB archive is **not** uploaded. At ~190 ms per request the cost
that matters is **request count** (§17), and §36 Q7 already established
that commit cost tracks file *count*, not bytes — so this registers a
realistic count with tiny payloads. Extrapolations are labelled.

### The write path is flat, and that is the good news

Per-commit request count does not move as files accumulate:

| commit | files in table | requests | wall |
|---|---|---|---|
| 1 | 1 | 7 | 0.9 s |
| 2 | 2 | **9** | 0.9 s |
| 16 | 16 | **9** | 0.9 s |
| 40 | 40 | **9** | 1.0 s |

Exactly 9 (`GET`×2, `PUT`×4, `HEAD`×3) from the second commit on — the
local millisecond growth in §36 Q7 does **not** become request growth.
So the bill is set by the **number of commits**, not the size of the
table:

- 4,564 single-file commits → ~40,800 requests, **~1.2 h** *(extrapolated)*
- 608 day-batched commits → ~5,400 requests, **~10 min** *(extrapolated)*

Batching is the whole lever on the write side.

The **reconciler is O(1)**: 2 requests to publish a hint, 1 to confirm it
unchanged, regardless of table size. It stays negligible at any scale,
which is what makes a short period affordable — relevant to the open
reconciler-period question, which is bounded by commit cost, not by this.

### The read path is the expensive side, and it does not flatten

A cold reader with no database: **205 requests for 40 data files**, ~5.1
per file, growing linearly. Attributed (Q6):

| what | requests (40 files) | scaling |
|---|---|---|
| `metadata.json` | 2 | **O(1)** |
| manifest list (`snap-*.avro`) | 2 | **O(1)** |
| manifests (`*.avro`) | 80 | O(files) |
| **data (`*.parquet`)** | **120** (40 HEAD + 80 GET) | O(files) |

The Iceberg metadata layer is *not* the problem — 4 requests total. The
cost is per data file.

**One third of it is our bug, not Iceberg's.** The HEAD before each file
asks the origin for a size the manifest already records
(`file_size_in_bytes=1362` sitting in the manifest while 40 HEADs go out).
`PelicanInputFile` caches size per *instance*, but pyiceberg builds a fresh
instance per file per scan, so the cache never survives. Measured with a
process-wide cache: **205 → 122 requests, 40% fewer**, every HEAD gone. The
remaining 2 GETs per file are the parquet footer then the row group, which
is Iceberg's access pattern and correct.

Projected to 4,564 files *(extrapolated)*:

| read | requests | wall at 190 ms |
|---|---|---|
| full scan, as-is | ~23,400 | **~74 min** |
| full scan, sizes cached | ~13,900 | ~44 min |
| one-week partition filter | **~2,900** | ~9 min |

### Partition pruning holds over the network (Q7)

§36 measured pruning locally in *files*; the projection above leans on it,
so it was re-measured here in *requests* against the origin. On a
60-partition table: full scan 302 requests / 2.1 s, one-week filter **37
requests / 0.8 s — 88% fewer**. It holds.

That is the real conclusion: a full-table scan over HTTPS is something to
design against, and **partition pruning, not a faster origin, is what makes
the read path work**. The write path and the reconciler are already cheap.

### Actions this produces

1. **Cache file sizes in `pelican_fileio.py`** — or better, plumb
   `file_size_in_bytes` from the manifest into `PelicanInputFile` so no
   HEAD is ever issued. 40% of reader requests, measured.
2. **Batch commits by day**, not per file: 10 minutes versus 1.2 hours for
   the same 4,564 files.
3. **Treat unpruned full scans as a misuse** of this deployment and say so
   wherever consumers are documented.

---

## 38. Querying the data: throughput, not request count (exp38)

§37 answered a narrower question than its own summary implied. It measured
query **planning** — which files a query must open — and found that side
scales. But every "read" in §37 moved a few hundred bytes, because it
registered a realistic file *count* with tiny payloads to isolate the commit
path. The number that decides whether consumers can actually use this — the
time to move the bytes of a real query — was never measured. §37 says so in
one line; this closes it.

`exp38_query_throughput.py`, **7/7**, uploads real archive parquet to a
nanosecond-stamped prefix (PROPFIND → 404 both ends, 37 and 45 objects
removed across two runs). Two runs: 4 files (169 MB → 24 data files) and 6
files (26 data files).

### The headline: sequential reads are unusable, concurrent ones are fine

| workers | run A (24 files) | run B (26 files) |
|---|---|---|
| 1 | 4.4 MB/s | 5.2 MB/s |
| 4 | 20.9 | 18.3 |
| 8 | 31.8 | 29.7 |
| 16 | **38.7** | **33.9** |

**A sequential Iceberg scan over Pelican runs at ~5 MB/s.** That is the
number that would have sunk this if it had gone unmeasured — at that rate a
one-week query takes **9 minutes**. With concurrency it takes **~1 minute**.

The good news is that pyiceberg **already parallelises**: `io/pyarrow.py:1847`
maps scan tasks onto an `ExecutorFactory` thread pool. No code change is
needed to get the 6.5–8.7× speedup, only configuration
(`PYICEBERG_MAX_WORKERS`). The pool is over **files**, so a scan touching one
file gets no benefit — another reason day-batching (§37) matters: it sets
how many files a day-range query can parallelise over.

The knee is at 8–16 workers around **30–39 MB/s**, consistent with §17's
~30 MB/s single-range ceiling being a *per-stream* limit rather than a link
limit. Run A was still gaining at 16 (1.21×), run B had saturated (1.14×) —
so treat ~30 MB/s as the dependable figure and anything above as headroom.

### Column projection saves bytes but not time (Q2)

This is the counter-intuitive result and the one most likely to mislead:

| | bytes | requests | wall | rate |
|---|---|---|---|---|
| all 13 columns | 141.7 MB | 84 | 32.0 s | 4.4 MB/s |
| 1 column | **8.6 MB** | **104** | 18.2 s | 0.5 MB/s |

**94% fewer bytes buys only 43% less time, and costs 20 *more* requests.**
A projected scan stops streaming bulk bytes and starts scattering small
column reads, so it becomes latency-bound — exactly §17's finding, now
reproduced through the Iceberg path. The practical rule: **size a query by
round trips, not by bytes.** The first draft of this experiment projected a
one-week single-column query by scaling the byte ratio and predicted ~5
seconds; scaling the measured *time* ratio gives ~35 s. The byte-based
estimate was wrong by an order of magnitude and was corrected before it
reached this document.

### The row-group shape, and an unexpected reprieve (Q5)

Archive files are **1 row group of 10M rows**, so Iceberg can prune to the
file and no further — the smallest addressable unit is the whole 42–78 MB
file. That is what `plans/3_findings.md` already recommends fixing with
`MaxRowsPerRowGroup`.

But ingest has to **rewrite** every file anyway (§36 Q1: `add_files` cannot
register ns timestamps), and pyiceberg wrote **2 row groups per file without
being asked**. So the exporter change is *additive*, not a prerequisite — the
rewrite already improves the shape. This weakens the urgency of that
recommendation without invalidating it.

### Write path, for completeness

Uploading real bytes ran at **3.9–5.2 MB/s** through `tbl.append()` — slower
than the 8.6 MB/s a raw PUT achieves, because append also casts ns→us and
re-encodes. At that rate the full 209 GB archive is **~12 hours of ingest**,
one-time. Stored size came to 83% of on-disk size.

Also worth recording: **4 appends became 24 data files.** pyiceberg sizes its
own output, which helps reads (more parallelism) but means file counts in
§36/§37 projections are pyiceberg's, not the archive's.

### What this changes

Nothing architectural. Every number here is a property of the *reader*, not
of Iceberg-on-Pelican, and the biggest one is a default setting rather than a
defect. But it adds two actions:

4. **Set `PYICEBERG_MAX_WORKERS` (8–16) wherever a consumer reads**, and
   document it. A sequential reader is 6–8× slower, and nothing in the
   default configuration hints at this.
5. **Document that queries are sized by round trips, not bytes** — a
   narrow column projection is *not* proportionally faster, and may be
   slower per byte than reading everything.

### Still not measured

Concurrent *readers* (this was one process at a time), NRDStor under load,
and whether the 30–39 MB/s holds from a well-connected host rather than a
laptop link. The *shape* — concurrency mandatory, latency-bound projection —
transfers; the absolute rate does not.

---

## 39. Does Iceberg add latency over plain Parquet? (exp39)

`plans/4` compares Iceberg against the custom catalog on safety, operations
and capability. Its only latency numbers are for the **commit** (9 requests
vs 5, ~0.9 s vs ~0.7 s). The question nobody had measured is the one a
reader of that document would actually ask: Iceberg's metadata layer sits
between a consumer and the bytes, and its write path re-encodes every file —
what does that cost?

`exp39_iceberg_vs_custom.py`, **5/5**, four runs. Same archive files, same
origin, same FileIO, same process, interleaved. `catalog.py`'s `CatalogStore`
ported to Pelican exactly as its docstring anticipates ("the Pelican port
replaces exactly this class").

### The answer: latency is not what separates these designs

| | Iceberg | custom | ratio |
|---|---|---|---|
| write 4 files (169 MB) | 23.9 s | 25.2 s | 0.95× |
| read all, 8 workers | 4.6 s | 7.1 s | 0.6× |
| read one day | 2.9 s | 3.6 s | 0.8× |
| metadata requests | 12 | 2 | — |

**My hypothesis was refuted and is recorded as such.** I expected the
mandatory ns→us rewrite (§36 Q1) to make Iceberg's write visibly slower. It
does not. Both paths are **network-bound at ~6 MB/s**, and the rewrite
*shrinks the payload 17%*, so Iceberg's extra CPU hides behind an upload with
less to send. Across runs the wall ratio was 0.86–1.05 and the effective-rate
ratio 0.79–0.96 — **the run-to-run spread on this link exceeds the difference
between the designs.** The first version of this check asserted a tight band
and failed twice before I widened it to match the measurement's own noise;
asserting anything tighter would be measuring the weather.

### Iceberg's metadata overhead is real but fixed

12 metadata requests against the custom catalog's 2 — a 6× ratio that sounds
bad and is not, because **neither grows with file count**. At 24 data files
Iceberg is already at 0.50 metadata requests per data file and still falling.
Any query large enough to care about latency amortises it to nothing.

### The one place the custom catalog scales worse (Q5)

Its snapshot JSON embeds **every live file**: 274 B/file here, so ~1.3 MB at
the archive's 4,564 files — **downloaded in full by every reader on every
read, and unprunable.** (§40 Q6 re-measured this with uniform realistic
entries and got **167 B/file, ~0.8 MB**; that is the better figure. The
conclusion is unchanged — the problem is the access pattern, not the size.) Iceberg splits the same information across `metadata.json` and
per-partition manifests, so a pruned reader fetches a subset. This is the
mirror image of §36's finding that Iceberg's `metadata.json` grows with
snapshot history: both designs have an O(n) metadata structure, and the
difference is that Iceberg's is *partitioned* and the custom catalog's is not.

### A methodological note worth keeping

The first run showed Iceberg reading **3× faster** than the custom catalog.
That was not a finding — pyiceberg's scan is parallel (`io/pyarrow.py:1847`)
and my custom reader was a sequential `for` loop. I was measuring my loop
against a thread pool. §38 had just established that this difference alone is
worth 6–8×, which is what made the result recognisable as an artefact rather
than a result. Both readers are now pinned to 8 workers, and the gap
disappeared (1.0×). **A comparison between two designs is only a comparison
if the thing you are not studying is held equal.**

### What this means for the decision

Latency does not choose between these designs. Both move bytes at the link's
rate; Iceberg's metadata overhead is a fixed handful of requests, and the
custom catalog's snapshot is the structure that scales worse with file count.

The decision therefore rests where `plans/4` already put it — concurrency
safety (Iceberg wins: the custom catalog lost an update 3/3 silently, §18),
row-level deletes (Iceberg), ecosystem (Iceberg), versus operational surface
and the ns-timestamp freedom (custom). This experiment removes "Iceberg is
slower" from that list as **unsupported**.

---

## 40. What storage does Iceberg add over plain Parquet? (exp40)

Asked directly after §39 and not previously measured. §37 counted metadata
in **requests** (4, O(1)); §36 Q5 measured the growth of *one* file (~0.7 KB
per snapshot). Neither answers "how many bytes does the metadata layer
occupy beside a 209 GB archive," which is what anyone sizing storage asks.

`exp40_metadata_storage.py`, **9/9**, four runs. Local: a tmpdir warehouse,
the archive read-only, nothing uploaded. Metadata bytes are identical
wherever the table lives, so the network adds nothing to this question. The
sample files carry **real archive rows**, because manifest entries store
per-column lower/upper bounds and the archive's string columns are what set
their width — synthetic data would understate it.

### The overhead has two shapes, and conflating them gives a wrong answer

| component | scales with | measured |
|---|---|---|
| manifests (`*-m0.avro`) | **data files** | **~545 B per file** (540/545/548 across runs) |
| manifest list (`snap-*.avro`) | commits | ~220 B per commit |
| `metadata.json` | **commits**, and embeds *all* history | ~880 B per snapshot, retained forever |

Only the first is what people picture when they ask this question, and it is
negligible: **4,564 files × 545 B = 2.5 MB of manifests for a 209 GB
archive — 0.0012%.** Per-file metadata overhead is, for practical purposes,
zero.

### The honest headline: Iceberg *saves* 35 GB, it does not cost

This is the number that reverses the question, and it comes from §38 rather
than from here. Ingest cannot register the archive in place (§36 Q1: ns
timestamps), so every file is **rewritten** — and the rewrite stored
**83% of on-disk size**. On the real archive:

| | bytes |
|---|---|
| archive as it sits today | 209 GB |
| same data rewritten through Iceberg | **~173.5 GB** |
| Iceberg metadata on top (608 day-commits) | +0.5 MB current, +163 MB if every version is retained |
| **net** | **~35 GB smaller** |

The re-encode recovers four orders of magnitude more space than the metadata
consumes. Anyone asking "what does Iceberg cost us in storage" should be
told the honest answer is a **saving**, with the caveat that this is a
property of re-encoding ns→us with current parquet defaults, not a property
of Iceberg as such — writing those same files with the same encoder and no
catalog would save the same 35 GB.

### Where storage can genuinely run away: retention, not file count

Keeping every `metadata.json` is quadratic, because each new version embeds
the entire snapshot history. At 40 commits the retained versions were
**71% of all metadata bytes** while the current file was 5% of that total.
Projected *(extrapolated)*:

| commit cadence | commits | current `metadata.json` | retained, never expired |
|---|---|---|---|
| **monthly × 10 y (the design point)** | **120** | **0.1 MB** | **6.4 MB** |
| monthly × 1 y | 12 | 0.01 MB | 0.1 MB |
| 608 day-commits (one archive pass) | 608 | 0.5 MB | 163 MB |
| 1/day × 1 y | 365 | 0.3 MB | 59 MB |
| 24/day × 1 y | 8,760 | 7.7 MB | **34 GB** |
| 96/day × 1 y | 35,040 | 30.9 MB | **541 GB** |

**Cadence decides whether any of this matters, and Sage's is monthly**
(stated 2026-10-15: InfluxDB is the hot store, a month accumulates there, is
committed to cold storage as one batch, then dropped from hot). At twelve
commits a year the quadratic never gets going: **6.4 MB of retained metadata
after a decade**, against ~1 TB of data. The rows above it are kept because
they are what makes the mechanism visible, and because they are what a
higher cadence would cost — at hourly commits the same mechanism reaches
34 GB/yr, **16% of the data archive, to describe it**. This is §36 Q5's
mechanism priced in total bytes rather than per-read bytes.

**CORRECTION to my first write-up of this section.** I wrote that snapshot
expiry is what bounds this growth, carrying the claim forward from §36 Q5.
That is **wrong for storage**, and the error came from reusing a conclusion
about *per-read* bytes to answer a question about *stored* bytes. Measured
directly (40 commits, pyiceberg 0.12):

| action | current `metadata.json` (per READ) | all retained json (STORAGE) |
|---|---|---|
| `expire_snapshots()` | 34,318 → **9,531 B (−72%)** | 717,930 → **727,461 B (+1%)** |
| `delete-after-commit.enabled=true`, `previous-versions-max=5` | — | 718,068 → **154,986 B (−78%)**, 41 files → 6 |

Snapshot expiry rewrites history *inside* a **new** metadata file and leaves
every old one on disk — it even adds one. It is the right knob for the
**read** cost (§36 Q5 measured exactly that, 17.3 → 5.5 KB, and stands), and
it does nothing for the storage number in the table above. The only knob
that reclaims stored metadata bytes is
`write.metadata.delete-after-commit.enabled`, which plans/6 deliberately
keeps **off** because a published hint may still name a deleted file.

So the two knobs are **not interchangeable**:

- `delete-after-commit` **off** (the default, and the recommendation) —
  hints always resolve; metadata grows without bound.
- `delete-after-commit` **on** with `previous-versions-max` — storage
  bounded; a hint older than the window **breaks** rather than going stale,
  which is a worse failure than the one plans/6 accepts.

**At monthly commits this is not a live trade, and §41 closes it.** The
growth being accepted by leaving deletion off is 6.4 MB per decade, and
turning it on would buy that back at the price of an outage mode. §41
measures the window's behaviour and shows the default already carries
~8.4 years of margin at this cadence.

### Expiry does not delete data files either (verified)

Worth stating because the word "expire" suggests reclamation. On an
append-only table, expiring 7 of 8 snapshots removed **0 data files, 0
manifests, and added 1 metadata.json**; all 8 rows stayed readable. pyiceberg's
`ExpireSnapshots._commit` only emits a `RemoveSnapshotsUpdate` — there is no
file deletion in the code path.

For Sage's append-only workload nothing is stranded, so this costs nothing
today. But after any operation that supersedes files — a `delete`, or the
compaction `plans/4` contemplates — the old files stay on storage with
nothing referencing them. Measured: after deleting half the rows and
expiring, **4 of 8 data files were orphaned** (live set 4, on disk 8).
pyiceberg 0.12 has **no orphan-file cleanup**; reclaiming those is a job
somebody has to write. Not a problem now, a prerequisite before compaction.

### Commit granularity is worth 32× on metadata too (Q4)

Identical data, 32 files, two strategies:

| | total metadata | of which `metadata.json` |
|---|---|---|
| 32 single-file commits | 765,779 B | 518,686 B in 33 files |
| 1 batched commit | **23,746 B** | 4,479 B in 2 files |

**32.2× less metadata for the same bytes of data.** Day-batching was already
justified on request count (§37: 10 min vs 1.2 h) and on read parallelism
(§38: the pool is over files). This is a third, independent reason, and it
is the one that keeps the quadratic above harmless.

### The custom catalog, measured the same way (Q6)

167 B per live file in its current snapshot at 40 files — **lower per-file
than Iceberg's 545 B**, and the same shape: it retains every snapshot too
(40 snapshots, 139 KB). Note this revises §39's figure of 274 B/file, which
was measured on a different entry shape; the two are not contradictory, but
**167 B is the better number** since this run used uniform realistic
entries. At the archive's 4,564 files that is ~0.8 MB rather than ~1.3 MB.

That does not change §39's conclusion, because the problem there was never
the size: it is that the custom catalog's snapshot is **unprunable and
fetched in full on every read**, whereas Iceberg's 2.5 MB of manifests is
partitioned and a pruned reader touches a fraction. Smaller total, worse
access pattern.

### What this produces

It corrects the framing — **storage overhead is not a reason to avoid
Iceberg here, and the migration is net negative on bytes** — and it puts a
number on day-batching (32× metadata, on top of 7× requests).

It also **corrects a claim plans/6 had recorded as settled.** "Set a
snapshot-retention policy" was written down as the answer to unbounded
metadata growth; it is not, because expiry deletes nothing from storage.
That correction stands on its own, independently of cadence — anyone who
reads §36 Q5 and concludes that expiry bounds storage is reading a
per-read result as a storage one.

What it does *not* produce is an open decision. At the monthly cadence
Sage intends, unbounded growth is 6.4 MB per decade, so the right action
is to leave deletion off and record why:

6. **Leave `write.metadata.delete-after-commit.enabled` off**, and treat
   the resulting growth as a known, bounded quantity rather than an
   unquantified risk: **~6.4 MB over 10 years at monthly commits**. Turn
   snapshot expiry on anyway, but for the *read* cost (−72% on the file
   every consumer downloads), not for storage. Revisit only if the commit
   cadence rises by more than an order of magnitude — the arithmetic for
   that case is in §41.

---

## 41. Can Iceberg bound old `metadata.json` by itself? (exp41)

Asked directly: *"can you set the retention of old metadata.json versions,
or is that something custom we would have to build?"* The short answer is
**built in, nothing to build** — but §40 left the knob's *behaviour*
unmeasured, and plans/6 hangs on exactly that behaviour, so this section
measures it rather than quoting the documentation.

**Read in order, this section and §40 end somewhere neither started.** Both
were written before Sage's commit cadence was stated; §40 treated retention
as an open decision and §41 measured it to make the decision possible. The
cadence turned out to be **monthly**, which does not change any measurement
below but does settle what to do about them: nothing. The measurements are
kept in full because they are the reason the conclusion is safe rather than
lucky, and because they are what transfers if the cadence changes.

`exp41_metadata_retention.py`, **6/6**. Local: tmpdir warehouse, no archive,
no Pelican. This is a property of pyiceberg's commit path, identical
wherever the table lives.

### It is built in (Q1)

Two stock table properties, no tooling:

```python
{"write.metadata.delete-after-commit.enabled": "true",
 "write.metadata.previous-versions-max": "5"}
```

40 commits, same data: **41 files / 715,652 B → 6 files / 154,725 B.**
Deletion happens inline on every commit — `ToSnapshotUpdate` trims
`metadata_log` to the configured count (`table/update/__init__.py:735-752`)
and the catalog deletes the difference (`catalog/__init__.py:858-870`).
Sage would write **no** retention code.

### The window is COUNT-based, not age-based (Q2)

This is the detail that matters and it is easy to assume wrong. 20 commits
inside one second left **6** files, not 20 — an age-based window would have
kept them all. There is no age option for metadata files anywhere in
pyiceberg 0.12. (`MAX_SNAPSHOT_AGE_MS`, default 5 days, is a **snapshot**
knob — a different mechanism, and §40 Q7 showed it reclaims nothing.)

So the safe value depends on the **commit rate**, not on wall-clock time.
"Keep 5" is five hours at hourly commits, five days at daily ones, and
**five months at Sage's monthly cadence** — which is why the default of 100
is already far outside any window that could matter here.

### It can delete the file a published hint names (Q3/Q4)

The plans/6 failure mode, reproduced end to end. Reconciler publishes a
hint naming the current metadata; committers keep committing:

```
reconciler publishes hint -> 00001-8121e69d-....metadata.json
  exists at publish time:  True
  hint target DELETED after commit #7   (previous-versions-max = 5)
  consumer following the hint:  FileNotFoundError
```

**This is a different failure from the one plans/6 accepts.** The design's
accepted worst case is a reader seeing an *older* snapshot — stale, not
lossy, self-repairing on the next reconcile. This is a reader seeing
*nothing*, with a hard exception, until the reconciler republishes.

### The break point is exact, so the window is sizeable (Q5)

| `previous-versions-max` | hint survives | breaks at commit |
|---|---|---|
| 2 | 3 further commits | #4 |
| 5 | 6 further commits | #7 |
| 10 | 11 further commits | #12 |

Exactly `max + 2` every time. That turns the "comfortable margin" §40 asked
for into arithmetic:

> **previous-versions-max > commits per reconciler period, plus margin**

### At Sage's cadence the inequality is satisfied by the defaults

Sage commits **monthly** (InfluxDB hot store, one batch per month into cold
storage). Against twelve commits a year:

| cadence | margin at the default `previous-versions-max=100` |
|---|---|
| **monthly** | **~8.4 years** |
| daily | ~3.4 months |
| hourly | ~4 days |

A reconciler that republishes within minutes of a commit has eight years of
slack. **The hint-breakage failure cannot occur at this cadence** unless the
reconciler is dead for most of a decade, in which case the stale hint is the
smaller problem.

That is the inequality satisfied without tuning anything — and since the
growth it would bound is 6.4 MB per decade, there is no reason to turn
deletion on in the first place. The rows above exist so the result transfers
if the cadence ever changes: at 96/day, where retention would actually be
necessary (541 GB/yr unretained), the window must be sized against a
reconciler known to be running, which is the same reason plans/6 item 1
wants it promoted from a cron job to a service.

### What this produces

It **closes** §40's open question in both halves. The factual unknown:
retention exists, it works, nobody builds it. The apparent design decision:
it is not one at Sage's cadence — the defaults satisfy the safety
inequality by eight years, and the growth that retention would bound is
6.4 MB per decade.

Confirms action 6 of §40, with the reasoning rather than the caution:

6. **Leave `write.metadata.delete-after-commit.enabled` off.** Not as a
   provisional default pending a decision, but because at monthly commits
   the cost it avoids (6.4 MB/decade) is smaller than the failure mode it
   introduces (a hint that resolves to a deleted file). Pin both properties
   explicitly anyway — plans/6 already argues this, and an inherited
   default that later changes is the kind of thing that breaks quietly.

The result worth carrying forward is the **method**, not the setting: the
window is count-based, so any future cadence change makes this a live
question again, and `previous-versions-max > commits per reconciler
period + margin` is the form of the answer.
