# Chat archive — cold storage layout and query language

Handoff from a design discussion on 2026-10-08. The user is Francisco. The working plan is `plans/1_draft.md` (InfluxDB plus custom Parquet cold storage). `plans/2_draft.md` is a separate Iceberg plus NRDStor draft and was not edited. `plans/design_ideas.md` is an earlier, broader pipeline sketch (schemas, run-id, Hive layout by app) and was not edited.

Nothing here was committed.

## What the user is deciding

Keep recent observations in InfluxDB and move older ones to native ZSTD Parquet that Beehive can query in place. Users keep using `sage-data-client` and the portal. Both call `beehive-data-api`. The user does not want a separate query language for cold storage.

## Example archive on sage03

SSH host `sage03` (`flozano@165.124.33.231`, identity `~/.ssh/cels`).

Path: `/media/local/archive-export-test/archive`

```text
archive/
  date=2025-01-01/
    data_0.parquet
    data_1.parquet
  date=2025-07-21/
    data_0.parquet
    ...
    data_13.parquet
  date=2026-08-31/
    data_0.parquet
```

Measured on 2026-10-08:

| Fact | Value |
|---|---|
| Day directories | 608, `2025-01-01` through `2026-08-31`, no gaps |
| Files | 4,564, every one named `data_N.parquet` |
| Size | 208,584,864,752 bytes (about 194 GiB) |
| File size | min about 60 KB, median about 40.5 MiB, max about 111 MiB |
| Files per day | 1 on 258 days; other days mostly 10–15, up to 18 |
| Owner / mtime | `sean`, directories around 2026-10-03 |
| Non-parquet entries | none (no `_SUCCESS`, no Iceberg metadata) |

`date=2025-07-21` is 14 files, about 620 MiB. `data_0.parquet` there is 100,728,595 bytes with a 3,598-byte footer. `date=2026-08-31` is one 18 MB file.

Sibling paths under `/media/local/archive-export-test/`:

- `aqt.parquet`, `avian.parquet`, `licor.parquet` — derived products
- `iio-data/` — same `date=` layout, narrower data
- `stats.txt`, `show-archive-stats` — DuckDB scans of `archive/**/*.parquet`
- `sage-data-rework/` — a checkout of this repo

`stats.txt` groups the full archive by plugin. `registry.sagecontinuum.org/jrobrien/waggle-wxt536:0.24.11.14` is 30,098,134,217 rows and 90.52%. That implies on the order of 33 billion rows. The same file lists 251 plugins. DuckDB was not on `PATH` when this was checked; `stats.txt` is the evidence for those counts. `pyarrow` was not installed. `/usr/local/go/bin/go` works. A throwaway reader lives at `/tmp/meta-sample` on sage03.

## How the files are written

`archive-exporter/main.go` is the writer that matches this tree.

- One UTC day per export: `[date T00:00:00Z, next day)`.
- Reads Influx via `influxd inspect export-lp` for bucket `b3a4e89ad74c5acc`.
- Batches of up to 10,000,000 observations.
- Each batch is sorted by `plugin`, `vsn`, `time`, written as `work/date=YYYY-MM-DD/data_N.parquet`, then the day directory is renamed into `archive/`.
- If `archive/date=...` already exists, the day is skipped. A published day is immutable. Late data has nowhere to go.
- Compression is ZSTD (`zstd.SpeedBetterCompression`) via `parquet-go` v0.32.0. A sampled footer contained `exported_at=2026-10-04T16:30:44Z`.
- `SortingWriterConfig` is set. `MaxRowsPerRowGroup` is not. The small footer on a 100 MB file is one row group, so row-group min/max cover the whole file. Page indexes are what can skip a `plugin` slice inside the file, because `plugin` is the leading sort key.

Schema (`Observation` in `archive-exporter/main.go`):

| Column | Role |
|---|---|
| `time` | nanosecond timestamp, delta encoded |
| `name` | measurement name, dictionary |
| `plugin`, `vsn`, `node`, `host`, `task`, `job`, `zone` | promoted tags, optional, dictionary. Missing tag is NULL, not `""` |
| `meta` | `map[string]string` of every other tag |
| `value_float`, `value_int`, `value_str` | exactly one is set |

`date` is only the directory name. DuckDB hive partitioning exposes it as a column when querying `archive/**/*.parquet`.

Promoted tags are removed from `meta`. The public API still returns them inside `meta`, so a cold backend has to put columns and the map back together.

## Conclusions already reached

### This tree fits `plans/1_draft.md` as the daily tier

It is native ZSTD Parquet, one UTC `[start, end)` interval per directory, several files on a busy day. That is the "recent archive" stage. Monthly, yearly, and historical "all" are later rewrites of these files, tracked by the catalog. The measurement/year tree in section 4 is illustrative.

The directory is not a catalog. Compaction that replaces daily files is unsafe until a published manifest says which files are authoritative. The exporter's "skip if the day exists" rule blocks late data; section 9 still needs a correction file or a replacement partition.

### Folder versus column

A directory decides which files open. A column lets page statistics skip inside a file.

`date` earns a folder: every query has a time range, each day is large, one value per directory.

`plugin` should stay a column and the leading sort key for the first layout:

- A query for one plugin still opens the day's files and reads their footers, then can skip other plugins' pages because of the sort.
- One plugin is about 90% of rows, so a directory for it is almost as big as the day.
- 251 plugins, some with a handful of rows. A directory per plugin version creates tiny files (footer, weak compression).
- The string includes the image version, and slashes must be encoded (`plugin.replace("/", "%2F")` in `process-lp-files.py`).
- Queries that do not filter on plugin would open every plugin directory instead of 1–18 files per day.
- Dictionary encoding means dropping `plugin` out of the file saves almost no space.

A measurement that is 1% of rows is not slower than a large one over the same dates. Both open the same files. Time follows the days requested, not the measurement's share of rows. Names are interleaved in time inside a plugin, so a `name` filter still walks that plugin's rows. A `plugin` filter can skip.

If Phase 2 shows small-plugin queries are dominated by opening shared daily files, the next step is a coarse split: one directory only for plugins that fill a file, rare plugins kept together. Measure bytes read and files opened first.

`name`, `vsn`, `node`, `host`, `task`, `job`, and `zone` stay columns. Too many distinct values for a directory per day.

### One file of Parquet footers

Yes, for finding candidate files. Each footer is a few KB (schema, row count, column stats). About 4,564 of them is on the order of 10–20 MB. The cost is one open per file. On local disk that is minor next to reading a day. Through Pelican each footer is an HTTP range request, and a year of this archive is a couple of thousand requests. One manifest replaces that fan-out.

`date=` already avoids opening other days. The manifest helps inside the days that remain.

It does not shrink the data read inside a chosen file. One row group means footer min/max for `plugin` spans every plugin in the batch. Skipping to one plugin still uses page indexes in that data file.

The useful form is the catalog manifest in section 7 (path, size, row count, stats that can reject a file), published with the catalog version. A raw concatenation of footers is the same idea with coarser stats. Iceberg manifests are this mechanism. The plan says to compare a small custom manifest with Iceberg before committing.

### Query language

Keep the existing JSON query. Do not make `sage-data-client` or the portal speak SQL.

Public API, documented in [Querying Measurements](https://github.com/waggle-sensor/waggle-beehive-v2/blob/main/docs/querying-measurements.md). The beehive-data-api README points there. Implementation: `https://github.com/waggle-sensor/beehive-data-api` (`types.go`, `influx_backend.go`). Client: [sage-data-client on PyPI](https://pypi.org/project/sage-data-client/) and [sagecontinuum/sage-data-client](https://github.com/sagecontinuum/sage-data-client). Portal: `https://portal.sagecontinuum.org/query-browser`. HTTP endpoint used in the docs: `POST https://data.sagecontinuum.org/api/v1/query`.

```json
{
  "start": "2025-07-21T00:00:00Z",
  "end": "2025-07-22T00:00:00Z",
  "tail": 1,
  "filter": {
    "name": "env.temperature",
    "vsn": "W019"
  }
}
```

- `start` / `end`: `YYYY-MM-DDTHH:MM:SSZ` or relative `±nh` / `±nm` / `±ns`.
- `filter`: map of field to pattern. Exact match, or regex when the pattern contains `*` or `|`. In `influx_backend.go`, `*` is embedded in `/^pattern$/` (so `env.*` is a regex, not SQL `LIKE`). `|` becomes `/^(a|b)$/`. Allowed characters are `[A-Za-z0-9+-_.*:| ]`.
- `name` is renamed to Influx `_measurement`. Other filter keys are tags.
- `head` / `tail`: earliest or latest N records per series (unique name plus metadata), not N for the whole query. Implemented as Flux `limit` / `tail`, which apply per table.
- Response: NDJSON `{timestamp, name, value, meta}`. Order is by time within a series. Global order across series is not promised.
- `experimental_func` (`mean`, `min`, `max`, `sum`, `count`) and `experimental_window` exist on the Go `Query` struct. The Python client's documented arguments are `start`, `end`, `head`, `tail`, `filter`.

The hot backend compiles this to Flux. The cold backend can compile the same object to SQL. `value_float` / `value_int` / `value_str` fold back into one `value`. A filter on a promoted tag hits that column. A filter on any other key hits `meta`. `bme680-example/get_data.py` filters `name=env.*`, `plugin=waggle/plugin-iio:.*`, `sensor=bme680`, `zone=shield`. `zone` is a column. `sensor` is inside `meta` today.

SQL stays appropriate for derived products (`archive-exporter/data-product-examples.md`, `bme680-example/run-example-queries`). That is a different audience.

GitHub MCP could not read `waggle-sensor` repos (fine-grained token lifetime rejected by the org). Raw GitHub URLs worked.

### `meta` map

Right as a catch-all for unknown tags. Weak as the place users filter.

Sample of `date=2025-07-21/data_0.parquet` only (first 10 million rows of that day, so the plugin slice at the start of the sort, not the whole archive):

- 9,498,597 of 10,000,000 rows have a non-empty map.
- Map sizes vary: many rows have 1 key, a large share have 14–17, some have 24–27.
- 40 distinct keys in that file.
- `sensor` is on 8,702,033 rows. Values seen included `bme280`, `bme680`, `es642`, `vaisala-wxt536`, `vaisala-aqt530`, `vaisala_cl61`, `smartflux`, `LI7500DS/uSonic-3`. This is the key to promote first.
- `units`, `missing` (`-9999.0`, `-9999.9`), and `description` are on 5,808,893 rows. Payload, not the first filter keys.
- LoRaWAN-style keys (`deviceName`, `devEui`, `applicationId`, `devAddr`, and others) are on about 3.4 million rows.
- Segmentation / image keys (`nsegments_found`, `filename`, `input=top_camera`, and others) are on about 2.2 million rows. `filename` is high cardinality.
- A meta key literally named `name` is on 2,108,419 rows (values like `co2.mole_fraction`). The API `name` filter is the measurement column, not this key.
- `site` has a handful of values (`ATMOS`, `CCICS`, `NEIU`, `UIC`) and is a plausible later promotion. `creator` was `CROCUS MST` in the sample.

Promote a key when it is common and people filter on it. Decide from Beehive filter logs plus archive frequency. Leave descriptions, filenames, hashes, and one-off plugin tags in the map so the schema stays fixed. The writer does not dictionary-encode map keys or values (`parquet:"meta,optional"` only), so repeated keys are stored as plain strings inside the repeated group.

## Edits already in `plans/1_draft.md`

Sections after the insert were renumbered.

- **Section 5, Folder partitioning vs a Parquet column.** Sage03 layout, why `date` is a folder, why `plugin` stays the leading sort column, the 1% measurement case, and a table of where each field goes. Phase 2 is the checkpoint before any coarser plugin split.
- **Section 7, Collected Parquet footers.** Manifest carries footer facts so planning is one read. Matching rows still come from data files. Page indexes stay in the data files. Iceberg comparison stays in this section. The file-manifest row of the catalog table now mentions footer statistics.
- **Phase 2** now includes measuring time spent opening footers against time spent reading one manifest.

Sections 6, 8, 9, 10, and the first milestone were only renumbered (compression, routing, late data, roadmap, milestone).

The query-language conclusion and the `meta` opinion are not in the plan yet.

## Open points

- Phase 2 numbers are not measured yet: bytes read and files opened for a short query and a year-long query, with and without a plugin filter; footer-open time versus one manifest, local disk and Pelican.
- Whether `sensor` (and later `site`) get promoted. Needs query logs, not just this one file.
- `head` / `tail` per series on Parquet, including series identity when some tags live in a map.
- Regex filter semantics must match `influx_backend.go`, not SQL `LIKE`.
- Late data after a day directory is published.
- Custom manifest versus Iceberg is explicitly undecided.
- `plans/2_draft.md` assumes Iceberg on NRDStor via Pelican. `plans/1_draft.md` keeps that as an evaluation inside the catalog, with the sage03 tree as the file layout.
