# Sage Data Pipeline v2

## Summary

SAGE’s current path stores observations in InfluxDB and serves them through the Beehive Data API, and that path is struggling with volume, cardinality, high-frequency data, and long-term retention. We have been exploring how a later pipeline might keep recent data queryable, move older observations into compressed Parquet, and still answer through Beehive.

Nothing here is a plan we have committed to. The notes in this repo are design sketches and measurements. The point of the ticket is to keep that research going until we know which ideas hold up.

## Why we are looking at this

- InfluxDB is not keeping up with the volume and cardinality SAGE collects, and there is no straightforward fix inside InfluxDB.
- High-frequency collection, such as CROCUS WXT and AQT measurements, is painful to ingest and to query. Those series also add little to the portal’s simple charts.
- InfluxDB has no first-class support for rich or tabular values, so users split related fields to store them and join them again to use them.
- There is no durable long-term archive. Historical observations need to stay queryable.
- Published data is hard to discover. There is no tracked record of which app publishes which kind of data.
- Permissions are difficult to enforce on a shared time-series namespace.
- There is no clean way to share a run’s time series for rollups, summaries, or captions.

## What we want to learn

- Can a shorter InfluxDB retention window, plus moving system metrics and high-frequency series off InfluxDB, make the current pipeline viable for the next few years?
- Can older observations live as native Parquet and still answer Beehive queries with the same logical results?
- What layout, compression, and file size make short and long historical queries cheap?
- Who should own the catalog of archived files: a small manifest we maintain, or something like Apache Iceberg?
- Can Pelican and NRDStor, which we already use for object upload, also serve selective Parquet reads?
- If the archive looks promising, what would a fuller publish path need: schemas, a latest-state view, a separate events path, and a run-scoped layout?

## Ideas under exploration

These are separate lines of research. They can inform each other. Adopting one does not require adopting the others.

### Relieve pressure without replacing the pipeline

An intermediate idea is to keep InfluxDB and change what we send it:

- Stand up a copy of InfluxDB and see whether a shorter retention window, on the order of six months, is enough.
- Redirect system metrics to Prometheus / Grafana. They are one of the largest sources and are mostly used as an instantaneous portal view. One metric, such as `sys.uptime`, might stay behind as a check that the Influx path and WES are alive. GPS publishing needs a look before anything moves.
- Stop sending high-frequency series to InfluxDB.
- If those two changes are not enough, estimate what it would take to reduce cardinality in publishing apps. Tags cannot simply be unindexed while keeping the records Beehive serves.

Alongside that, archive a subset of the measurements we care about. Back-define schemas, ETL them to Parquet, and put the files on Pelican. Run identity is missing in places, so grouping by run would be approximate. Bhupendra’s CROCUS WXT and AQT archive is the precedent: the same data is much faster to query once it is laid out this way. A handful of per-app archivers would be an acceptable research outcome. Existing apps would keep publishing, and the sample would show the variety of data shapes we actually have, including whether a directory layout can carry permissions.

### A custom cold archive beside InfluxDB

One sketch keeps recent data in InfluxDB and writes older observations to native Parquet with ZSTD. A catalog records which files cover which time interval. Beehive would route a query to InfluxDB, to Parquet, or to both, and merge the results. Users would keep using Beehive.

Ideas in that sketch, still unproven:

- One authoritative copy per time interval. A compacted replacement becomes that copy only after it is verified and published. Superseded files go away after active readers finish.
- “All” is a logical archive split across files, so a short historical query reads the files that can match.
- Daily, monthly, and yearly are possible lifecycle stages, not separate products and not mandatory file sizes. Compaction would consider age, file count, file size, and query cost.
- A query pins a catalog version for its duration, so compaction can publish a replacement without changing a query already in flight.
- The manifest carries Parquet footer facts (path, size, row count, column statistics). Planning then reads one manifest. Matching rows still come from the data files.
- Archival of new data is separate from backfill. Late observations for an interval already archived would land as a small correction or a replacement partition.
- InfluxDB retention would move only after an interval is verified in the archive.
- ZIP or GZIP wrapping of Parquet is a poor fit for selective reads, because it hides the file structure the reader uses to skip data. A deep archive that must be restored before it can be queried is a later question, only if some old periods turn out to be rarely read.

The open design question in this sketch is the catalog. Snapshots, planning from statistics, and safe file rewrite are real work to own. Apache Iceberg already does those jobs, which is why the next sketch exists.

### Iceberg on the Pelican path we already have

A second sketch asks whether Iceberg should be the historical table format, with InfluxDB 3 as a possible hot store and NRDStor as the physical archive. Pelican would be the access path, in the same family as `sage-storage-loader`: produce a file, PUT it with a bearer token, and treat NRDStor as the authoritative copy. Local files would be buffers during a write.

```text
SAGE ingestion
      |
      v
 hot time-series store     recent observations
      |
 archive of settled intervals
      |
      v
 historical table            snapshots, manifests, Parquet
      |
 Pelican                     authenticated HTTP
      |
      v
 NRDStor                     cold copy

User query → Beehive → planner → hot store, historical table, or both → merge
```

This is attractive if a custom catalog gets too complicated to build and operate. It is not a decision. Iceberg would own the table tree, and cleanup would go through snapshot expiration and file rewrites. Whether Pelican can be an Iceberg `FileIO` is unknown. The loader already does authenticated PUT. Iceberg also wants authenticated GET, HTTP range reads, existence checks, DELETE, defined overwrite behavior, concurrent readers and writers, visibility immediately after PUT, predictable behavior on a failed PUT, and tolerable performance when planning touches many small metadata files. Range reads matter because a historical query should fetch the Parquet pages it needs.

Related ideas in that sketch: one logical table rather than daily, monthly, and yearly tables; a configurable hot window (7, 14, and 30 days are the candidates to measure); a settling window so late data can arrive before an interval is archived; a checkpoint outside the table; half-open intervals so a mixed query has no gap and no duplicate; aggregations combined from sufficient statistics, so a mean is `(sum_cold + sum_hot) / (count_cold + count_hot)`.

### A fuller publish path, if the archive looks promising

A broader sketch tries to address discovery, validation, sharing, and permissions together. It is further out than the storage experiments.

- Each published dataset would have a schema tracked with the app repo and container image. Tooling would generate a first schema (names and types) when a new dataset appears; the author would edit and confirm it, including ranges where the science needs them.
- The collector would batch Parquet from that schema for bulk archive, and keep a latest-state table for the portal, synced on a controlled interval. High-frequency tabular series could move in bulk while the portal stays current. The node might keep the same split locally.
- Urgent signals might need a separate events path so batching latency does not delay them. A first version could be free-form logs.
- Data would be isolated per run:

```text
/run-id=/app-id=/data-schema-id=/node-id=/date=/…parquet
```

A run id would be one scheduled run of apps across nodes, so a whole run could be shared and a sensitive app could stay hidden inside it. The identity of a dataset would be `app-id` + `data-schema-id`, which would let measurement names stay short (`temp`, `pressure`, `rh`).

## What we have learned so far

The export on sage03 (`/media/local/archive-export-test/archive`) is the main evidence. It is one `date=YYYY-MM-DD` directory per day, one or more `data_N.parquet` files per day, native ZSTD: 608 continuous days (2025-01-01 through 2026-08-31), 4,564 files, about 194 GiB. Each file is one batch of up to 10 million rows, one row group, sorted by `plugin`, `vsn`, then `time`.

On this dataset, a `date` directory matches how queries are asked. A three-day query can open those three directories and leave the rest of the archive alone.

A directory per `plugin` looks expensive here:

- `waggle-wxt536` is about 90% of the rows, so its partition would be almost the whole day.
- 251 plugins appear in a full scan, and some contribute a handful of rows. Each would become its own tiny Parquet file.
- The plugin string includes the image version, and slashes in the name have to be encoded.
- A query that does not filter on plugin would open every plugin directory for the requested days.

Page statistics can already isolate one plugin when it is the leading sort key. Row-group statistics cannot, because each file is a single row group whose min/max cover every plugin in the batch. `name`, `vsn`, `node`, `host`, `task`, `job`, and `zone` look like columns: useful filters, too many distinct values for a directory per day. Leaving `plugin` in the file costs almost no space, because it is dictionary-encoded.

A year of this archive is a couple of thousand files and on the order of 10–20 MB of footers. Opening each footer is cheap on local disk. Through Pelican, each footer would be an HTTP range request, which is the argument for copying those statistics into a manifest.

These measurements say what is worth trying next. They do not freeze a partition scheme. If small-plugin queries turn out to spend their time opening shared daily files, a coarse split is the experiment to run: one directory for plugins that fill a file on their own, and one shared directory for the rest. Compare bytes read and files opened for a short query and a year-long query, with and without a plugin filter.

## Experiments worth running

Each of these is a way to learn something. The order is a guess about what is most uncertain, not a commitment.

| Experiment | What it would tell us |
| --- | --- |
| InfluxDB copy with a shorter retention window | Whether retention alone relieves the current cluster |
| Move system metrics, then stop high-frequency publishers | How much of the load those two sources are |
| Subset of important measurements exported to Parquet on Pelican | Which data shapes we actually have, and whether directory layout can express access control |
| Pelican GET, range GET, existence, DELETE, overwrite, and failed PUT | Whether NRDStor can back selective Parquet reads, not only whole-object upload |
| Tiny Iceberg table whose files live only on NRDStor | Whether Iceberg can reopen, append, and query through Pelican after a restart |
| InfluxDB 3 Iceberg plugin, or a PyIceberg `PelicanFileIO` | Whether an existing plugin fits this storage path |
| One day or one week archived, then queried through Beehive | Whether logical results match InfluxDB, including a retry after a forced failure |
| Compact a month of daily files and query before and after | Whether replacement and pruning can preserve results |
| Hot-only, cold-only, and mixed Beehive queries | Whether ordering, limits, head/tail, and aggregations survive a split |
| Bounded backfill of existing Influx history | What verification has to check before anything is eligible to age out |

Failure behavior is part of the archival experiments: Pelican PUT failure, JWT expiry, partial upload, manifest or commit failure, checkpoint failure after a successful commit, process restarts, duplicate or overlapping windows, late observations, and Pelican or NRDStor being unavailable. A useful result is that a failed attempt leaves the archive frontier unchanged unless the published snapshot actually contains the interval.

Verification ideas to try, not a required checklist: record count, timestamp min and max, measurements, VSNs, field types, metadata, then a few real Beehive queries against the source.

## Still open

- How long do late SAGE observations actually take to arrive?
- Which hot window and Parquet file size are worth measuring (7, 14, 30 days; 128, 256, 512 MiB are only candidates)?
- For a subset archive, which measurements belong in the first schema set, and how close can run grouping get without run metadata?
- Does a Hive-style run layout carry permissions, or does access control need something beside the directories?
- When would an events path need to be more than free-form logs?
- At what point is a custom catalog more work than Iceberg on Pelican?

## References

- [Design Ideas (Google doc)](https://docs.google.com/document/d/12mEnUVbIljYFRKvtaFD3mwIdteCTlygcrcU403D3aHs/edit?usp=sharing) - doc explaining design ideas and issues. A good place to start off.
- [Sage-Data-Rework](https://github.com/seanshahkarami/sage-data-rework) - Research work supporting Sage Data Pipeline v2. Also includes design ideas and draft plans. A good place to start off.
- [beehive-data-api](https://github.com/waggle-sensor/beehive-data-api) - Our query interface.
- [sage-storage-loader](https://github.com/waggle-sensor/sage-storage-loader) — current Pelican PUT path that can be used to help complete the ticket.
- [sage-storage-uploader deployment files](https://github.com/waggle-sensor/honeyhouse-config/tree/main/applications/beehive-sage/sage-storage-uploader) - files deploying sage-storage-loader
- [InfluxDB 3 to Iceberg plugin](https://docs.influxdata.com/influxdb3/enterprise/plugins/library/official/influxdb-to-iceberg) - If our custom cold storage gets too complicated we can explore this
- [Iceberg FileIO](https://iceberg.apache.org/docs/latest/fileio)- If our custom cold storage gets too complicated we can explore this
- [Pelican](https://pelicanplatform.org/) with [NRDStor](https://hcc.unl.edu/nrdstor): Our current object storage
