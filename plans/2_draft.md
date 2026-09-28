# SAGE Time-Series Storage Design Plan — InfluxDB 3 + Apache Iceberg

## 1. Objective

Move SAGE toward tiered time-series storage where:

- **InfluxDB 3** holds recent/hot observations.
- **Apache Iceberg** manages historical/cold observations.
- **NRDStor** remains the physical long-term storage system.
- **Pelican** provides authenticated access to NRDStor, following the same general storage pathway already used by `sage-storage-loader`.
- **Beehive Data API** remains the single query interface.
- Users do not need to know whether results came from InfluxDB or Iceberg.
- Historical data remains directly queryable; no restore/re-hydration step is required.

The central architecture becomes:

```text
InfluxDB 3 = hot time-series database

Iceberg    = historical table format / metadata

NRDStor    = physical cold storage

Pelican    = access path to NRDStor

Beehive    = unified query layer
```

---

# 2. High-level architecture

```text
                         SAGE DATA
                             |
                             v
                       Data Ingestion
                             |
                             v
                  +----------------------+
                  |      InfluxDB 3      |
                  |                      |
                  |     HOT STORAGE      |
                  |   recent ~7-30 days  |
                  +----------+-----------+
                             |
                             | archive eligible
                             v
                  +----------------------+
                  | InfluxDB -> Iceberg  |
                  |       process        |
                  +----------+-----------+
                             |
                             v
                       Apache Iceberg
                             |
                       Pelican FileIO
                             |
                  authenticated HTTP
                             |
                             v
                         Pelican
                             |
                             v
                         NRDStor
```

NRDStor remains the authoritative physical archive.

---

# 3. Existing SAGE storage pattern

The current [`sage-storage-loader` repository](https://github.com/waggle-sensor/sage-storage-loader) already establishes the basic storage pattern we want to preserve.

Today:

```text
Node
 |
 | rsync
 v
Beehive staging directory
 |
 v
sage-storage-loader
 |
 | authenticated HTTP PUT
 v
Pelican
 |
 v
NRDStor
```

The Pelican uploader currently performs approximately:

```text
PUT
https://nrdstor.../sage/<bucket>/<object>

Authorization: Bearer <JWT>
```

After a successful upload, the local staged file can be deleted.

For time-series storage, we want to preserve that fundamental idea:

```text
Produce file
     ↓
send through Pelican
     ↓
NRDStor becomes authoritative
     ↓
temporary local copy disappears
```

There is no need to maintain a second permanent archive locally.

---

# 4. Iceberg storage model

Unlike an image archive, an Iceberg table isn't merely a collection of independent files.

Iceberg manages:

```text
Iceberg table
│
├── metadata
│   ├── table metadata
│   ├── snapshots
│   ├── manifest lists
│   └── manifests
│
└── data
    ├── *.parquet
    ├── *.parquet
    └── ...
```

So on NRDStor we could logically have:

```text
/sage/
    ...
    timeseries/
        observations/
            metadata/
                ...
            data/
                ...
```

Iceberg is the authority over everything beneath the table.

External SAGE processes should **not independently rename, reorganize, or delete these files**.

---

# 5. Pelican FileIO

This becomes one of the most important pieces of the POC.

Instead of:

```text
PyIceberg
   |
S3FileIO
   |
S3
```

we want to investigate:

```text
PyIceberg
   |
PelicanFileIO
   |
Pelican
   |
NRDStor
```

The existing `sage-storage-loader` already proves the write concept:

```text
local file
   |
HTTP PUT + JWT
   |
Pelican
```

But Iceberg needs a richer interface than the current `UploadFile()` abstraction.

Conceptually:

```text
PelicanFileIO

WRITE
  ├── create metadata
  ├── create manifests
  └── create Parquet files

READ
  ├── metadata
  ├── manifests
  └── Parquet

RANGE READ
  └── portions of Parquet files

DELETE
  ├── expired metadata
  └── superseded data files

EXISTS / metadata operations
```

Iceberg's `FileIO` abstraction is specifically designed to separate the table format from the underlying storage implementation. 

The first POC therefore needs to determine whether Pelican provides all of the semantics required for an efficient Iceberg `FileIO`.

---

# 6. Write path

The desired archival path becomes:

```text
                      InfluxDB 3
                           |
                    archive window
                           |
                           v
               InfluxDB -> Iceberg
                      plugin/process
                           |
                           v
                       PyIceberg
                           |
                           v
                    PelicanFileIO
                           |
                     HTTP + JWT
                           |
                           v
                       Pelican
                           |
                           v
                       NRDStor
```

There may be temporary files/buffers while Parquet files are constructed, but these are **not another storage tier**.

Once committed and verified:

```text
NRDStor = authoritative historical copy
```

---

# 7. Hot/cold lifecycle

Initially, I'd keep the lifecycle simple:

| Data age | Authoritative query source |
|---|---|
| Recent | InfluxDB 3 |
| Older | Iceberg / NRDStor |

For example:

```text
Today
 |
 |<--------- 30 days --------->|
 |
 +-----------------------------+--------------------------->
              HOT                         COLD
          InfluxDB 3              Iceberg / NRDStor
```

The exact boundary should be configurable.

Start by benchmarking:

```text
7 days
14 days
30 days
```

rather than hard-coding 30 days into the architecture.

---

# 8. Continuous archival

Archiving should operate on completed time intervals.

For example:

```text
archived through 12:00
        |
        v

archive [12:00, 13:00)
        |
        v
write Iceberg data
        |
        v
commit Iceberg snapshot
        |
        v
verify
        |
        v
advance checkpoint → 13:00
```

Maintain a small operational checkpoint outside of Iceberg:

```text
source_start
source_end
status
record_count
iceberg_snapshot
completed_at
```

Possible state machine:

```text
PENDING
   |
   v
EXPORTING
   |
   v
WRITTEN
   |
   v
VERIFYING
  / \
 /   \
FAIL  VERIFIED
 |       |
retry    v
     SAFE_FOR_RETENTION
```

InfluxDB retention must **never outrun the verified archive frontier**.

---

# 9. Settling window

We should not archive right up to the current timestamp.

For example:

```text
                 now
                  |
                  v

HOT =======================================>

             settling
              window
          <------------>

COLD ====================|
                         ^
                 archive frontier
```

If we choose a three-day settling window:

```text
September 28
      |
      v

safe archive frontier = September 25
```

This allows delayed observations to arrive before an interval becomes cold.

The actual window should be determined from SAGE ingestion behavior.

---

# 10. InfluxDB retention

Retention occurs only after an interval is confirmed in Iceberg.

For example:

```text
                  InfluxDB

<----- archived ------><-- safety --><--- active --->

                       ^
                retention frontier


                  Iceberg

<---------------- archived + verified --------------->
```

This gives us overlap rather than creating a dangerous hard boundary.

Example only:

```text
Influx retention:       30 days
Archive data older than: 24 days
Safety overlap:           6 days
```

We should choose the actual values after measuring archive reliability and late-arriving observations.

---

# 11. Iceberg table structure

I would continue with **one logical historical table**, not daily/monthly/yearly tables.

Something conceptually like:

```text
sage_observations
```

with fields representing the canonical SAGE observation:

```text
timestamp
measurement
value
vsn
plugin
host
metadata
...
```

The exact schema should come from the existing observation/query semantics rather than from the BME680 wide-table experiment.

We want the historical table to preserve the original observation semantics losslessly.

---

# 12. Partitioning

Start conservatively.

Candidate:

```text
measurement + time
```

or potentially:

```text
time
```

depending on real query patterns.

Avoid:

```text
measurement / VSN / plugin / year / month / day / ...
```

until benchmarks prove we need it.

Iceberg already gives us multiple pruning layers:

```text
Query
  |
  v
Iceberg partition pruning
  |
  v
manifest/file statistics
  |
  v
Parquet row-group statistics
  |
  v
actual rows
```

Sorting data within Parquet files by something like:

```text
measurement
VSN
timestamp
```

may ultimately be more useful than aggressively partitioning by high-cardinality fields.

---

# 13. Compaction

The original idea that older data should become more consolidated still makes sense.

But instead of manually creating:

```text
daily archive
monthly archive
yearly archive
all archive
```

Iceberg keeps one logical table:

```text
                 sage_observations
                        |
        +---------------+---------------+
        |               |               |
      recent          older           old
       files           files          files
        |               |               |
     smaller        compacted       stable
```

Maintenance jobs can rewrite many small files into fewer larger files.

Target file sizes should be benchmarked, perhaps:

```text
128 MiB
256 MiB
512 MiB
```

rather than chosen upfront.

---

# 14. Query architecture

Beehive remains the only query API.

```text
                         USER
                           |
                           v
                  +------------------+
                  |     Beehive      |
                  |    Data API      |
                  +--------+---------+
                           |
                     Query Planner
                           |
              +------------+------------+
              |                         |
              v                         v
        Influx Backend            Iceberg Backend
              |                         |
              v                         v
         InfluxDB 3                 Iceberg
             HOT                       |
                                       v
                                Pelican FileIO
                                       |
                                       v
                                    Pelican
                                       |
                                       v
                                    NRDStor
```

The existing Beehive backend abstraction remains the integration point.

---

# 15. Cold-only queries

For:

```text
January 1, 2025
       →
January 3, 2025
```

Beehive determines that the entire interval is cold:

```text
Beehive
   |
   v
Iceberg Backend
   |
   v
Iceberg metadata
   |
   v
determine relevant Parquet files
   |
   v
Pelican
   |
   v
NRDStor
```

Iceberg should prevent Beehive from blindly scanning the entire archive.

---

# 16. Hot-only queries

Recent query:

```text
last 2 hours
```

becomes:

```text
Beehive
   |
   v
Influx Backend
   |
   v
InfluxDB 3
```

Iceberg isn't touched.

---

# 17. Mixed queries

Suppose hot retention is 30 days and the user requests 90 days.

```text
                        90 days

       COLD                            HOT
<-------------------->|<------------------------->
                      ^
                hot/cold boundary
```

Beehive splits it:

```text
                         Beehive
                            |
                     Query Planner
                      /           \
                     /             \
                    v               v
             Iceberg Backend   Influx Backend
                    |               |
                 Pelican        InfluxDB 3
                    |
                 NRDStor
                    |
                    +-------+-------+
                            |
                            v
                      Merge Results
                            |
                            v
                          User
```

Use half-open intervals:

```text
Iceberg: [start, boundary)

Influx:  [boundary, end)
```

to prevent gaps and duplicate records.

---

# 18. Beehive semantics

The Iceberg backend needs to preserve the behavior users already expect from Beehive:

```text
timestamps
values
metadata
filters
ordering
limits
head / tail
grouping
wildcards
aggregations
windowing
```

Mixed queries need special care for operations such as:

```text
mean
min
max
count
sum
```

For example, don't do:

```text
mean(
    mean(cold),
    mean(hot)
)
```

Instead combine sufficient statistics:

```text
cold: sum=X count=A
hot:  sum=Y count=B

mean = (X + Y) / (A + B)
```

---

# 19. Historical backfill

Existing Influx history can be migrated through bounded backfill jobs.

```text
                 Existing InfluxDB
                        |
                        v
                 historical ranges
                        |
              +---------+---------+
              |         |         |
             Jan       Feb       Mar
              |         |         |
              v         v         v
                   Iceberg
                      |
                PelicanFileIO
                      |
                   Pelican
                      |
                   NRDStor
```

For each interval:

```text
export
  ↓
write
  ↓
Iceberg commit
  ↓
verify
  ↓
record checkpoint
```

Only after verification should the corresponding Influx data become eligible for deletion/retention.

---

# 20. Verification

Before marking an archive interval safe, compare source and destination.

At minimum:

```text
record count
timestamp min/max
measurements
VSNs
field types
metadata
```

Then run representative Beehive queries:

```text
Influx result
      vs
Iceberg result
```

The logical observations should match.

---

# 21. Failure handling

We specifically need to test:

```text
Pelican PUT failure

JWT expiration

partial Parquet upload

manifest upload failure

Iceberg commit failure

commit succeeds but checkpoint update fails

archive process restart

InfluxDB restart

duplicate archival attempt

overlapping archival windows

late observations

Pelican/NRDStor unavailable
```

A failure must **never advance the archive frontier** unless the corresponding Iceberg snapshot is known to contain the interval.

---

# 22. Pelican requirements to validate

This is now an explicit early milestone.

We already know:

```text
✓ authenticated HTTP PUT
```

because `sage-storage-loader` uses it today.

We need to validate:

```text
? authenticated GET

? HTTP range reads

? object existence/stat

? DELETE

? overwrite semantics

? concurrent reads/writes

? visibility immediately after PUT

? behavior during failed/interrupted PUT

? performance for many metadata files
```

Range reads are particularly important because we don't want historical queries downloading entire Parquet objects when only part of the file is needed.

---

# 23. Implementation roadmap

I would reorder our phases now.

**Phase 1 — Pelican storage experiment.** Take the existing `sage-storage-loader` authentication approach and test PUT, GET, range GET, existence, DELETE, overwrite behavior, and failure recovery against the SAGE NRDStor namespace.

**Phase 2 — Iceberg on Pelican POC.** Create a tiny Iceberg table whose authoritative files live in NRDStor through Pelican. Write Parquet, metadata, manifests and snapshots; then close the writer, restart it, reopen the table entirely from Pelican, append more data and query it.

**Phase 3 — InfluxDB 3 → Iceberg.** Test the official InfluxDB 3 Iceberg plugin against that storage path. Determine whether a custom PyIceberg `FileIO` can be supplied cleanly or whether the plugin needs a small adaptation. [InfluxDB to Iceberg plugin documentation](https://docs.influxdata.com/influxdb3/enterprise/plugins/library/official/influxdb-to-iceberg)

**Phase 4 — Archive correctness.** Implement interval/checkpoint tracking, settling window, verification, retries and failure recovery.

**Phase 5 — Beehive Iceberg backend.** Add an Iceberg-backed implementation to Beehive's existing backend architecture and initially support bounded raw historical queries.

**Phase 6 — Query federation.** Add hot-only, cold-only and mixed query planning, including correct ordering, limits, head/tail and aggregation behavior.

**Phase 7 — Historical backfill.** Migrate existing Influx history into Iceberg on NRDStor in bounded, verified intervals.

**Phase 8 — Enable Influx retention.** Only once archival and Beehive historical querying are proven should we reduce Influx retention.

**Phase 9 — Optimize.** Tune Iceberg partitioning, sort order, Parquet sizes, compaction, snapshot expiration and metadata cleanup based on actual SAGE workloads.

---

# 24. First end-to-end experiment

I think the most valuable POC is now very concrete:

```text
             Representative SAGE data
                       |
                       v
                   InfluxDB 3
                       |
                       v
             Influx -> Iceberg
                       |
                       v
                  PyIceberg
                       |
                       v
                Pelican FileIO
                       |
                       v
                    Pelican
                       |
                       v
                    NRDStor
```

Then:

```text
Beehive
   |
Iceberg Backend
   |
Pelican
   |
NRDStor
   |
same archived observations
```

Use perhaps one day or one week of representative SAGE observations rather than immediately migrating a month.

The test succeeds if we can:

1. Archive from Influx into Iceberg through Pelican.
2. Remove the temporary/local files.
3. Restart the Iceberg process.
4. Reopen the table using only the authoritative NRDStor/Pelican copy.
5. Query historical observations.
6. Retrieve only necessary portions of Parquet objects rather than entire archives.
7. Get equivalent logical results from Beehive.
8. Append another archive interval safely.
9. Retry a deliberately failed archival operation without corruption or duplicate logical observations.

If that experiment works, we've validated the most uncertain part of the architecture.

## Final target

```text
                    SAGE INGESTION
                          |
                          v
                     InfluxDB 3
                    [HOT STORAGE]
                          |
                    archive process
                          |
                          v
                       Iceberg
                          |
                   Pelican FileIO
                          |
                          v
                       Pelican
                          |
                          v
                       NRDStor
                   [COLD STORAGE]


                        USER
                          |
                          v
                       Beehive
                          |
                    Query Planner
                   /             \
                  v               v
            Influx Backend   Iceberg Backend
                  |               |
             InfluxDB 3         Pelican
                                  |
                                NRDStor
                   \              /
                    \            /
                     Merge Results
                          |
                          v
                         USER
```

## Resources
- https://youtu.be/4W_gXrBgQPk?si=OouGJXbmbqV6myQ6
- https://youtu.be/EUgn8aa3kmI?si=qItQqCS3cgQmDy6G
- https://youtu.be/PgPiabdTqBs?si=w0NsiMl3A62oC2fI
- https://medium.com/@aparnamohan1312/monarch-db-simplified-googles-time-series-database-ec93158c8e17
- https://www.influxdata.com/glossary/fdap-stack/
- https://iceberg.apache.org/docs/latest/fileio
- https://pelicanplatform.org/
- https://docs.pelicanplatform.org/about-pelican
- https://github.com/waggle-sensor/sage-storage-loader/tree/main
- https://github.com/waggle-sensor/honeyhouse-config/tree/main/applications/beehive-sage/sage-storage-uploader
- https://hcc.unl.edu/nrdstor
- https://hcc.unl.edu/docs/handling_data/data_storage/NRDSTOR/
- https://osg-htc.org/services/osdf