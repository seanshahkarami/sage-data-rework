# Sort key for the cold day file

Decided 2026-10-20 from a one-day rewrite of `2025-03-01` and the same queries run locally and against Pelican. This is the cold day file written by `tiered_system/demo/tools/seed_cold.py`, not the archive export.

The large-read section at the end is the current decision: sort the cold day file by `plugin`, then `vsn`, then `time`, and keep the groups large. The time-leading recommendation below is the measurement that led there. DuckDB 1.5.6 does not use page indexes to fetch fewer bytes ([3_findings.md](3_findings.md)), so only the leading sort key can reject a row group.

## What a query always filters

`beehive-data-api/cold_sql.go` rejects a query with no start. The statement always has `time >= start AND time < end`. `name`, `plugin`, `vsn`, and the other promoted tags are optional filters on top of that window.

A row group can be tight on one column. The leading sort column is the one whose min/max can reject a group. The others are applied inside the groups the time window keeps.

## March 1, and why the other three cannot lead

The day is 140,578,674 rows.

| column | distinct | where the rows sit |
|---|---:|---|
| plugin | 39 | one plugin, `waggle-wxt536:0.24.11.14`, is 134.7 million rows |
| vsn | 91 | 14 vsns hold 136.9 million rows; 72 vsns are under 100,000 rows and together are 2.5 million |
| name | 433 | 10 names are over 1 million rows; the median name has 6 rows |

A ten-minute slice of that fleet still contains every plugin, every vsn, and the common names. Sorting by `vsn` or `plugin` first would make a single-node or single-plugin query open one group, and it would make a time window that does not name that value open one group per value. `name` is the same shape with more values.

## What the time sort changed

`sort_by_time` sorts the day by `time` and cuts a row group every 1,048,576 rows. The unsorted file is the layout already published for this day (921,005,396 bytes on Pelican). The sorted file is 1,238.5 MB, 135 groups. Same row count. The sorted object was uploaded only to `/cold-storage-demo-rowgroupbench-1792502133971307000/2025-03-01.parquet`. The live catalog was not changed.

Groups overlapping `2025-03-01 00:00`–`00:01`: 122 of 141 unsorted, 1 of 135 sorted.

Bytes DuckDB actually fetched for that minute:

| query | rows | unsorted | sorted |
|---|---:|---|---|
| `name = wxt.env.temp`, count | 9,664 | 80.3 MB, 27 requests | 3.4 MB, 138 requests |
| same, returning rows | 9,664 | 82.4 MB, 51 requests | 8.1 MB, 140 requests |
| same plus `vsn = W0A0` | 698 | 12.5 MB, 7 requests | 3.7 MB, 409 requests |

Wall time from this machine to Pelican:

| query | published file | sorted file |
|---|---:|---:|
| one-minute count | 1.56 s, then 1.38 s | 1.57 s, then 1.60 s |
| `vsn = W0A0` count | 0.83 s, then 0.86 s | 3.94 s, then 4.01 s |
| one-minute rows | 1.41 s | 1.65 s |

The byte drop is the row-group skip. Pelican latency did not follow it. The `W0A0` query got slower because the old plugin-then-vsn order let footer stats reject most groups in 7 requests. After the time sort every group contains every node, so that skip is gone and the request count goes to 409. Those extra requests are bloom filters, described below.

## Recommendation

Sort the cold day file by `time` only. Leave `name`, `vsn`, and `plugin` as columns. Do not partition the day into a file per plugin, vsn, or name.

## Larger row groups, measured the same day

`ROW_GROUP_ROWS` is still 1,048,576 in `seed_cold.py`. The same sorted day was rewritten at two larger sizes and queried again. 16,777,216 rows did not finish: DuckDB ran out of memory under the seeder's 4 GB limit while writing that group.

| rows per group | file | groups | one group spans |
|---:|---:|---:|---|
| 1,048,576 | 1,239 MB | 135 | 10.6 min |
| 4,194,304 | 1,234 MB | 34 | 42 min |
| 8,388,608 | 1,196 MB | 17 | 1 h 24 min |

Bytes fetched, and Pelican wall time. Row counts matched across all three files.

| query | 1,048,576 | 4,194,304 | 8,388,608 |
|---|---|---|---|
| one minute, `wxt.env.temp` | 3.4 MB, 138 req, 1.7 / 1.6 s | 13.3 MB, 37 req, 0.8 / 2.2 s | 21.6 MB, 20 req, 2.2 / 1.0 s |
| that minute, `vsn=W0A0` | 3.7 MB, 409 req, 3.8 / 3.8 s | 14.4 MB, 106 req, 1.4 / 1.4 s | 23.7 MB, 55 req, 1.2 / 1.1 s |
| one hour, `wxt.env.temp` | 18.7 MB, 148 req, 1.9 / 1.9 s | 26.1 MB, 39 req, 1.8 / 1.8 s | 21.6 MB, 20 req, 2.6 / 1.5 s |
| that hour, `vsn=W0A0` | 20.4 MB, 424 req, 4.4 / 4.3 s | 28.3 MB, 109 req, 1.7 / 2.6 s | 23.7 MB, 55 req, 1.1 / 1.7 s |
| whole day, `vsn=W0A0` | 340 MB, 406 req, 6.4 s | 364 MB, 103 req, 4.5 s | 283 MB, 52 req, 3.9 s |

A one-minute read gets bigger, which is the expected cost. The `vsn` queries get faster because there are fewer groups to probe: the one-minute `W0A0` query goes from 3.8 s to 1.1 s, and the whole day of that node from 6.4 s to 3.9 s. The hour-long read is the same size at 8,388,608 as the minute, because that group already covers an hour and a half. 8,388,608 is the size to try in the seeder. The whole-day `vsn` read is still hundreds of megabytes, because every group still contains that node.

## Smaller groups read fewer bytes and send more requests

DuckDB writes a bloom filter for each column of each row group and stores the filters after the column data. A time range is decided from the min/max already in the footer, so a one-minute query reads the column data of the one group that overlaps that minute. An equality such as `name = 'wxt.env.temp'` is decided from the bloom filter, and DuckDB fetches that filter for every row group, including the groups the time range has already excluded.

On the 1,048,576-row file those are the 135 reads of 272 bytes. Group 0's `name` filter is at byte 1,199,911,546 and is 272 bytes long, and that range is one of the requests. One filter per group, so the request count follows the number of groups in the file rather than the number of groups the minute overlaps. Adding `vsn = 'W0A0'` fetches that column's filter too, about three small reads per group instead of one.

That is the tradeoff. A smaller group holds less of the day, so the one group a short query opens is a smaller download. The file then contains more groups, and each one costs a bloom-filter request. Pelican charges a round trip per request, which is why 3.4 MB and 138 requests was not faster than 21.6 MB and 20 requests.

## Cut row groups on a time span, and keep the day partition 
>TODO: would we still do a dynamic grouping implementation with plugin > vsn > time sorting?

A fixed `ROW_GROUP_ROWS` covers a fixed amount of time only on a day of one size. The archive is 608 days, 2025-01-01 through 2026-08-31. A day is 18 MB at the smallest, 418 MB at the median, and 795 MB at the largest. On March 1, 8,388,608 rows was about an hour and a half. On the quietest day that same count is the whole file, so a one-minute query reads every measurement. On a busier day it becomes more groups, and each group is another bloom-filter request.

The hot-to-cold writer should cut a row group when the sorted rows cross a time boundary of about an hour, and also cap the group at a maximum row count so one dense hour cannot become a multi-gigabyte group. A one-hour query then opens one group on a quiet day and on a busy day. A one-minute query reads about an hour of the fleet, which is the cold-read size measured above. Do not cut a group every few minutes. That brings back one bloom-filter request per group.

The Iceberg partition stays one day. Every query has a time range, and a day is the unit `add_files` places from the file's min and max. Hour partitions, or a partition per file of N rows, add a footer and a manifest entry for each piece. The archive exporter's batch of 10 million rows is a memory limit in `archive-exporter/main.go`, not a query layout. The cold writer should not copy it. One file per day is enough until a day passes a size worth uploading and reading as one object. Split that day on the hour, so each piece is still one day partition with a tight time range.

>NOTE: This would replace ROW_GROUP_ROWS with a dynamic grouping implementation.

## Optimize the large read

The hour-long group above is the layout for a short cold query. That is the wrong target. A one-minute window is a hot query, and making it cheap on cold saves seconds. A multi-day extract reads gigabytes, and that is where minutes come off.

A window longer than a group opens every group it covers. Cutting groups at an hour does not shrink that read. It adds a bloom-filter request per group. On the sizes already measured, the whole day of `W0A0` went from 6.4 s and 406 requests at 1,048,576 rows per group to 3.9 s and 52 requests at 8,388,608, and the file shrank from 1,239 MB to 1,196 MB. The one-minute download grew from 3.4 MB to 21.6 MB. Take that trade.

Use large groups, a few per day file. Writer memory is the cap: 16,777,216 rows ran out of memory under the seeder's 4 GB limit. A fixed row count still spans a different amount of time on an 18 MB day and a 795 MB day, so keep a coarse time cut only as a ceiling, on the order of the day rather than the hour. Do not cut every hour to protect a one-minute read.

Time as the leading sort key stops paying once the window covers the groups. Every group overlaps, and a single-node read still scans the other nodes. `W0A0` is 9.5 million of 140.6 million rows on March 1, about 7%, but the time-sorted whole-day read was still 283 MB.

The archive order is the one that moves those bytes. `archive-exporter/main.go` sorts each batch by `plugin`, then `vsn`, then `time`. That order was rewritten for the whole of March 1, still at 8,388,608 rows per group. The file shrank from 1,196 MB to 671 MB, because a group is now one plugin and a short run of vsns and those columns compress. Fifteen of the seventeen groups are a single plugin. `W0A0` overlaps 4 groups instead of all 17: the two groups that hold its run inside `waggle-wxt536`, plus the first and last groups, which are the other plugins and also contain that vsn.

| query | time, then nothing | plugin, then vsn, then time |
|---|---|---|
| whole day, `vsn=W0A0` | 283 MB, 52 req, 3.4 / 3.4 s | 47 MB, 13 req, 1.0 / 1.1 s |
| that day, `W0A0` and `wxt.env.temp` | 393 MB, 103 req, 4.9 / 4.9 s | 38 MB, 36 req, 2.6 / 1.4 s |
| whole day, `wxt.env.temp`, all nodes | 356 MB, 52 req, 4.4 / 4.8 s | 238 MB, 52 req, 3.1 / 4.1 s |
| one minute, `wxt.env.temp` | 21.6 MB, 20 req, 2.6 / 1.7 s | 211 MB, 48 req, 3.3 / 3.0 s |

The node extract is the one that gets cheaper. A name filter does not skip groups, because `name` is not a leading key; it only reads a smaller file. The one-minute read opens every group, because each group now spans the whole day, and it downloads 211 MB instead of 21.6 MB. On this link that was about one extra second. That is the cost already accepted above.

The archive files themselves do not get this skip. Each is one row group of up to 10 million rows, and that group's vsn range still covers many nodes, so a `W0A0` query opens 13 of the 15 March 1 files. The sort key is right. The group has to be cut inside a vsn's run, which the 8,388,608-row group does and the 10-million-row file does not.

`name` stays a column, not a leading key. A long single-measurement extract is about 10% of the day, and putting `name` first would make every query that does not filter it open one group per name. When a query names both a node and a measurement, `vsn` is the more selective of the two on this day.

The object is `/cold-storage-demo-rowgroupbench-1792520276630561000/pvt_8388608.parquet`. `seed_cold.py` still sorts by `time` and still uses 1,048,576. The day partition stays. It already drops the days outside a long range.

## A whole month, on the files that are published

March 2025 is 31 day files under `/cold-storage-demo-1792109487566380000/staged/`. 4.37 billion rows, 4,388 row groups, 28.6 GB of row-group payload. These are the published files, one object per day, not the plugin-then-vsn rewrite above. That rewrite exists for March 1 only.

Counts from this machine, two passes. The second pass is a little faster. Returning the rows would read the whole overlapping group, which is the last column.

| query | rows | time | groups overlapped | those groups |
|---|---:|---|---:|---:|
| March 1, `vsn=W0A0` | 9,540,369 | 5.5 s | | |
| March, `vsn=W0A0` | 236,043,169 | 79 s, then 64 s | 799 of 4,388 | 5.4 GB |
| March, `wxt.env.temp` | 444,617,288 | 58 s, then 50 s | 537 of 4,388 | 3.3 GB |
| March, `W0A0` and `wxt.env.temp` | 25,940,207 | 14 s, then 14 s | 66 of 4,388 | 0.41 GB |

A month opens every day file. The day partition drops nothing inside the month. The skip is inside the file: a `W0A0` count opens 799 groups, about a fifth of them, and that is still a minute. The same count on the rewritten March 1 was 1.0 s and 47 MB. The other thirty days are not in that layout, so the month number for it has not been measured.

The archive is the expensive version of the same month. It is 455 files, and a `W0A0` query can skip 6 of them. The time and vsn columns of the other 449 are 13.6 GB. A `wxt.env.temp` query can skip down to 75 files, 2.2 GB, because many archive files are a single plugin and that name is not in them.
