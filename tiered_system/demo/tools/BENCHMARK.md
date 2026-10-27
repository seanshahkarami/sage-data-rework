# Phase 0 — InfluxDB ingest benchmark

Historical measurement. The benchmark stack shares port 18086 with the demo;
run it separately. Current Iceberg v3 storage preserves nanoseconds.

The plan said nothing downstream should be built until this number exists.
It now exists, and it changes the hot-tier scope decision: **a full month is
reachable.**

Date: 2026-10-15. Hardware: M-series Mac, 18 cores / 24 GB. InfluxDB runs in
the Rancher Desktop VM, which has **6 CPUs and 8 GB** — that VM, not the
host, is the constraint that matters.

## Headline

One full real day of production data, seeded end to end:

| | |
|---|---|
| day | 2025-05-01 (15 files, 104% of the May daily mean) |
| rows ingested | **146,129,405** |
| wall clock | **362 s (6.0 min)** |
| aggregate rate | **403,653 pts/s** |
| writer processes | 4 |
| series cardinality | **905,468** |
| engine on disk | 1.42 GB |
| container RSS | 2.44 GB of a 5 GB cap, flat |

**Projection for May 2025 (4,366,082,870 rows): ~3.0 hours.**

## Verified, not assumed

Three checks against the archive, all exact:

* **Row count**: InfluxDB returns `146,129,405` for the day — identical to
  the Parquet row count. Nothing was dropped, including the 1,480
  newline-bearing and 98 quote-bearing `value_str` rows that would have
  silently corrupted their batches if the escaping were wrong.
* **Cardinality**: `influxdb.cardinality()` returns `905,468`, exactly the
  distinct-series count measured from Parquet. Flattening every `meta` key
  to a tag is therefore a faithful round-trip of what production does.
* **Nanosecond precision survives ingest**: sampled timestamps come back as
  `2025-05-01T00:00:00.393861083Z` — nine fractional digits. The hot tier
  keeps the real-time resolution. The later Iceberg v3 cold tier also preserves
  nanoseconds.

## Decay

| sample | rows so far | interval rate | engine | RSS |
|---|---|---|---|---|
| t=120 s | 54,450,000 | 453,727 pts/s | 1.82 GB | 2.61 GB |
| t=241 s | 106,354,405 | 428,461 pts/s | 1.24 GB | 2.80 GB |

Rate fell about **6%** while cardinality climbed toward 905k, and memory
stayed flat. There is no cardinality cliff within one day. (The engine
figure dips at t=241 because compaction had just rewritten the TSM set; the
end-state figure of 1.42 GB is the one to use.)

Encoding, not InfluxDB, is the cost centre: 843.9 s of CPU time encoding
against 436.2 s posting. That is why the seeder parallelizes by process.

## The failure that produced the concurrency limit

The first attempt at this run used **15 processes** — one per archive file,
reasoning from the host's 18 cores. It hung, and what actually died was the
**Docker daemon**, not InfluxDB: `docker info` stopped answering and the
container was unreachable. No OOM-kill was recorded, because the container
was never the thing that got killed; the 6-CPU / 8 GB VM was oversubscribed
by 15 host-side writer processes plus InfluxDB's own compaction.

Two changes came out of it, both now in the code:

1. `bench/docker-compose.yml` caps the container at **5 GB / 4 CPUs**, below
   the VM's size, so a runaway seed degrades the container instead of taking
   the host's daemon down — and so the measurement stays attributable.
2. `--procs` is separate from `--files`, defaults to 4, and both
   `bench_mp.py` and `seed_hot.py` document why. 4 processes sustain 400k+
   pts/s with memory flat; the extra 11 bought nothing but a dead VM.

## Consequence for the demo

The plan's fallback — a configurable `DEMO_HOT_DAYS` instead of a full month
— **is not needed**. At ~400k pts/s, May 2025 seeds in about 3 hours
unattended, and `seed_hot.py` is resumable at day granularity, so an
interrupted run costs at most one day of work.

`seed_hot.py` still takes `--days` for a quick demo (one day is 6 minutes),
but the full-month path is the default and is now known to be reachable.

## Reproducing

```
cd tiered_system/demo/tools/bench
docker compose down -v && docker compose up -d
python -I bench_mp.py --files 15 --procs 4 --rows-per-file 0 \
       --batch 25000 --budget 2400 --sample 120
```
