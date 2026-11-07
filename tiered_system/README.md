# Tiered system

Hot storage is InfluxDB. Cold storage is Iceberg on Pelican. Both are queried through `beehive-data-api` with the same request the portal already sends.

Pelican work stays on an isolated prefix (`/cold-storage-demo-*`).

Python uses the `sage_pipeline` conda environment.

## demo/

The runnable stack: InfluxDB for May 2025, Iceberg for March and April 2025, and the data API in front of both. Cold reads run inside the API with DuckDB `iceberg_scan`. The cold profile defaults to the adopted Iceberg v3 warehouse. Export `COLD_WAREHOUSE` empty to keep that API on Influx only.

From `demo/`:

```sh
docker compose up -d                 # InfluxDB
./tools/seed_hot.sh                  # restore data/influx-may2025
./tools/seed_hot.sh parquet --start 2025-05-01 --end 2025-05-31   # rebuild May from the archive
docker compose --profile cold up -d  # data-api on the adopted v3 warehouse
```

See the [demo guide](demo/README.md) for that warehouse URL and how to override it.

Cold reads require Iceberg v3 with nanosecond time and one stored VARIANT value.
The published replica already satisfies that contract, including chronological
Parquet batches. Republishing uses the isolated REST catalog and v3 seeding
commands in [plan 10](../plans/10_iceberg_v3.md). `tools/seed_cold.py` prints
`COLD_WAREHOUSE` after a successful Pelican publication. `COLD_BORDER` is
the cut: rows strictly before it are cold, rows at or after it are hot. The
demo default is `2025-05-01T00:00:00Z`.

## experiments/

The measurements that led here. Findings are in `results/SUMMARY.md` inside the split archive next to the scripts (`experiments/results.z01`, `experiments/results.z02`, `experiments/results.z03`, and `experiments/results.zip`). The `experiments/results/` directory itself is not in the repository. How to unpack the archive and how to run a script are in `experiments/README.md`.

The adopted warehouse uses day partitions, chronological file batches of up to
10 million rows, plugin/vsn/time sorting within each file, and plugin-pure row
groups capped at 262,144 rows. Time-first and other layouts remain historical
controls. See the [demo guide](demo/README.md) for the current warehouse, tools,
and explicit adopted-policy seeder flags, and [plan 10](../plans/10_iceberg_v3.md)
for the full March/April validation and adoption decision.

## Continuous archival

The demo remains a reproducible benchmark fixture. The continuous service lives
in [beehive-hot-to-cold-loader](https://github.com/waggle-sensor/beehive-hot-to-cold-loader/blob/main/README.md), with loader fencing, durable
late arrivals, managed API read views, verified publication and explicit hot
retention deletion. See [plan 12](../plans/12_dynamic_archival_service.md).
