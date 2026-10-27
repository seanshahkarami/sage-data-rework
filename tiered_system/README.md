# Tiered system

Hot storage is InfluxDB. Cold storage is Iceberg on Pelican. Both are queried through `beehive-data-api` with the same request the portal already sends.

Pelican work stays on an isolated prefix (`/cold-storage-demo-*`).

Python uses the `sage_pipeline` conda environment.

## demo/

The runnable stack: InfluxDB for May 2025, Iceberg for March and April 2025, and the data API in front of both. Cold reads run inside the API with DuckDB `iceberg_scan`. Setting `COLD_WAREHOUSE` turns that path on; leaving it empty keeps the service on Influx only.

From `demo/`:

```sh
docker compose up -d                 # InfluxDB
./tools/seed_hot.sh                  # restore data/influx-may2025
./tools/seed_hot.sh parquet --start 2025-05-01 --end 2025-05-31   # rebuild May from the archive
docker compose --profile cold up -d  # data-api, after seed_cold.py has published a version-hint
```

See the [demo guide](demo/README.md) to use the already-published adopted warehouse.

Cold now requires Iceberg v3 with nanosecond time and one stored VARIANT value.
Use the isolated REST catalog and v3 seeding commands in [plan 10](../plans/10_iceberg_v3.md) before starting the cold profile. `tools/seed_cold.py` prints `COLD_WAREHOUSE` after successful Pelican publication. `COLD_WATERMARK` is the cut: rows strictly before it are cold, rows at or after it are hot.

## experiments/

The measurements that led here. Findings are in `experiments/results/SUMMARY.md`. How to run a script is in `experiments/README.md`.

The adopted warehouse uses day partitions, chronological file batches of up to
10 million rows, plugin/vsn/time sorting within each file, and plugin-pure row
groups capped at 262,144 rows. Time-first and other layouts remain historical
controls. See the [demo guide](demo/README.md) for the current warehouse, tools,
and explicit adopted-policy seeder flags, and [plan 10](../plans/10_iceberg_v3.md)
for the full March/April validation and adoption decision.
