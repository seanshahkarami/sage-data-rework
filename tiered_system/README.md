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

`tools/seed_cold.py` prints `COLD_WAREHOUSE` and the `version-hint.text` path. `COLD_WATERMARK` is the cut: rows strictly before it are cold, rows at or after it are hot.

## experiments/

The measurements that led here. Findings are in `experiments/results/SUMMARY.md`. How to run a script is in `experiments/README.md`.
