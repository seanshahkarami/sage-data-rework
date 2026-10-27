# Tiered storage demo

[Tiered system overview](../README.md)

The demo runs May 2025 in InfluxDB and reads March/April 2025 from Iceberg v3
on Pelican through `beehive-data-api`. The API owns cold query execution;
there is no separate Python query service or SQLite hint reconciler.

## Start the existing demo

Run these commands from this directory:

```sh
docker compose up -d influxdb
```

For an empty hot tier, `./tools/seed_hot.sh` restores the May backup from
`../../data/influx-may2025`. A full restore replaces the target InfluxDB contents.
If rebuilding from the archive instead, use:

```sh
./tools/seed_hot.sh parquet --start 2025-05-01 --end 2025-05-31
```

Start the API against the adopted, already-published warehouse:

```sh
export COLD_WAREHOUSE=https://nrdstor.nationalresearchplatform.org:8443/sage/cold-storage-demo-v3-bounded-full-1793018629285065000/warehouse
export COLD_WATERMARK=2025-05-01T00:00:00Z
docker compose --profile cold up -d --build data-api
```

The API is available at `http://localhost:10000/api/v1/query`. The signing key
is mounted read-only from the sibling `honeyhouse-config` checkout. The Compose
project name `demo` and its `demo_demo-influx-data` volume are retained.
See the [API documentation](../../../beehive-data-api/docs/README.md) for request
formats, configuration, and profiling.

## Warehouse policy

The adopted warehouse uses Iceberg v3, `TIMESTAMPTZ_NS` time, one `VARIANT`
value column (field ID 11), and MAP metadata with promoted tags. Files are day
partitioned, contain chronological batches of at most 10 million rows, and
sort each batch by plugin, then vsn, then time. Plugin-pure row groups are capped
at 262,144 rows: large plugins span groups and small plugins share files.

The layout experiments and adoption evidence are in
[plan 10](../../plans/10_iceberg_v3.md). Historical measurements remain in
[experiments/results](../experiments/results/).

## Tools

Use Python from the `sage_pipeline` environment. The cold publisher dependencies
are pinned in `tools/requirements-v3.txt`.

| Tool | Purpose |
| --- | --- |
| `tools/seed_hot.sh` | Restore the hot backup or invoke Parquet ingestion |
| `tools/influx_snapshot.sh` | Save or restore the demo InfluxDB backup |
| `tools/seed_hot.py`, `tools/lineproto.py` | Encode source archive rows and seed InfluxDB |
| `tools/verify_hot.py` | Compare seeded hot counts, cardinality, and timestamp precision with the archive |
| `tools/seed_cold.py` | Commit archive days to an isolated REST v3 table, audit and publish a replica |
| `tools/v3_layout.py` | Build and audit physical layouts; invoke the native Go layout helper |
| `tools/v3_publish.py` | Export committed snapshot graphs and publish the hint last |
| `tools/pelican_fileio.py`, `tools/pelican_token.py` | Pelican transport and scoped authentication for publication |
| `tools/seed_time_control.py` | Reproduce a byte-identical time-first comparison control |
| `tools/bench/`, `tools/BENCHMARK.md` | Historical Phase 0 hot-ingest benchmark and findings |

`DEMO_ARCHIVE` overrides the default source at `sage-data-rework/data/archive`.
Hot tools accept `INFLUX_URL`, `INFLUX_TOKEN`, `INFLUX_ORG`, and `INFLUX_BUCKET`.
The snapshot wrapper uses `INFLUX_SNAPSHOT` and `INFLUX_HOST` instead of the
Python tools' archive/URL settings. See each script for its specific arguments.

### Publish a new warehouse

Follow plan 10's isolated REST setup and native writer validation before seeding.
`docker-compose.v3.yml` provides the catalog fixture; it requires an existing,
isolated absolute `COLD_V3_ROOT`. The catalog and Python publisher must see the
same absolute source path. Build the Go `layoutpack` helper from
`../experiments/icebergo/cmd/layoutpack` as described in that plan.

For the adopted policy, pass these flags explicitly to `tools/seed_cold.py`:

```sh
--layout plugin-vsn-time --batch-order time --row-group-rows 262144 \
  --layout-helper /absolute/path/to/layoutpack
```

The script retains its historical time-first default for reproducing controls;
that default does **not** select the adopted policy. It also requires `--root`,
`--start`, inclusive `--end`, and a fresh `--export` directory. Optional
`--pelican-prefix` must identify a fresh isolated `/cold-storage-demo-v3-*`
publication. Never use `/node-data`. Keep source warehouses, replicas, progress
journals, and existing published objects intact. Append guards reject mixed
layouts, occupied intervals, and unresolved commits.

`docker-compose.layout-trial.yml` retains a separate comparison API, reusing a
selected baseline image and the existing `demo_default` network. Its warehouse,
image, port, and profile mounts are configured independently of the primary API.
The historical ingest benchmark also uses port 18086, so run its stack separately
from the demo InfluxDB.

## Validation

From `sage-data-rework`, run:

```sh
conda run -n sage_pipeline python -m unittest discover \
  -s tiered_system/demo/tools -p 'test_*.py'
```

Native Parquet checks are opt-in through `NATIVE_DUCKDB_PROBE` and
`COLD_V3_LAYOUT_HELPER`. Build the DuckDB probe with `-tags=duckdb_arrow` as
specified in plan 10. These checks use temporary local fixtures; full REST,
Pelican publication, and live API benchmarks are separate integration steps.
The line-protocol checks also sample local archive rows when available.
