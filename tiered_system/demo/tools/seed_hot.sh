#!/usr/bin/env bash
# Fill the demo hot tier.
#
# Default: restore the May 2025 backup with `influx restore --full`.
# That backup is sage-data-rework/data/influx-may2025.
#
# Parquet ingest is the second option, for when the backup is missing or
# should be rebuilt from the archive:
#
#   ./seed_hot.sh
#   ./seed_hot.sh parquet --start 2025-05-01 --end 2025-05-31
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
mode="${1:-backup}"

case "$mode" in
  backup|restore)
    exec "$ROOT/influx_snapshot.sh" restore
    ;;
  parquet)
    shift
    exec conda run -n sage_pipeline --no-capture-output \
      python -I "$ROOT/seed_hot.py" "$@"
    ;;
  *)
    echo "usage: $0 [backup|parquet] [seed_hot.py args...]" >&2
    echo "  backup   restore data/influx-may2025 (default)" >&2
    echo "  parquet  stream the archive into InfluxDB" >&2
    exit 2
    ;;
esac
