#!/usr/bin/env bash
# Save or restore the demo InfluxDB with the v2 backup API.
#
#   influx backup   https://docs.influxdata.com/influxdb/v2/admin/backup-restore/backup/
#   influx restore  https://docs.influxdata.com/influxdb/v2/admin/backup-restore/restore/
#
# `save` writes a portable backup directory while influxd is running.
# `restore` loads it into a running server with --full, which replaces the
# server contents, so the next demo start does not need seed_hot.py.
#
# The backup lives in sage-data-rework/data, which is gitignored. A May 2025
# backup is about 16 GB, and the Rancher VM disk cannot hold a second copy.
#
#   ./influx_snapshot.sh save
#   ./influx_snapshot.sh restore
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
DEMO="$(cd "$ROOT/.." && pwd)"
SNAP="${INFLUX_SNAPSHOT:-$(cd "$DEMO/../../data" && pwd)/influx-may2025}"
TOKEN="${INFLUX_TOKEN:-demo-token-not-a-secret}"
# Share the server's network namespace. The published host port is not
# reachable from another container on this Docker VM, and the server
# container has no host directory mounted for the backup files.
HOST_URL="${INFLUX_HOST:-http://127.0.0.1:8086}"

cd "$DEMO"

wait_healthy() {
  local i status
  for i in $(seq 1 60); do
    status="$(docker inspect demo-influxdb --format '{{if .State.Health}}{{.State.Health.Status}}{{end}}' 2>/dev/null || true)"
    if [[ "$status" == "healthy" ]]; then
      return 0
    fi
    sleep 2
  done
  echo "demo-influxdb did not become healthy" >&2
  exit 1
}

run_influx() {
  # Mount only the backup directory. Its parent is the archive, and that
  # must not be attached to this container.
  docker run --rm \
    --network container:demo-influxdb \
    -v "${SNAP}:/backup" \
    influxdb:2.7.7 \
    influx "$@"
}

cmd_save() {
  docker compose up -d --no-deps influxdb >/dev/null
  wait_healthy
  if [[ -e "$SNAP" ]]; then
    echo "refusing to overwrite $SNAP" >&2
    exit 1
  fi
  mkdir -p "$SNAP"
  echo "influx backup -> $SNAP"
  run_influx backup /backup \
    --host "$HOST_URL" \
    --token "$TOKEN"
  du -sh "$SNAP"
}

cmd_restore() {
  if [[ ! -d "$SNAP" ]]; then
    echo "backup not found: $SNAP" >&2
    exit 1
  fi
  docker compose up -d --no-deps influxdb >/dev/null
  wait_healthy
  echo "influx restore --full <- $SNAP"
  run_influx restore /backup \
    --host "$HOST_URL" \
    --token "$TOKEN" \
    --full
  echo "restored"
}

case "${1:-}" in
  save) cmd_save ;;
  restore) cmd_restore ;;
  *)
    echo "usage: $0 save|restore" >&2
    exit 2
    ;;
esac
