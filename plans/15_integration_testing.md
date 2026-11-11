# Production integration test: archive from coverage start, deletion off

Run the installed
`honeyhouse-config/applications/beehive-sage/beehive-hot-to-cold-loader/README.md`
against the production `waggle` bucket and its bound warehouse. Archival starts
at `HOT_TO_COLD_START=2021-03-17` and continues forward. Deletion stays off for
the whole test. The pass condition is the first covered day reaching `ROUTED`
with its hot rows still in Influx, and the scheduler then admitting the next
eligible day.

This uses the Phase B journal. It does not create a second Deployment, a second
claim, or a second warehouse. Context is
`kubernetes-admin@sage-cluster.local`.

## Starting point

`activate/` is already applied. One Ready pod on `node3` runs
`waggle/beehive-hot-to-cold-loader:pr-1` with `--coordinate-only=true`,
`--writers-coordinated=false`, `--readers-coordinated=false`, and
`--delete-enabled=false`. The claim `beehive-hot-to-cold-loader-state` is Bound
at 512 GiB. The journal frontier is `2021-03-17T00:00:00Z`, metadata is empty,
and generation is 0. The warehouse is
`https://nrdstor.nationalresearchplatform.org:8443/sage/cold-storage-archive-live-test-20261109/warehouse`.

Those identity values are already written into the journal. This test keeps
them. `2021-03-17` is both the coverage start and the first day the scheduler
may admit.

## What stays off

- `--delete-enabled=false` and `--readers-coordinated=false`. Hot rows remain
  in Influx after each day reaches `ROUTED`.
- No `--archive-before`. The run is not limited to a single day.
- No bootstrap warehouse.
- No change to the public API, the `waggle-recent` bucket, or the coordinator
  tokens.
- No `kubectl delete -k`. That command deletes the state claim and its directory.

`--hot-retention=720h` still applies. The scheduler admits a complete UTC day
only after that day is older than 30 days. Days inside the retention window
stay hot. The single `rideshare.log` row at `2000-01-01T06:00:00Z` stays
outside coverage.

## Gates before archival starts

The process refuses to leave `--coordinate-only` until the source inputs exist
and `--writers-coordinated=true`. Set that flag only after every main `waggle`
loader is the coordinated `pr-6` image from
[Phase C](15_deployment.md) and a direct writer of `waggle` has been found or
explicitly accepted. `waggle-recent` is not one of those writers.

Confirm the read-only mount before changing mode. The Influx engine is
`/var/lib/influxdb2/engine` on claim `influxdb-data-beehive-influxdb-0`. The
archiver pod is already required to sit on `node3` with `beehive-influxdb-0`.
Mount that claim read-only and set `INFLUX_ENGINE_PATH` to its `engine`
directory. If the volume driver rejects the second mount, stop this test and
use a sidecar in the Influx pod. Do not remount the claim read-write.

The source settings that mode requires are:

| Setting | Value |
| --- | --- |
| `INFLUX_URL` | `http://beehive-influxdb.shared.svc.sage-cluster.local:8086` |
| `INFLUX_ORG` | `waggle` |
| `INFLUX_BUCKET` | `waggle` |
| `INFLUX_BUCKET_ID` | `b3a4e89ad74c5acc` |
| `INFLUX_TOKEN` | the existing source query token, from a Secret |
| `PELICAN_KEY_PATH` | the trusted signing key, mounted read-only |
| `HOT_TO_COLD_START` | `2021-03-17`, already bound |

The 512 GiB claim holds the journal and the scratch for the day in progress.
Published objects go to the Pelican prefix. Because deletion is off, Influx
keeps every archived day as well.

## Run

Work from the workspace root. Every Sage command passes
`--context kubernetes-admin@sage-cluster.local`. The current kube context may be
`nautilus`. Do not print role tokens or `INFLUX_TOKEN`.

1. Confirm the Phase C loader gate and the current coordinate-only pod:

```sh
kubectl --context kubernetes-admin@sage-cluster.local -n beehive-sage \
  get deploy,po -l app=beehive-influxdb-loader -o wide
kubectl --context kubernetes-admin@sage-cluster.local -n shared \
  get po -l app=beehive-hot-to-cold-loader -o wide
kubectl --context kubernetes-admin@sage-cluster.local -n shared \
  get deploy beehive-hot-to-cold-loader -o jsonpath='{.spec.template.spec.containers[0].args}{"\n"}'
```

   Every main `waggle` loader is the coordinated `pr-6` image. The archiver
   args are still `--coordinate-only=true` and `--writers-coordinated=false`.

2. Record status, Prometheus gauges, and the Influx count for `2021-03-17`
   before changing mode. `hot_to_cold_deletion_enabled` is 0 and
   `hot_to_cold_coordinate_only` is 1. Frontier is `2021-03-17T00:00:00Z`
   and generation is 0.

```sh
kubectl --context kubernetes-admin@sage-cluster.local -n shared \
  port-forward svc/beehive-hot-to-cold-loader 18001:10001
```

   In another shell:

```sh
ADMIN_TOKEN=$(kubectl --context kubernetes-admin@sage-cluster.local -n shared \
  get secret beehive-hot-to-cold-loader-tokens \
  -o jsonpath='{.data.HOT_TO_COLD_ADMIN_TOKEN}' | base64 --decode)
curl -sS -X POST http://127.0.0.1:18001/v1/status \
  -H "Authorization: Bearer ${ADMIN_TOKEN}" \
  -H 'Content-Type: application/json' \
  -d '{"bucket":"waggle"}'
curl -sS -G 'https://prometheus.sagecontinuum.org/api/v1/query' \
  --data-urlencode 'query=hot_to_cold_coordinate_only{job="beehive-hot-to-cold-loader"}'
curl -sS -G 'https://prometheus.sagecontinuum.org/api/v1/query' \
  --data-urlencode 'query=hot_to_cold_deletion_enabled{job="beehive-hot-to-cold-loader"}'
curl -sS -G 'https://prometheus.sagecontinuum.org/api/v1/query' \
  --data-urlencode 'query=hot_to_cold_deletions_pending{job="beehive-hot-to-cold-loader"}'
curl -sS -G 'https://prometheus.sagecontinuum.org/api/v1/query' \
  --data-urlencode 'query=hot_to_cold_routing_frontier_timestamp_seconds{job="beehive-hot-to-cold-loader"}'
cat >/tmp/waggle-2021-03-17-count.flux <<'EOF'
from(bucket: "waggle")
  |> range(start: 2021-03-17T00:00:00Z, stop: 2021-03-18T00:00:00Z)
  |> count()
  |> group(columns: [])
  |> sum(column: "_value")
EOF
influx query --org waggle --file /tmp/waggle-2021-03-17-count.flux
```

3. Confirm the prepared `integrate/` render. It keeps one replica and the same
   state claim, mounts the Influx engine read-only, and adds `INFLUX_TOKEN`,
   `INFLUX_BUCKET_ID`, and the Pelican key. Args are
   `--coordinate-only=false`, `--writers-coordinated=true`,
   `--delete-enabled=false`, and `--readers-coordinated=false`.
   `--archive-before` is unset.

```sh
KUBECONFIG=/dev/null kubectl kustomize \
  honeyhouse-config/applications/beehive-sage/beehive-hot-to-cold-loader/integrate \
  | grep -E 'coordinate-only|writers-coordinated|readers-coordinated|delete-enabled|archive-before|INFLUX_|PELICAN_|claimName:|replicas:'
```

4. Apply that tree. The Recreate strategy replaces the coordinate-only pod.

```sh
kubectl --context kubernetes-admin@sage-cluster.local apply -k \
  honeyhouse-config/applications/beehive-sage/beehive-hot-to-cold-loader/integrate
kubectl --context kubernetes-admin@sage-cluster.local -n shared \
  rollout status deploy/beehive-hot-to-cold-loader --timeout=5m
kubectl --context kubernetes-admin@sage-cluster.local -n shared \
  get po -l app=beehive-hot-to-cold-loader -o wide
kubectl --context kubernetes-admin@sage-cluster.local -n shared \
  get deploy beehive-hot-to-cold-loader -o jsonpath='{.spec.template.spec.containers[0].args}{"\n"}'
kubectl --context kubernetes-admin@sage-cluster.local -n shared \
  get po -l app=beehive-hot-to-cold-loader \
  -o jsonpath='{range .items[*].spec.containers[0].volumeMounts[*]}{.name}{" "}{.mountPath}{" ro="}{.readOnly}{"\n"}{end}'
```

   The pod is Ready on `node3`. Args show `--coordinate-only=false` and
   `--writers-coordinated=true`. The engine mount is read-only at
   `/influx-engine`. `integrate/` runs the process as UID/GID 1000 so it can
   read Influx engine directories that are mode `0700` owned by `influxdb`.Confirm the mount
   lists `data/` and `wal/` before trusting export:

```sh
kubectl --context kubernetes-admin@sage-cluster.local -n shared \
  exec deploy/beehive-hot-to-cold-loader -- \
  sh -c 'id; ls -ld /influx-engine /influx-engine/data /influx-engine/data/b3a4e89ad74c5acc; find /influx-engine/data/b3a4e89ad74c5acc -name "*.tsm" | wc -l'
```

5. Watch the first job. It is `2021-03-17`. It has to pass export,
   normalization, independent Influx reconciliation, packing, publication, and
   remote proof, and stop at `ROUTED`. With deletion off, that archive job stays
   in `ROUTED` and counts toward `deletions_pending` until deletion is enabled
   later; hot rows are not removed. The frontier becomes
   `2021-03-18T00:00:00Z` and generation advances once. The published metadata
   carries Iceberg v3, `timestamptz_ns`, VARIANT value, MAP metadata, and
   `bounded-plugin-v1`.

```sh
kubectl --context kubernetes-admin@sage-cluster.local -n shared \
  logs -f deploy/beehive-hot-to-cold-loader
curl -sS -X POST http://127.0.0.1:18001/v1/status \
  -H "Authorization: Bearer ${ADMIN_TOKEN}" \
  -H 'Content-Type: application/json' \
  -d '{"bucket":"waggle"}'
curl -sS -G 'https://prometheus.sagecontinuum.org/api/v1/query' \
  --data-urlencode 'query=hot_to_cold_coordinate_only{job="beehive-hot-to-cold-loader"}'
curl -sS -G 'https://prometheus.sagecontinuum.org/api/v1/query' \
  --data-urlencode 'query=hot_to_cold_deletion_enabled{job="beehive-hot-to-cold-loader"}'
curl -sS -G 'https://prometheus.sagecontinuum.org/api/v1/query' \
  --data-urlencode 'query=hot_to_cold_deletions_pending{job="beehive-hot-to-cold-loader"}'
curl -sS -G 'https://prometheus.sagecontinuum.org/api/v1/query' \
  --data-urlencode 'query=hot_to_cold_jobs_pending{job="beehive-hot-to-cold-loader"}'
curl -sS -G 'https://prometheus.sagecontinuum.org/api/v1/query' \
  --data-urlencode 'query=hot_to_cold_jobs_failed{job="beehive-hot-to-cold-loader"}'
curl -sS -G 'https://prometheus.sagecontinuum.org/api/v1/query' \
  --data-urlencode 'query=hot_to_cold_routing_frontier_timestamp_seconds{job="beehive-hot-to-cold-loader"}'
```

   After the first day reaches `ROUTED`, `hot_to_cold_coordinate_only` is 0,
   `hot_to_cold_deletion_enabled` is 0, `hot_to_cold_deletions_pending` is at
   least 1, `hot_to_cold_jobs_failed` is 0, and
   `hot_to_cold_routing_frontier_timestamp_seconds` is 1616025600
   (`2021-03-18T00:00:00Z`). Status also shows `generation` 1 and a non-empty
   warehouse metadata URL.

6. Confirm the hot count for `2021-03-17` is unchanged, then prove the cold
   day through the dynamic API canary at
   `https://data.sagecontinuum.org/api/v2-canary/query`. Keep production
   `/api/v1/query` routed to the existing public Service.

```sh
influx query --org waggle --file /tmp/waggle-2021-03-17-count.flux
```

   The Influx count matches step 2.

7. Stand up the prepared canary
   `[beehive-data-api/canary](../../`honeyhouse-config/applications/beehive-sage/beehive-data-api/canary/README.md`.
   It overlays the shared API `_base_`, so Influx, RabbitMQ, and TLS stay the
   same as the public API. Deployment and Service `beehive-data-api-canary`,
   label `app: beehive-data-api-canary`, image `waggle/beehive-data-api:pr-12`.
   That label is already allowed by the coordinator NetworkPolicy and is not
   selected by the public Service. It sets `COLD_FRONTIER=dynamic` against the
   live-test warehouse, mounts the Pelican key, takes the reader token from
   its namespace-local Secret, and sets `HOT_TO_COLD_LOADER_READER_OWNER` from
   the pod UID. `COLD_BORDER` is unset. Resources are 2 CPU / 4 GiB requested
   and 4 CPU / 8 GiB limited, on `node3`. Its separate Ingress routes
   `/api/v2-canary/query` on the existing hostname to the canary Service and
   rewrites the upstream path to `/api/v1/query`. The canary overlay must be
   applied with this Ingress before using the endpoint below.

```sh
kubectl --context kubernetes-admin@sage-cluster.local apply -k \
  honeyhouse-config/applications/beehive-sage/beehive-data-api/canary
kubectl --context kubernetes-admin@sage-cluster.local -n beehive-sage \
  get deploy,po,svc -l app=beehive-data-api-canary -o wide
kubectl --context kubernetes-admin@sage-cluster.local -n beehive-sage \
  rollout status deploy/beehive-data-api-canary --timeout=5m
kubectl --context kubernetes-admin@sage-cluster.local -n shared \
  get po -l app=beehive-hot-to-cold-loader \
  -o jsonpath='{.items[0].status.podIP}{"\n"}'
```

8. Query the canary for the archived day through its HTTPS endpoint.
```sh
curl --fail-with-body --silent --show-error \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/x-ndjson' \
  --data-binary '{"start":"2021-03-17T00:00:00Z","end":"2021-03-18T00:00:00Z"}' \
  https://data.sagecontinuum.org/api/v2-canary/query > /tmp/waggle-2021-03-17-canary.ndjson
curl --fail-with-body --silent --show-error \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/vnd.apache.arrow.stream' \
  --data-binary '{"start":"2021-03-17T00:00:00Z","end":"2021-03-18T00:00:00Z"}' \
  https://data.sagecontinuum.org/api/v2-canary/query > /tmp/waggle-2021-03-17-canary.arrows
```

   The canary serves that window from cold. The response matches the hot rows
   for `2021-03-17` in nanoseconds, metadata, and values. A start before
   `2021-03-17T00:00:00Z` is rejected. While the query runs, status shows a
   reader grant; when the response closes, readers return to 0. Leave the
   public API on `0.13.0` untouched.

9. Query the same day through
   `sage-data-client` against the canary
   HTTPS endpoint from step 8. Set `endpoint` explicitly; the client default
   remains the production `/api/v1/query` endpoint. Arrow is requested by default.

```sh
cd sage-data-client
python3 -m venv .venv # or use conda create
. .venv/bin/activate # or use conda activate
python -m pip install -e .
python - <<'PY'
import sage_data_client

endpoint = "https://data.sagecontinuum.org/api/v2-canary/query"
frame = sage_data_client.query(
    start="2021-03-17T00:00:00Z",
    end="2021-03-18T00:00:00Z",
    endpoint=endpoint,
)
print(len(frame), frame["timestamp"].min(), frame["timestamp"].max())
print(frame.head())
try:
    sage_data_client.query(
        start="2021-03-16T00:00:00Z",
        end="2021-03-17T00:00:00Z",
        endpoint=endpoint,
    )
except Exception as exc:
    print("precoverage:", exc)
else:
    raise SystemExit("expected pre-coverage rejection")
PY
```

   The dataframe has 478 rows. The first timestamp is
   `2021-03-17T21:38:10.578058496Z`. Pre-coverage fails with the managed
   coverage error. Leave the public API on `0.13.0` untouched.

10. Leave the archiver running. It admits the next complete UTC day older than
   30 days, one day at a time, each ending at `ROUTED` with deletion still
   off. The frontier advances by one UTC day per routed day. A day inside the
   retention window is not admitted. Keep watching status and the gauges from
   step 5. Scale the canary to 0 when the cold check is done:

```sh
kubectl --context kubernetes-admin@sage-cluster.local -n beehive-sage \
  scale deploy/beehive-data-api-canary --replicas=0
```

A row count or an HTTP 200 is not the proof. The job record, remote inventory,
and the unchanged Influx count are the proof. Pause by returning
`--coordinate-only=true` if the state claim or `node3` disk grows without the
current job reaching `ROUTED`. Keep the journal.

## Stop

The test run itself continues from `2021-03-17` with deletion off. Returning
to `--coordinate-only=true` pauses new archival and keeps fences, receipts, and
the warehouse. Do not repoint the warehouse or the coverage start. Do not
enable deletion in this test.

```sh
kubectl --context kubernetes-admin@sage-cluster.local apply -k \
  honeyhouse-config/applications/beehive-sage/beehive-hot-to-cold-loader/activate
kubectl --context kubernetes-admin@sage-cluster.local -n shared \
  rollout status deploy/beehive-hot-to-cold-loader --timeout=5m
kubectl --context kubernetes-admin@sage-cluster.local -n shared \
  get deploy beehive-hot-to-cold-loader -o jsonpath='{.spec.template.spec.containers[0].args}{"\n"}'
```

The args are `--coordinate-only=true` again. Do not use `kubectl delete -k`.

## Canary performance follow-up

After the integration checks, use
[Canary versus production query performance](16_canary_query_performance.md)
for the frontier-anchored HTTP and sage-data-client sweep. It reuses the
coordinator status port-forward above and records separate Arrow, NDJSON, and
dataframe timings with deletion off. For the public canary route, use
`https://data.sagecontinuum.org/api/v2-canary/query` wherever that performance
runbook specifies the local canary query URL; its metrics checks still use the
internal Service. Production `/api/v1/query` remains unchanged.
