#!/usr/bin/env python3
from datetime import date, timedelta

INFLUXD = "/home/sean/influxdb2-2.7.12/usr/bin/influxd"
BUCKET_ID = "b3a4e89ad74c5acc"
ENGINE_PATH = "/media/local/pvc-6da578ef-e9bc-47fc-9f64-cfe30a24ff5e_shared_influxdb-data-beehive-influxdb-0/engine"


start = date(2025, 1, 1)
end = date.today()

d = start

while d < end:
    next = d + timedelta(days=1)
    print(
        f"{INFLUXD} inspect export-lp --bucket-id '{BUCKET_ID}' --engine-path '{ENGINE_PATH}' --output-path /media/local/influxdb-export-lp/{d}.lp.gz --start {d}T00:00:00Z --end {next}T00:00:00Z --compress"
    )
    d = next
