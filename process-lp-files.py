from gzip import GzipFile
from datetime import datetime
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as parquet
from dataclasses import dataclass

# We have some weird data like this, where the newline is in the value... which means we can't really do line by line...
# invalid right part: b'mobotix.move.status,host=0000e45f0198f3d7.ws-rpi,job=mobotix-scan-direction-2380,node=000048b02d3ae27a,plugin=registry.sagecontinuum.org/bhupendraraut/mobotix-scan:0.24.8.20,task=mobotix-scan-direction,vsn=W020,zone=shield value="OK\n'
# invalid line: b'" 1735775154855394890\n'

buckets = {}

SPACE = ord(" ")
LSLASH = ord("\\")

year = 2025
month = 1
day = 1

start_timestamp = datetime.now()
chunk_id = 0

with GzipFile("export-lp/2025/01/01/data.lp.gz", "r") as f:
    measurement_tags = {}

    for linenum, line in enumerate(f):
        line = line.decode()
        measurement_tags.clear()

        left, sep, right = line.partition(" value=")
        if not sep:
            print(f"invalid line: {line}")
            continue

        left = left.replace("\\ ", " ")
        measurement_name, *tagstr = left.split(",")
        try:
            valuestr, timestampstr = right.rsplit(maxsplit=1)
            if "." in valuestr:
                measurement_value = float(valuestr)
            else:
                measurement_value = int(valuestr)
            measurement_timestamp = datetime.fromtimestamp(int(timestampstr) / 10**9)
        except ValueError:
            print(f"invalid right part: {line}")
            continue

        for s in tagstr:
            key, sep, value = s.partition("=")
            if not sep:
                continue
            measurement_tags[key] = value

        if linenum > 0 and linenum % 1000000 == 0:
            print("elapsed", datetime.now() - start_timestamp)
            print("records", linenum)
            print(
                "bucket stats",
                len(buckets),
                sum(len(b) for b in buckets.values()) / len(buckets),
            )
            print()

        bucket_key = (measurement_tags["vsn"], measurement_tags["host"])

        if bucket_key not in buckets:
            buckets[bucket_key] = []

        buckets[bucket_key].append(
            (
                measurement_name,
                measurement_timestamp,
                measurement_value,
                measurement_tags,
            )
        )

        if linenum == 5000000:
            break

### write dataset

# we should just build this ahead of time instead of reshaping here...?

for (vsn, host), bucket in buckets.items():
    table = pa.table(
        {
            "name": [name for name, _, _, _ in bucket],
            "timestamp": [ts for _, ts, _, _ in bucket],
            "value": [val for _, _, val, _ in bucket],
            "tags": [tags for _, _, _, tags in bucket],
        }
    )

    table = table.sort_by(
        [
            ("name", "ascending"),
            ("timestamp", "ascending"),
        ]
    )

    print(table)
    print(table.schema)

    # writer_path = Path(
    #     f"data/year={year}/month={month}/day={day}/vsn={vsn}/host={host}/plugin={plugin}/name={name}/{chunk_id}.parquet"
    # )
    writer_path = Path(
        f"data/year={year}/month={month}/day={day}/vsn={vsn}/host={host}/{chunk_id}.parquet"
    )
    writer_path.parent.mkdir(parents=True, exist_ok=True)
    chunk_id += 1

    print(f"started writing {writer_path}")
    writer = parquet.ParquetWriter(
        writer_path,
        schema=table.schema,
        version="2.6",
        write_statistics=True,
        compression="snappy",
        use_dictionary=["vsn", "host"],
    )
    writer.write_table(table)
    print(f"finished writing {writer_path}")

# hmm... another interesting idea is... we can run a generally grouper
# on the data... i wonder host much memory this would take to actually load almost everything???
# we can be smart about it too.
