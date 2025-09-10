from gzip import GzipFile
from datetime import datetime
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as parquet
import line_protocol_parser

buckets = {}

SPACE = ord(" ")
LSLASH = ord("\\")

year = 2025
month = 1
day = 1

start_timestamp = datetime.now()
chunk_id = 0

# One really useful assumption we can make... the LP *might* already be grouped
# by similar tags so we can just groupby a batch so long at the meta we care about
# matches.
#
# This also allows nice things like possibly allocating a single max size buffer


def flush_bucket_with_key(key):
    global chunk_id
    plugin, measurement = key
    bucket = buckets[key]

    schema = pa.schema(
        [
            pa.field("vsn", pa.string(), nullable=False),
            pa.field("host", pa.string(), nullable=False),
            pa.field("timestamp", pa.timestamp("ns", tz="UTC"), nullable=False),
            pa.field("meta", pa.map_(pa.string(), pa.string()), nullable=False),
        ],
        metadata={
            "vsn": "Node name.",
            "host": "Internal compute host name.",
            "timestamp": "Timestamp of measurement in nano-seconds UTC.",
            "meta": "Additional system and user defined metadata.",
        },
    )

    table = pa.table(
        {
            "vsn": [r[0] for r in bucket],
            "host": [r[1] for r in bucket],
            "timestamp": [r[2] for r in bucket],
            "value": [r[3] for r in bucket],
            "meta": [r[4] for r in bucket],
        },
        schema=schema,
    )

    table = table.sort_by(
        [
            ("vsn", "ascending"),
            ("host", "ascending"),
            ("timestamp", "ascending"),
        ]
    )

    print(table)
    print(table.schema)

    writer_path = Path(
        f"data/plugin={plugin}/measurement={measurement}/date={year:04d}-{month:02d}-{day:02d}/{chunk_id}.parquet"
    )
    writer_path.parent.mkdir(parents=True, exist_ok=True)
    chunk_id += 1

    print(f"started writing {writer_path}")
    writer = parquet.ParquetWriter(
        writer_path,
        schema=table.schema,
        version="2.6",
        write_statistics=True,
        data_page_version="2.0",
        compression="snappy",
        use_dictionary=["vsn", "host"],
    )
    writer.write_table(table)
    print(f"finished writing {writer_path}")

    bucket.clear()


def read_line_protocol_file(path):
    with GzipFile(path, "r") as f:
        while f:
            data = f.readline()
            while True:
                try:
                    yield line_protocol_parser.parse_line(data)
                    break
                except line_protocol_parser.LineFormatError:
                    data += f.readline()


for point_num, point in enumerate(
    read_line_protocol_file("export-lp/2025/01/01/data.lp.gz")
):
    if point_num > 0 and point_num % 1000000 == 0:
        print("elapsed", datetime.now() - start_timestamp)
        print("records", point_num)
        print(
            "bucket stats",
            len(buckets),
            sum(len(b) for b in buckets.values()) / len(buckets),
        )
        print()

    # {'measurement': 'cmv.mean.dir.degn', 'tags': {'zone': 'enclosure', 'vsn': 'W08D', 'task': 'cloud-motion-v1', 'seg_size': '89615', 'seg_rank': '5', 'seg_id': '12', 'quality': '2', 'plugin': 'registry.sagecontinuum.org/bhupendraraut/cloud-motion:1.24.11.8a', 'nsegments_found': '54', 'nsegments_asked': '100', 'node': '000048b02d3ae277', 'input': 'top_camera', 'image_frac': '0.9', 'host': '0000d83addad1d0b.ws-rpi', 'channel': '0'}, 'fields': {'value': 173.0}, 'time': 1735774763707129217}

    tags = point["tags"]

    try:
        vsn = tags["vsn"]
        del tags["vsn"]
        host = tags["host"]
        del tags["host"]
    except KeyError:
        print("failed to get required tags for point:", point)
        continue

    try:
        plugin = tags["plugin"]
        del tags["plugin"]
    except KeyError:
        plugin = "system"

    plugin = plugin.replace("/", "__")
    plugin = plugin.replace(":", "__")

    timestamp = point["time"]
    value = point["fields"]["value"]

    bucket_key = (plugin, point["measurement"])

    if bucket_key not in buckets:
        buckets[bucket_key] = []

    buckets[bucket_key].append(
        (
            vsn,
            host,
            timestamp,
            value,
            tags,
        )
    )

    if len(buckets[bucket_key]) >= 500_000:
        print(f"bucket for {bucket_key} is large! fliushing now!")
        flush_bucket_with_key(bucket_key)

### write dataset

# we should just build this ahead of time instead of reshaping here...?

for key in buckets.keys():
    flush_bucket_with_key(key)

# hmm... another interesting idea is... we can run a generally grouper
# on the data... i wonder host much memory this would take to actually load almost everything???
# we can be smart about it too.

# TODO(sean) fix timestamp to ensure in utc... seems to be messed up
