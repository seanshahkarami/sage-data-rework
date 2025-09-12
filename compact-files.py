import pyarrow as pa
import pyarrow.parquet as parquet
from pathlib import Path

# compact_files = []
# total_num_rows = []

tables = []

# want to do compacting at the leaves if needed...
for path in Path(
    "data/plugin=registry.sagecontinuum.org%2Fbhupendraraut%2Ffile-forager:0.25.5.13"
).glob("**/*.parquet"):
    metadata = parquet.read_metadata(path)
    table = parquet.read_table(path, partitioning=None)
    tables.append(table)

combined = pa.concat_tables(tables).sort_by(
    [
        ("vsn", "ascending"),
        ("host", "ascending"),
        ("timestamp", "ascending"),
    ]
)

print(combined)

# parquet.write_table(combined, "compacted.parquet")

# table = parquet.read_table("sample.parquet")
# # do we actually need to sort to be safe or can we assume sorted chunks?
# table = table.sort_by("timestamp")
# print(table)

# writer = parquet.ParquetWriter(
#     "sample-compact.parquet",
#     schema=table.schema,
#     version="2.6",
#     write_statistics=True,
#     compression="snappy",
#     use_dictionary=False,
#     sorting_columns=["timestamp"],
# )

# writer.write_table(table)
