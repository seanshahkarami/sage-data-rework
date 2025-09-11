import pyarrow.parquet as parquet

table = parquet.read_table("sample.parquet")
# do we actually need to sort to be safe or can we assume sorted chunks?
table = table.sort_by("timestamp")
print(table)

writer = parquet.ParquetWriter(
    "sample-compact.parquet",
    schema=table.schema,
    version="2.6",
    write_statistics=True,
    compression="snappy",
    use_dictionary=False,
    sorting_columns=["timestamp"],
)

writer.write_table(table)
