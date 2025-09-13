import pyarrow as pa
import pyarrow.parquet as parquet
from pathlib import Path

for base_dir in Path("data").glob("*/*/*"):
    parquet_files = sorted(base_dir.glob("*.parquet"))

    if len(parquet_files) <= 1:
        continue

    total_rows = 0

    for parquet_file in parquet_files:
        metadata = parquet.read_metadata(parquet_file)
        total_rows += metadata.num_rows

    if total_rows >= 500_000:
        continue

    print(base_dir, len(parquet_files), total_rows)

    tables = [
        parquet.read_table(parquet_file, partitioning=None)
        for parquet_file in parquet_files
    ]

    try:
        combined = pa.concat_tables(tables).sort_by(
            [
                ("vsn", "ascending"),
                ("host", "ascending"),
                ("timestamp", "ascending"),
            ]
        )
    except pa.ArrowException as err:
        print(f"failed to concat tables for {base_dir} with error: {err}")
        continue

    write_path = Path(base_dir, "0000.parquet.write")
    done_path = Path(base_dir, "0000.parquet.done")
    final_path = Path(base_dir, "0000.parquet")

    writer = parquet.ParquetWriter(
        write_path,
        schema=combined.schema,
        version="2.6",
        write_statistics=True,
        data_page_version="2.0",
        compression="zstd",
        use_dictionary=["vsn", "host"],
    )
    writer.write_table(combined)

    write_path.rename(done_path)

    for parquet_file in parquet_files:
        parquet_file.unlink()

    done_path.rename(final_path)

    print(f"compacted {base_dir} from {len(parquet_files)} files to 1")
