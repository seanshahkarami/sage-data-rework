import pyarrow as pa
import pyarrow.parquet as parquet
from pathlib import Path

for base_dir in Path("data").glob("*/*/*"):
    parquet_files = sorted(base_dir.glob("*.parquet"))

    write_file = Path(base_dir, "0000.parquet.write")
    done_file = Path(base_dir, "0000.parquet.done")
    final_file = Path(base_dir, "0000.parquet")

    # if done file exists, then we finished writing data and just need to finish the clean up step.
    if done_file.exists():
        for parquet_file in parquet_files:
            parquet_file.unlink()
        done_file.rename(final_file)
        continue

    # if there's only a single (or no files), then no need to compact.
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

    writer = parquet.ParquetWriter(
        write_file,
        schema=combined.schema,
        version="2.6",
        write_statistics=True,
        data_page_version="2.0",
        compression="zstd",
        use_dictionary=["vsn", "host"],
    )
    writer.write_table(combined)

    write_file.rename(done_file)

    for parquet_file in parquet_files:
        parquet_file.unlink()

    done_file.rename(final_file)

    print(f"compacted {base_dir} from {len(parquet_files)} files to 1")
