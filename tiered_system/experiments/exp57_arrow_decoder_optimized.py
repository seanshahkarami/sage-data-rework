"""Compare the frozen pre-optimization decoder with the workspace decoder.

Fresh-process local timings/RSS, excluding IPC fixture generation and imports.
Repeated saved rows model low cardinality; a second fixture makes every meta
object distinct. These are controlled fixtures, not month-query measurements.
No network, server, or storage changes.
"""
import argparse
import gc
import importlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import time

import pandas as pd
import psutil
import pyarrow as pa

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[2] / "sage-data-client/src"))
client = importlib.import_module("sage_data_client.query")

# Frozen from the workspace decoder before the batchwise optimization.
def legacy_load_arrow(fileobj) -> pd.DataFrame:
    """legacy_load_arrow reads an Arrow IPC stream into the same dataframe as load."""
    import pyarrow.ipc as ipc

    table = ipc.open_stream(fileobj).read_all()
    if table.num_rows == 0:
        return pd.DataFrame(
            {
                "timestamp": pd.to_datetime([], utc=True),
                "name": pd.Series([], dtype=str),
                "value": [],
            }
        )

    timestamp = table.column("timestamp").to_pandas(types_mapper=pd.ArrowDtype)
    if getattr(timestamp.dtype, "tz", None) is None:
        timestamp = timestamp.dt.tz_localize("UTC")
    frame = {
        "timestamp": timestamp,
        "name": table.column("name").to_pandas(types_mapper=pd.ArrowDtype),
        "value": legacy_arrow_values(table),
    }
    frame.update(legacy_meta_columns(table.column("meta")))
    return pd.DataFrame(frame)


def legacy_arrow_values(table):
    """legacy_arrow_values parses the JSON scalar stored in the value column."""
    values = []
    for raw in table.column("value").to_pylist():
        if raw is None:
            values.append(None)
        else:
            values.append(json.loads(raw))
    return values


def legacy_meta_columns(column):
    """legacy_meta_columns expands the JSON object stored in the meta column."""
    parsed = []
    keys = []
    seen = set()
    for row in column.to_pylist():
        item = {}
        if row is None:
            parsed.append(item)
            continue
        if isinstance(row, str):
            row = json.loads(row) or {}
        pairs = row.items() if isinstance(row, dict) else row
        for k, v in pairs:
            item[k] = v
            if k not in seen:
                seen.add(k)
                keys.append(k)
        parsed.append(item)
    return {f"meta.{k}": [item.get(k) for item in parsed] for k in keys}

def worker(path, mode):
    start = time.perf_counter()
    with path.open("rb") as stream:
        frame = legacy_load_arrow(stream) if mode == "legacy" else client._load_arrow(stream)
    elapsed = time.perf_counter() - start
    print(json.dumps(dict(mode=mode, rows=len(frame), seconds=elapsed,
                         dataframe_bytes=int(frame.memory_usage(deep=True).sum()),
                         dtypes={k: str(v) for k, v in frame.dtypes.items()})), flush=True)


def measure(path, mode):
    with tempfile.TemporaryFile(mode="w+") as log:
        proc = subprocess.Popen([sys.executable, "-I", str(Path(__file__).resolve()),
                                 str(path), "--worker", mode], stdout=log, stderr=subprocess.STDOUT)
        monitor, peak = psutil.Process(proc.pid), 0
        while proc.poll() is None:
            try:
                peak = max(peak, monitor.memory_info().rss)
            except psutil.NoSuchProcess:
                break
            time.sleep(0.005)
        code = proc.wait()
        log.seek(0)
        output = log.read()
    if code:
        raise RuntimeError(output)
    result = json.loads(output)
    result["peak_rss_bytes"] = peak
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("sample", type=Path)
    parser.add_argument("--worker", choices=["legacy", "optimized"])
    parser.add_argument("--rows", nargs="+", type=int, default=[200000, 1000000])
    parser.add_argument("--batch-size", type=int, default=65536)
    parser.add_argument("--output", type=Path, default=HERE / "results/exp57_arrow_decoder_optimized.json")
    args = parser.parse_args()
    if args.worker:
        worker(args.sample, args.worker)
        return
    with args.sample.open("rb") as stream:
        sample = pa.ipc.open_stream(stream).read_all()
    with args.sample.open("rb") as stream:
        expected = legacy_load_arrow(stream)
    with args.sample.open("rb") as stream:
        actual = client._load_arrow(stream)
    pd.testing.assert_frame_equal(expected, actual, check_dtype=False)
    del expected, actual
    out = dict(sample=str(args.sample), sample_rows=len(sample),
               sample_unique_metadata=len(sample["meta"].unique()),
               pandas=pd.__version__, pyarrow=pa.__version__,
               batch_size=args.batch_size, sample_values_match=True,
               scope="Local repeated-sample and unique-metadata fixtures; no network or month replay",
               measurements=[])
    with tempfile.TemporaryDirectory(prefix="sage-arrow-decoder-") as root:
        path = Path(root) / "fixture.arrows"
        for rows in args.rows:
            for cardinality in ["repeated", "unique"]:
                table = pa.concat_tables([sample] * ((rows + len(sample) - 1) // len(sample))).slice(0, rows).combine_chunks()
                if cardinality == "unique":
                    # Keep the original tags and append one unique string tag.
                    original = [json.loads(raw) or {} for raw in sample["meta"].to_pylist()]
                    tags = pa.array([json.dumps(dict(original[i % len(sample)], experiment_row_id=str(i)),
                                                separators=(",", ":")) for i in range(rows)])
                    table = table.set_column(table.schema.get_field_index("meta"), "meta", tags)
                    del tags, original
                with pa.ipc.new_stream(path, table.schema) as writer:
                    writer.write_table(table, max_chunksize=args.batch_size)
                # Validate a bounded slice for both fixture types; not part of timings.
                sink = pa.BufferOutputStream()
                with pa.ipc.new_stream(sink, table.schema) as writer:
                    writer.write_table(table.slice(0, min(rows, 10003)), max_chunksize=2048)
                fixture = sink.getvalue()
                expected = legacy_load_arrow(pa.BufferReader(fixture))
                actual = client._load_arrow(pa.BufferReader(fixture))
                pd.testing.assert_frame_equal(expected, actual, check_dtype=False)
                del table, fixture, sink, expected, actual
                gc.collect()
                pa.default_memory_pool().release_unused()
                for mode in ["legacy", "optimized"]:
                    result = measure(path, mode)
                    result["metadata"] = cardinality
                    result["fixture_values_match"] = True
                    out["measurements"].append(result)
                    print(json.dumps(result), flush=True)
                args.output.write_text(json.dumps(out, indent=2) + "\n")


if __name__ == "__main__":
    main()
