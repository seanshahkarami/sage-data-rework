"""Investigate Arrow decoding using a saved current-schema API response.

python -I exp56_arrow_client_costs.py /path/to/small-response.arrows

No server or storage changes. Repeated sample rows are a controlled fixture,
not a replay of a complete month. Each decoder runs in a fresh process.
The prototype changes only metadata expansion and is not a production patch.
"""
import argparse
import cProfile
import gc
import importlib
import json
import os
from pathlib import Path
import pstats
import subprocess
import sys
import tempfile
import time

import pandas as pd
import psutil
import pyarrow as pa
import pyarrow.compute as pc

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[2] / "sage-data-client/src"))
client = importlib.import_module("sage_data_client.query")
arrow_decoder = importlib.import_module("sage_data_client._arrow")


def prototype(fileobj):
    table = pa.ipc.open_stream(fileobj).read_all()
    timestamp = table["timestamp"].to_pandas(types_mapper=pd.ArrowDtype)
    if getattr(timestamp.dtype, "tz", None) is None:
        timestamp = timestamp.dt.tz_localize("UTC")
    frame = {
        "timestamp": timestamp,
        "name": table["name"].to_pandas(types_mapper=pd.ArrowDtype),
        # Keep the current scalar decoder to isolate metadata expansion.
        "value": arrow_decoder._arrow_values(table),
    }
    encoded = table["meta"].dictionary_encode().unify_dictionaries()
    dictionary = encoded.chunk(0).dictionary
    objects = [json.loads(v) if v is not None else {} for v in dictionary.to_pylist()]
    keys = list(dict.fromkeys(key for obj in objects for key in obj))
    for key in keys:
        values = pa.array([obj.get(key) for obj in objects], type=pa.string())
        expanded = pa.chunked_array([pc.take(values, chunk.indices) for chunk in encoded.chunks])
        frame["meta." + key] = expanded.to_pandas(types_mapper=pd.ArrowDtype)
    return pd.DataFrame(frame)


def worker(path, mode, profile):
    profiler = cProfile.Profile() if profile else None
    if profiler:
        profiler.enable()
    start = time.perf_counter()
    with open(path, "rb") as f:
        frame = client._load_arrow(f) if mode == "current" else prototype(f)
    elapsed = time.perf_counter() - start
    if profiler:
        profiler.disable()
    out = dict(mode=mode, rows=len(frame), seconds=elapsed,
               dataframe_bytes=int(frame.memory_usage(deep=True).sum()),
               dtypes={k: str(v) for k, v in frame.dtypes.items()})
    if profiler:
        stats = pstats.Stats(profiler)
        functions = []
        for (file, line, name), (primitive, calls, own, cumulative, callers) in stats.stats.items():
            if file.endswith("sage_data_client/query.py") or (file.endswith("json/__init__.py") and name == "loads"):
                functions.append(dict(function=name, calls=calls, own_seconds=own, cumulative_seconds=cumulative))
        out["profile"] = sorted(functions, key=lambda f: -f["cumulative_seconds"])
    print(json.dumps(out), flush=True)


def timed_worker(path, mode, profile=False):
    args = [sys.executable, "-I", str(Path(__file__).resolve()), str(path), "--worker", mode]
    if profile:
        args.append("--profile")
    with tempfile.TemporaryFile(mode="w+") as log:
        proc = subprocess.Popen(args, stdout=log, stderr=subprocess.STDOUT)
        monitor = psutil.Process(proc.pid)
        peak = 0
        while proc.poll() is None:
            try:
                peak = max(peak, monitor.memory_info().rss)
            except psutil.NoSuchProcess:
                break
            time.sleep(0.01)
        code = proc.wait()
        log.seek(0)
        output = log.read()
    if code:
        raise RuntimeError(output)
    out = json.loads(output)
    out["peak_rss_bytes"] = peak
    return out


def ipc_size(table, compression=None):
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema,
                          options=pa.ipc.IpcWriteOptions(compression=compression)) as writer:
        writer.write_table(table)
    return sink.getvalue().size


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("sample", type=Path)
    parser.add_argument("--rows", type=int, nargs="+", default=[200000, 1000000])
    parser.add_argument("--worker", choices=["current", "prototype"])
    parser.add_argument("--profile", action="store_true")
    parser.add_argument("--output", type=Path, default=HERE / "results/exp56_arrow_client_costs.json")
    args = parser.parse_args()
    if args.worker:
        worker(args.sample, args.worker, args.profile)
        return
    sample = pa.ipc.open_stream(args.sample).read_all()
    if not sample.num_rows or not pa.types.is_string(sample["meta"].type):
        raise ValueError("requires a nonempty API response with JSON-string metadata")
    with open(args.sample, "rb") as f:
        expected = client._load_arrow(f)
    with open(args.sample, "rb") as f:
        actual = prototype(f)
    pd.testing.assert_frame_equal(expected, actual, check_dtype=False)
    del expected, actual
    encoded = sample
    for name in ["name", "meta"]:
        i = encoded.schema.get_field_index(name)
        encoded = encoded.set_column(i, name, encoded[name].dictionary_encode())
    out = dict(
        sample=str(args.sample), sample_rows=sample.num_rows,
        pandas=pd.__version__, pyarrow=pa.__version__,
        sample_schema=str(sample.schema),
        sample_columns_bytes={name: sample[name].nbytes for name in sample.column_names},
        sample_unique_meta=len(sample["meta"].unique()),
        sample_ipc_sizes=dict(plain=ipc_size(sample), zstd=ipc_size(sample, "zstd"),
                              dictionary_name_meta=ipc_size(encoded)),
        prototype_sample_values_match=True,
        scope="Repeated saved API sample; fresh-process local decoder costs, not a month or network benchmark",
        measurements=[])
    with tempfile.TemporaryDirectory(prefix="sage-arrow-costs-") as root:
        path = Path(root) / "fixture.arrows"
        for rows in args.rows:
            table = pa.concat_tables([sample] * ((rows + sample.num_rows - 1) // sample.num_rows)).slice(0, rows).combine_chunks()
            with pa.ipc.new_stream(path, table.schema) as writer:
                writer.write_table(table, max_chunksize=65536)
            del table
            gc.collect()
            pa.default_memory_pool().release_unused()
            for mode in ["current", "prototype"]:
                measurement = timed_worker(path, mode)
                out["measurements"].append(measurement)
                print(json.dumps(measurement), flush=True)
            if rows == args.rows[0]:
                out["current_profile"] = timed_worker(path, "current", profile=True)
            args.output.write_text(json.dumps(out, indent=2) + "\n")


if __name__ == "__main__":
    main()
