"""Measure the production Go IPC writer and optimized Python decoder locally.

Build the probe from beehive-data-api with `go test -c -o /tmp/arrow-probe.test ./internal/arrowio`.
Pass a saved current-schema API IPC stream and that binary. Fixtures are local,
not month-query replays. No server, Pelican, or NRDStor changes are made.
"""
import argparse
import gc
import hashlib
import importlib
import json
import os
from pathlib import Path
import platform
import psutil
import statistics
import subprocess
import sys
import tempfile
import time

import pandas as pd
import pyarrow as pa

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[2] / "sage-data-client/src"))
client = importlib.import_module("sage_data_client.query")


def monitored(args, env=None):
    with tempfile.TemporaryFile(mode="w+") as log:
        proc = subprocess.Popen(args, env=env, stdout=log, stderr=subprocess.STDOUT)
        monitor = psutil.Process(proc.pid)
        peak = 0
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
    return output, peak


def decoded_summary(path):
    # Digest the complete reconstructed dataframe, including column names and
    # scalar dtypes, so compressed Go output is checked against uncompressed.
    start_wall, start_cpu = time.perf_counter(), time.process_time()
    with path.open("rb") as stream:
        frame = client._load_arrow(stream)
    elapsed, cpu = time.perf_counter() - start_wall, time.process_time() - start_cpu
    digest = hashlib.sha256(pd.util.hash_pandas_object(frame, index=False).values.tobytes()).hexdigest()
    return dict(decode_seconds=elapsed, decode_cpu_seconds=cpu, rows=len(frame),
                dataframe_bytes=int(frame.memory_usage(deep=True).sum()), digest=digest,
                columns=list(frame), dtypes={key: str(value) for key, value in frame.dtypes.items()})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("sample", type=Path)
    parser.add_argument("--probe-binary", type=Path)
    parser.add_argument("--rows", nargs="+", type=int, default=[200000, 1000000])
    parser.add_argument("--batch-sizes", nargs="+", type=int, default=[2048, 65536])
    parser.add_argument("--reps", type=int, default=3)
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--build-note", default="probe built against workspace go.mod")
    parser.add_argument("--output", type=Path, default=HERE / "results/exp59_arrow_ipc_compression.json")
    args = parser.parse_args()
    if args.worker:
        print(json.dumps(decoded_summary(args.sample)), flush=True)
        return
    if args.probe_binary is None:
        parser.error("--probe-binary is required")
    if args.reps < 1 or any(n < 1 for n in args.rows + args.batch_sizes):
        parser.error("rows, batch sizes, and repetitions must be positive")
    sample = pa.ipc.open_stream(args.sample).read_all()
    if not len(sample):
        parser.error("sample must contain rows")
    try:
        build_info = subprocess.check_output(["go", "version", "-m", str(args.probe_binary.resolve())], text=True)
    except (OSError, subprocess.CalledProcessError):
        build_info = None
    out = dict(sample=str(args.sample), sample_rows=len(sample),
               sample_unique_metadata=len(sample["meta"].unique()),
               pandas=pd.__version__, pyarrow=pa.__version__, platform=platform.platform(),
               probe_build_note=args.build_note, reps=args.reps,
               probe_build_info=build_info,
               scope="local Go IPC encoding and Python decoding; no query execution, network, or month replay",
               peak_rss_scope="whole subprocess, including imports and validation; 5 ms parent sampling",
               measurements=[])
    with tempfile.TemporaryDirectory(prefix="sage-arrow-compression-") as directory:
        root = Path(directory)
        source, encoded, report = [root / name for name in ["source.arrows", "encoded.arrows", "report.json"]]
        for rows in args.rows:
            for cardinality in ["repeated", "unique"]:
                table = pa.concat_tables([sample] * ((rows + len(sample) - 1) // len(sample))).slice(0, rows).combine_chunks()
                if cardinality == "unique":
                    original = [json.loads(raw) or {} for raw in sample["meta"].to_pylist()]
                    tags = pa.array([json.dumps(dict(original[i % len(sample)],
                        experiment_unique=hashlib.sha256(str(i).encode()).hexdigest()), separators=(",", ":"))
                        for i in range(rows)], type=pa.string())
                    table = table.set_column(3, "meta", tags)
                    del tags, original
                for batch_size in args.batch_sizes:
                    with pa.ipc.new_stream(source, table.schema) as writer:
                        writer.write_table(table, max_chunksize=batch_size)
                    gc.collect()
                    pa.default_memory_pool().release_unused()
                    measured = {mode: [] for mode in ["none", "zstd", "lz4"]}
                    reference = None
                    # Rotate mode order across repetitions to reduce ordering bias.
                    modes = list(measured)
                    for rep in range(args.reps):
                        for mode in modes[rep % len(modes):] + modes[:rep % len(modes)]:
                            env = dict(os.environ, ARROW_PROBE_INPUT=str(source),
                                       ARROW_PROBE_OUTPUT=str(encoded), ARROW_PROBE_REPORT=str(report),
                                       ARROW_PROBE_MODE=mode)
                            _, peak_encoder = monitored([str(args.probe_binary.resolve()),
                                "-test.run=^TestArrowCompressionProbe$", "-test.count=1"], env)
                            run = json.loads(report.read_text())
                            raw, peak_decoder = monitored([sys.executable, "-I", str(Path(__file__).resolve()),
                                str(encoded), "--worker"])
                            decode = json.loads(raw)
                            signature = {key: decode[key] for key in ["rows", "digest", "columns", "dtypes"]}
                            if reference is None:
                                reference = signature
                            if signature != reference:
                                raise AssertionError("decoded dataframe changed across compression modes")
                            run.update(decode)
                            run.update(encoder_peak_rss_bytes=peak_encoder, decoder_peak_rss_bytes=peak_decoder)
                            measured[mode].append(run)
                    for mode, runs in measured.items():
                        record = dict(mode=mode, rows=rows, metadata=cardinality, batch_size=batch_size,
                                      dataframe_matches_uncompressed=True, runs=runs)
                        for field in ["bytes", "encode_seconds", "encode_cpu_seconds", "first_batch_seconds",
                                      "decode_seconds", "decode_cpu_seconds", "dataframe_bytes"]:
                            record[field] = statistics.median(run[field] for run in runs)
                        for field in ["encoder_peak_rss_bytes", "decoder_peak_rss_bytes"]:
                            record[field] = max(run[field] for run in runs)
                        record["local_encode_plus_decode_seconds"] = record["encode_seconds"] + record["decode_seconds"]
                        out["measurements"].append(record)
                        print(json.dumps({k: v for k, v in record.items() if k != "runs"}), flush=True)
                    args.output.parent.mkdir(parents=True, exist_ok=True)
                    args.output.write_text(json.dumps(out, indent=2) + "\n")
                del table
                gc.collect()
                pa.default_memory_pool().release_unused()


if __name__ == "__main__":
    main()
