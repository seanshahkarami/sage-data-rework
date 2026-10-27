"""Compare negotiated typed values with JSON-valued Arrow, keeping Zstd fixed.

Uses actual cold SQL projections exported by a Go test probe, local DuckDB
fixtures, the production Go IPC writer, and the optimized Python client. These
are controlled local costs, not Iceberg scans or a live month replay.
Build the probe with `go test -c -o /tmp/sage-arrow-typed.test` in beehive-data-api.
"""
import argparse
import gc
import importlib
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys
import tempfile
import time

import duckdb
import pandas as pd
import pyarrow as pa

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from exp59_arrow_ipc_compression import monitored, decoded_summary, client


def sql_worker(sql_path, output):
    root = output.parent
    connection = duckdb.connect(config={"threads": 2, "memory_limit": "512MB",
                                       "temp_directory": str(root / "spill")})
    begin, cpu = time.perf_counter(), time.process_time()
    result = connection.execute(sql_path.read_text()).to_arrow_reader(batch_size=2048)
    first = None
    count = 0
    with pa.ipc.new_stream(output, result.schema) as writer:
        for batch in result:
            if first is None:
                first = time.perf_counter() - begin
            writer.write_batch(batch)
            count += len(batch)
    print(json.dumps(dict(sql_and_ipc_seconds=time.perf_counter()-begin,
                          sql_and_ipc_cpu_seconds=time.process_time()-cpu,
                          first_python_duckdb_batch_seconds=first, rows=count)), flush=True)
    connection.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--probe-binary", type=Path)
    parser.add_argument("--rows", type=int, default=1000000)
    parser.add_argument("--reps", type=int, default=3)
    parser.add_argument("--worker", choices=["sql", "decode"])
    parser.add_argument("--input", type=Path)
    parser.add_argument("--encoded", type=Path)
    parser.add_argument("--build-note", default="workspace pinned dependencies")
    parser.add_argument("--output", type=Path, default=HERE / "results/exp61_arrow_typed_values.json")
    args = parser.parse_args()
    if args.worker == "sql":
        sql_worker(args.input, args.encoded)
        return
    if args.worker == "decode":
        print(json.dumps(decoded_summary(args.input)), flush=True)
        return
    if args.probe_binary is None or min(args.rows, args.reps) < 1:
        parser.error("--probe-binary and positive row/repetition counts are required")
    probe = str(args.probe_binary.resolve())
    out = dict(rows=args.rows, reps=args.reps, pandas=pd.__version__, pyarrow=pa.__version__,
               duckdb=duckdb.__version__, build_note=args.build_note,
               probe_build_info=subprocess.check_output(["go", "version", "-m", probe], text=True),
               scope="local cold projection/IPC, production Go Zstd encoding, client decoding; no Iceberg/network",
               measurements=[])
    with tempfile.TemporaryDirectory(prefix="sage-arrow-typed-") as directory:
        root = Path(directory)
        env = dict(os.environ, ARROW_TYPED_FIXTURE_DIR=str(root))
        monitored([probe, "-test.run=^TestTypedArrowInteropProbe$", "-test.count=1"], env)
        frames = []
        for name in ["legacy", "typed"]:
            with (root / f"{name}.arrows").open("rb") as stream:
                frames.append(client._load_arrow(stream))
        pd.testing.assert_frame_equal(*frames)
        out["actual_go_mixed_values_match"] = True
        del frames
        meta = "MAP {'vsn':'W0A0','plugin':'test:1.0','sensor':'vaisala-wxt536','units':'C','node':'0000000000000001'}"
        for fixture in ["repeated", "unique", "mixed"]:
            if fixture == "repeated":
                values = "(i % 7) * 0.1 + 0.125 AS value_float, NULL::BIGINT AS value_int, NULL::VARCHAR AS value_str"
            elif fixture == "unique":
                values = "i * 0.1 + 0.125 AS value_float, NULL::BIGINT AS value_int, NULL::VARCHAR AS value_str"
            else:
                values = """CASE WHEN i%3=0 THEN i*0.1+0.125 ELSE NULL END::DOUBLE AS value_float,
                            CASE WHEN i%3=1 THEN 9007199254740993+i ELSE NULL END::BIGINT AS value_int,
                            CASE WHEN i%3=2 THEN 's'||i::VARCHAR ELSE NULL END AS value_str"""
            inner = f"""SELECT TIMESTAMPTZ '2025-03-01' + ({args.rows}-i) * INTERVAL '1 microsecond' AS timestamp,
                        'wxt.env.temp' AS name, {values}, {meta} AS meta FROM range({args.rows}) t(i)"""
            reference = None
            measured = {schema: [] for schema in ["legacy", "typed"]}
            for repetition in range(args.reps):
                modes = ["legacy", "typed"] if repetition % 2 == 0 else ["typed", "legacy"]
                for schema in modes:
                    sql = (root / f"{schema}.sql").read_text().replace("__SAGE_TYPED_INNER__", inner)
                    statement, source, encoded, report = [root / name for name in ["statement.sql", "source.arrows", "encoded.arrows", "report.json"]]
                    statement.write_text(sql)
                    raw, sql_peak = monitored([sys.executable, "-I", str(Path(__file__).resolve()),
                        "--worker", "sql", "--input", str(statement), "--encoded", str(source)])
                    record = json.loads(raw)
                    env = dict(os.environ, ARROW_PROBE_INPUT=str(source), ARROW_PROBE_OUTPUT=str(encoded),
                               ARROW_PROBE_REPORT=str(report), ARROW_PROBE_MODE="zstd",
                               ARROW_PROBE_SCHEMA="2" if schema == "typed" else "1")
                    _, encode_peak = monitored([probe, "-test.run=^TestArrowCompressionProbe$", "-test.count=1"], env)
                    record.update(json.loads(report.read_text()))
                    raw, decode_peak = monitored([sys.executable, "-I", str(Path(__file__).resolve()),
                        "--worker", "decode", "--input", str(encoded)])
                    decoded = json.loads(raw)
                    signature = {key: decoded[key] for key in ["rows", "digest", "columns", "dtypes"]}
                    if reference is None:
                        reference = signature
                    if signature != reference:
                        raise AssertionError(f"legacy/typed dataframe mismatch for {fixture}")
                    record.update(decoded)
                    record.update(sql_peak_rss_bytes=sql_peak, encoder_peak_rss_bytes=encode_peak,
                                  decoder_peak_rss_bytes=decode_peak)
                    measured[schema].append(record)
            for schema, runs in measured.items():
                record = dict(schema=schema, fixture=fixture, rows=args.rows, dataframe_matches=True, runs=runs)
                for key in ["sql_and_ipc_seconds", "sql_and_ipc_cpu_seconds", "first_python_duckdb_batch_seconds",
                            "bytes", "encode_seconds", "encode_cpu_seconds", "first_batch_seconds",
                            "decode_seconds", "decode_cpu_seconds", "dataframe_bytes"]:
                    record[key] = statistics.median(run[key] for run in runs)
                for key in ["sql_peak_rss_bytes", "encoder_peak_rss_bytes", "decoder_peak_rss_bytes"]:
                    record[key] = max(run[key] for run in runs)
                out["measurements"].append(record)
                print(json.dumps({key:value for key,value in record.items() if key != "runs"}), flush=True)
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(out, indent=2) + "\n")
            gc.collect()
            pa.default_memory_pool().release_unused()


if __name__ == "__main__":
    main()
