"""Profile actual cold response SQL without changing the public wire schema.

Build the Go probe in beehive-data-api with `go test -c -o /tmp/cold-execution.test ./internal/backend/cold`.
Real archive Parquet files are read-only. SQL, profiles, spill and IPC files live
in an isolated local temporary directory. No network or remote writes.
"""
import argparse
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys
import tempfile
import time

import duckdb
import numpy as np
import pandas as pd
import pyarrow as pa

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from exp59_arrow_ipc_compression import monitored

MARKER = "iceberg_scan('__SAGE_METADATA__')"


def checksums(path):
    count, summed, xor, last = 0, 0, 0, None
    with path.open("rb") as stream:
        reader = pa.ipc.open_stream(stream)
        schema = str(reader.schema)
        for batch in reader:
            stamp = batch.column("timestamp").cast(pa.int64()).to_numpy()
            if len(stamp) and (np.any(stamp[1:] < stamp[:-1]) or (last is not None and stamp[0] < last)):
                raise AssertionError("timestamp ordering changed")
            if len(stamp): last = stamp[-1]
            hashes = pd.util.hash_pandas_object(batch.to_pandas(), index=False).to_numpy()
            count += len(batch)
            summed = (summed + int(hashes.sum(dtype=np.uint64))) % (2**64)
            xor ^= int(np.bitwise_xor.reduce(hashes, initial=np.uint64(0)))
    return dict(rows=count, row_hash_sum=str(summed), row_hash_xor=str(xor), schema=schema)


def operators(profile):
    result = []
    def walk(node):
        if node.get("operator_name"):
            result.append(dict(name=node["operator_name"], seconds=node.get("operator_timing"),
                rows=node.get("operator_cardinality"), rows_scanned=node.get("operator_rows_scanned"),
                extra=node.get("extra_info")))
        for child in node.get("children", []): walk(child)
    walk(profile)
    return result


def worker(sql_path, memory):
    root = sql_path.parent
    profile_path, output = root / "profile.json", root / "body.arrows"
    connection = duckdb.connect(config={"threads":2, "memory_limit":memory,
        "temp_directory":str(root / "spill")})
    connection.execute("SET TimeZone='UTC'")
    sql = sql_path.read_text()
    plan = connection.execute("EXPLAIN " + sql).fetchall()[0][1]
    connection.execute("SET enable_profiling='json'")
    connection.execute("SET profiling_output='" + str(profile_path).replace("'", "''") + "'")
    begin, cpu = time.perf_counter(), time.process_time()
    result = connection.execute(sql).to_arrow_reader(batch_size=2048)
    first = None
    with pa.ipc.new_stream(output, result.schema) as writer:
        for batch in result:
            if first is None: first = time.perf_counter()-begin
            writer.write_batch(batch)
    elapsed, cpu = time.perf_counter()-begin, time.process_time()-cpu
    connection.close()
    profile = json.loads(profile_path.read_text())
    report = dict(seconds=elapsed, cpu_seconds=cpu, first_batch_seconds=first,
        ipc_bytes=output.stat().st_size, operators=operators(profile), plan=plan,
        duckdb_buffer_peak_bytes=profile.get("system_peak_buffer_memory"),
        spill_peak_bytes=profile.get("system_peak_temp_dir_size"))
    report.update(checksums(output))
    output.unlink()
    print(json.dumps(report), flush=True)


def semantic_checks(root):
    """Execute exact exported SQL over nulls, collisions, escaping and head/tail."""
    connection = duckdb.connect()
    connection.execute("SET TimeZone='UTC'")
    metadata = [None, {}, {"sensor":"x", "units":"C"}, {"plugin":"old", "sensor":"x"},
        {"node":None, "x":"null stays null"}, {"quote":"中文\"\\\n", "__proto__":"plain string"},
        {"VSN":"case sensitive", "vsn":"raw"}, {"x":None}]
    n = 240
    columns = {
        "time":pa.array([1740787200000000+i for i in range(n)], type=pa.timestamp("us", tz="UTC")),
        "name":pa.array(["wxt.env.temp"]*n),
        "value_float":pa.array([i/10 if i%3==0 else None for i in range(n)], type=pa.float64()),
        "value_int":pa.array([9007199254740993+i if i%3==1 else None for i in range(n)], type=pa.int64()),
        "value_str":pa.array(["s"+str(i) if i%3==2 else None for i in range(n)], type=pa.string()),
        "meta":pa.array([list((metadata[i%len(metadata)] or {}).items()) if metadata[i%len(metadata)] is not None else None for i in range(n)], type=pa.map_(pa.string(),pa.string())),
    }
    for j, tag in enumerate(["plugin","vsn","node","host","task","job","zone"]):
        columns[tag] = pa.array([None if (i+j)%3==0 else ("" if (i+j)%5==0 else tag+str(i%2)) for i in range(n)], type=pa.string())
    connection.register("obs", pa.table(columns))
    checks = []
    for case in ["semantics", "head", "tail"]:
        for fmt in ["", "_ndjson"]:
            frames = []
            for mode in ["baseline", "optimized"]:
                sql = (root / f"{case}-{mode}{fmt}.sql").read_text().replace(MARKER, "obs")
                frames.append(connection.execute(sql).to_arrow_table())
            if not frames[0].equals(frames[1]):
                raise AssertionError(f"metadata/scalar/schema mismatch: {case}{fmt}")
            checks.append(dict(case=case, format="arrow" if not fmt else "ndjson projection", rows=len(frames[0]), exact_arrow_table_matches=True))
    connection.close()
    return checks


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--probe-binary", type=Path)
    parser.add_argument("--archive-day", type=Path, default=HERE.parents[1] / "data/archive/date=2025-03-01")
    parser.add_argument("--reps", type=int, default=3)
    parser.add_argument("--memory", default="512MB")
    parser.add_argument("--stress-rows", type=int, default=2000000)
    parser.add_argument("--include-delayed", action="store_true")
    parser.add_argument("--worker", type=Path)
    parser.add_argument("--build-note", default="workspace pinned Go dependencies")
    parser.add_argument("--output", type=Path, default=HERE / "results/exp63_cold_execution.json")
    args = parser.parse_args()
    if args.worker:
        worker(args.worker, args.memory)
        return
    if args.probe_binary is None or args.reps < 1 or args.stress_rows < 1:
        parser.error("probe binary and positive repetitions/stress rows are required")
    files = sorted(args.archive_day.glob("*.parquet"))
    if not files: parser.error("archive day has no local Parquet files")
    start, end = "2025-03-01T00:00:00Z", "2025-03-02T00:00:00Z"
    query = dict(start=start, end=end)
    cases = [dict(Name="day", Query=dict(query, filter={"name":"wxt.env.temp", "vsn":"W0A0"})),
             dict(Name="minute", Query=dict(query, end="2025-03-01T00:01:00Z", filter={"name":"wxt.env.temp", "vsn":"W0A0"})),
             dict(Name="semantics", Query=query), dict(Name="head", Query=dict(query, head=1)),
             dict(Name="tail", Query=dict(query, tail=1)), dict(Name="stress", Query=query)]
    out = dict(duckdb=duckdb.__version__, pyarrow=pa.__version__, pandas=pd.__version__,
        archive_day=str(args.archive_day.resolve()), archive_files=len(files),
        reps=args.reps, memory_limit=args.memory, threads=2, build_note=args.build_note,
        probe_build_info=subprocess.check_output(["go","version","-m",str(args.probe_binary.resolve())],text=True),
        scope="local archive Parquet and synthetic spill stress; SQL + uncompressed IPC, no Iceberg/network/Zstd",
        memory_scope="whole subprocess RSS includes imports and post-timing checksum validation",
        measurements=[])
    with tempfile.TemporaryDirectory(prefix="sage-cold-execution-") as directory:
        root = Path(directory)
        query_file = root / "queries.json"
        query_file.write_text(json.dumps(cases))
        env = dict(os.environ, COLD_EXECUTION_PROBE_DIR=str(root), COLD_EXECUTION_QUERIES=str(query_file))
        monitored([str(args.probe_binary.resolve()), "-test.run=^TestColdExecutionProbe$", "-test.count=1"], env)
        out["semantic_checks"] = semantic_checks(root)
        archive_scan = "read_parquet([" + ",".join("'"+str(p.resolve()).replace("'","''")+"'" for p in files) + "], hive_partitioning=false)"
        n = args.stress_rows
        stress = f"""(SELECT TIMESTAMPTZ '2025-03-01' + ({n}-i)*INTERVAL '1 microsecond' AS time,
           'wxt.env.temp' AS name, i*0.1+0.125 AS value_float, NULL::BIGINT AS value_int, NULL::VARCHAR AS value_str,
           MAP {{'sensor':'vaisala','unique':repeat(md5(i::VARCHAR),4)}} AS meta,
           'test:1.0' AS plugin, 'W0A0' AS vsn, NULL::VARCHAR AS node, NULL::VARCHAR AS host,
           NULL::VARCHAR AS task,NULL::VARCHAR AS job,NULL::VARCHAR AS zone FROM range({n}) t(i))"""
        for case in ["day", "minute", "stress"]:
            modes = ["baseline","optimized"] + (["delayed_json"] if args.include_delayed else [])
            measured = {mode:[] for mode in modes}
            reference = None
            for rep in range(args.reps):
                for mode in modes[rep%len(modes):] + modes[:rep%len(modes)]:
                    sql = (root/f"{case}-{mode}.sql").read_text().replace(MARKER, stress if case=="stress" else archive_scan)
                    statement = root/"statement.sql"
                    statement.write_text(sql)
                    raw, peak = monitored([sys.executable,"-I",str(Path(__file__).resolve()),"--worker",str(statement),"--memory",args.memory])
                    run = json.loads(raw)
                    signature = {key:run[key] for key in ["rows","row_hash_sum","row_hash_xor","schema"]}
                    if reference is None: reference=signature
                    if signature!=reference: raise AssertionError(f"row/schema/JSON text mismatch for {case}")
                    run["peak_rss_bytes"] = peak
                    measured[mode].append(run)
                    print(f"{case} {mode} #{rep+1}: rows={run['rows']} total={run['seconds']:.3f}s first={run['first_batch_seconds']:.3f}s rss={peak/1e6:.0f}MB spill={run['spill_peak_bytes']/1e6:.0f}MB",flush=True)
            for mode,runs in measured.items():
                summary = dict(case=case, mode=mode, rows=runs[0]["rows"], rows_schema_exact_json_match=True,runs=runs)
                for key in ["seconds","cpu_seconds","first_batch_seconds","ipc_bytes"]:
                    summary[key]=statistics.median(run[key] for run in runs)
                for key in ["peak_rss_bytes","duckdb_buffer_peak_bytes","spill_peak_bytes"]:
                    summary[key]=max(run[key] for run in runs)
                out["measurements"].append(summary)
            args.output.parent.mkdir(parents=True,exist_ok=True)
            args.output.write_text(json.dumps(out,indent=2)+"\n")


if __name__=="__main__": main()
