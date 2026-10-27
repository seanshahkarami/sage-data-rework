"""Measure the Arrow codec selected by the running API.

Short hot/cold windows run by default; --include-months adds March and May.
Each measured request runs in a fresh Python process. The codec comes from
ARROW_COMPRESSION on the server. The script reads X-Arrow-Compression from
the response and does not send a request override. No remote storage is written.
"""
import argparse
from email.message import Message
import importlib
import json
from pathlib import Path
import resource
import statistics
import subprocess
import sys
import time
import tempfile
from urllib.request import Request, urlopen
from unittest.mock import patch

import numpy as np
import pandas as pd
import psutil
import pyarrow.ipc as ipc

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[2] / "sage-data-client/src"))
client = importlib.import_module("sage_data_client.query")
# Same windows as exp52/exp58; keep this harness independent of their Docker
# and DuckDB dependencies.
ONE = {"name": "wxt.env.temp", "vsn": "W0A0"}
ALL = {"vsn": "W0A0"}
CASES = [
    ("hot 1 min, one name", "2025-05-01T00:00:00Z", "2025-05-01T00:01:00Z", ONE, False, "180"),
    ("hot 1 hour, one name", "2025-05-01T00:00:00Z", "2025-05-01T01:00:00Z", ONE, False, "180"),
    ("hot 6 hours, one name", "2025-05-01T00:00:00Z", "2025-05-01T06:00:00Z", ONE, False, "300"),
    ("hot 1 min, all names", "2025-05-01T00:00:00Z", "2025-05-01T00:01:00Z", ALL, False, "180"),
    ("hot 1 hour, all names", "2025-05-01T00:00:00Z", "2025-05-01T01:00:00Z", ALL, False, "300"),
    ("cold 1 min, one name", "2025-03-01T00:00:00Z", "2025-03-01T00:01:00Z", ONE, False, "180"),
    ("cold 1 hour, one name", "2025-03-01T00:00:00Z", "2025-03-01T01:00:00Z", ONE, False, "180"),
    ("cold 1 min, all names", "2025-03-01T00:00:00Z", "2025-03-01T00:01:00Z", ALL, False, "180"),
    ("cold 1 hour, all names", "2025-03-01T00:00:00Z", "2025-03-01T01:00:00Z", ALL, False, "300"),
    ("hot May, one name", "2025-05-01T00:00:00Z", "2025-06-01T00:00:00Z", ONE, True, "1200"),
    ("cold March, one name", "2025-03-01T00:00:00Z", "2025-04-01T00:00:00Z", ONE, True, "1200"),
]


class CountingReader:
    def __init__(self, response):
        self.raw = response
        self.n = 0
        self.first = None

    def read(self, size=-1):
        raw = self.raw.read(size)
        if raw and self.first is None:
            self.first = time.perf_counter()
        self.n += len(raw)
        return raw

    def readinto(self, buffer):
        raw = self.read(len(buffer))
        buffer[:len(raw)] = raw
        return len(raw)

    def readable(self):
        return True

    @property
    def closed(self):
        return self.raw.closed

    def close(self):
        self.raw.close()


def once(endpoint, case, arrow_schema=1):
    label, start, end, filters, month, timeout = case
    request = Request(endpoint, json.dumps(dict(start=start, end=end, filter=filters)).encode(),
        {"Content-Type": "application/json", "Accept": ("application/vnd.apache.arrow.stream; sage-schema=2"
            if arrow_schema == 2 else "application/vnd.apache.arrow.stream"),
         "Accept-Encoding": "identity"})
    first_batch = None
    native_open = ipc.open_stream

    class TimedBatches:
        def __init__(self, stream):
            self.reader = native_open(stream)

        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.reader.close()

        def __iter__(self):
            nonlocal first_batch
            for batch in self.reader:
                if first_batch is None:
                    first_batch = time.perf_counter()
                yield batch

    faults = resource.getrusage(resource.RUSAGE_SELF).ru_majflt
    begin, cpu = time.perf_counter(), time.process_time()
    with urlopen(request, timeout=10800 if month else int(timeout)) as response:
        mode = response.headers.get("X-Arrow-Compression")
        if mode not in ("none", "zstd", "lz4"):
            raise RuntimeError("server did not identify its Arrow IPC codec")
        content_type = response.headers.get("Content-Type", "")
        media = Message()
        media["Content-Type"] = content_type
        selected_schema = int(media.get_param("sage-schema", "1"))
        if selected_schema != arrow_schema:
            raise RuntimeError("server did not return the requested experimental Arrow schema")
        if content_type.split(";")[0] != "application/vnd.apache.arrow.stream":
            raise RuntimeError("server did not return Arrow IPC")
        if response.headers.get("Content-Encoding", "identity") != "identity":
            raise RuntimeError("unexpected outer HTTP compression")
        counted = CountingReader(response)
        with patch.object(ipc, "open_stream", TimedBatches):
            frame = client._load_arrow(counted)
    elapsed, cpu = time.perf_counter() - begin, time.process_time() - cpu
    faults = resource.getrusage(resource.RUSAGE_SELF).ru_majflt - faults
    # ORDER BY timestamp does not guarantee tie ordering. Compare row content
    # independently of ordering using full-frame uint64 sum/XOR checksums.
    hashes = pd.util.hash_pandas_object(frame, index=False).to_numpy()
    return dict(mode=mode, arrow_schema=selected_schema, query=label, rows=len(frame), bytes=counted.n,
        total_seconds=elapsed, client_cpu_seconds=cpu, client_major_faults=faults,
        first_body_seconds=None if counted.first is None else counted.first-begin,
        first_batch_seconds=None if first_batch is None else first_batch-begin,
        dataframe_bytes=int(frame.memory_usage(deep=True).sum()),
        row_hash_sum=str(int(hashes.sum(dtype=np.uint64))),
        row_hash_xor=str(int(np.bitwise_xor.reduce(hashes, initial=np.uint64(0)))),
        columns=list(frame), dtypes={key:str(value) for key,value in frame.dtypes.items()})


def measured(endpoint, case):
    with tempfile.TemporaryFile(mode="w+") as log:
        proc = subprocess.Popen([sys.executable, "-I", str(Path(__file__).resolve()),
            "--endpoint", endpoint, "--worker", "--case-json", json.dumps(case)],
            stdout=log, stderr=subprocess.STDOUT)
        monitor, peak = psutil.Process(proc.pid), 0
        while proc.poll() is None:
            try:
                peak = max(peak, monitor.memory_info().rss)
            except psutil.NoSuchProcess:
                break
            time.sleep(0.02)
        code = proc.wait()
        log.seek(0)
        raw = log.read()
    if code:
        raise RuntimeError(raw)
    result = json.loads(raw)
    result["client_peak_rss_bytes"] = peak
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", default="http://127.0.0.1:10000/api/v1/query")
    parser.add_argument("--include-months", action="store_true")
    parser.add_argument("--match", default="", help="only labels containing this text")
    parser.add_argument("--reps", type=int, default=3)
    parser.add_argument("--month-reps", type=int, default=1)
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--case-json")
    parser.add_argument("--output", type=Path, default=HERE / "results/exp60_arrow_compression_live.json")
    args = parser.parse_args()
    if args.worker:
        print(json.dumps(once(args.endpoint, json.loads(args.case_json))), flush=True)
        return
    if min(args.reps, args.month_reps) < 1:
        parser.error("repetition counts must be positive")
    cases = [case for case in CASES if (args.include_months or not case[4]) and args.match in case[0]]
    if not cases:
        parser.error("no matching cases")
    out = dict(endpoint=args.endpoint, pandas=pd.__version__, client=str(client.__file__),
        scope="live IPC compression; same queries and optimized dataframe decoder, fresh process per request",
        rss_scope="whole client subprocess including imports and row checksum validation, sampled every 20 ms",
        server_metrics="not sampled; collect API CPU and memory externally during this experiment",
        results=[])
    for case in cases:
        if not case[4]:
            measured(args.endpoint, case)
        runs = []
        reference = None
        reps = args.month_reps if case[4] else args.reps
        for rep in range(reps):
            run = measured(args.endpoint, case)
            signature = {key:run[key] for key in ["rows", "row_hash_sum", "row_hash_xor", "columns", "dtypes"]}
            if reference is None:
                reference = signature
            if signature != reference:
                raise AssertionError(f"query rows/content changed between repetitions: {case[0]}")
            runs.append(run)
            print(f"{case[0]} {run['mode']}: {run['rows']} rows, {run['bytes']} bytes, {run['total_seconds']:.3f}s", flush=True)
            out["results"].append(dict(case=case, repetition=rep+1, **run))
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(out, indent=2) + "\n")
        print(f"median {case[0]} {runs[0]['mode']}: {statistics.median(run['total_seconds'] for run in runs):.3f}s", flush=True)


if __name__ == "__main__":
    main()
