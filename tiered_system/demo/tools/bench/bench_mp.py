"""Phase 0, multi-process: the real ingest-rate measurement.

The threaded version measured 78% of wall time inside the Python encoder,
which the GIL serializes, so threads cannot help. Each archive file holds a
single 10M-row row group, and the day has 15 files on an 18-core machine,
so the unit of parallelism is the FILE and the mechanism is processes.

Concurrency is NOT free here. The host has 18 cores but the Rancher Desktop
VM that runs InfluxDB has only 6 CPUs and 8 GB; a 15-process run took the
whole VM down (the docker daemon itself stopped answering). The writer
processes are host-side, so --files oversubscribes the VM, not the host.
Keep it at or below 4 and let the container's own cap absorb the rest.

The point of this script is the DECAY CURVE, not a single headline rate:
2025-05-01 has 905,468 distinct series once meta is flattened to tags, and
the question is whether ingest holds up as the index grows. So it samples
rate, engine size and container RSS on a fixed interval and prints marks.

Usage:
  python -I bench_mp.py --files 4 --rows-per-file 1000000 --batch 25000
  python -I bench_mp.py --files 4 --rows-per-file 0 --budget 1800
"""
import argparse
import glob
import gzip
import multiprocessing as mp
import os
from pathlib import Path
import subprocess
import sys
import threading
import time

import pyarrow.parquet as pq
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lineproto import encode_row, PROMOTED_TAGS

ARCHIVE = os.environ.get("DEMO_ARCHIVE", str(Path(__file__).resolve().parents[4] / "data" / "archive"))
URL = "http://localhost:18086"
TOKEN = "bench-token-not-a-secret"
ORG, BUCKET = "waggle", "waggle"
COLS = ["time", "name", *PROMOTED_TAGS, "meta", "value_float", "value_int", "value_str"]


def engine_bytes():
    try:
        r = subprocess.run(["docker", "exec", "bench-influxdb",
                            "du", "-sk", "/var/lib/influxdb2/engine"],
                           capture_output=True, text=True, timeout=120)
        if r.returncode == 0:
            return int(r.stdout.split()[0]) * 1024
    except Exception:
        pass
    return 0


def container_mem():
    """Container RSS in bytes, or 0 if docker is not answering."""
    try:
        r = subprocess.run(
            ["docker", "stats", "--no-stream", "--format", "{{.MemUsage}}",
             "bench-influxdb"], capture_output=True, text=True, timeout=60)
        if r.returncode == 0:
            used = r.stdout.split("/")[0].strip()
            unit = used[-3:].lower()
            num = float(used[:-3])
            return int(num * {"gib": 1 << 30, "mib": 1 << 20, "kib": 1 << 10}.get(unit, 1))
    except Exception:
        pass
    return 0


def series_count():
    """Distinct series currently in the bucket, via the cardinality API."""
    try:
        r = requests.get(
            f"{URL}/api/v2/query", params={"org": ORG},
            headers={"Authorization": f"Token {TOKEN}",
                     "Content-Type": "application/vnd.flux",
                     "Accept": "application/csv"},
            data=f'import "influxdata/influxdb"\n'
                 f'influxdb.cardinality(bucket: "{BUCKET}", '
                 f'start: 1970-01-01T00:00:00Z)',
            timeout=120)
        for line in r.text.splitlines():
            parts = line.split(",")
            if len(parts) > 3 and parts[-1].strip().isdigit():
                return int(parts[-1].strip())
    except Exception:
        pass
    return 0


# Rows written so far, summed across processes. A plain shared counter is
# enough: it is only read for progress, never for correctness.
_progress = None


def _init(counter):
    global _progress
    _progress = counter


def worker(task):
    path, max_rows, batch, deadline = task
    session = requests.Session()
    sent = 0
    encode_s = 0.0
    post_s = 0.0
    buf = []

    def flush():
        nonlocal buf, sent, post_s
        if not buf:
            return
        body = gzip.compress("\n".join(buf).encode(), compresslevel=1)
        t0 = time.time()
        for attempt in range(6):
            r = session.post(
                f"{URL}/api/v2/write",
                params={"org": ORG, "bucket": BUCKET, "precision": "ns"},
                headers={"Authorization": f"Token {TOKEN}",
                         "Content-Encoding": "gzip",
                         "Content-Type": "text/plain; charset=utf-8"},
                data=body, timeout=300)
            if r.status_code in (200, 204):
                break
            if r.status_code == 400:
                return ("error", f"400 encoder bug: {r.text[:400]}")
            time.sleep(min(2 ** attempt * 0.5, 15))
        else:
            return ("error", f"write failed {r.status_code}: {r.text[:200]}")
        post_s += time.time() - t0
        sent += len(buf)
        if _progress is not None:
            with _progress.get_lock():
                _progress.value += len(buf)
        buf = []
        return None

    pf = pq.ParquetFile(path)
    for rb in pf.iter_batches(batch_size=batch, columns=COLS):
        t0 = time.time()
        d = rb.to_pydict()
        times, names, metas = d["time"], d["name"], d["meta"]
        promoted = [d[k] for k in PROMOTED_TAGS]
        vfs, vis, vss = d["value_float"], d["value_int"], d["value_str"]
        for i in range(rb.num_rows):
            ts = times[i]
            ns = ts.value if hasattr(ts, "value") else int(ts.timestamp() * 1e9)
            line = encode_row(names[i], tuple(p[i] for p in promoted),
                              metas[i], vfs[i], vis[i], vss[i], ns)
            if line:
                buf.append(line)
        encode_s += time.time() - t0
        if len(buf) >= batch:
            err = flush()
            if err:
                return err
        if max_rows and sent >= max_rows:
            break
        if deadline and time.time() >= deadline:
            # A time budget, not a row target: the decay curve is the
            # deliverable, and a fixed wall-clock window gives every run
            # the same shape regardless of how far it gets.
            break
    err = flush()
    if err:
        return err
    return ("ok", sent, encode_s, post_s, os.path.basename(path))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--files", type=int, default=4,
                    help="how many of the day's 15 archive files to ingest")
    ap.add_argument("--procs", type=int, default=0,
                    help="concurrent writer processes; default = --files. "
                         "Keep <= 4: the VM has 6 CPUs and 15 writers "
                         "took the docker daemon down.")
    ap.add_argument("--rows-per-file", type=int, default=1_000_000)
    ap.add_argument("--batch", type=int, default=25_000)
    ap.add_argument("--date", default="2025-05-01")
    ap.add_argument("--budget", type=float, default=0,
                    help="wall-clock seconds to ingest for; 0 means no limit")
    ap.add_argument("--sample", type=float, default=60,
                    help="seconds between decay-curve samples")
    args = ap.parse_args()

    files = sorted(glob.glob(f"{ARCHIVE}/date={args.date}/*.parquet"))[:args.files]
    deadline = time.time() + args.budget if args.budget else 0
    tasks = [(f, args.rows_per_file, args.batch, deadline) for f in files]
    procs = min(args.procs or len(files), len(files))
    print(f"date={args.date} files={len(files)} procs={procs} "
          f"rows/file={args.rows_per_file:,} "
          f"batch={args.batch:,} budget={args.budget or 'none'}s", flush=True)

    counter = mp.Value("q", 0)
    d0 = engine_bytes()
    t0 = time.time()

    # Sample on a fixed interval so the marks are comparable across runs.
    # Each mark reports the rate over the PRECEDING interval, not the
    # cumulative average -- a cumulative number hides decay by construction.
    marks = []
    stop = threading.Event()

    def sampler():
        last_rows, last_t = 0, t0
        while not stop.wait(args.sample):
            now = time.time()
            rows = counter.value
            inst = (rows - last_rows) / max(now - last_t, 1e-9)
            eng, mem = engine_bytes(), container_mem()
            marks.append((now - t0, rows, inst, eng, mem))
            print(f"  t={now-t0:7.0f}s  {rows:>12,} rows  "
                  f"{inst:>9,.0f} pts/s (interval)  "
                  f"engine={eng/1e9:5.2f}GB  rss={mem/1e9:4.2f}GB", flush=True)
            last_rows, last_t = rows, now

    th = threading.Thread(target=sampler, daemon=True)
    th.start()
    try:
        with mp.Pool(procs, initializer=_init, initargs=(counter,)) as pool:
            results = pool.map(worker, tasks)
    finally:
        stop.set()
    el = time.time() - t0
    d1 = engine_bytes()

    bad = [r for r in results if r[0] == "error"]
    if bad:
        print("ERRORS:", bad[:2])
        return 1

    total = sum(r[1] for r in results)
    enc = sum(r[2] for r in results)
    post = sum(r[3] for r in results)
    print(f"\n=== RESULT ({procs} processes, {len(files)} files) ===")
    print(f"rows written      {total:,}")
    print(f"elapsed           {el:,.1f}s")
    print(f"AGGREGATE RATE    {total/el:,.0f} pts/s")
    print(f"cpu-time encode   {enc:,.1f}s   post {post:,.1f}s")
    print(f"engine disk       {d0/1e9:.2f} -> {d1/1e9:.2f} GB "
          f"({(d1-d0)/max(total,1):.1f} bytes/row)")

    cards = series_count()
    if cards:
        print(f"series cardinality {cards:,}")

    # The decay curve is the actual deliverable. Compare the first and last
    # full sampling intervals; a sustained drop means a full month of hot
    # tier is not reachable and DEMO_HOT_DAYS has to bound the scope.
    if len(marks) >= 3:
        first, last = marks[0][2], marks[-1][2]
        print(f"\n=== DECAY ===")
        print(f"first interval    {first:,.0f} pts/s")
        print(f"last interval     {last:,.0f} pts/s  "
              f"({100*(last-first)/max(first,1):+.0f}%)")
        rate = last  # project from the degraded rate, not the optimistic one
        basis = "last-interval"
    else:
        rate = total / el
        basis = "whole-run average"

    print(f"\nprojections from the {basis} rate ({rate:,.0f} pts/s):")
    print(f"  146.1M rows (one day):  {146_129_405/max(rate,1)/3600:,.2f} h")
    print(f"  4.37B rows (May 2025):  {4_366_082_870/max(rate,1)/3600:,.1f} h")
    return 0


if __name__ == "__main__":
    sys.exit(main())
