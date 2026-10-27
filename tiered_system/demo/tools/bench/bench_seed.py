"""Phase 0: measure real InfluxDB ingest rate and cardinality growth.

Seeds a slice of 2025-05-01 into the throwaway bench InfluxDB and reports
sustained points/sec, TSI/TSM growth and whether the rate decays as
cardinality climbs. That decay is the thing that decides whether a full
month of hot tier is reachable at all.

Usage:
  python -I bench_seed.py --rows 2000000 --batch 20000 --workers 4
"""
import argparse
import gzip
import os
from pathlib import Path
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import pyarrow.parquet as pq
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lineproto import encode_row, PROMOTED_TAGS

ARCHIVE = os.environ.get("DEMO_ARCHIVE", str(Path(__file__).resolve().parents[4] / "data" / "archive"))
URL = "http://localhost:18086"
TOKEN = "bench-token-not-a-secret"
ORG, BUCKET = "waggle", "waggle"
COLS = ["time", "name", *PROMOTED_TAGS, "meta", "value_float", "value_int", "value_str"]


def container_disk():
    """TSM + TSI bytes inside the engine dir, measured in-container."""
    out = {}
    for label, path in (("tsm", "/var/lib/influxdb2/engine/data"),
                        ("tsi", "/var/lib/influxdb2/engine/data")):
        try:
            r = subprocess.run(
                ["docker", "exec", "bench-influxdb", "du", "-sk", path],
                capture_output=True, text=True, timeout=60)
            if r.returncode == 0:
                out[label] = int(r.stdout.split()[0]) * 1024
        except Exception:
            pass
    return out.get("tsm", 0)


def write_batch(session, lines):
    body = gzip.compress("\n".join(lines).encode(), compresslevel=1)
    for attempt in range(6):
        r = session.post(
            f"{URL}/api/v2/write",
            params={"org": ORG, "bucket": BUCKET, "precision": "ns"},
            headers={"Authorization": f"Token {TOKEN}",
                     "Content-Encoding": "gzip",
                     "Content-Type": "text/plain; charset=utf-8"},
            data=body, timeout=180)
        if r.status_code in (200, 204):
            return len(lines)
        if r.status_code == 400:
            # malformed line protocol is a bug in the encoder, not transient
            raise SystemExit(f"400 from InfluxDB (encoder bug): {r.text[:600]}")
        time.sleep(min(2 ** attempt * 0.5, 15))
    raise SystemExit(f"write failed after retries: {r.status_code} {r.text[:300]}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=2_000_000)
    ap.add_argument("--batch", type=int, default=20_000)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--date", default="2025-05-01")
    args = ap.parse_args()

    files = sorted(__import__("glob").glob(f"{ARCHIVE}/date={args.date}/*.parquet"))
    print(f"date={args.date} files={len(files)} target_rows={args.rows:,} "
          f"batch={args.batch:,} workers={args.workers}", flush=True)

    session = requests.Session()
    disk0 = container_disk()
    t_start = time.time()
    sent = 0
    encode_s = 0.0
    marks = []

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        pending = []
        buf = []
        for f in files:
            if sent >= args.rows:
                break
            pf = pq.ParquetFile(f)
            for rb in pf.iter_batches(batch_size=args.batch, columns=COLS):
                t0 = time.time()
                d = rb.to_pydict()
                times = d["time"]
                names = d["name"]
                promoted = [d[k] for k in PROMOTED_TAGS]
                metas = d["meta"]
                vfs, vis, vss = d["value_float"], d["value_int"], d["value_str"]
                for i in range(rb.num_rows):
                    ts = times[i]
                    ns = ts.value if hasattr(ts, "value") else int(ts.timestamp() * 1e9)
                    line = encode_row(names[i], tuple(p[i] for p in promoted),
                                      metas[i], vfs[i], vis[i], vss[i], ns)
                    if line:
                        buf.append(line)
                encode_s += time.time() - t0

                if len(buf) >= args.batch:
                    pending.append(pool.submit(write_batch, session, buf))
                    buf = []
                    # keep the queue bounded so memory stays flat
                    while len(pending) >= args.workers * 2:
                        sent += pending.pop(0).result()
                        if sent % (args.batch * 20) < args.batch:
                            el = time.time() - t_start
                            marks.append((sent, el, container_disk()))
                            print(f"  {sent:>12,} rows  {el:7.1f}s  "
                                  f"{sent/el:>10,.0f} pts/s  "
                                  f"disk={marks[-1][2]/1e9:.2f} GB", flush=True)
                if sent >= args.rows:
                    break
        if buf:
            pending.append(pool.submit(write_batch, session, buf))
        for p in pending:
            sent += p.result()

    el = time.time() - t_start
    disk1 = container_disk()
    print(f"\n=== RESULT ===")
    print(f"rows written      {sent:,}")
    print(f"elapsed           {el:,.1f}s")
    print(f"sustained rate    {sent/el:,.0f} pts/s")
    print(f"encode time       {encode_s:,.1f}s ({100*encode_s/el:.0f}% of wall)")
    print(f"engine disk       {disk0/1e9:.2f} -> {disk1/1e9:.2f} GB "
          f"({(disk1-disk0)/max(sent,1):.1f} bytes/row)")
    if len(marks) >= 3:
        first = marks[0][0] / marks[0][1]
        last = (marks[-1][0] - marks[-3][0]) / max(marks[-1][1] - marks[-3][1], 1e-9)
        print(f"rate decay        start {first:,.0f} -> recent {last:,.0f} pts/s "
              f"({100*(last-first)/first:+.0f}%)")
    print(f"\nprojected for 4.37B rows (May 2025): "
          f"{4_366_082_870/(sent/el)/3600:,.1f} h at this rate")


if __name__ == "__main__":
    main()
