"""Seed the hot tier: archive Parquet -> line protocol -> InfluxDB.

Streams; line protocol is never written to disk. One day of the archive is
~0.6 GB of Parquet but ~50 GB of line protocol, and May 2025 is ~1.49 TB on
the wire -- there are only 352 GB free, so materializing is not an option.
gzip takes it down ~27x, so the network is not the constraint either.

Timestamps stay in NANOSECONDS (`precision=ns`). InfluxDB is the real-time
tier and keeps full resolution; the ns->us reduction happens at migration
into Iceberg, never at ingest.

Concurrency is bounded on purpose. The Python encoder runs ~171k rows/s on
one core and is GIL-bound, so the unit of parallelism is the file and the
mechanism is processes -- but the Docker VM hosting InfluxDB has 6 CPUs and
8 GB, and a 15-process run took the docker daemon down. Phase 0 measured
~450k pts/s at 4 processes with memory flat, so 4 is the default and the
ceiling is advisory rather than discovered the hard way.

Resumable at day granularity: each finished day is appended to a progress
file, and a restart skips them. Days are ingested newest-last so a partial
seed still yields a contiguous recent window.

This is the second way to fill the hot tier. The default is
`./seed_hot.sh`, which restores data/influx-may2025 with `influx restore`.
Run this file only when that backup should be rebuilt from the archive:

  ./seed_hot.sh parquet --start 2025-05-01 --end 2025-05-31
  ./seed_hot.sh parquet --days 7            # the last 7 days of May
"""
import argparse
import glob
import gzip
import multiprocessing as mp
import os
import sys
import time
from datetime import date, timedelta

import pyarrow.parquet as pq
import requests

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)
from lineproto import encode_row, PROMOTED_TAGS

ARCHIVE = os.environ.get(
    "DEMO_ARCHIVE",
    "/Users/franciscolozano/Workspaces/sage_data_pipeline_v2/sage-data-rework/data/archive")
URL = os.environ.get("INFLUX_URL", "http://localhost:18086")
TOKEN = os.environ.get("INFLUX_TOKEN", "demo-token-not-a-secret")
ORG = os.environ.get("INFLUX_ORG", "waggle")
BUCKET = os.environ.get("INFLUX_BUCKET", "waggle")

COLS = ["time", "name", *PROMOTED_TAGS, "meta",
        "value_float", "value_int", "value_str"]


def days_between(start, end):
    d, out = start, []
    while d <= end:
        out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def encode_batch(rb):
    """A record batch -> list of line-protocol lines."""
    d = rb.to_pydict()
    times, names, metas = d["time"], d["name"], d["meta"]
    promoted = [d[k] for k in PROMOTED_TAGS]
    vfs, vis, vss = d["value_float"], d["value_int"], d["value_str"]
    lines = []
    for i in range(rb.num_rows):
        ts = times[i]
        ns = ts.value if hasattr(ts, "value") else int(ts.timestamp() * 1e9)
        line = encode_row(names[i], tuple(p[i] for p in promoted),
                          metas[i], vfs[i], vis[i], vss[i], ns)
        if line:
            lines.append(line)
    return lines


def post(session, lines):
    """Write one gzipped batch. Returns None on success, else an error str.

    A 5xx during this seed is the engine refusing a batch while it compacts
    the series index. On the May load that compaction ran 50-200s, and the
    previous six quick retries (about 30s) expired inside the stall and
    aborted the day. Keep trying for 10 minutes, and keep the response body
    so the next failure says why.
    """
    body = gzip.compress("\n".join(lines).encode(), compresslevel=1)
    deadline = time.monotonic() + 600
    attempt = 0
    last = "no response"
    while True:
        try:
            r = session.post(
                f"{URL}/api/v2/write",
                params={"org": ORG, "bucket": BUCKET, "precision": "ns"},
                headers={"Authorization": f"Token {TOKEN}",
                         "Content-Encoding": "gzip",
                         "Content-Type": "text/plain; charset=utf-8"},
                data=body, timeout=120)
        except requests.RequestException as e:
            # The server going away mid-seed is the failure mode Phase 0
            # actually hit; back off and retry rather than lose the day.
            last = f"{type(e).__name__}: {e}"
        else:
            if r.status_code in (200, 204):
                return None
            if r.status_code == 400:
                # Malformed line protocol is an encoder bug, not transient.
                # Fail loudly: a silent skip would corrupt the demo's premise.
                return f"400 malformed line protocol: {r.text[:400]}"
            last = f"{r.status_code} {(r.text or '').strip()[:300]}"
        attempt += 1
        if time.monotonic() >= deadline:
            break
        wait = min(2 ** min(attempt, 5), 30)
        if attempt == 1 or attempt % 4 == 0:
            print(f"      write retry {attempt}: {last} "
                  f"(waiting {wait:.0f}s)", flush=True)
        time.sleep(wait)
    return f"write failed after retries: {last}"


def seed_file(task):
    """Encode and write one archive file. Runs in its own process."""
    path, batch = task
    session = requests.Session()
    sent = 0
    buf = []
    try:
        pf = pq.ParquetFile(path)
        for rb in pf.iter_batches(batch_size=batch, columns=COLS):
            buf.extend(encode_batch(rb))
            while len(buf) >= batch:
                err = post(session, buf[:batch])
                if err:
                    return ("error", os.path.basename(path), err)
                sent += batch
                buf = buf[batch:]
        if buf:
            err = post(session, buf)
            if err:
                return ("error", os.path.basename(path), err)
            sent += len(buf)
    except Exception as e:
        return ("error", os.path.basename(path), f"{type(e).__name__}: {e}")
    return ("ok", os.path.basename(path), sent)


def load_progress(path):
    if not path or not os.path.exists(path):
        return set()
    with open(path) as f:
        return {ln.strip() for ln in f if ln.strip()}


def mark_done(path, day):
    if not path:
        return
    with open(path, "a") as f:
        f.write(day + "\n")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--start", help="first day, YYYY-MM-DD")
    ap.add_argument("--end", help="last day inclusive, YYYY-MM-DD")
    ap.add_argument("--days", type=int,
                    help="instead of --start, seed the last N days ending at --end")
    ap.add_argument("--procs", type=int, default=4,
                    help="concurrent writer processes (4 measured safe when "
                         "InfluxDB is the only load; 15 took the docker VM "
                         "down; use 2 while another heavy job shares the host)")
    ap.add_argument("--batch", type=int, default=25_000)
    ap.add_argument("--progress", default=os.path.join(_HERE, "logs", "seed_hot.progress"),
                    help="file of completed days; makes the seed resumable")
    args = ap.parse_args()

    end = date.fromisoformat(args.end) if args.end else date(2025, 5, 31)
    if args.days:
        start = end - timedelta(days=args.days - 1)
    elif args.start:
        start = date.fromisoformat(args.start)
    else:
        ap.error("need --start or --days")

    done = load_progress(args.progress)
    days = [d for d in days_between(start, end) if d not in done]
    if done:
        print(f"resuming: {len(done)} day(s) already done, {len(days)} to go")
    if not days:
        print("nothing to do")
        return 0

    print(f"seeding {days[0]} .. {days[-1]} ({len(days)} days) "
          f"procs={args.procs} batch={args.batch:,} -> {URL}/{BUCKET}",
          flush=True)

    t_all = time.time()
    grand = 0
    for day in days:
        files = sorted(glob.glob(f"{ARCHIVE}/date={day}/*.parquet"))
        if not files:
            print(f"  {day}: no archive files, skipping", flush=True)
            mark_done(args.progress, day)
            continue
        t0 = time.time()
        with mp.Pool(min(args.procs, len(files))) as pool:
            results = pool.map(seed_file, [(f, args.batch) for f in files])
        bad = [r for r in results if r[0] == "error"]
        if bad:
            # Do NOT mark the day done: a resume must retry it whole.
            print(f"  {day}: FAILED on {len(bad)} file(s)", flush=True)
            for r in bad[:3]:
                print(f"      {r[1]}: {r[2]}", flush=True)
            return 1
        rows = sum(r[2] for r in results)
        grand += rows
        el = time.time() - t0
        mark_done(args.progress, day)
        done_n = days.index(day) + 1
        rate_all = grand / max(time.time() - t_all, 1e-9)
        remain = (len(days) - done_n) * (time.time() - t_all) / done_n
        print(f"  {day}: {rows:>12,} rows  {el:6.0f}s  {rows/el:>9,.0f} pts/s"
              f"   cum {grand:>14,}  eta {remain/3600:5.2f}h", flush=True)

    el = time.time() - t_all
    print(f"\nseeded {grand:,} rows in {el/3600:.2f}h "
          f"({grand/max(el,1):,.0f} pts/s average)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
