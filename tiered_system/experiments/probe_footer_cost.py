#!/usr/bin/env python
"""
Probe: what does a planning-only query actually fetch over HTTP?

exp06 and exp09 both report a per-file "footer" cost for the same archive and
disagree by ~30x (60-76 KB/file vs ~2.2 KB/file). They are measuring different
things, and SUMMARY.md sections 5 and 7 quote them as if they were the same.

This resolves it by logging every Range request a `count(*)` makes and
splitting each tail read into the part that is actually the footer and the
part that is not:

  - footer   : the Thrift footer, what pq.metadata.serialized_size counts
  - slack    : the rest of the tail chunk the reader fetched around it
  - elsewhere: any request that does not reach EOF

Result: DuckDB does not issue a footer-sized request. It fetches one
power-of-two tail chunk per file (32 KiB / 64 KiB / 128 KiB, scaling with
file size) and parses the footer out of it. The archive's footers are ~2.2 KB,
so planning transfers 12-35x more than the footer bytes suggest. Both
numbers are correct measurements of different things; only this one is the
cost a manifest would avoid.

Usage: python -I probe_footer_cost.py <day-dir>
"""
import os
import re
import sys
import threading
from collections import defaultdict
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer

import duckdb
import pyarrow.parquet as pq

REQS = defaultdict(list)  # path -> [(start, length)]
LOCK = threading.Lock()


def make_handler(root):
    class H(SimpleHTTPRequestHandler):
        def __init__(self, *a, **kw):
            super().__init__(*a, directory=root, **kw)

        def log_message(self, *a):
            pass

        def do_HEAD(self):
            self._serve(False)

        def do_GET(self):
            self._serve(True)

        def _serve(self, want_body):
            path = self.translate_path(self.path)
            if not os.path.isfile(path):
                self.send_error(404)
                return
            size = os.path.getsize(path)
            start, end, partial = 0, size - 1, False
            rng = self.headers.get("Range")
            if rng:
                m = re.match(r"bytes=(\d*)-(\d*)", rng.strip())
                if m:
                    g1, g2 = m.group(1), m.group(2)
                    if g1 == "":
                        start, end = max(0, size - int(g2)), size - 1
                    else:
                        start = int(g1)
                        end = int(g2) if g2 else size - 1
                    partial = True
            end = min(end, size - 1)
            length = max(0, end - start + 1)
            if want_body:
                with LOCK:
                    REQS[self.path].append((start, length))
            self.send_response(206 if partial else 200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("Content-Length", str(length))
            if partial:
                self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
            self.end_headers()
            if want_body and length:
                with open(path, "rb") as f:
                    f.seek(start)
                    rem = length
                    while rem > 0:
                        chunk = f.read(min(65536, rem))
                        if not chunk:
                            break
                        self.wfile.write(chunk)
                        rem -= len(chunk)

    return H


def main():
    day = os.path.abspath(sys.argv[1])
    root = os.path.dirname(day)
    srv = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(root))
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    port = srv.server_address[1]

    rel = os.path.basename(day)
    names = sorted(f for f in os.listdir(day) if f.endswith(".parquet"))
    urls = ", ".join(f"'http://127.0.0.1:{port}/{rel}/{f}'" for f in names)
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"SELECT count(*) FROM read_parquet([{urls}])").fetchone()

    # Ground truth per file: size and the Thrift footer length.
    info = {}
    for fn in sorted(os.listdir(day)):
        if fn.endswith(".parquet"):
            p = os.path.join(day, fn)
            info[fn] = (os.path.getsize(p), pq.ParquetFile(p).metadata.serialized_size)

    print(f"{'file':16s} {'size':>12s} {'footer':>8s} {'reqs':>5s} {'fetched':>10s} "
          f"{'footer':>9s} {'slack':>9s} {'other':>8s}  tail chunk")
    tot = defaultdict(int)
    for fn, (size, footer) in info.items():
        rs = REQS.get(f"/{rel}/{fn}", [])
        got_footer = slack = other = 0
        # Thrift footer lives in the last `footer + 8` bytes (magic + length).
        foot_len = footer + 8
        chunks = []
        for start, length in rs:
            if start + length >= size - 8:  # a read that reaches EOF
                got_footer += min(length, foot_len)
                slack += max(0, length - foot_len)
                chunks.append(length)
            else:
                other += length
        fetched = sum(l for _, l in rs)
        tot["reqs"] += len(rs); tot["fetched"] += fetched
        tot["footer_got"] += got_footer; tot["slack"] += slack; tot["other"] += other
        tot["footer"] += footer
        shape = "+".join(f"{c//1024}K" for c in chunks) or "-"
        print(f"{fn:16s} {size:>12,} {footer:>8,} {len(rs):>5d} {fetched:>10,} "
              f"{got_footer:>9,} {slack:>9,} {other:>8,}  {shape}")

    n = len(info)
    print()
    print(f"files                 : {n}")
    print(f"thrift footers        : {tot['footer']:,} bytes  ({tot['footer']/n:,.0f}/file)  <- exp09/SUMMARY 7 counts this")
    print(f"actually fetched      : {tot['fetched']:,} bytes  ({tot['fetched']/n:,.0f}/file)  <- exp06/SUMMARY 5 counts this")
    print(f"  footer within tail  : {tot['footer_got']:,}")
    print(f"  slack in tail chunk : {tot['slack']:,}")
    print(f"  not a tail read     : {tot['other']:,}")
    print(f"requests              : {tot['reqs']} ({tot['reqs']/n:.1f}/file)")
    print(f"ratio fetched/footer  : {tot['fetched']/max(tot['footer'],1):.1f}x")
    srv.shutdown()


if __name__ == "__main__":
    main()
