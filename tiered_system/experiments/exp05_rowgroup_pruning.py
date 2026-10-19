#!/usr/bin/env python
"""
Experiment 5: replay the exp03 HTTP measurement against each row-group variant.

exp03's result: over HTTP, a query returning 3 rows read the same 53.9 MB as a
full scan. exp04 rewrote the same 10M rows at five row group sizes. This asks
the question that decides the layout:

  does cutting the file into row groups make a selective query read less?

Bytes read is the metric, because every byte becomes a Pelican HTTP range
request in the real deployment. Wall time on a local SSD hides this entirely.

Usage: python -I exp05_rowgroup_pruning.py <dir-of-variants> <source.parquet>
"""
import json
import os
import re
import sys
import threading
import time
from http.server import HTTPServer, SimpleHTTPRequestHandler
from socketserver import ThreadingMixIn

STATS = {"requests": 0, "bytes": 0}
LOCK = threading.Lock()


class TServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


class H(SimpleHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def _serve(self, want_body):
        path = self.translate_path(self.path)
        if not os.path.isfile(path):
            self.send_error(404)
            return
        size = os.path.getsize(path)
        rng = self.headers.get("Range")
        start, end, partial = 0, size - 1, False
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
        with LOCK:
            STATS["requests"] += 1
            if want_body:
                STATS["bytes"] += length
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
                    c = f.read(min(1 << 20, rem))
                    if not c:
                        break
                    self.wfile.write(c)
                    rem -= len(c)

    def do_GET(self):
        self._serve(True)

    def do_HEAD(self):
        self._serve(False)


def measure(con, url, sql_tpl, label):
    with LOCK:
        STATS["requests"] = 0
        STATS["bytes"] = 0
    t0 = time.perf_counter()
    rows = con.execute(sql_tpl.format(url=url)).fetchall()
    dt = time.perf_counter() - t0
    with LOCK:
        return {
            "case": label,
            "rows": len(rows),
            "seconds": round(dt, 3),
            "http_requests": STATS["requests"],
            "http_bytes": STATS["bytes"],
        }


def main():
    vdir = os.path.abspath(sys.argv[1])
    src = os.path.abspath(sys.argv[2])

    files = {"go_exporter(1rg)": src}
    for f in sorted(os.listdir(vdir)):
        if f.endswith(".parquet"):
            n = int(f.replace("rg_", "").replace(".parquet", ""))
            files[f"pyarrow_rg={n:,}"] = os.path.join(vdir, f)

    # Serve a directory containing all of them via symlinks.
    serve_root = os.path.join(vdir, "_serve")
    os.makedirs(serve_root, exist_ok=True)
    urlnames = {}
    for label, p in files.items():
        name = os.path.basename(p)
        link = os.path.join(serve_root, name)
        if not os.path.exists(link):
            os.symlink(p, link)
        urlnames[label] = name

    os.chdir(serve_root)
    httpd = TServer(("127.0.0.1", 0), H)
    port = httpd.server_address[1]
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{port}"

    import duckdb

    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("PRAGMA threads=4")
    try:
        con.execute("SET enable_http_metadata_cache=false")
    except Exception:
        pass

    counts = con.execute(
        f"select plugin, count(*) c from read_parquet('{src}') group by plugin order by c desc"
    ).fetchall()
    big, big_n = counts[0]
    rare, rare_n = counts[-1]
    print(f"big : {big} ({big_n:,} rows)")
    print(f"rare: {rare} ({rare_n:,} rows)\n")

    cols = "time, name, plugin, vsn, value_float"
    cases = [
        ("full_scan", f"select {cols} from read_parquet('{{url}}')"),
        ("big_plugin", f"select {cols} from read_parquet('{{url}}') where plugin = '{big}'"),
        ("rare_plugin", f"select {cols} from read_parquet('{{url}}') where plugin = '{rare}'"),
        ("name_filter", f"select {cols} from read_parquet('{{url}}') where name = 'env.temperature'"),
    ]

    all_results = {}
    for label, name in urlnames.items():
        url = f"{base}/{name}"
        size = os.path.getsize(files[label])
        res = []
        for cname, tpl in cases:
            res.append(measure(con, url, tpl, cname))
        all_results[label] = {"file_size": size, "results": res}

    print(f"{'variant':22s} {'file_size':>12s} | " + " | ".join(f"{c:>14s}" for c, _ in cases))
    print("-" * 100)
    for label, d in all_results.items():
        cells = []
        for r in d["results"]:
            cells.append(f"{r['http_bytes']:>14,}")
        print(f"{label:22s} {d['file_size']:>12,} | " + " | ".join(cells))

    print()
    print("as % of that variant's own file size:")
    print(f"{'variant':22s} | " + " | ".join(f"{c:>14s}" for c, _ in cases))
    print("-" * 90)
    for label, d in all_results.items():
        cells = [f"{100.0*r['http_bytes']/d['file_size']:>13.1f}%" for r in d["results"]]
        print(f"{label:22s} | " + " | ".join(cells))

    print()
    print("rows returned (sanity: must be identical across variants):")
    for label, d in all_results.items():
        print(f"  {label:22s} " + " ".join(f"{r['case']}={r['rows']:,}" for r in d["results"]))

    os.makedirs(os.path.dirname(os.path.abspath(__file__)) + "/results", exist_ok=True)
    with open(
        os.path.dirname(os.path.abspath(__file__)) + "/results/exp05_rowgroup_pruning.json", "w"
    ) as f:
        json.dump({"big_plugin": big, "rare_plugin": rare, "variants": all_results}, f, indent=2)


if __name__ == "__main__":
    main()
