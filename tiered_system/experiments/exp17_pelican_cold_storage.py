#!/usr/bin/env python
"""
Experiment 17: test Pelican/NRDStor as cold storage for real.

This is the gating experiment named in `plans/4_catalog_recommendation.md`
("Measure Pelican before building either") and the open question in
`plans/3_findings.md` ("whether NRDStor serves range reads at all is still
unmeasured and is the biggest remaining unknown").

Everything before this ran against a LOCAL HTTP server standing in for
Pelican. This runs against the actual origin,
`nrdstor.nationalresearchplatform.org:8443/sage`, with real SciTokens.

All objects live under /sage/cold-storage-test/ -- a prefix created for
this experiment. The sage-storage-loader writes under /sage (root) and
waggle-auth-app reads /sage/node-data; neither is touched.

What it measures:

  A. Does a real Parquet file on Pelican support the read pattern DuckDB
     needs -- footer suffix read, then selective column-chunk ranges?
  B. The exporter fix (`MaxRowsPerRowGroup`) measured over Pelican rather
     than localhost: bytes pulled for a selective query, 1 row group vs 119.
  C. Does the catalog's one required primitive -- atomic pointer swap --
     exist here, and is it safe under concurrency?

Usage:
  python exp17_pelican_cold_storage.py [--skip-upload]

Prerequisites: the two bench files already PUT to
/sage/cold-storage-test/bench/ (the script reports if they are missing).
"""
import json
import os
import ssl
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pelican_token as pt  # noqa: E402

BENCH = "/cold-storage-test/bench"
TODAY = f"{BENCH}/today_data_0.parquet"        # 78 MB, 1 row group
FIXED = f"{BENCH}/fixed_compacted_0.parquet"   # 313 MB, 119 row groups

results = {}
_ctx = ssl.create_default_context()


def fetch(path, byte_range=None, method="GET", timeout=120):
    """One authenticated request. Returns (status, headers, body, seconds)."""
    url = pt.url_for(path, scope=f"read:{path} write:{path}", lifetime=900)
    req = urllib.request.Request(url, method=method)
    if byte_range:
        req.add_header("Range", f"bytes={byte_range}")
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=_ctx) as r:
            body = r.read()
            return r.status, dict(r.headers), body, time.perf_counter() - t0
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers or {}), e.read(), time.perf_counter() - t0


class PelicanFile:
    """Minimal random-access file over Pelican, counting every byte.

    This is the measurement instrument: pyarrow drives it like a normal
    seekable file, and it records exactly what would cross the network.
    """

    def __init__(self, path, size):
        self.path, self.size, self._pos = path, size, 0
        self.requests, self.bytes_read = 0, 0
        self.ranges = []

    def seek(self, pos, whence=0):
        self._pos = {0: pos, 1: self._pos + pos, 2: self.size + pos}[whence]
        return self._pos

    def tell(self):
        return self._pos

    def read(self, n=-1):
        if n is None or n < 0:
            n = self.size - self._pos
        if n == 0:
            return b""
        lo, hi = self._pos, min(self._pos + n, self.size) - 1
        st, _, body, _ = fetch(self.path, f"{lo}-{hi}")
        if st not in (200, 206):
            raise IOError(f"range {lo}-{hi} -> HTTP {st}")
        self.requests += 1
        self.bytes_read += len(body)
        self.ranges.append((lo, hi))
        self._pos += len(body)
        return body

    # pyarrow's python_file wrapper expects these. `closed` must be an
    # attribute, not a method: pyarrow reads it directly, and a bound
    # method is always truthy -> "I/O operation on closed file".
    closed = False

    def readable(self): return True
    def seekable(self): return True
    def writable(self): return False
    def close(self): pass
    def flush(self): pass


def head_size(path):
    st, h, _, _ = fetch(path, method="HEAD")
    return int(h.get("Content-Length", 0)) if st == 200 else None


def section(t):
    print(f"\n{'=' * 68}\n{t}\n{'=' * 68}")


def main():
    section("A. Transport capabilities (real NRDStor)")
    caps = {}
    sz = head_size(TODAY)
    if sz is None:
        print(f"MISSING: {TODAY} -- upload the bench files first.")
        sys.exit(1)
    print(f"{TODAY}  {sz:,} bytes")
    caps["accept_ranges"] = fetch(TODAY, method="HEAD")[1].get("Accept-Ranges")

    st, h, b, _ = fetch(TODAY, "-8")
    caps["suffix_range"] = (st == 206)
    print(f"suffix range (last 8B)     : HTTP {st}  {h.get('Content-Range')}")
    # A Parquet file ends with the 4-byte magic 'PAR1'.
    caps["suffix_is_par1"] = b[-4:] == b"PAR1"
    print(f"  last 4 bytes             : {b[-4:]!r}  (PAR1 = valid footer)")

    st, h, b, _ = fetch(TODAY, "0-3")
    caps["head_is_par1"] = b == b"PAR1"
    print(f"first 4 bytes              : {b!r}")

    st, h, _, _ = fetch(TODAY, "0-99,200-299")
    caps["multi_range"] = "multipart/byteranges" in (h.get("Content-Type") or "")
    print(f"multi-range                : HTTP {st}  {h.get('Content-Type')}")
    results["capabilities"] = caps

    section("B. Real Parquet read over Pelican -- does pruning work?")
    import pyarrow.parquet as pq

    for label, path in (("today  (1 row group)", TODAY),
                        ("fixed  (119 row groups)", FIXED)):
        size = head_size(path)
        if size is None:
            print(f"{label}: MISSING on Pelican, skipping")
            continue
        f = PelicanFile(path, size)
        t0 = time.perf_counter()
        pf = pq.ParquetFile(f)
        meta_req, meta_bytes = f.requests, f.bytes_read
        md = pf.metadata
        print(f"\n{label}")
        print(f"  file size              : {size:,} bytes")
        print(f"  open+footer            : {meta_req} requests, "
              f"{meta_bytes:,} bytes, {time.perf_counter() - t0:.2f}s")
        print(f"  row groups             : {md.num_row_groups}, "
              f"rows {md.num_rows:,}")

        # The selective query from the findings: one rare plugin, one column
        # pair. Read only the columns a `plugin = <rare>` filter needs.
        before_req, before_bytes = f.requests, f.bytes_read
        t1 = time.perf_counter()
        tbl = pf.read(columns=["plugin"], use_threads=False)
        secs = time.perf_counter() - t1
        print(f"  read 'plugin' column   : {f.requests - before_req} requests, "
              f"{f.bytes_read - before_bytes:,} bytes, {secs:.1f}s "
              f"({tbl.num_rows:,} values)")
        results[path] = {
            "size": size, "row_groups": md.num_row_groups, "rows": md.num_rows,
            "footer_requests": meta_req, "footer_bytes": meta_bytes,
            "plugin_col_requests": f.requests - before_req,
            "plugin_col_bytes": f.bytes_read - before_bytes,
            "plugin_col_seconds": round(secs, 2),
        }

        # Single row group read -- only meaningful on the fixed file, and the
        # whole point of the exporter change.
        if md.num_row_groups > 1:
            b0r, b0b = f.requests, f.bytes_read
            t2 = time.perf_counter()
            rg = pf.read_row_group(0, columns=["plugin"])
            print(f"  ONE row group (of {md.num_row_groups}): "
                  f"{f.requests - b0r} requests, {f.bytes_read - b0b:,} bytes, "
                  f"{time.perf_counter() - t2:.1f}s ({rg.num_rows:,} rows)")
            results[path]["one_rowgroup_bytes"] = f.bytes_read - b0b
            results[path]["one_rowgroup_rows"] = rg.num_rows

    section("C. Catalog primitive: atomic pointer swap under concurrency")
    base = "/cold-storage-test/swap"
    ptr = f"{base}/current.json"

    def put(path, data):
        url = pt.url_for(path, scope=f"read:{path} write:{path}", lifetime=900)
        req = urllib.request.Request(url, method="PUT", data=data)
        with urllib.request.urlopen(req, timeout=120, context=_ctx) as r:
            return r.status

    def move(src, dst_path):
        url = pt.url_for(src, scope=f"read:{base} write:{base}", lifetime=900)
        req = urllib.request.Request(url, method="MOVE")
        req.add_header("Destination",
                       f"{pt.ROOT_URL}{dst_path}")
        try:
            with urllib.request.urlopen(req, timeout=120, context=_ctx) as r:
                return r.status
        except urllib.error.HTTPError as e:
            return e.code

    put(ptr, json.dumps({"snapshot": 0}).encode())

    # Readers hammer the pointer while writers swap it. A torn or missing
    # read here would sink the design; the catalog assumes readers always
    # see one valid snapshot.
    stop = time.time() + 12
    observed, errors = [], []

    def reader():
        while time.time() < stop:
            st, _, body, _ = fetch(ptr)
            if st != 200:
                errors.append(st)
                continue
            try:
                observed.append(json.loads(body)["snapshot"])
            except Exception as e:
                errors.append(f"TORN:{e}:{body[:40]!r}")

    def writer():
        n = 0
        while time.time() < stop:
            n += 1
            stage = f"{base}/staging_{n}.json"
            put(stage, json.dumps({"snapshot": n}).encode())
            move(stage, ptr)
            time.sleep(0.3)
        return n

    with ThreadPoolExecutor(max_workers=5) as ex:
        futs = [ex.submit(reader) for _ in range(4)]
        wn = ex.submit(writer).result()
        for f_ in futs:
            f_.result()

    print(f"writer swaps           : {wn}")
    print(f"reader observations    : {len(observed)}")
    print(f"distinct snapshots seen: {sorted(set(observed))}")
    print(f"errors / torn reads    : {len(errors)}"
          + (f"  {errors[:5]}" if errors else "  (none)"))
    monotonic = all(b >= a for a, b in zip(observed, observed[1:]))
    print(f"never went backwards   : {monotonic}")
    results["swap"] = {
        "writer_swaps": wn, "reads": len(observed),
        "distinct": sorted(set(observed)), "errors": errors[:20],
        "monotonic": monotonic,
    }

    out = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                       "results", "exp17_pelican_cold_storage.json")
    with open(out, "w") as fh:
        json.dump(results, fh, indent=2)
    print(f"\nwrote {out}")


if __name__ == "__main__":
    main()
