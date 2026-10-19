#!/usr/bin/env python
"""
Experiment 9: build the section 7 manifest and measure what it saves.

plans/1_draft.md section 7 proposes a manifest carrying each file's footer
facts (path, size, row count, column statistics) so query planning is one read
instead of a footer open per file. exp06 measured the cost it would replace:
60-76 KB and 2 HTTP requests per file, just to plan.

This builds that manifest over the local archive and answers:

  1. How big is it, and how long does one read take, vs opening N footers?
  2. Can it actually reject files? A manifest whose statistics never prune
     anything is pure overhead, so we measure files-rejected for real query
     shapes.

The second question is the one that matters. plans/1_draft.md already notes
that with one row group per file, min/max for `plugin` spans every plugin in
the batch -- so a manifest built from today's archive should be able to prune
on `time` but almost never on `plugin`. This measures whether that is true,
and what changes after the row-group fix from exp04/exp07.

Usage: python -I exp09_manifest.py <archive-root> [--days N]
"""
import json
import os
import sys
import time

import pyarrow.parquet as pq

STAT_COLS = ["time", "name", "plugin", "vsn", "node", "host", "task", "job", "zone"]


def build_manifest(root, days):
    """One entry per data file, with per-row-group statistics."""
    entries = []
    t0 = time.perf_counter()
    footer_bytes = 0
    for d in days:
        ddir = os.path.join(root, d)
        for fn in sorted(os.listdir(ddir)):
            if not fn.endswith(".parquet"):
                continue
            path = os.path.join(ddir, fn)
            md = pq.ParquetFile(path).metadata
            footer_bytes += md.serialized_size
            groups = []
            for g in range(md.num_row_groups):
                rg = md.row_group(g)
                stats = {}
                for c in range(rg.num_columns):
                    col = rg.column(c)
                    nm = col.path_in_schema
                    if nm in STAT_COLS and col.is_stats_set:
                        s = col.statistics
                        stats[nm] = {
                            "min": str(s.min) if s.has_min_max else None,
                            "max": str(s.max) if s.has_min_max else None,
                            "nulls": s.null_count,
                        }
                groups.append({"rows": rg.num_rows, "bytes": rg.total_byte_size, "stats": stats})
            entries.append({
                "path": os.path.relpath(path, root),
                "date": d,
                "size": os.path.getsize(path),
                "rows": md.num_rows,
                "row_groups": md.num_row_groups,
                "footer_size": md.serialized_size,
                "groups": groups,
            })
    return entries, time.perf_counter() - t0, footer_bytes


def prunes(entries, col, value):
    """How many files can the manifest reject for col == value?"""
    rejected = kept = 0
    for e in entries:
        hit = False
        for g in e["groups"]:
            s = g["stats"].get(col)
            if not s or s["min"] is None:
                hit = True
                break
            if s["min"] <= value <= s["max"]:
                hit = True
                break
        if hit:
            kept += 1
        else:
            rejected += 1
    return rejected, kept


def main():
    root = os.path.abspath(sys.argv[1])
    ndays = int(sys.argv[sys.argv.index("--days") + 1]) if "--days" in sys.argv else 30
    days = sorted(d for d in os.listdir(root) if d.startswith("date="))[:ndays]

    entries, build_s, footer_bytes = build_manifest(root, days)
    nfiles = len(entries)
    total_rows = sum(e["rows"] for e in entries)
    total_bytes = sum(e["size"] for e in entries)
    total_groups = sum(e["row_groups"] for e in entries)

    path = os.path.dirname(os.path.abspath(__file__)) + "/results/manifest.json"
    with open(path, "w") as f:
        json.dump({"root": root, "entries": entries}, f)
    msize = os.path.getsize(path)

    # compact form: drop per-group stats we do not prune on
    slim = [{"path": e["path"], "rows": e["rows"], "size": e["size"],
             "groups": [{"rows": g["rows"],
                         "time": [g["stats"].get("time", {}).get("min"),
                                  g["stats"].get("time", {}).get("max")],
                         "plugin": [g["stats"].get("plugin", {}).get("min"),
                                    g["stats"].get("plugin", {}).get("max")]}
                        for g in e["groups"]]} for e in entries]
    spath = os.path.dirname(os.path.abspath(__file__)) + "/results/manifest_slim.json"
    with open(spath, "w") as f:
        json.dump(slim, f)
    ssize = os.path.getsize(spath)

    t0 = time.perf_counter()
    with open(path) as f:
        json.load(f)
    read_full = time.perf_counter() - t0
    t0 = time.perf_counter()
    with open(spath) as f:
        json.load(f)
    read_slim = time.perf_counter() - t0

    print(f"archive slice : {len(days)} days, {nfiles} files, {total_rows:,} rows, {total_bytes:,} bytes")
    print(f"row groups    : {total_groups} ({total_groups/max(nfiles,1):.1f} per file)")
    print()
    print(f"footer opens  : {nfiles} files, {footer_bytes:,} bytes of footer, {build_s:.2f}s to read them all")
    print(f"manifest full : {msize:,} bytes, read in {read_full*1000:.0f} ms")
    print(f"manifest slim : {ssize:,} bytes, read in {read_slim*1000:.0f} ms")
    print(f"speedup       : {build_s/max(read_slim,1e-6):.0f}x  (one slim manifest read vs opening every footer)")
    print()

    # Scale to the whole 4,564-file archive.
    scale = 4564 / max(nfiles, 1)
    print(f"extrapolated to the full 4,564-file archive:")
    print(f"  footer reads : {footer_bytes*scale/1e6:,.0f} MB, ~{build_s*scale:.0f}s local, {nfiles*2*scale:,.0f} HTTP requests")
    print(f"  slim manifest: {ssize*scale/1e6:,.1f} MB, one request")
    print()

    # Can it prune?
    plugins = {}
    for e in entries:
        for g in e["groups"]:
            s = g["stats"].get("plugin")
            if s and s["min"]:
                plugins.setdefault(s["min"], 0)
                plugins[s["min"]] += 1
    probe = sorted(plugins)[0] if plugins else None
    print("pruning power of the manifest (files it can reject):")
    for col, val in [("plugin", "zzz-nonexistent-plugin"),
                     ("plugin", probe or "x"),
                     ("name", "env.temperature")]:
        if val is None:
            continue
        rej, kept = prunes(entries, col, val)
        print(f"  {col} = {val[:48]:48s} reject {rej:>4}/{nfiles}  ({100.0*rej/max(nfiles,1):.0f}%)")

    with open(os.path.dirname(os.path.abspath(__file__)) + "/results/exp09_manifest.json", "w") as f:
        json.dump({"days": len(days), "files": nfiles, "rows": total_rows,
                   "bytes": total_bytes, "row_groups": total_groups,
                   "footer_bytes": footer_bytes, "footer_read_seconds": build_s,
                   "manifest_full_bytes": msize, "manifest_slim_bytes": ssize,
                   "manifest_slim_read_seconds": read_slim}, f, indent=2)


if __name__ == "__main__":
    main()
