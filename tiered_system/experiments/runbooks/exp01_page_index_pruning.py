#!/usr/bin/env python
"""
Experiment 1: does page-index pruning actually reduce bytes read for a plugin filter?

plans/1_draft.md section 5 and the handoff both claim: because `plugin` is the
leading sort key, page statistics can isolate one plugin inside a file, even
though the file is a single row group whose min/max span every plugin.

That claim has never been measured. This measures it, by counting the bytes a
reader actually pulls off disk for three shapes of the same query:

  full          read the columns with no filter at all
  big_plugin    filter to waggle-wxt536, ~90% of rows
  rare_plugin   filter to a plugin with a tiny share of rows

If page-index pruning works, rare_plugin reads far fewer bytes than full.
If it does not, all three read about the same, and the design needs either a
coarse directory split or multiple row groups per file.

Usage: python -I exp01_page_index_pruning.py <file.parquet>
"""
import json
import sys
import time

import pyarrow.dataset as ds
import pyarrow.parquet as pq


class CountingFile:
    """Wraps a file object and counts bytes actually read."""

    def __init__(self, path):
        self._f = open(path, "rb")
        self.bytes_read = 0
        self.read_calls = 0

    def read(self, nbytes=None):
        data = self._f.read() if nbytes is None else self._f.read(nbytes)
        self.bytes_read += len(data)
        self.read_calls += 1
        return data

    def seek(self, pos, whence=0):
        return self._f.seek(pos, whence)

    def tell(self):
        return self._f.tell()

    def close(self):
        return self._f.close()

    def readable(self):
        return True

    def seekable(self):
        return True

    def writable(self):
        return False

    @property
    def closed(self):
        return self._f.closed


def run_case(path, label, filt, columns):
    counter = CountingFile(path)
    t0 = time.perf_counter()
    pf = pq.ParquetFile(counter)
    footer_bytes = counter.bytes_read
    dataset = ds.dataset(path, format="parquet")
    # Read through pyarrow.dataset for filter pushdown, but count via a second
    # pass with the counting wrapper so we measure real IO.
    tbl = pq.read_table(counter, columns=columns, filters=filt)
    elapsed = time.perf_counter() - t0
    result = {
        "case": label,
        "rows_returned": tbl.num_rows,
        "bytes_read": counter.bytes_read,
        "footer_bytes": footer_bytes,
        "read_calls": counter.read_calls,
        "seconds": round(elapsed, 3),
    }
    counter.close()
    del dataset, pf
    return result


def main():
    path = sys.argv[1]

    meta = pq.ParquetFile(path).metadata
    file_size = meta.serialized_size
    import os

    file_size = os.path.getsize(path)

    # Find the biggest and a rare plugin by actually scanning the plugin column.
    plugin_col = pq.read_table(path, columns=["plugin"])["plugin"]
    counts = plugin_col.to_pandas().value_counts()
    big_plugin = counts.index[0]
    rare_plugin = counts.index[-1]

    print(f"file: {path}")
    print(f"size: {file_size:,} bytes   rows: {meta.num_rows:,}   row_groups: {meta.num_row_groups}")
    print(f"distinct plugins in file: {len(counts)}")
    print(f"big  plugin: {big_plugin}  ({counts.iloc[0]:,} rows, {100*counts.iloc[0]/meta.num_rows:.2f}%)")
    print(f"rare plugin: {rare_plugin}  ({counts.iloc[-1]:,} rows, {100*counts.iloc[-1]/meta.num_rows:.4f}%)")
    print()

    # Query a realistic column set: what a Beehive raw query needs.
    columns = ["time", "name", "plugin", "vsn", "value_float"]

    cases = [
        ("full_scan", None),
        ("big_plugin", [("plugin", "==", big_plugin)]),
        ("rare_plugin", [("plugin", "==", rare_plugin)]),
        ("name_filter", [("name", "==", "env.temperature")]),
    ]

    results = []
    for label, filt in cases:
        try:
            r = run_case(path, label, filt, columns)
        except Exception as e:  # noqa: BLE001
            r = {"case": label, "error": str(e)}
        results.append(r)
        print(json.dumps(r))

    print()
    base = next((r["bytes_read"] for r in results if r["case"] == "full_scan"), None)
    if base:
        print(f"{'case':14s} {'rows':>12s} {'bytes_read':>14s} {'vs full':>9s} {'sec':>7s}")
        for r in results:
            if "error" in r:
                print(f"{r['case']:14s} ERROR {r['error'][:60]}")
                continue
            pct = 100.0 * r["bytes_read"] / base
            print(
                f"{r['case']:14s} {r['rows_returned']:>12,} {r['bytes_read']:>14,} "
                f"{pct:>8.1f}% {r['seconds']:>7.2f}"
            )

    out = {
        "file": path,
        "file_size": file_size,
        "num_rows": meta.num_rows,
        "num_row_groups": meta.num_row_groups,
        "distinct_plugins": int(len(counts)),
        "big_plugin": str(big_plugin),
        "rare_plugin": str(rare_plugin),
        "columns_read": columns,
        "results": results,
    }
    with open("results/exp01_page_index_pruning.json", "w") as f:
        json.dump(out, f, indent=2)


if __name__ == "__main__":
    main()
