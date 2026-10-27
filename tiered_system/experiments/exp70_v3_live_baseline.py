"""Live v3 baseline for March 1, one series, Arrow and NDJSON.

Windows are a minute, an hour, and the day, all wxt.env.temp on vsn=W0A0.
Each measured response is checked in full against the archive through the
current Go response SQL. Timestamps stay nanoseconds. JSON objects are
compared by value, and Arrow is also compared as exact strings. No storage
writes and no change to the running API.
"""
import argparse
import calendar
import json
from datetime import datetime, timezone
from pathlib import Path
import statistics
import subprocess
import sys
import tempfile
import time
from urllib.request import Request, urlopen

import duckdb
import pyarrow as pa
import pyarrow.ipc as ipc

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
API = ROOT / "beehive-data-api"
ARCHIVE = ROOT / "sage-data-rework/data/archive/date=2025-03-01"
FILTER = {"name": "wxt.env.temp", "vsn": "W0A0"}
CASES = [
    ("minute", "2025-03-01T00:00:00Z", "2025-03-01T00:01:00Z"),
    ("hour", "2025-03-01T00:00:00Z", "2025-03-01T01:00:00Z"),
    ("day", "2025-03-01T00:00:00Z", "2025-03-02T00:00:00Z"),
]
MARKER = "iceberg_scan('__SAGE_METADATA__')"


def sql_for(name, start, end):
    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)
        queries = root / "queries.json"
        queries.write_text(json.dumps([{"Name": name, "Query": {"start": start, "end": end, "filter": FILTER}}]))
        env = dict(**{k: v for k, v in __import__("os").environ.items()},
                   COLD_EXECUTION_PROBE_DIR=str(root), COLD_EXECUTION_QUERIES=str(queries))
        proc = subprocess.run(["go", "test", "-count=1", "-run", "^TestColdExecutionProbe$", "./internal/backend/cold"],
                              cwd=API, env=env, capture_output=True, text=True)
        if proc.returncode:
            raise RuntimeError(proc.stdout + proc.stderr)
        return (root / f"{name}-optimized.sql").read_text()


def archive_source():
    files = sorted(ARCHIVE.glob("*.parquet"))
    if not files:
        raise RuntimeError(f"no archive parquet in {ARCHIVE}")
    listed = ", ".join("'" + str(path).replace("'", "''") + "'" for path in files)
    # The response SQL reads the stored v3 value column. The archive still has
    # the split fields, converted here the same way the seeder converts them.
    return f"""(SELECT CAST(time AS TIMESTAMPTZ_NS) AS time, name, plugin, vsn, node, host, task, job, zone, meta,
 CASE WHEN value_float IS NOT NULL THEN value_float::DOUBLE::VARIANT
      WHEN value_int IS NOT NULL THEN value_int::BIGINT::VARIANT
      ELSE value_str::VARCHAR::VARIANT END AS value
 FROM read_parquet([{listed}], hive_partitioning=false))"""


class _Body:
    def __init__(self, response, begin):
        self.response = response
        self.begin = begin
        self.first_byte = None
        self.buf = bytearray()

    def read(self, n=-1):
        raw = self.response.read(n)
        if raw and self.first_byte is None:
            self.first_byte = time.perf_counter()
        self.buf.extend(raw)
        return raw

    @property
    def closed(self):
        return False

    def readable(self):
        return True


def fetch(endpoint, fmt, start, end, timeout):
    accept = "application/vnd.apache.arrow.stream" if fmt == "arrow" else "application/x-ndjson"
    body = json.dumps({"start": start, "end": end, "filter": FILTER}).encode()
    request = Request(endpoint, body, {"Content-Type": "application/json", "Accept": accept, "Accept-Encoding": "identity"})
    begin = time.perf_counter()
    with urlopen(request, timeout=timeout) as response:
        mode = response.headers.get("X-Arrow-Compression")
        stream = _Body(response, begin)
        first_batch = None
        table = None
        if fmt == "arrow":
            reader = ipc.open_stream(stream)
            batches = []
            for batch in reader:
                if first_batch is None:
                    first_batch = time.perf_counter()
                batches.append(batch)
            table = pa.Table.from_batches(batches, schema=reader.schema)
            # Drain any trailer the IPC reader did not consume.
            while stream.read(1024 * 1024):
                pass
        else:
            while True:
                raw = response.read(1024 * 1024)
                if not raw:
                    break
                if stream.first_byte is None:
                    stream.first_byte = time.perf_counter()
                if first_batch is None and b"\n" in raw:
                    first_batch = time.perf_counter()
                stream.buf.extend(raw)
            if first_batch is None and stream.buf:
                first_batch = time.perf_counter()
    elapsed = time.perf_counter() - begin
    return dict(body=bytes(stream.buf), table=table, bytes=len(stream.buf), total_seconds=elapsed,
                first_batch_seconds=None if first_batch is None else first_batch - begin, mode=mode)


def timestamp_ns(value):
    if isinstance(value, int):
        return value
    text = str(value)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    frac = ""
    if "." in text:
        head, tail = text.split(".", 1)
        digits = []
        offset = ""
        for index, char in enumerate(tail):
            if char.isdigit():
                digits.append(char)
            else:
                offset = tail[index:]
                break
        frac = "".join(digits)
        text = head + offset
    parsed = datetime.fromisoformat(text).astimezone(timezone.utc).replace(microsecond=0)
    return calendar.timegm(parsed.utctimetuple()) * 1_000_000_000 + int(frac.ljust(9, "0")[:9] or "0")


def ndjson_table(body):
    timestamps, names, values, metas = [], [], [], []
    for line in body.splitlines():
        if not line:
            continue
        row = json.loads(line)
        timestamps.append(timestamp_ns(row["timestamp"]))
        names.append(row["name"])
        values.append(json.dumps(row["value"], separators=(",", ":")))
        metas.append(json.dumps(row["meta"], separators=(",", ":")))
    return pa.table({"ts": timestamps, "name": names, "value": values, "meta": metas})


def canon_json(value):
    if value is None:
        return None
    return json.dumps(json.loads(value), sort_keys=True, separators=(",", ":"))


def compare(con, response, expected_sql, fmt):
    if fmt == "arrow":
        table = response["table"]
        table = table.set_column(0, "timestamp", table.column("timestamp").cast(pa.int64()))
        con.register("api_raw", table)
        con.execute("""CREATE OR REPLACE TABLE api AS
            SELECT timestamp::BIGINT AS ts, name::VARCHAR AS name, value::VARCHAR AS value,
                   meta::VARCHAR AS meta, row_number() OVER () AS rn
            FROM api_raw""")
    else:
        con.register("api_ndjson", ndjson_table(response["body"]))
        con.execute("""CREATE OR REPLACE TABLE api AS
            SELECT ts::BIGINT AS ts, name::VARCHAR AS name, value::VARCHAR AS value,
                   meta::VARCHAR AS meta, row_number() OVER () AS rn
            FROM api_ndjson""")
    con.execute(f"""CREATE OR REPLACE TABLE expected AS
        SELECT epoch_ns(timestamp)::BIGINT AS ts, name::VARCHAR AS name,
               value::VARCHAR AS value, meta::VARCHAR AS meta
        FROM ({expected_sql})""")
    exact = con.execute("""
        SELECT (SELECT count(*) FROM (SELECT ts, name, value, meta FROM api EXCEPT ALL SELECT ts, name, value, meta FROM expected))
             + (SELECT count(*) FROM (SELECT ts, name, value, meta FROM expected EXCEPT ALL SELECT ts, name, value, meta FROM api))""").fetchall()[0][0]
    canonical_mismatch = con.execute("""
        SELECT (SELECT count(*) FROM (
                    SELECT ts, name, canon_json(value) AS value, canon_json(meta) AS meta FROM api
                    EXCEPT ALL
                    SELECT ts, name, canon_json(value), canon_json(meta) FROM expected))
             + (SELECT count(*) FROM (
                    SELECT ts, name, canon_json(value) AS value, canon_json(meta) AS meta FROM expected
                    EXCEPT ALL
                    SELECT ts, name, canon_json(value), canon_json(meta) FROM api))""").fetchall()[0][0]
    rows = con.execute("SELECT count(*) FROM api").fetchall()[0][0]
    ordered = con.execute("SELECT count(*) FROM (SELECT ts, lag(ts) OVER (ORDER BY rn) AS prev FROM api) WHERE prev > ts").fetchall()[0][0]
    sample = []
    if canonical_mismatch:
        sample = [list(row) for row in con.execute("""
            SELECT 'api' AS side, ts, name, value, meta FROM api
            EXCEPT ALL
            SELECT 'api', ts, name, value, meta FROM expected
            LIMIT 3""").fetchall()]
    return dict(rows=int(rows), exact_string_mismatches=int(exact),
                canonical_mismatches=int(canonical_mismatch), out_of_order=int(ordered), sample=sample)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", default="http://127.0.0.1:10000/api/v1/query")
    parser.add_argument("--reps", type=int, default=3)
    parser.add_argument("--output", type=Path, default=HERE / "results/exp70_v3_live_baseline.json")
    args = parser.parse_args()
    if args.reps < 1:
        parser.error("positive repetitions required")
    source = archive_source()
    statements = {name: sql_for(name, start, end).replace(MARKER, source) for name, start, end in CASES}
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    con.create_function("canon_json", canon_json, ["VARCHAR"], "VARCHAR")
    out = dict(label="v3 live March 1 baseline, one series", endpoint=args.endpoint,
               series=FILTER, scope="Arrow and NDJSON timing plus full archive comparison; profiling off",
               results=[], summaries=[])
    for name, start, end in CASES:
        if name != "day":
            fetch(args.endpoint, "arrow", start, end, 180)
            fetch(args.endpoint, "ndjson", start, end, 180)
        for fmt in ("arrow", "ndjson"):
            runs = []
            for repetition in range(args.reps):
                run = fetch(args.endpoint, fmt, start, end, 1800)
                check = compare(con, run, statements[name], fmt) if repetition == 0 else None
                run.pop("body", None)
                run.pop("table", None)
                if check is not None:
                    run.update(check)
                runs.append(run)
                out["results"].append(dict(query=name, format=fmt, repetition=repetition + 1, **run))
                args.output.parent.mkdir(parents=True, exist_ok=True)
                args.output.write_text(json.dumps(out, indent=2) + "\n")
                print(f"{name} {fmt} #{repetition+1}: rows={run.get('rows', '?')} "
                      f"first={run['first_batch_seconds']:.3f}s total={run['total_seconds']:.3f}s "
                      f"exact_mismatches={run.get('exact_string_mismatches')}", flush=True)
            summary = dict(query=name, format=fmt, rows=runs[0].get("rows"),
                           total_seconds=statistics.median(r["total_seconds"] for r in runs),
                           first_batch_seconds=statistics.median(r["first_batch_seconds"] for r in runs),
                           exact_string_mismatches=runs[0].get("exact_string_mismatches"),
                           canonical_mismatches=runs[0].get("canonical_mismatches"),
                           out_of_order=runs[0].get("out_of_order"))
            out["summaries"].append(summary)
            args.output.write_text(json.dumps(out, indent=2) + "\n")
            print(json.dumps(summary), flush=True)
            if summary["canonical_mismatches"] or summary["out_of_order"]:
                raise SystemExit(f"{name} {fmt} does not match the archive")


if __name__ == "__main__":
    main()
