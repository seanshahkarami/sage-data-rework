"""Small independent audit fixtures; no network or archive modifications.

Run: python -I audit_local.py
This records behavior, including known defects, rather than treating a
successful process exit as proof of API equivalence.
"""
import json
import pathlib
import sys
import tempfile

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import cold_backend as cb


def main():
    out = {"duckdb": duckdb.__version__, "pyarrow": pa.__version__}
    con = duckdb.connect()
    with tempfile.TemporaryDirectory(prefix="sage-audit-") as directory:
        root = pathlib.Path(directory)
        day = root / "date=2025-01-01"
        day.mkdir()
        schema = pa.schema([
            ("time", pa.timestamp("ns", tz="UTC")), ("name", pa.string()),
            *[(tag, pa.string()) for tag in cb.PROMOTED_TAGS],
            ("meta", pa.map_(pa.string(), pa.string())),
            ("value_float", pa.float64()), ("value_int", pa.int64()),
            ("value_str", pa.string()),
        ])
        rows = []
        for index, meta in enumerate([
            [("a", "1"), ("b", "2")], [("b", "2"), ("a", "1")],
        ]):
            rows.append(dict(time=1735689600000000001 + index * 1_000_000_000,
                             name="env.temperature", meta=meta, value_int=index + 1))
        pq.write_table(pa.Table.from_pylist(rows, schema=schema), day / "test.parquet")
        query = dict(start="2025-01-01T00:00:00Z", end="2025-01-02T00:00:00Z",
                     filter={"name": "env.temperature"})
        for label, extra in [
            ("tail_same_tags_different_order", {"tail": 1}),
            ("count_request", {"experimental_func": "count"}),
            ("private_bucket_request", {"bucket": "_private"}),
        ]:
            records, _, _ = cb.query_archive(con, directory, query | extra)
            out[label] = {"records": records, "row_count": len(records)}
        out["submicrosecond_parse"] = cb.parse_time(
            "2025-01-01T00:00:00.000000999Z").isoformat()
        for tz in [None, "UTC"]:
            path = root / ("naive.parquet" if tz is None else "utc.parquet")
            pq.write_table(pa.table({"t": pa.array([1735689606915629791],
                                                  type=pa.timestamp("ns", tz=tz))}), path)
            out["parquet_timestamp_" + str(tz)] = con.execute(
                "select typeof(t), epoch_ns(t) from read_parquet(?)", [str(path)]).fetchall()
    con.close()
    output = json.dumps(out, indent=2) + "\n"
    (HERE / "results" / "audit_local.json").write_text(output)
    print(output, end="")


if __name__ == "__main__":
    main()
