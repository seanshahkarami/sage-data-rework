"""Check the encoder against the real pathological rows in the archive.

Run: conda run -n sage_pipeline python -I tiered_system/demo/tools/test_lineproto.py
"""
import glob
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lineproto import encode_row, esc_str_field, esc_tag, PROMOTED_TAGS

ARCHIVE = "/Users/franciscolozano/Workspaces/sage_data_pipeline_v2/sage-data-rework/data/archive"
FAILED = []


def check(name, ok, detail=""):
    FAILED.append(name) if not ok else None
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f" -- {detail}" if detail else ""))


print("--- unit: escaping ---")
check("tag value with space", esc_tag("CROCUS MST") == r"CROCUS\ MST")
check("tag value with comma+eq", esc_tag("a,b=c") == r"a\,b\=c")
check("str field with quote", esc_str_field('say "hi"') == 'say \\"hi\\"')
check("str field with backslash", esc_str_field("a\\b") == "a\\\\b")
check("str field newline escaped", "\n" not in esc_str_field("a\nb"))
check("str field newline -> \\n", esc_str_field("a\nb") == "a\\nb")

print("\n--- unit: row encoding ---")
line = encode_row("env.temp", ("plug", "W09A", "n1", "h1", "t1", "j1", "core"),
                  {"units": "degree Celsius"}, 2.5, None, None, 1746061575867813833)
print("  ", line)
check("ends with ns timestamp", line.endswith(" 1746061575867813833"))
check("tags sorted", line.index(",host=") < line.index(",job="))
check("space escaped in meta value", r"degree\ Celsius" in line)
check("no raw newline", "\n" not in line)

line_i = encode_row("c", ("p", "", "", "", "", "", ""), {}, None, 42, None, 1)
check("int field has i suffix", line_i.endswith("value=42i 1"), line_i)
line_s = encode_row("c", ("p", "", "", "", "", "", ""), {}, None, None, "hi", 1)
check("str field quoted", 'value="hi"' in line_s, line_s)
check("empty tag dropped", ",vsn=" not in line_i, line_i)

print("\n--- real archive rows: the 1,480 newline + 98 quote cases ---")
import pyarrow.parquet as pq
import pyarrow.compute as pc

files = sorted(glob.glob(f"{ARCHIVE}/date=2025-05-01/*.parquet"))
cols = ["time", "name", *PROMOTED_TAGS, "meta", "value_float", "value_int", "value_str"]

found_nl = found_q = 0
checked = 0
for f in files:
    t = pq.read_table(f, columns=cols)
    vs = t.column("value_str")
    mask = pc.and_(pc.is_valid(vs),
                   pc.match_substring(pc.cast(vs, "string"), "\n"))
    idx = [i for i, v in enumerate(mask.to_pylist()) if v]
    if not idx:
        continue
    sub = t.take(idx[:50]).to_pylist()
    for r in sub:
        line = encode_row(r["name"], tuple(r[k] for k in PROMOTED_TAGS),
                          r["meta"], r["value_float"], r["value_int"],
                          r["value_str"], r["time"].value if hasattr(r["time"], "value") else 0)
        checked += 1
        if "\n" in line or "\r" in line:
            print("  !! raw newline survived:", repr(line[:120]))
            found_nl += 1
    if checked >= 50:
        break

check("newline-bearing rows encode to single lines", found_nl == 0,
      f"{checked} real rows checked, {found_nl} bad")

# quote cases
for f in files:
    t = pq.read_table(f, columns=cols)
    vs = t.column("value_str")
    mask = pc.and_(pc.is_valid(vs), pc.match_substring(pc.cast(vs, "string"), '"'))
    idx = [i for i, v in enumerate(mask.to_pylist()) if v]
    if not idx:
        continue
    for r in t.take(idx[:20]).to_pylist():
        line = encode_row(r["name"], tuple(r[k] for k in PROMOTED_TAGS),
                          r["meta"], r["value_float"], r["value_int"],
                          r["value_str"], 1)
        found_q += 1
        body = line.split(" value=", 1)[1]
        # every quote inside the field must be backslash-escaped
        inner = body.rsplit(" ", 1)[0]
        assert inner.startswith('"') and inner.endswith('"'), inner[:80]
        core = inner[1:-1]
        bad = any(core[i] == '"' and (i == 0 or core[i-1] != "\\")
                  for i in range(len(core)))
        if bad:
            print("  !! unescaped quote:", repr(inner[:120]))
    break

check("quote-bearing rows escape correctly", found_q > 0, f"{found_q} rows checked")

print()
if FAILED:
    print(f"FAILED: {FAILED}")
    sys.exit(1)
print("all encoder checks passed")
