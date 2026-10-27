"""Historical prototype: compare JSON-valued and typed Arrow.

The current public API intentionally serves only the original single-value
schema. This harness is retained for experimental servers and will reject the
public server for the typed request; rebuilding does not enable the prototype.

The server's compression setting is unchanged between requests (Zstd default).
New schema negotiation is checked, so old servers cannot silently masquerade
as a typed-values response. Short windows default; --include-months adds the
March/May queries. No remote storage writes or container changes.
"""
import argparse
import json
from pathlib import Path
import statistics
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import exp60_arrow_compression_live as live
from exp59_arrow_ipc_compression import monitored


def measured(endpoint, schema, case):
    raw, peak = monitored([sys.executable, "-I", str(Path(__file__).resolve()),
        "--endpoint", endpoint, "--worker", str(schema), "--case-json", json.dumps(case)])
    result = json.loads(raw)
    result["client_peak_rss_bytes"] = peak
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", default="http://127.0.0.1:10000/api/v1/query")
    parser.add_argument("--include-months", action="store_true")
    parser.add_argument("--match", default="")
    parser.add_argument("--reps", type=int, default=3)
    parser.add_argument("--month-reps", type=int, default=1)
    parser.add_argument("--worker", type=int, choices=[1, 2])
    parser.add_argument("--case-json")
    parser.add_argument("--output", type=Path, default=HERE / "results/exp62_arrow_typed_live.json")
    args = parser.parse_args()
    if args.worker:
        print(json.dumps(live.once(args.endpoint, json.loads(args.case_json), args.worker)), flush=True)
        return
    if min(args.reps, args.month_reps) < 1:
        parser.error("repetitions must be positive")
    cases = [case for case in live.CASES if (args.include_months or not case[4]) and args.match in case[0]]
    if not cases:
        parser.error("no matching cases")
    out = dict(endpoint=args.endpoint, scope="live schema v1/v2 comparison with fixed server compression",
               client=live.client.__file__, results=[],
               memory_scope="whole client process, including imports and checksum validation; 5 ms RSS sampling")
    for case in cases:
        if not case[4]:
            for schema in [1, 2]:
                measured(args.endpoint, schema, case)
        reference = None
        codec = None
        runs = {1: [], 2: []}
        reps = args.month_reps if case[4] else args.reps
        for rep in range(reps):
            for schema in ([1, 2] if rep % 2 == 0 else [2, 1]):
                result = measured(args.endpoint, schema, case)
                signature = {key:result[key] for key in ["rows", "row_hash_sum", "row_hash_xor", "columns", "dtypes"]}
                if reference is None:
                    reference, codec = signature, result["mode"]
                if signature != reference or result["mode"] != codec:
                    raise AssertionError(f"row content, dtypes, or codec changed for {case[0]}")
                runs[schema].append(result)
                out["results"].append(dict(case=case, repetition=rep+1, **result))
                args.output.parent.mkdir(parents=True, exist_ok=True)
                args.output.write_text(json.dumps(out, indent=2) + "\n")
                print(f"{case[0]} schema={schema} codec={codec}: {result['rows']} rows, {result['bytes']} bytes, {result['total_seconds']:.3f}s", flush=True)
        for schema, measured_runs in runs.items():
            print(f"median {case[0]} schema={schema}: {statistics.median(run['total_seconds'] for run in measured_runs):.3f}s", flush=True)


if __name__ == "__main__":
    main()
