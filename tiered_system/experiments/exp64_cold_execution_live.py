"""Measure cold API execution with the unchanged public Arrow schema and codec.

Run before/after API image rebuilds using separate --label and --output values,
or compare rows/content and timing with an exp60 Zstd baseline via --baseline.
Cold short windows default; --include-months adds March. No storage writes,
container operations, compression override, or schema opt-in.
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


def measured(endpoint, case):
    raw, peak = monitored([sys.executable,"-I",str(Path(__file__).resolve()),
        "--endpoint",endpoint,"--worker","--case-json",json.dumps(case)])
    result=json.loads(raw)
    result["client_peak_rss_bytes"]=peak
    return result


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint",default="http://127.0.0.1:10000/api/v1/query")
    parser.add_argument("--label",default="optimized metadata projection")
    parser.add_argument("--include-months",action="store_true")
    parser.add_argument("--match",default="")
    parser.add_argument("--reps",type=int,default=3)
    parser.add_argument("--month-reps",type=int,default=1)
    parser.add_argument("--baseline",type=Path)
    parser.add_argument("--worker",action="store_true")
    parser.add_argument("--case-json")
    parser.add_argument("--output",type=Path,default=HERE/"results/exp64_cold_execution_live.json")
    args=parser.parse_args()
    if args.worker:
        print(json.dumps(live.once(args.endpoint,json.loads(args.case_json))),flush=True)
        return
    if min(args.reps,args.month_reps)<1:parser.error("positive repetitions required")
    cases=[c for c in live.CASES if c[0].startswith("cold ") and (args.include_months or not c[4]) and args.match in c[0]]
    if not cases:parser.error("no matching cold cases")
    baseline={}
    if args.baseline:
        prior=json.loads(args.baseline.read_text())
        for run in prior["results"]:
            if run.get("mode")=="zstd" and run.get("arrow_schema",1)==1 and "row_hash_sum" in run:
                baseline.setdefault(run["query"],[]).append(run)
    out=dict(label=args.label,endpoint=args.endpoint,scope="cold public-schema query timing; fixed server codec",
        baseline=None if args.baseline is None else str(args.baseline),results=[],summaries=[],
        memory_scope="whole client process includes checksum validation; server CPU/memory must be sampled externally")
    for case in cases:
        if not case[4]:measured(args.endpoint,case)
        reference=None
        codec=None
        runs=[]
        for repetition in range(args.month_reps if case[4] else args.reps):
            run=measured(args.endpoint,case)
            signature={key:run[key] for key in ["rows","row_hash_sum","row_hash_xor","columns","dtypes"]}
            if reference is None:reference,codec=signature,run["mode"]
            if signature!=reference or codec!=run["mode"]:raise AssertionError("rows/dtypes/codec changed across repetitions")
            old=baseline.get(case[0],[])
            if args.baseline and not old:raise AssertionError(f"no usable Zstd public-schema baseline for {case[0]}")
            for prior in old:
                if {key:prior[key] for key in signature}!=signature or prior["mode"]!=codec:
                    raise AssertionError(f"content, column order, dtypes or codec differs from baseline: {case[0]}")
            runs.append(run)
            out["results"].append(dict(case=case,repetition=repetition+1,**run))
            args.output.parent.mkdir(parents=True,exist_ok=True)
            args.output.write_text(json.dumps(out,indent=2)+"\n")
            print(f"{case[0]}: rows={run['rows']} first_batch={run['first_batch_seconds']:.3f}s total={run['total_seconds']:.3f}s codec={codec}",flush=True)
        summary=dict(query=case[0],rows=runs[0]["rows"],mode=codec,
            total_seconds=statistics.median(r["total_seconds"] for r in runs),
            first_batch_seconds=statistics.median(r["first_batch_seconds"] for r in runs))
        if baseline.get(case[0]):
            old=baseline[case[0]]
            summary.update(baseline_total_seconds=statistics.median(r["total_seconds"] for r in old),
                baseline_first_batch_seconds=statistics.median(r["first_batch_seconds"] for r in old),
                rows_content_dtypes_match_baseline=True)
        out["summaries"].append(summary)
        args.output.write_text(json.dumps(out,indent=2)+"\n")
        print(json.dumps(summary),flush=True)


if __name__=="__main__":main()
