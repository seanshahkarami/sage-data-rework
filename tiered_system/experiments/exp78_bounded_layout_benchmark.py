"""Benchmark bounded candidates against time-first on all nine query shapes.

Consumes a complete exp77 report; preserves exp74 raw timings/profiles and adds
per-query ratios. No workload weighting or production promotion is implied.
"""
import argparse
import json
from pathlib import Path
import subprocess
import sys

HERE=Path(__file__).resolve().parent
CASES=('series_minute','series_hour','series_day','all_plugins_vsn_minute','all_plugins_vsn_hour',
       'fleet_minute','fleet_hour','plugin_hour','plugin_vsn_day')


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--build-report',type=Path,required=True)
    p.add_argument('--baseline',type=Path,required=True)
    p.add_argument('--native-probe',type=Path,required=True)
    p.add_argument('--sql-probe',type=Path,required=True)
    p.add_argument('--reps',type=int,default=3)
    p.add_argument('--output',type=Path,default=HERE/'results/exp78_bounded_layout_benchmark.json')
    a=p.parse_args();built=json.loads(a.build_report.read_text())
    if built.get('status')!='complete':p.error('all exp77 candidates must be complete before timing')
    args=[sys.executable,'-I',str(HERE/'exp74_layout_scan_profiles.py'),'--dataset','time_first='+str(a.baseline),
          '--native-probe',str(a.native_probe),'--sql-probe',str(a.sql_probe),'--reps',str(a.reps),'--output',str(a.output)]
    for label,data in built['datasets'].items():
        if not data['audit'].get('bounded_plugin_groups'):p.error('candidate did not pass bounded plugin audit')
        args+=['--dataset',label+'='+data['path']]
    for case in CASES:args+=['--case',case]
    subprocess.run(args,check=True)
    report=json.loads(a.output.read_text());summary={(r['case'],r['layout']):r for r in report['summaries']}
    comparisons=[]
    for label,data in built['datasets'].items():
        ratios={case:summary[(case,label)]['total_seconds']/summary[(case,'time_first')]['total_seconds'] for case in CASES}
        comparisons.append(dict(layout=label,bytes=data['audit']['bytes'],row_groups=data['audit']['row_groups'],
            ratio_to_time_first=ratios,worst_case_ratio=max(ratios.values()),
            all_medians_no_slower_than_baseline=all(ratio<=1 for ratio in ratios.values())))
    report['bounded_build_report']=str(a.build_report)
    report['comparisons']=sorted(comparisons,key=lambda r:r['worst_case_ratio'])
    report['comparison_notes']='Ratios below 1 are faster; equal query weighting is not assumed. Three-repetition medians are observations, not significance tests. Local native measurements exclude HTTP/Pelican/Iceberg metadata.'
    a.output.write_text(json.dumps(report,indent=2)+'\n')
    for result in report['comparisons']:print(result['layout'],result['ratio_to_time_first'],flush=True)


if __name__=='__main__':main()
