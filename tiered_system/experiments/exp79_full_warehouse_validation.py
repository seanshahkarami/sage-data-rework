"""Read-only, complete-row validation of isolated March/April v3 layouts.

Writes fresh local reports only. Never commits, publishes, or changes APIs.
Requires DuckDB Python 2.0.x and the original local committed metadata heads.
"""
import argparse
from datetime import date,timedelta
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import time

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE))
sys.path.insert(0,str(HERE.parent/'demo/tools'))
from exp73_plugin_live_compare import full_warehouse_coverage,validate_candidate_audit
from v3_layout import audit_layout,row_fingerprint_sql,sql_quote
from v3_publish import local_path


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline-head',required=True)
    parser.add_argument('--candidate-head',required=True)
    parser.add_argument('--output-dir',type=Path,required=True,help='fresh local report directory')
    args=parser.parse_args()
    heads={layout:local_path(head).resolve() for layout,head in
           [('time',args.baseline_head),('plugin_vsn_time',args.candidate_head)]}
    output=args.output_dir.resolve()
    if any('node-data' in path.parts for path in [output,*heads.values()]):parser.error('/node-data is outside this experiment')
    if heads['time']==heads['plugin_vsn_time']:parser.error('distinct source heads required')
    if output.exists():parser.error('--output-dir must be fresh; previous reports are preserved')
    import duckdb
    if not duckdb.__version__.startswith('2.0.'):parser.error('DuckDB Python 2.0.x required')
    output.mkdir(parents=True)
    report=dict(status='running',scope='full stored-row fingerprint comparison, outside query timings',
                duckdb_version=duckdb.__version__,heads={k:dict(path=str(p),sha256=hashlib.sha256(p.read_bytes()).hexdigest()) for k,p in heads.items()},days=[])
    def save():
        pending=output/'validation.pending'
        pending.write_text(json.dumps(report,indent=2)+'\n');pending.replace(output/'validation.json')
    save()
    try:
        baseline=audit_layout(heads['time'],'time',verify_order=False)
        candidate=audit_layout(heads['plugin_vsn_time'],'plugin-vsn-time')
        validate_candidate_audit(candidate,full_warehouse=True)
        report['coverage']=full_warehouse_coverage(baseline,candidate)
        with tempfile.TemporaryDirectory(prefix='sage-full-layout-spill-') as spill,duckdb.connect() as con:
            for sql in ("SET TimeZone='UTC'","SET threads=2","SET memory_limit='3GB'",'SET temp_directory='+sql_quote(spill),'LOAD iceberg'):
                con.execute(sql)
            day=date(2025,3,1)
            while day<date(2025,5,1):
                end=day+timedelta(days=1);fingerprints={};began=time.monotonic()
                for layout,head in heads.items():
                    source="(SELECT * FROM iceberg_scan("+sql_quote(head)+") WHERE time >= TIMESTAMPTZ_NS '"+str(day)+" 00:00:00+00' AND time < TIMESTAMPTZ_NS '"+str(end)+" 00:00:00+00')"
                    fingerprints[layout]=list(con.execute(row_fingerprint_sql(source)).fetchone())
                if fingerprints['time']!=fingerprints['plugin_vsn_time']:
                    raise ValueError(f'{day}: full stored-row fingerprints differ: {fingerprints}')
                expected=report['coverage']['daily_rows'][str((day-date(1970,1,1)).days)]
                if fingerprints['time'][0]!=expected:raise ValueError(f'{day}: fingerprint row count differs from inventory')
                report['days'].append(dict(day=str(day),fingerprint=fingerprints['time'],seconds=time.monotonic()-began));save()
                print(f'{day}: full stored-row fingerprints match ({expected} rows)',flush=True)
                day=end
        # Audit files become usable by the HTTP benchmark only after every day matches.
        for name,audit in [('baseline-audit.json',baseline),('candidate-audit.json',candidate)]:
            (output/name).write_text(json.dumps(audit,indent=2)+'\n')
        report['status']='complete';save()
    except Exception as exc:
        report['status']='failed';report['error']=str(exc);save();raise


if __name__=='__main__':main()
