"""Repack fixed plugin-sorted files at bounded row-group sizes, read-only inputs.

Preserves exact file membership/order for an isolated row-group-cap experiment.
Both archive-order and chronological file batches can be supplied. No Iceberg
commits, Pelican writes or API changes. Outputs remain for exp74 benchmarks.
"""
import argparse
import hashlib
from functools import reduce
from operator import xor
import json
import os
from pathlib import Path
import sys
import time

HERE=Path(__file__).resolve().parent
sys.path[:0]=[str(HERE),str(HERE.parent/'demo/tools')]
from exp59_arrow_ipc_compression import monitored
from exp72_plugin_layout import scan
from exp74_layout_scan_profiles import inventory
from exp76_time_batch_layout import native,fingerprint_sql,fingerprint
from v3_layout import audit_data_files


def combine_fingerprints(values):
    return [sum(v[0] for v in values),str(sum(int(v[1]) for v in values)),str(reduce(xor,(int(v[2]) for v in values),0))]


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--dataset',action='append',required=True,help='archive=directory or time=directory; already plugin/vsn/time sorted')
    p.add_argument('--cap',type=int,action='append')
    p.add_argument('--native-probe',type=Path,required=True)
    p.add_argument('--layout-helper',type=Path,required=True)
    p.add_argument('--work-dir',type=Path,required=True)
    p.add_argument('--memory',default='3GB')
    p.add_argument('--output',type=Path,default=HERE/'results/exp77_bounded_plugin_groups.json')
    a=p.parse_args();caps=list(dict.fromkeys(a.cap or [100000,262144,524288,1000000]));sources={};root=a.work_dir.resolve()
    if any(not 100000<=cap<=1000000 for cap in caps):p.error('caps must be between 100000 and 1000000')
    if root.exists() or 'node-data' in root.parts:p.error('fresh isolated work directory required')
    for argument in a.dataset:
        policy,sep,directory=argument.partition('=')
        if not sep or policy not in ('archive','time') or policy in sources:p.error('unique archive=directory/time=directory required')
        sources[policy]=inventory(directory)
    root.mkdir(parents=True);(root/'probes').mkdir()
    report=dict(scope=__doc__,status='running',work_dir=str(root),caps=caps,datasets={},
                helper_sha256=hashlib.sha256(a.layout_helper.read_bytes()).hexdigest(),
                native_sha256=hashlib.sha256(a.native_probe.read_bytes()).hexdigest(),
                fingerprints_checked_files=0)
    def save():
        a.output.parent.mkdir(parents=True,exist_ok=True);a.output.write_text(json.dumps(report,indent=2)+'\n')
    def raw_fingerprint(path,label):
        table,_=native(a.native_probe.resolve(),fingerprint_sql(scan([path])),a.memory,root/'probes'/label)
        return fingerprint(table)
    began=time.perf_counter();save()
    for policy,(files,info) in sources.items():
        expected={path:raw_fingerprint(path,f'{policy}-source-{index}') for index,path in enumerate(files)}
        day_fingerprint=combine_fingerprints(list(expected.values()))
        if report.get('complete_day_fingerprint',day_fingerprint)!=day_fingerprint:
            raise ValueError('batch policies do not contain the same complete raw day')
        report['complete_day_fingerprint']=day_fingerprint
        for cap in caps:
            label=f'{policy}_{cap}';directory=root/label/'data';directory.mkdir(parents=True)
            metadata={'format-version':3,'table-uuid':'local-cap-experiment','current-schema-id':0,'snapshots':[],
                      'properties':{'sage.batch.order':policy,'sage.row-group.rows':str(cap)},
                      'schemas':[{'schema-id':0,'fields':[{'name':'value','id':11,'type':'variant'}]}]}
            started=time.perf_counter();peak=0;outputs={}
            print('Packing',label,flush=True)
            for index,path in enumerate(files):
                output=directory/f'batch-{index:04d}.parquet'
                _,rss=monitored([str(a.layout_helper.resolve()),'--input',str(path),'--output',str(output),
                                 '--row-group-rows',str(cap)],dict(os.environ))
                peak=max(peak,rss)
                actual=raw_fingerprint(output,f'{label}-verify-{index}')
                if actual!=expected[path]:raise ValueError(label+': raw file fingerprint changed')
                outputs[output]=actual[0];report['fingerprints_checked_files']+=1
            audit=audit_data_files(metadata,outputs,'plugin-vsn-time')
            report['datasets'][label]=dict(path=str(directory),source_inventory=info,audit=audit,
                build_and_verify_seconds=time.perf_counter()-started,helper_peak_rss_bytes=peak,
                raw_rows_checked=sum(outputs.values()),file_fingerprints=[expected[path] for path in files])
            save();print(label,'complete',audit['row_groups'],'groups',audit['bytes'],'bytes',flush=True)
    report['total_wall_seconds']=time.perf_counter()-began;report['status']='complete';save();print('Report:',a.output)


if __name__=='__main__':main()
