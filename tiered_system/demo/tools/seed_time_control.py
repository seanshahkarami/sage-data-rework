"""Publish a one-day time-first control by copying original Parquet bytes.

Fresh isolated REST catalog/root only. No re-encoding, append or source edits.
Matches candidate coverage/snapshot count without changing the full baseline.
"""
import argparse
from datetime import date
import fcntl
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE))
from seed_cold import create_table_sql,validate_source_head,sql_quote
from v3_layout import snapshot_files,audit_data_files,audit_layout,row_fingerprint_sql
from v3_publish import catalog_head,local_path,export_snapshot,upload_snapshot
import pyarrow.parquet as pq


def select_day(files,day):
    low=(date.fromisoformat(day)-date(1970,1,1)).days*86_400_000_000_000;high=low+86_400_000_000_000
    selected={}
    for path,rows in files.items():
        parquet=pq.ParquetFile(path);bounds=[]
        for i in range(parquet.metadata.num_row_groups):
            group=parquet.metadata.row_group(i)
            col=next(group.column(j) for j in range(group.num_columns) if group.column(j).path_in_schema=='time')
            stats=col.statistics
            if not stats or not stats.has_min_max:raise ValueError('day control requires time statistics')
            bounds.append((stats.min_raw,stats.max_raw))
        if any(hi>=low and lo<high for lo,hi in bounds):
            if any(lo<low or hi>=high for lo,hi in bounds):raise ValueError('source file crosses control day; refusing rewrite')
            selected[path]=rows
    if not selected:raise ValueError('no source files for control day')
    return selected


def digest(path):
    checksum=hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda:stream.read(8*1024*1024),b''):checksum.update(block)
    return checksum.hexdigest()


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--source-head',type=Path,required=True)
    p.add_argument('--root',type=Path,required=True)
    p.add_argument('--endpoint',required=True)
    p.add_argument('--layout-helper',type=Path,required=True)
    p.add_argument('--day',default='2025-03-01')
    p.add_argument('--export',type=Path,required=True)
    p.add_argument('--pelican-prefix')
    a=p.parse_args();root=local_path(a.root)
    if not root.is_dir() or any(root.iterdir()) or a.export.exists():p.error('empty isolated root and fresh export directory required')
    metadata,all_files=snapshot_files(a.source_head);files=select_day(all_files,a.day)
    if metadata.get('properties',{}).get('sage.layout','time')!='time':p.error('source must be the original time-first warehouse')
    audit_data_files(metadata,files,'time',verify_order=False)
    import duckdb
    if not duckdb.__version__.startswith('2.0.'):p.error('DuckDB Python 2.0.x required')
    with (root/'.sage-v3-publisher.lock').open('w') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        con=duckdb.connect()
        try:
            for sql in ("SET TimeZone='UTC'","SET threads=2","SET memory_limit='3GB'",'INSTALL iceberg','LOAD iceberg'):con.execute(sql)
            con.execute("ATTACH 'warehouse' AS lake (TYPE iceberg,ENDPOINT "+sql_quote(a.endpoint)+",AUTHORIZATION_TYPE 'none')")
            con.execute('CREATE SCHEMA IF NOT EXISTS lake.sage');con.execute(create_table_sql('time'))
            before=catalog_head(a.endpoint);target=validate_source_head(before,root)
            table=local_path(target['location']);data=table/'data'/('date='+a.day);data.mkdir(parents=True,exist_ok=False)
            source='read_parquet(['+','.join(sql_quote(path) for path in files)+'],hive_partitioning=false)'
            expected=list(con.execute(row_fingerprint_sql(source)).fetchone())
            copied=[];checksums={}
            for index,path in enumerate(files):
                destination=data/(str(index)+'-'+path.name)
                shutil.copyfile(path,destination)
                checksum=digest(path)
                if digest(destination)!=checksum:raise ValueError('control copy differs from source bytes')
                copied.append(str(destination));checksums[str(destination)]=checksum
            listing=root/'control-files.json';listing.write_text(json.dumps(copied))
            pending=root/'seed-v3-pending.json';pending.write_text(json.dumps(dict(day=a.day,before_metadata=before,source_metadata=str(a.source_head))))
            subprocess.run([str(a.layout_helper.resolve()),'--mode','commit','--endpoint',a.endpoint,'--head',before,'--files',str(listing)],check=True)
            head=catalog_head(a.endpoint);current=validate_source_head(head,root)
            if len(current['snapshots'])!=1:raise ValueError('control did not produce exactly one snapshot')
            actual=list(con.execute(row_fingerprint_sql('iceberg_scan('+sql_quote(head)+')')).fetchone())
            if actual!=expected:raise ValueError('committed control differs from original rows')
            audit=audit_layout(head,'time');(root/'layout-audit.json').write_text(json.dumps(audit,indent=2)+'\n')
            provenance=dict(source_metadata=str(a.source_head),source_files=[str(p) for p in files],sha256=checksums,fingerprint=actual)
            (root/'control-provenance.json').write_text(json.dumps(provenance,indent=2)+'\n');pending.unlink()
            uri=None
            if a.pelican_prefix:
                import pelican_token,pelican_fileio
                uri=pelican_token.ROOT_URL+a.pelican_prefix+'/warehouse/sage/obs'
            summary=export_snapshot(head,a.export,uri)
            summary.update(layout='time',control='byte-identical one-day baseline',layout_audit='layout-audit.json')
            for name in ('layout-audit.json','control-provenance.json'):shutil.copyfile(root/name,a.export/name)
            (a.export/'publication.json').write_text(json.dumps(summary,indent=2)+'\n')
            if a.pelican_prefix:upload_snapshot(a.export,a.pelican_prefix,pelican_fileio)
            print(json.dumps(summary,indent=2))
        finally:con.close()


if __name__=='__main__':main()
