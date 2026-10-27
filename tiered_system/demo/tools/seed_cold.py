"""Seed an isolated v3 REST warehouse from the archive, then publish a replica.

The source archive still has split fields; they are converted once at ingestion.
Stored cold data is nanosecond TIMESTAMPTZ_NS plus one value VARIANT. Requires
DuckDB Python 2.0.x and a REST catalog sharing --root at the same absolute path.
No v2 seeding, in-place upgrades, implicit resumes or hot-data deletion.
--append explicitly adds an empty time interval to an existing v3 source.
"""
import argparse
import fcntl
import json
from pathlib import Path
import sys
import tempfile
import time
from datetime import date,timedelta

HERE = Path(__file__).resolve().parent
sys.path.insert(0,str(HERE))
from v3_publish import catalog_head,export_snapshot,upload_snapshot,local_path,restore_registration
from v3_layout import LAYOUTS,BATCH_ORDERS,ROW_GROUP_ROWS,MIN_ROW_GROUP_ROWS,MAX_ROW_GROUP_ROWS,write_plugin_day,row_fingerprint_sql,audit_layout

TAGS = ('plugin','vsn','node','host','task','job','zone')


def sql_quote(value):
    return "'"+str(value).replace("'","''")+"'"


def archive_source(files,day):
    end = date.fromisoformat(day)+timedelta(days=1)
    source = 'read_parquet(['+','.join(sql_quote(Path(f).resolve()) for f in files)+'],hive_partitioning=false)'
    return f"""SELECT CAST(time AS TIMESTAMPTZ_NS) AS time,name,{','.join(TAGS)},meta,
 CASE WHEN value_float IS NOT NULL THEN value_float::DOUBLE::VARIANT
      WHEN value_int IS NOT NULL THEN value_int::BIGINT::VARIANT
      ELSE value_str::VARCHAR::VARIANT END AS value
 FROM {source}
 WHERE CAST(time AS TIMESTAMPTZ_NS) >= TIMESTAMPTZ_NS '{day} 00:00:00+00'
   AND CAST(time AS TIMESTAMPTZ_NS) < TIMESTAMPTZ_NS '{end} 00:00:00+00'"""


def archive_select(files,day):
    return archive_source(files,day)+' ORDER BY time'


def create_table_sql(layout,batch_order='archive',row_group_rows=ROW_GROUP_ROWS):
    if layout not in LAYOUTS:raise ValueError('unknown layout')
    if batch_order not in BATCH_ORDERS:raise ValueError('unknown batch order')
    properties="'format-version'=3,'sage.layout'="+sql_quote(layout)
    if not MIN_ROW_GROUP_ROWS<=row_group_rows<=MAX_ROW_GROUP_ROWS:raise ValueError('invalid row-group cap')
    if layout=='plugin-vsn-time':properties+=",'sage.layout.version'='bounded-plugin-v1','sage.batch.order'="+sql_quote(batch_order)+",'sage.row-group.rows'="+sql_quote(row_group_rows)
    # Keep this column order: DuckDB assigns value Iceberg field ID 11.
    return """CREATE TABLE lake.sage.obs (
 time TIMESTAMPTZ_NS,name VARCHAR,plugin VARCHAR,vsn VARCHAR,node VARCHAR,
 host VARCHAR,task VARCHAR,job VARCHAR,zone VARCHAR,meta MAP(VARCHAR,VARCHAR),value VARIANT
) PARTITIONED BY (day(time)) WITH ("""+properties+')'


def empty_interval_sql(start,end):
    """Inclusive archive dates become an exact half-open stored-time range."""
    high=date.fromisoformat(end)+timedelta(days=1)
    return f"SELECT count(*) FROM lake.sage.obs WHERE time >= TIMESTAMPTZ_NS '{date.fromisoformat(start)} 00:00:00+00' AND time < TIMESTAMPTZ_NS '{high} 00:00:00+00'"


def validate_source_head(head,root):
    path=local_path(head)
    if Path(root).resolve() not in path.parents:
        raise ValueError('REST catalog points outside --root; check shared mount/configuration')
    metadata=json.loads(path.read_text())
    schema=next(s for s in metadata['schemas'] if s['schema-id']==metadata['current-schema-id'])
    types={field['name']:field['type'] for field in schema['fields']}
    value=next(f for f in schema['fields'] if f['name']=='value')
    if metadata['format-version']!=3 or types.get('time')!='timestamptz_ns' or types.get('value')!='variant' or value['id']!=11:
        raise ValueError('seed requires v3 timestamptz_ns / VARIANT field ID 11')
    return metadata


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--endpoint',default='http://127.0.0.1:18181')
    parser.add_argument('--root',type=Path,required=True,help='local REST warehouse root, shared with catalog')
    parser.add_argument('--archive',type=Path,default=HERE.parents[2]/'data/archive')
    parser.add_argument('--start',required=True)
    parser.add_argument('--end',required=True,help='inclusive last archive day')
    parser.add_argument('--export',type=Path,required=True,help='fresh local replica directory')
    parser.add_argument('--pelican-prefix',help='optional fresh /cold-storage-demo-v3-<unique> prefix')
    parser.add_argument('--append',action='store_true',help='append only if the entire requested time interval is empty')
    parser.add_argument('--restore-publication',type=Path,help='with --append, restore missing REST registration from a prior publication.json source head')
    parser.add_argument('--layout',choices=LAYOUTS,default='time',help='opt-in physical layout; the Iceberg partition spec remains day(time)')
    parser.add_argument('--layout-helper',type=Path,help='compiled icebergo/cmd/layoutpack binary, required for plugin-vsn-time')
    parser.add_argument('--batch-order',choices=BATCH_ORDERS,default='archive',help='plugin layout only: fix file batches from archive scan order or a staged chronological stream')
    parser.add_argument('--row-group-rows',type=int,default=ROW_GROUP_ROWS,help='plugin-pure row-group cap, 100000..1000000; plugin changes also flush a group')
    args = parser.parse_args()
    start,end = date.fromisoformat(args.start),date.fromisoformat(args.end)
    if end < start: parser.error('end precedes start')
    root = args.root.resolve()
    if 'node-data' in root.parts or not root.is_dir(): parser.error('root must be an existing isolated warehouse')
    if args.export.exists(): parser.error('--export must not exist')
    if (root/'sage'/'obs').exists() and not args.append: parser.error('sage.obs already exists; use --append for an empty interval or a fresh warehouse')
    if args.restore_publication and not args.append:parser.error('--restore-publication requires --append')
    if args.layout=='plugin-vsn-time' and (not args.layout_helper or not args.layout_helper.is_file()):
        parser.error('--layout plugin-vsn-time requires --layout-helper pointing to the compiled layoutpack binary')
    if args.layout!='plugin-vsn-time' and args.batch_order!='archive':parser.error('--batch-order time requires --layout plugin-vsn-time')
    if not MIN_ROW_GROUP_ROWS<=args.row_group_rows<=MAX_ROW_GROUP_ROWS:parser.error('--row-group-rows must be between 100000 and 1000000')
    if args.layout!='plugin-vsn-time' and args.row_group_rows!=ROW_GROUP_ROWS:parser.error('--row-group-rows applies only to --layout plugin-vsn-time')
    days = []
    day = start
    while day <= end:
        files = sorted((args.archive/('date='+str(day))).glob('*.parquet'))
        if not files: parser.error(f'missing archive files for {day}')
        days.append((str(day),files)); day += timedelta(days=1)
    import duckdb
    if not duckdb.__version__.startswith('2.0.'):
        parser.error(f'DuckDB Python 2.0.x required, found {duckdb.__version__}')
    # Single local writer. --append rejects an occupied interval, including a
    # commit acknowledged by the catalog before a prior process crashed.
    with (root/'.sage-v3-publisher.lock').open('w') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        pending=root/'seed-v3-pending.json'
        if pending.exists():
            raise ValueError('unresolved INSERT journal exists; inspect its day and the authoritative catalog before continuing')
        if args.restore_publication:
            restore_registration(args.endpoint,args.restore_publication,root)
        with tempfile.TemporaryDirectory(prefix='sage-v3-spill-') as spill:
            con = duckdb.connect()
            try:
                for sql in ("SET TimeZone='UTC'","SET threads=2","SET memory_limit='3GB'",
                            'SET temp_directory='+sql_quote(spill),'INSTALL iceberg','LOAD iceberg'):
                    con.execute(sql)
                con.execute("ATTACH 'warehouse' AS lake (TYPE iceberg, ENDPOINT "+sql_quote(args.endpoint)+", AUTHORIZATION_TYPE 'none')")
                if args.append:
                    previous=validate_source_head(catalog_head(args.endpoint),root)
                    if previous.get('properties',{}).get('sage.layout','time')!=args.layout:
                        raise ValueError('refusing to mix layouts in one source table; use a fresh trial warehouse')
                    if args.layout=='plugin-vsn-time' and previous.get('properties',{}).get('sage.layout.version')!='bounded-plugin-v1':
                        raise ValueError('old uncapped trial cannot be appended with the bounded writer')
                    if args.layout=='plugin-vsn-time' and previous.get('properties',{}).get('sage.batch.order','archive')!=args.batch_order:
                        raise ValueError('refusing to mix file batch orders; use a fresh warehouse')
                    if args.layout=='plugin-vsn-time' and int(previous.get('properties',{}).get('sage.row-group.rows',0))!=args.row_group_rows:
                        raise ValueError('refusing to mix row-group caps; use a fresh warehouse')
                    occupied=con.execute(empty_interval_sql(args.start,args.end)).fetchone()[0]
                    if occupied:
                        raise ValueError(f'append interval already contains {occupied} rows; refusing replay. Select only remaining empty days.')
                else:
                    con.execute('CREATE SCHEMA IF NOT EXISTS lake.sage')
                    con.execute(create_table_sql(args.layout,args.batch_order,args.row_group_rows))
                validate_source_head(catalog_head(args.endpoint),root)
                # Local durable commits per day bound recovery work and avoid
                # retaining a month transaction. No hot data is evicted here.
                for day,files in days:
                    began = time.monotonic()
                    journal=pending.with_suffix('.pending')
                    before=catalog_head(args.endpoint)
                    before_metadata=validate_source_head(before,root)
                    journal.write_text(json.dumps(dict(day=day,layout=args.layout,batch_order=args.batch_order,before_metadata=before),indent=2)+'\n')
                    journal.replace(pending)
                    detail={}
                    if args.layout=='plugin-vsn-time':
                        detail=write_plugin_day(con,archive_source(files,day),head=before,
                            endpoint=args.endpoint,helper=args.layout_helper.resolve(),day=day,batch_order=args.batch_order,row_group_rows=args.row_group_rows)
                        after=catalog_head(args.endpoint)
                        metadata=validate_source_head(after,root)
                        if len(metadata.get('snapshots',[]))!=len(before_metadata.get('snapshots',[]))+1:
                            raise ValueError('AddFiles did not produce one daily snapshot; refusing publication')
                        source='(SELECT * FROM iceberg_scan('+sql_quote(after)+') WHERE '+empty_interval_sql(day,day).split(' WHERE ',1)[1]+')'
                        actual=list(con.execute(row_fingerprint_sql(source)).fetchone())
                        if actual!=detail['expected_fingerprint']:
                            raise ValueError('committed plugin day differs from the clipped archive')
                    else:
                        con.execute('INSERT INTO lake.sage.obs '+archive_select(files,day))
                    # Audit progress, not permission to replay INSERTs. After a
                    # crash the catalog's data remains authoritative.
                    progress=root/'seed-v3-progress.json'
                    temporary=progress.with_suffix('.pending')
                    temporary.write_text(json.dumps(dict(last_committed_day=day,
                        source_metadata=catalog_head(args.endpoint),layout=args.layout,batch_order=args.batch_order,verification=detail,
                        requested_start=args.start,requested_end=args.end),indent=2)+'\n')
                    temporary.replace(progress)
                    pending.unlink()
                    print(f'{day}: committed in {time.monotonic()-began:.2f}s',flush=True)
                head = catalog_head(args.endpoint)
                validate_source_head(head,root)
                # Validate committed Iceberg files, not arbitrary COPY outputs.
                # A failed purity/order/type check never reaches Pelican.
                audit=audit_layout(head,args.layout)
                (root/'layout-audit.json').write_text(json.dumps(audit,indent=2)+'\n')
                uri = None
                if args.pelican_prefix:
                    import pelican_fileio,pelican_token
                    uri = pelican_token.ROOT_URL+args.pelican_prefix+'/warehouse/sage/obs'
                summary = export_snapshot(head,args.export,uri)
                summary['layout']=args.layout
                summary['batch_order']=args.batch_order if args.layout=='plugin-vsn-time' else None
                summary['layout_audit']='layout-audit.json'
                (args.export/'layout-audit.json').write_text(json.dumps(audit,indent=2)+'\n')
                (args.export/'publication.json').write_text(json.dumps(summary,indent=2)+'\n')
                if args.pelican_prefix:
                    upload_snapshot(args.export,args.pelican_prefix,pelican_fileio)
                    print('COLD_WAREHOUSE='+pelican_token.ROOT_URL+args.pelican_prefix+'/warehouse')
                print(json.dumps(summary,indent=2))
            finally:
                con.close()


if __name__=='__main__':
    main()
