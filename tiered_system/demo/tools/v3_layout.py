"""Physical v3 layout checks and bounded-memory plugin ingestion.

VARIANT's typed_value child is an encoding detail. The logical value field
must remain VARIANT with Iceberg field ID 11 in every committed data file.
"""
from pathlib import Path
import json
import re
import subprocess
import tempfile
import uuid
import resource
import sys
from datetime import date

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pyiceberg.avro.file import AvroFile
from pyiceberg.io.pyarrow import PyArrowFileIO

from v3_publish import local_path

LAYOUTS=('time','plugin-vsn-time')
BATCH_ORDERS=('archive','time')
FILE_ROWS=10_000_000
ROW_GROUP_ROWS=262_144
MIN_ROW_GROUP_ROWS=100_000
MAX_ROW_GROUP_ROWS=1_000_000
FIELD_IDS="{'time':1,'name':2,'plugin':3,'vsn':4,'node':5,'host':6,'task':7,'job':8,'zone':9,'meta':{__duckdb_field_id:10,key:12,value:13},'value':11}"


def sql_quote(value):
    return "'"+str(value).replace("'","''")+"'"


def peak_rss_bytes(who):
    # macOS reports bytes; Linux reports KiB. This is a per-process high-water
    # mark, not simultaneous combined parent/child memory.
    return int(resource.getrusage(who).ru_maxrss)*(1 if sys.platform=='darwin' else 1024)


def row_fingerprint_sql(source):
    fields='time,name,value,meta,plugin,vsn,node,host,task,job,zone'
    return f'SELECT count(*),sum(hash({fields}))::VARCHAR,bit_xor(hash({fields}))::VARCHAR FROM {source}'


def ordered_day_sql(source,path):
    """External sort straight to disk, avoiding an unspillable whole-day CTAS."""
    return ('COPY (SELECT * FROM ('+source+') ORDER BY time NULLS LAST) TO '+sql_quote(path)+
            ' (FORMAT parquet,COMPRESSION zstd,COMPRESSION_LEVEL 3,ROW_GROUP_SIZE 122880,FIELD_IDS '+FIELD_IDS+')')


def batch_sql(source,offset,file_rows,ordered_path=None):
    if ordered_path is not None:
        return ('(SELECT * EXCLUDE (file_row_number) FROM read_parquet('+sql_quote(ordered_path)+
                f',file_row_number=true,hive_partitioning=false) WHERE file_row_number >= {offset} AND file_row_number < {offset+file_rows})')
    return f"""(SELECT * EXCLUDE (_sage_row) FROM (
 SELECT row_number() OVER ()-1 AS _sage_row,* FROM ({source})
) WHERE _sage_row >= {offset} AND _sage_row < {offset+file_rows})"""


def write_plugin_day(con,source,*,head,endpoint,helper,day,namespace='sage',file_rows=FILE_ROWS,batch_order='archive',row_group_rows=ROW_GROUP_ROWS):
    """Fix batch membership, pack bounded plugin-pure groups, then AddFiles.

    Archive order uses single-thread scan windows; time order first sorts the
    day straight to disk. Each fixed window is then sorted by plugin/vsn/time.
    No whole-day DuckDB table is materialized.
    Only the Go helper commits; a failure leaves the seeder's pending journal.
    """
    if not 1<=file_rows<=FILE_ROWS:raise ValueError('invalid file row limit')
    if not MIN_ROW_GROUP_ROWS<=row_group_rows<=MAX_ROW_GROUP_ROWS:raise ValueError('row groups must be capped between 100K and 1M rows')
    if batch_order not in BATCH_ORDERS:raise ValueError('invalid batch order')
    metadata=json.loads(local_path(head).read_text())
    table_root=local_path(metadata['location'])
    table_root.mkdir(parents=True,exist_ok=True)
    spec=next(s for s in metadata['partition-specs'] if s['spec-id']==metadata['default-spec-id'])
    if len(spec['fields'])!=1 or spec['fields'][0]['transform']!='day' or spec['fields'][0]['source-id']!=1:
        raise ValueError('layout writer requires only day(time) partitioning')
    partition_name=spec['fields'][0]['name']
    if Path(partition_name).name!=partition_name:raise ValueError('invalid partition field name')
    epoch_day=(date.fromisoformat(day)-date(1970,1,1)).days
    data_dir=table_root/'data'/(partition_name+'='+str(epoch_day))
    expected=con.execute(row_fingerprint_sql('('+source+')')).fetchone()
    if expected[0]==0:raise ValueError('no observations in the requested day')
    # Single-thread scan order assigns batch membership. Each window is sorted
    # after that membership is fixed.
    threads=con.execute("SELECT current_setting('threads')").fetchone()[0]
    memory=con.execute("SELECT current_setting('memory_limit')").fetchone()[0]
    con.execute('SET threads=1')
    # One March day is tens of GB as a single in-memory table and that CREATE
    # does not spill. Row numbers still come from one single-thread scan order;
    # each fixed batch is filtered and sorted on its own.
    con.execute("SET memory_limit='6GB'")
    try:
        batches=[];paths=[]
        with tempfile.TemporaryDirectory(prefix='sage-plugin-pack-',dir=table_root) as scratch:
            ordered_path=Path(scratch)/'time-ordered-day.parquet' if batch_order=='time' else None
            if ordered_path is not None:
                con.execute(ordered_day_sql(source,ordered_path))
                staged='read_parquet('+sql_quote(ordered_path)+',hive_partitioning=false)'
                if list(con.execute(row_fingerprint_sql(staged)).fetchone())!=list(expected):
                    raise ValueError('time-ordered staging differs from the source day')
            previous_high=None
            for offset in range(0,expected[0],file_rows):
                batch=batch_sql(source,offset,file_rows,ordered_path)
                fingerprint=list(con.execute(row_fingerprint_sql(batch)).fetchone())
                if fingerprint[0]!=min(file_rows,expected[0]-offset):raise ValueError('fixed batch row count mismatch')
                intermediate=Path(scratch)/'batch.parquet'
                con.execute('COPY (SELECT * FROM '+batch+' ORDER BY plugin NULLS LAST,vsn NULLS LAST,time NULLS LAST) TO '+sql_quote(intermediate)+
                            ' (FORMAT parquet,COMPRESSION zstd,COMPRESSION_LEVEL 3,ROW_GROUP_SIZE 122880,FIELD_IDS '+FIELD_IDS+')')
                output=data_dir/('plugin-batch-'+uuid.uuid4().hex+'.parquet')
                subprocess.run([str(helper),'--mode','pack','--input',str(intermediate),'--output',str(output),
                                '--row-group-rows',str(row_group_rows)],check=True)
                intermediate.unlink()
                # Audit the actual encoding and groups before any catalog commit.
                audit=audit_data_files(metadata,{output:fingerprint[0]},'plugin-vsn-time')
                if audit['min_timestamp_ns']//86_400_000_000_000!=epoch_day:raise ValueError('packed file is outside the requested UTC day')
                if batch_order=='time' and previous_high is not None and audit['min_timestamp_ns']<previous_high:
                    raise ValueError('time-ordered file batches overlap out of order')
                previous_high=audit['max_timestamp_ns']
                actual=list(con.execute(row_fingerprint_sql('read_parquet('+sql_quote(output)+',hive_partitioning=false)')).fetchone())
                if actual!=fingerprint:raise ValueError('packed file differs from its fixed input batch')
                paths.append(str(output));batches.append(dict(path=str(output),offset=offset,rows=fingerprint[0],fingerprint=fingerprint,
                    min_timestamp_ns=audit['min_timestamp_ns'],max_timestamp_ns=audit['max_timestamp_ns']))
            # One daily AddFiles transaction; table partition values and v3 row
            # lineage/manifest metadata are assigned by iceberg-go, not invented.
            record=table_root/('layout-batches-'+uuid.uuid4().hex+'.json')
            record.write_text(json.dumps(dict(file_rows=file_rows,batch_order=batch_order,row_group_rows=row_group_rows,batches=batches),indent=2)+'\n')
            listing=Path(scratch)/'files.json';listing.write_text(json.dumps(paths))
            subprocess.run([str(helper),'--mode','commit','--endpoint',endpoint,'--namespace',namespace,
                            '--head',str(head),'--files',str(listing)],check=True)
        return dict(files=len(paths),expected_fingerprint=list(expected),batch_manifest=str(record),file_row_limit=file_rows,batch_order=batch_order,row_group_rows=row_group_rows,
                    seeder_peak_rss_bytes=peak_rss_bytes(resource.RUSAGE_SELF),helper_peak_rss_bytes=peak_rss_bytes(resource.RUSAGE_CHILDREN),
                    rss_scope='separate parent and child process high-water marks since seeder startup; not additive or simultaneous')
    finally:
        con.execute('DROP TABLE IF EXISTS _sage_layout_day')
        con.execute('SET threads='+str(int(threads)))
        con.execute('SET memory_limit='+sql_quote(memory))


def snapshot_files(head):
    """Read only files reachable from the authoritative current snapshot."""
    document=json.loads(local_path(head).read_text())
    snapshot=next(s for s in document['snapshots'] if s['snapshot-id']==document['current-snapshot-id'])
    table=local_path(document['location'])
    io=PyArrowFileIO();files={}
    with AvroFile(io.new_input(str(local_path(snapshot['manifest-list'])))) as manifests:
        fields={f.name:i for i,f in enumerate(manifests.schema.fields)}
        for manifest in manifests:
            if manifest[fields['content']]!=0:raise ValueError('layout trial requires append-only data manifests')
            with AvroFile(io.new_input(str(local_path(manifest[fields['manifest_path']])))) as entries:
                columns={f.name:i for i,f in enumerate(entries.schema.fields)}
                nested=entries.schema.fields[columns['data_file']].field_type.fields
                data_columns={f.name:i for i,f in enumerate(nested)}
                for entry in entries:
                    if entry[columns['status']]==2:continue
                    data=entry[columns['data_file']]
                    if data[data_columns['content']]!=0:raise ValueError('delete files are not part of this trial')
                    path=local_path(data[data_columns['file_path']]);path.relative_to(table)
                    files[path]=data[data_columns['record_count']]
    return document,files


def ordered_pair_violations(vsn,time,last=None):
    """Vectorized vsn ASC NULLS LAST, time ASC check, including batch seams."""
    if len(vsn)==0:return 0,last
    if last is None:
        before_v,before_t=vsn.slice(0,len(vsn)-1),time.slice(0,len(time)-1)
        current_v,current_t=vsn.slice(1),time.slice(1)
    else:
        before_v=pa.concat_arrays([pa.array([last[0]],type=vsn.type),vsn.slice(0,len(vsn)-1)])
        before_t=pa.concat_arrays([pa.array([last[1]],type=time.type),time.slice(0,len(time)-1)])
        current_v,current_t=vsn,time
    same=pc.or_(pc.fill_null(pc.equal(current_v,before_v),False),pc.and_(pc.is_null(current_v),pc.is_null(before_v)))
    backwards=pc.or_(pc.fill_null(pc.less(current_v,before_v),False),pc.and_(pc.is_null(before_v),pc.is_valid(current_v)))
    backwards=pc.or_(backwards,pc.and_(same,pc.fill_null(pc.less(current_t,before_t),False)))
    if pc.any(pc.is_null(time)).as_py():raise ValueError('stored observation time is null')
    return int(pc.sum(pc.cast(backwards,pa.int64())).as_py() or 0),(vsn[-1].as_py(),time[-1].as_py())


def audit_layout(head,layout,verify_order=True):
    if layout not in LAYOUTS:raise ValueError('unknown layout')
    metadata,files=snapshot_files(head)
    result=audit_data_files(metadata,files,layout,verify_order)
    result['metadata_location']=str(head)
    return result


def audit_data_files(metadata,files,layout,verify_order=True):
    schema=next(s for s in metadata['schemas'] if s['schema-id']==metadata['current-schema-id'])
    value=next(f for f in schema['fields'] if f['name']=='value')
    if metadata['format-version']!=3 or value['id']!=11 or value['type']!='variant':
        raise ValueError('expected Iceberg v3 value VARIANT, field ID 11')
    cap=metadata.get('properties',{}).get('sage.row-group.rows') if layout=='plugin-vsn-time' else None
    cap=int(cap) if cap is not None else None
    if cap is not None and not MIN_ROW_GROUP_ROWS<=cap<=MAX_ROW_GROUP_ROWS:raise ValueError('invalid declared row-group cap')
    result=dict(layout=layout,table_uuid=metadata['table-uuid'],
                snapshot_id=metadata.get('current-snapshot-id'),snapshots=len(metadata.get('snapshots',[])),
                value_field_id=11,value_type='variant',files=len(files),rows=0,bytes=0,
                row_groups=0,mixed_plugin_groups=0,ordering_checked=layout=='plugin-vsn-time' and verify_order,
                physical_typed_value_types={},min_timestamp_ns=None,max_timestamp_ns=None,
                file_row_limit=FILE_ROWS if layout=='plugin-vsn-time' else None,
                one_group_per_plugin=layout=='plugin-vsn-time',
                plugin_pure_groups=layout=='plugin-vsn-time',row_group_row_limit=cap,
                bounded_plugin_groups=layout=='plugin-vsn-time' and cap is not None,max_row_group_rows=0,
                batch_order=metadata.get('properties',{}).get('sage.batch.order','archive') if layout=='plugin-vsn-time' else None)
    file_ranges=[]
    for path,expected_rows in files.items():
        parquet=pq.ParquetFile(path);field=parquet.schema_arrow.field('value')
        if field.metadata is None or field.metadata.get(b'PARQUET:field_id')!=b'11' or not re.search(r'\bgroup field_id=11 value \(Variant\(1\)\)',str(parquet.schema)):
            raise ValueError(f'{path}: outer value is not Parquet VARIANT with field ID 11')
        time_field=parquet.schema_arrow.field('time')
        if not pa.types.is_timestamp(time_field.type) or time_field.type.unit!='ns' or time_field.type.tz!='UTC':
            raise ValueError(f'{path}: time is not UTC nanoseconds')
        if parquet.metadata.num_rows!=expected_rows:raise ValueError('manifest/file row count mismatch')
        typed=str(field.type.field('typed_value').type) if field.type.get_field_index('typed_value')>=0 else 'unshredded'
        result['physical_typed_value_types'][typed]=result['physical_typed_value_types'].get(typed,0)+1
        result['rows']+=parquet.metadata.num_rows;result['bytes']+=path.stat().st_size
        if layout=='plugin-vsn-time' and parquet.metadata.num_rows>FILE_ROWS:raise ValueError('file exceeds row limit')
        previous_plugin=None;seen_plugin=False;previous_rows=None;last=None
        file_min,file_max=None,None
        for i in range(parquet.metadata.num_row_groups):
            group=parquet.metadata.row_group(i);result['row_groups']+=1
            result['max_row_group_rows']=max(result['max_row_group_rows'],group.num_rows)
            if cap is not None and group.num_rows>cap:raise ValueError(f'{path}: row group exceeds cap')
            time_column=next(group.column(j) for j in range(group.num_columns) if group.column(j).path_in_schema=='time')
            time_stats=time_column.statistics
            if time_stats and time_stats.has_min_max:
                # min/max convert through datetime and reject nanoseconds. The physical values are epoch nanoseconds.
                low,high=time_stats.min_raw,time_stats.max_raw
                file_min=low if file_min is None else min(file_min,low)
                file_max=high if file_max is None else max(file_max,high)
                result['min_timestamp_ns']=low if result['min_timestamp_ns'] is None else min(low,result['min_timestamp_ns'])
                result['max_timestamp_ns']=high if result['max_timestamp_ns'] is None else max(high,result['max_timestamp_ns'])
            column=next(group.column(j) for j in range(group.num_columns) if group.column(j).path_in_schema=='plugin')
            stats=column.statistics
            pure=stats is not None and (stats.null_count==group.num_rows or
                 (stats.null_count==0 and stats.has_min_max and stats.min==stats.max))
            if not pure:result['mixed_plugin_groups']+=1
            if layout=='plugin-vsn-time':
                if not pure:raise ValueError(f'{path}: mixed or unverifiable plugin row group')
                plugin=None if stats.null_count==group.num_rows else stats.min
                same_plugin=seen_plugin and plugin==previous_plugin
                if same_plugin:
                    result['one_group_per_plugin']=False
                    if cap is not None and previous_rows!=cap:raise ValueError(f'{path}: plugin group flushed before its cap')
                if seen_plugin and not same_plugin and (previous_plugin is None or (plugin is not None and plugin<previous_plugin)):
                    raise ValueError(f'{path}: plugin groups are out of order')
                if not same_plugin:last=None
                previous_plugin=plugin;previous_rows=group.num_rows;seen_plugin=True
                if verify_order:
                    for batch in parquet.iter_batches(batch_size=65536,row_groups=[i],columns=['vsn','time']):
                        violations,last=ordered_pair_violations(batch.column(0),pc.cast(batch.column(1),pa.int64()),last)
                        if violations:raise ValueError(f'{path}: vsn/time order is not preserved')
        if layout=='plugin-vsn-time' and (file_min is None or file_max is None or file_min//86_400_000_000_000!=file_max//86_400_000_000_000):
            raise ValueError(f'{path}: file must stay within one UTC day')
        file_ranges.append(dict(path=str(path),rows=expected_rows,min_timestamp_ns=file_min,max_timestamp_ns=file_max))
    if layout=='plugin-vsn-time' and result['batch_order']=='time':
        previous_high=None;previous_day=None
        for bounds in sorted(file_ranges,key=lambda r:(r['min_timestamp_ns'],r['max_timestamp_ns'])):
            day=bounds['min_timestamp_ns']//86_400_000_000_000
            if day==previous_day and bounds['min_timestamp_ns']<previous_high:raise ValueError('time-ordered file ranges overlap')
            previous_high=bounds['max_timestamp_ns'];previous_day=day
        result['time_batch_ranges_checked']=True
    result['file_time_ranges']=file_ranges
    return result
