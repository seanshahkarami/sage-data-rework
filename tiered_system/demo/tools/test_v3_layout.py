import json
import os
from pathlib import Path
import time
import tempfile
import unittest
from unittest.mock import Mock,patch

import pyarrow as pa

from seed_cold import archive_source,create_table_sql,validate_source_head
from v3_layout import write_plugin_day,ordered_pair_violations,row_fingerprint_sql,audit_layout
from v3_publish import catalog_head


class TestLayoutWriter(unittest.TestCase):
    def run_writer(self,fail=False,batch_order='archive'):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);head=root/'head.json'
            head.write_text(json.dumps({'location':str(root),'default-spec-id':0,'partition-specs':[
                {'spec-id':0,'fields':[{'source-id':1,'transform':'day','name':'day_time_1'}]}]}))
            statements=[];commands=[];con=Mock();batch_count=5
            def execute(sql):
                nonlocal batch_count
                statements.append(sql)
                if 'current_setting' in sql:return Mock(fetchone=lambda:(2,))
                if sql.startswith('COPY'):
                    path=sql.split(" TO '")[1].split("'")[0];Path(path).touch()
                if sql.startswith('SELECT count(*)') and ('_sage_row >= ' in sql or 'file_row_number >= ' in sql):
                    batch_count=3 if '_sage_row >= 0' in sql or 'file_row_number >= 0' in sql else 2
                if sql.startswith('SELECT count(*)') and ('_sage_row >= ' in sql or 'read_parquet(' in sql):
                    return Mock(fetchone=lambda:(batch_count,'123','456'))
                return Mock(fetchone=lambda:(5,'123','456'))
            def run(command,check):
                commands.append(command)
                if command[2]=='pack' and fail:raise RuntimeError('pack failed')
            con.execute.side_effect=execute
            with patch('v3_layout.subprocess.run',side_effect=run),patch('v3_layout.audit_data_files',return_value={'min_timestamp_ns':1740787200000000000,'max_timestamp_ns':1740787200000000000}):
                if fail:
                    with self.assertRaisesRegex(RuntimeError,'pack failed'):
                        write_plugin_day(con,'SELECT * FROM input',head=head,endpoint='local',helper='pack',day='2025-03-01',file_rows=3,batch_order=batch_order)
                else:
                    result=write_plugin_day(con,'SELECT * FROM input',head=head,endpoint='local',helper='pack',day='2025-03-01',file_rows=3,batch_order=batch_order)
                    self.assertEqual(result['files'],2)
                    manifest=json.loads(Path(result['batch_manifest']).read_text())
                    self.assertEqual([b['offset'] for b in manifest['batches']],[0,3])
                    self.assertEqual([b['rows'] for b in manifest['batches']],[3,2])
                    self.assertEqual(manifest['batch_order'],batch_order)
            return statements,commands

    def test_batches_fixed_before_sorting_and_one_daily_commit(self):
        statements,commands=self.run_writer()
        self.assertEqual([c[2] for c in commands],['pack','pack','commit'])
        copies=[s for s in statements if s.startswith('COPY')]
        self.assertIn('_sage_row >= 0 AND _sage_row < 3',copies[0])
        self.assertIn('_sage_row >= 3 AND _sage_row < 6',copies[1])
        self.assertTrue(all('ORDER BY plugin NULLS LAST,vsn NULLS LAST,time NULLS LAST' in s for s in copies))
        self.assertFalse(any(s.startswith('INSERT') for s in statements))

    def test_pack_failure_does_not_commit_and_restores_connection(self):
        statements,commands=self.run_writer(fail=True)
        self.assertEqual([c[2] for c in commands],['pack'])
        self.assertEqual(statements[-3:],['DROP TABLE IF EXISTS _sage_layout_day','SET threads=2',"SET memory_limit='2'"])

    def test_time_order_stages_once_before_fixed_windows_and_plugin_sort(self):
        statements,commands=self.run_writer(batch_order='time')
        copies=[s for s in statements if s.startswith('COPY')]
        self.assertEqual(len(copies),3)
        self.assertIn('ORDER BY time NULLS LAST',copies[0])
        self.assertTrue(all('file_row_number=true' in s for s in copies[1:]))
        self.assertIn('file_row_number >= 0 AND file_row_number < 3',copies[1])
        self.assertIn('file_row_number >= 3 AND file_row_number < 6',copies[2])
        self.assertEqual([c[2] for c in commands],['pack','pack','commit'])
        self.assertFalse(any('CREATE TEMP TABLE' in s for s in statements))

    def test_nulls_last_and_batch_boundary_order(self):
        vsn=pa.array(['A','A','B',None,None]);stamps=pa.array([3,4,1,5,6],type=pa.int64())
        n,last=ordered_pair_violations(vsn,stamps)
        self.assertEqual(n,0)
        n,_=ordered_pair_violations(pa.array(['B',None,None]),pa.array([7,8,2]),last)
        self.assertEqual(n,2) # B after NULL; same NULL key with decreasing time
        n,_=ordered_pair_violations(pa.array(['A']),pa.array([2]),('A',3))
        self.assertEqual(n,1)
        with self.assertRaisesRegex(ValueError,'time is null'):
            ordered_pair_violations(pa.array(['A']),pa.array([None],type=pa.int64()))

    def test_schema_and_source_are_unordered_until_each_insert(self):
        sql=archive_source([Path('quoted.parquet')],'2025-03-01')
        self.assertNotIn('ORDER BY',sql)
        ddl=create_table_sql('plugin-vsn-time')
        self.assertIn("PARTITIONED BY (day(time))",ddl)
        self.assertIn("'sage.layout'='plugin-vsn-time'",ddl)
        self.assertIn('meta MAP(VARCHAR,VARCHAR),value VARIANT',ddl)


@unittest.skipUnless(os.getenv('COLD_V3_LAYOUT_ENDPOINT') and os.getenv('COLD_V3_LAYOUT_ROOT') and os.getenv('COLD_V3_LAYOUT_HELPER'),
                     'isolated REST fixture, layoutpack and DuckDB Python 2.0 required')
class TestNativeLayoutRoundTrip(unittest.TestCase):
    def test_committed_files_and_daily_transaction(self):
        self.roundtrip('archive')

    def test_time_ordered_file_batches_and_daily_transaction(self):
        self.roundtrip('time')

    def roundtrip(self,batch_order):
        import duckdb
        self.assertTrue(duckdb.__version__.startswith('2.0.'))
        endpoint=os.environ['COLD_V3_LAYOUT_ENDPOINT'];root=Path(os.environ['COLD_V3_LAYOUT_ROOT']).resolve()
        namespace='layout_test_'+str(time.time_ns());table='lake.'+namespace+'.obs'
        source="""SELECT time,'test'::VARCHAR AS name,plugin,vsn,NULL::VARCHAR AS node,
 NULL::VARCHAR AS host,NULL::VARCHAR AS task,NULL::VARCHAR AS job,NULL::VARCHAR AS zone,
 MAP {}::MAP(VARCHAR,VARCHAR) AS meta,value FROM (VALUES
 (TIMESTAMPTZ_NS '2025-03-01 00:00:00.000000194+00','p1','W2',21.5::DOUBLE::VARIANT),
 (TIMESTAMPTZ_NS '2025-03-01 00:00:00.000000193+00','p1','W1',9007199254740993::BIGINT::VARIANT),
 (TIMESTAMPTZ_NS '2025-03-01 00:00:00.000000195+00','p2','W1',true::VARIANT),
 (TIMESTAMPTZ_NS '2025-03-01 00:00:00.000000196+00',NULL,NULL,'123'::VARIANT)
 ) t(time,plugin,vsn,value)"""
        from v3_layout import sql_quote
        con=duckdb.connect()
        try:
            for sql in ["SET TimeZone='UTC'","SET threads=2","SET memory_limit='3GB'",'INSTALL iceberg','LOAD iceberg']:
                con.execute(sql)
            con.execute("ATTACH 'warehouse' AS lake (TYPE iceberg,ENDPOINT "+sql_quote(endpoint)+",AUTHORIZATION_TYPE 'none')")
            con.execute('CREATE SCHEMA lake.'+namespace)
            con.execute(create_table_sql('plugin-vsn-time',batch_order).replace('lake.sage.obs',table,1))
            before=catalog_head(endpoint,namespace=namespace)
            before_snapshots=len(json.loads(Path(before).read_text()).get('snapshots',[]))
            result=write_plugin_day(con,source,head=before,endpoint=endpoint,
                helper=os.environ['COLD_V3_LAYOUT_HELPER'],day='2025-03-01',namespace=namespace,file_rows=3,batch_order=batch_order)
            head=catalog_head(endpoint,namespace=namespace)
            validate_source_head(head,root)
            after=json.loads(Path(head).read_text())
            self.assertEqual(len(after['snapshots']),before_snapshots+1)
            actual=list(con.execute(row_fingerprint_sql('iceberg_scan('+sql_quote(head)+')')).fetchone())
            self.assertEqual(actual,result['expected_fingerprint'])
            audit=audit_layout(head,'plugin-vsn-time')
            self.assertEqual(audit['rows'],4);self.assertEqual(audit['mixed_plugin_groups'],0)
            self.assertEqual(audit['files'],2);self.assertEqual(audit['row_groups'],3)
            self.assertTrue(audit['one_group_per_plugin'])
            self.assertEqual(audit['batch_order'],batch_order)
            if batch_order=='time':self.assertTrue(audit['time_batch_ranges_checked'])
            self.assertEqual(audit['value_field_id'],11);self.assertTrue(audit['ordering_checked'])
        finally:
            con.close()


if __name__=='__main__':unittest.main()
