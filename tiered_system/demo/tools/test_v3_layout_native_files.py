"""Real DuckDB 2.0 Parquet checks; set NATIVE_DUCKDB_PROBE to the Go probe."""
import os
from pathlib import Path
import sys
import tempfile
import subprocess
import unittest
from unittest.mock import patch

from v3_layout import audit_layout,sql_quote


@unittest.skipUnless(os.getenv('NATIVE_DUCKDB_PROBE'),'native DuckDB 2.0 Go SQL probe required')
class TestNativeParquetContract(unittest.TestCase):
    @unittest.skipUnless(os.getenv('COLD_V3_LAYOUT_HELPER'),'compiled layoutpack binary required')
    def test_shared_file_exact_groups_and_native_semantic_roundtrip(self):
        import json
        import pyarrow.parquet as pq
        from v3_layout import FIELD_IDS,row_fingerprint_sql
        sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'experiments'))
        from exp67_metadata_projection import native_table
        metadata={'format-version':3,'current-schema-id':0,'current-snapshot-id':1,'table-uuid':'fixture',
                  'snapshots':[{'snapshot-id':1}],'schemas':[{'schema-id':0,'fields':[{'name':'value','id':11,'type':'variant'}]}]}
        source="""SELECT TIMESTAMPTZ_NS '2025-03-01 00:00:00.000000194+00'+n::BIGINT*INTERVAL 1 NANOSECOND AS time,
 'test'::VARCHAR AS name,CASE WHEN n<70000 THEN 'large' WHEN n<70002 THEN 'small' ELSE NULL END AS plugin,
 CASE WHEN n%2=0 THEN 'W1' ELSE 'W2' END AS vsn,NULL::VARCHAR AS node,NULL::VARCHAR AS host,
 NULL::VARCHAR AS task,NULL::VARCHAR AS job,NULL::VARCHAR AS zone,MAP {'sample':n::VARCHAR} AS meta,
 CASE WHEN n%5=0 THEN 9007199254740993::BIGINT::VARIANT WHEN n%5=1 THEN 21.5::DOUBLE::VARIANT
 WHEN n%5=2 THEN true::VARIANT WHEN n%5=3 THEN 'text'::VARIANT ELSE NULL::VARIANT END AS value
 FROM range(70005) r(n)"""
        # epoch_ns avoids any dependency on interval nanosecond arithmetic.
        source=source.replace("TIMESTAMPTZ_NS '2025-03-01 00:00:00.000000194+00'+n::BIGINT*INTERVAL 1 NANOSECOND",
                              "make_timestamp_ns(1740787200000000194+n::BIGINT)::TIMESTAMPTZ_NS")
        with tempfile.TemporaryDirectory(prefix='sage-shared-file-') as directory:
            root=Path(directory);path=root/'input.parquet';packed=root/'packed.parquet'
            probe=Path(os.environ['NATIVE_DUCKDB_PROBE'])
            sql=f"""WITH w AS (COPY ({source} ORDER BY plugin NULLS LAST,vsn NULLS LAST,time NULLS LAST)
 TO {sql_quote(path)} (FORMAT parquet,ROW_GROUP_SIZE 8192,FIELD_IDS {FIELD_IDS}))
 SELECT TIMESTAMP_NS '2025-03-01' AS timestamp,'write'::VARCHAR AS name,Count::VARCHAR AS value,'{{}}'::VARCHAR AS meta FROM w"""
            native_table(probe,sql,'512MB',root/'write')
            subprocess.run([os.environ['COLD_V3_LAYOUT_HELPER'],'--input',str(path),'--output',str(packed)],check=True)
            file=pq.ParquetFile(packed)
            self.assertEqual([file.metadata.row_group(i).num_rows for i in range(file.metadata.num_row_groups)],[70000,2,3])
            expected_sort=pq.SortingColumn.from_ordering(file.schema_arrow,[('plugin','ascending'),('vsn','ascending'),('time','ascending')],null_placement='at_end')
            self.assertTrue(all(file.metadata.row_group(i).sorting_columns==expected_sort for i in range(3)))
            with patch('v3_layout.snapshot_files',return_value=(metadata,{packed:70005})):
                result=audit_layout('fixture','plugin-vsn-time')
                self.assertEqual(result['row_groups'],3);self.assertTrue(result['one_group_per_plugin'])
            # Changing provenance cannot turn overlapping archive-order files
            # into chronological file batches.
            import shutil
            from v3_layout import audit_data_files
            duplicate=root/'duplicate.parquet';shutil.copyfile(packed,duplicate)
            chronological=dict(metadata,properties={'sage.batch.order':'time'})
            with self.assertRaisesRegex(ValueError,'overlap'):
                audit_data_files(chronological,{packed:70005,duplicate:70005},'plugin-vsn-time',verify_order=False)
            for name,field_id in [('time',1),('meta',10),('value',11)]:
                self.assertEqual(file.schema_arrow.field(name).metadata[b'PARQUET:field_id'],str(field_id).encode())
            def digest(file,run):
                fields='time,name,value,meta,plugin,vsn,node,host,task,job,zone'
                query=f"SELECT TIMESTAMP_NS '2025-03-01' AS timestamp,count(*)::VARCHAR AS name,sum(hash({fields}))::VARCHAR AS value,bit_xor(hash({fields}))::VARCHAR AS meta FROM read_parquet({sql_quote(file)},hive_partitioning=false)"
                return native_table(probe,query,'512MB',root/run).to_pylist()
            self.assertEqual(digest(path,'before'),digest(packed,'after'))
            # Input's arbitrary 8192-row groups flush the same plugin too early
            # under the bounded contract.
            with patch('v3_layout.snapshot_files',return_value=(dict(metadata,properties={'sage.row-group.rows':'100000'}),{path:70005})):
                with self.assertRaises(ValueError):audit_layout('fixture','plugin-vsn-time')
            # An already-existing output must never be overwritten.
            failed=subprocess.run([os.environ['COLD_V3_LAYOUT_HELPER'],'--input',str(path),'--output',str(packed)],capture_output=True)
            self.assertNotEqual(failed.returncode,0);self.assertEqual(pq.ParquetFile(packed).metadata.num_rows,70005)

    @unittest.skipUnless(os.getenv('COLD_V3_LAYOUT_HELPER'),'compiled bounded layoutpack required')
    def test_large_plugin_splits_at_cap_and_preserves_variants_across_seams(self):
        import pyarrow.parquet as pq
        from v3_layout import FIELD_IDS,audit_data_files
        sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'experiments'))
        from exp67_metadata_projection import native_table
        source="""SELECT make_timestamp_ns(1740787200000000194+n::BIGINT)::TIMESTAMPTZ_NS AS time,
 'test'::VARCHAR AS name,CASE WHEN n<200005 THEN 'large' WHEN n<200007 THEN 'small' ELSE NULL END AS plugin,
 CASE WHEN n%2=0 THEN 'W1' ELSE 'W2' END AS vsn,NULL::VARCHAR AS node,NULL::VARCHAR AS host,
 NULL::VARCHAR AS task,NULL::VARCHAR AS job,NULL::VARCHAR AS zone,MAP {'sample':n::VARCHAR} AS meta,
 CASE WHEN n%5=0 THEN 9007199254740993::BIGINT::VARIANT WHEN n%5=1 THEN 21.5::DOUBLE::VARIANT
 WHEN n%5=2 THEN true::VARIANT WHEN n%5=3 THEN 'text'::VARIANT ELSE NULL::VARIANT END AS value
 FROM range(200010) r(n)"""
        metadata={'format-version':3,'current-schema-id':0,'table-uuid':'fixture','schemas':[{'schema-id':0,
                  'fields':[{'name':'value','id':11,'type':'variant'}]}],'properties':{'sage.row-group.rows':'100000'}}
        with tempfile.TemporaryDirectory(prefix='sage-bounded-plugin-') as directory:
            root=Path(directory);path=root/'input.parquet';packed=root/'bounded.parquet'
            probe=Path(os.environ['NATIVE_DUCKDB_PROBE'])
            query=f"""WITH w AS (COPY ({source} ORDER BY plugin NULLS LAST,vsn NULLS LAST,time NULLS LAST)
 TO {sql_quote(path)} (FORMAT parquet,ROW_GROUP_SIZE 8192,FIELD_IDS {FIELD_IDS}))
 SELECT TIMESTAMP_NS '2025-03-01' AS timestamp,'write'::VARCHAR AS name,Count::VARCHAR AS value,'{{}}'::VARCHAR AS meta FROM w"""
            native_table(probe,query,'512MB',root/'write')
            subprocess.run([os.environ['COLD_V3_LAYOUT_HELPER'],'--input',str(path),'--output',str(packed),'--row-group-rows','100000'],check=True)
            file=pq.ParquetFile(packed)
            self.assertEqual([file.metadata.row_group(i).num_rows for i in range(file.metadata.num_row_groups)],[100000,100000,5,2,3])
            result=audit_data_files(metadata,{packed:200010},'plugin-vsn-time')
            self.assertTrue(result['bounded_plugin_groups']);self.assertFalse(result['one_group_per_plugin'])
            self.assertEqual(result['max_row_group_rows'],100000)
            fields='time,name,value,meta,plugin,vsn,node,host,task,job,zone'
            hashes=[]
            for index,file in enumerate((path,packed)):
                sql=f"SELECT TIMESTAMP_NS '2025-03-01' AS timestamp,count(*)::VARCHAR AS name,sum(hash({fields}))::VARCHAR AS value,bit_xor(hash({fields}))::VARCHAR AS meta FROM read_parquet({sql_quote(file)},hive_partitioning=false)"
                hashes.append(native_table(probe,sql,'512MB',root/f'hash-{index}').to_pylist())
            self.assertEqual(hashes[0],hashes[1])

    def test_variant_annotation_id_plugin_purity_and_order(self):
        here=Path(__file__).resolve().parents[2]/'experiments'
        sys.path.insert(0,str(here))
        from exp67_metadata_projection import native_table
        metadata={'format-version':3,'current-schema-id':0,'current-snapshot-id':1,'table-uuid':'fixture',
                  'snapshots':[{'snapshot-id':1}],'schemas':[{'schema-id':0,'fields':[{'name':'value','id':11,'type':'variant'}]}]}
        with tempfile.TemporaryDirectory(prefix='sage-v3-physical-') as directory:
            root=Path(directory)
            base="""SELECT TIMESTAMPTZ_NS '2025-03-01 00:00:00+00' AS time,
 'p'::VARCHAR AS plugin,vsn,value FROM (VALUES ('W2',21.5::DOUBLE::VARIANT),
 ('W1',9007199254740993::BIGINT::VARIANT)) t(vsn,value)"""
            variants=[('valid',11,'vsn,time',base,True),('unsorted',11,'vsn DESC,time',base,False),
                      ('wrong-id',12,'vsn,time',base,False),
                      ('plain-struct',11,'vsn,time',base.replace('vsn,value FROM','vsn,struct_pack(typed_value:=1.0) AS value FROM'),False)]
            for name,field_id,order,source,passes in variants:
                path=root/(name+'.parquet')
                sql=f"""WITH written AS (COPY ({source} ORDER BY {order}) TO {sql_quote(path)}
 (FORMAT parquet,FIELD_IDS {{'value':{field_id}}}))
 SELECT TIMESTAMP_NS '2025-03-01' AS timestamp,'write'::VARCHAR AS name,Count::VARCHAR AS value,'{{}}'::VARCHAR AS meta FROM written"""
                native_table(Path(os.environ['NATIVE_DUCKDB_PROBE']),sql,'512MB',root/('run-'+name))
                with patch('v3_layout.snapshot_files',return_value=(metadata,{path:2})):
                    if passes:
                        result=audit_layout('fixture','plugin-vsn-time')
                        self.assertEqual(result['value_field_id'],11);self.assertEqual(result['mixed_plugin_groups'],0)
                        self.assertTrue(result['ordering_checked'])
                    else:
                        with self.assertRaises(ValueError):audit_layout('fixture','plugin-vsn-time')


if __name__=='__main__':unittest.main()
