"""Offline publication checks: unittest discover -s tiered_system/demo/tools."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock

from pyiceberg.avro.file import AvroFile, AvroOutputFile
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, LongType, IntegerType, StructType
from pyiceberg.typedef import Record

from seed_cold import archive_select,empty_interval_sql,validate_source_head
from v3_publish import catalog_head, export_snapshot, upload_snapshot,restore_registration


class TestV3Publisher(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='sage-v3-unit-')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve()

    def fixture(self):
        source=self.root/'source'; (source/'metadata').mkdir(parents=True); (source/'data').mkdir()
        data=source/'data'/'file.parquet'; data.write_bytes(b'unchanged-data')
        manifest=source/'metadata'/'manifest.avro'
        listpath=source/'metadata'/'list.avro'
        schema=Schema(NestedField(0,'status',IntegerType(),required=True),
                      NestedField(2,'data_file',StructType(
                          NestedField(100,'file_path',StringType(),required=True),
                          NestedField(134,'content',IntegerType(),required=True)),required=True))
        io=PyArrowFileIO()
        with AvroOutputFile(io.new_output(str(manifest)),schema,'manifest_entry',metadata={'format-version':'3','schema':'{"fields":[{"type":"variant"}]}','avro.codec':'deflate'}) as w:
            w.write_block([Record(1,Record(str(data),0))])
        schema=Schema(NestedField(500,'manifest_path',StringType(),required=True),
                      NestedField(501,'manifest_length',LongType(),required=True))
        with AvroOutputFile(io.new_output(str(listpath)),schema,'manifest_file',metadata={'avro.codec':'deflate'}) as w:
            w.write_block([Record(str(manifest),manifest.stat().st_size)])
        head=source/'metadata'/'00001-head.metadata.json'
        head.write_text(json.dumps({'format-version':3,'location':str(source),'table-uuid':'test',
            'current-snapshot-id':1,'snapshots':[{'snapshot-id':1,'summary':{'operation':'append'},'manifest-list':str(listpath)}]}))
        (source/'metadata'/'99999-orphan.metadata.json').write_text('{}')
        return head

    def test_graph_references_sizes_and_hint(self):
        head=self.fixture(); out=self.root/'export'
        result=export_snapshot(head,out)
        table=out/'sage'/'obs'; io=PyArrowFileIO()
        self.assertEqual(result['hint'],'00001-head')
        self.assertEqual((table/'metadata/version-hint.text').read_bytes(),b'00001-head')
        self.assertFalse((table/'metadata/99999-orphan.metadata.json').exists())
        self.assertEqual((table/'data/file.parquet').read_bytes(),b'unchanged-data')
        with AvroFile(io.new_input(str(table/'metadata/list.avro'))) as f:
            item=next(f)
            self.assertEqual(item[0],str(table/'metadata/manifest.avro'))
            self.assertEqual(item[1],(table/'metadata/manifest.avro').stat().st_size)
        with AvroFile(io.new_input(str(table/'metadata/manifest.avro'))) as f:
            self.assertEqual(next(f)[1][0],str(table/'data/file.parquet'))
            self.assertEqual(f.header.meta['format-version'],'3')
            self.assertIn('variant',f.header.meta['schema'])
        with self.assertRaises(FileExistsError): export_snapshot(head,out)

    def test_failure_does_not_publish_hint(self):
        head=self.fixture(); (head.parent.parent/'data/file.parquet').unlink()
        out=self.root/'failed'
        with self.assertRaises(FileNotFoundError): export_snapshot(head,out)
        self.assertFalse((out/'sage/obs/metadata/version-hint.text').exists())

    def test_hint_is_last_remote_write_and_failure_stops_it(self):
        head=self.fixture(); out=self.root/'remote'
        export_snapshot(head,out,'https://storage.test/sage/cold-storage-demo-v3-test/warehouse/sage/obs')
        transport=Mock(); transport.propfind.return_value=(404,[])
        writes=[]
        def put(path,method,data,headers):
            self.assertEqual(len(data.read()),int(headers['Content-Length']))
            writes.append(path); return 201,{},b''
        transport._request.side_effect=put
        upload_snapshot(out,'/cold-storage-demo-v3-test',transport)
        self.assertTrue(writes[-1].endswith('/version-hint.text'))
        transport._request.side_effect=lambda *a,**k:(500,{},b'failed')
        with self.assertRaises(IOError): upload_snapshot(out,'/cold-storage-demo-v3-test',transport)
        for prefix in ['/node-data','/cold-storage-demo-v3-test/../node-data','/cold-storage-demo-v3-%2e%2e']:
            with self.assertRaises(ValueError): upload_snapshot(out,prefix,transport)
        transport.propfind.return_value=(207,[])
        with self.assertRaises(ValueError): upload_snapshot(out,'/cold-storage-demo-v3-test',transport)

    def test_catalog_head_uses_load_table(self):
        session=Mock()
        config=Mock();config.json.return_value={'overrides':{'prefix':'catalog'}}
        load=Mock();load.json.return_value={'metadata-location':'/committed/head.metadata.json','metadata':{'format-version':3}}
        session.get.side_effect=[config,load]
        self.assertEqual(catalog_head('http://catalog',session=session),'/committed/head.metadata.json')
        self.assertEqual(session.get.call_args.args[0],'http://catalog/v1/catalog/namespaces/sage/tables/obs')

    def test_source_conversion_and_half_open_clip(self):
        sql=archive_select([Path("file'quoted.parquet")],'2025-03-31')
        self.assertIn('value_int::BIGINT::VARIANT',sql)
        self.assertIn("TIMESTAMPTZ_NS '2025-04-01 00:00:00+00'",sql)
        self.assertNotIn('timestamp_us',sql.lower())
        self.assertIn("file''quoted.parquet",sql)

    def test_append_interval_is_half_open_and_validated(self):
        sql=empty_interval_sql('2025-03-02','2025-04-30')
        self.assertIn("time >= TIMESTAMPTZ_NS '2025-03-02 00:00:00+00'",sql)
        self.assertIn("time < TIMESTAMPTZ_NS '2025-05-01 00:00:00+00'",sql)
        with self.assertRaises(ValueError):empty_interval_sql("2025-03-02';DROP TABLE x",'2025-04-30')
        head=self.fixture();metadata=json.loads(head.read_text())
        metadata.update({'current-schema-id':0,'schemas':[{'schema-id':0,'fields':[
            {'id':1,'name':'time','type':'timestamptz_ns'},{'id':11,'name':'value','type':'variant'}]}]})
        head.write_text(json.dumps(metadata))
        validate_source_head(head,self.root)
        metadata['schemas'][0]['fields'][0]['type']='timestamptz'
        head.write_text(json.dumps(metadata))
        with self.assertRaises(ValueError):validate_source_head(head,self.root)

    def test_restore_registers_known_source_head_without_replacing_table(self):
        head=self.fixture();saved=self.root/'publication.json'
        saved.write_text(json.dumps({'source_metadata':str(head),'table_uuid':'test'}))
        config=Mock();config.json.return_value={}
        missing=Mock(status_code=404)
        loaded=Mock(status_code=200);loaded.json.return_value={'metadata-location':str(head),'metadata':{'table-uuid':'test'}}
        session=Mock();session.get.side_effect=[config,missing,loaded]
        session.post.side_effect=[Mock(status_code=200),Mock(status_code=200)]
        self.assertEqual(restore_registration('http://catalog',saved,self.root,session),str(head))
        self.assertEqual(session.post.call_args.args[0],'http://catalog/v1/namespaces/sage/register')
        self.assertEqual(session.post.call_args.kwargs['json'],{'name':'obs','metadata-location':str(head)})
        session.reset_mock();session.get.side_effect=[config,loaded]
        self.assertEqual(restore_registration('http://catalog',saved,self.root,session),str(head))
        session.post.assert_not_called()
        loaded.json.return_value={'metadata-location':str(head),'metadata':{'table-uuid':'different'}}
        session.get.side_effect=[config,loaded]
        with self.assertRaises(ValueError):restore_registration('http://catalog',saved,self.root,session)
        session.post.assert_not_called()

    def test_restore_prefers_acknowledged_progress_and_refuses_pending_insert(self):
        head=self.fixture();saved=self.root/'publication.json'
        saved.write_text(json.dumps({'source_metadata':str(head),'table_uuid':'test'}))
        newer=head.with_name('00002-later.metadata.json');newer.write_bytes(head.read_bytes())
        (self.root/'seed-v3-progress.json').write_text(json.dumps({'source_metadata':str(newer),'last_committed_day':'2025-03-02'}))
        config=Mock();config.json.return_value={}
        missing=Mock(status_code=404)
        loaded=Mock(status_code=200);loaded.json.return_value={'metadata-location':str(newer),'metadata':{'table-uuid':'test'}}
        session=Mock();session.get.side_effect=[config,missing,loaded]
        session.post.side_effect=[Mock(status_code=200),Mock(status_code=200)]
        restore_registration('http://catalog',saved,self.root,session)
        self.assertEqual(session.post.call_args.kwargs['json']['metadata-location'],str(newer))
        (self.root/'seed-v3-pending.json').write_text('{"day":"2025-03-03"}')
        session.reset_mock()
        with self.assertRaises(ValueError):restore_registration('http://catalog',saved,self.root,session)
        session.get.assert_not_called();session.post.assert_not_called()


if __name__=='__main__':
    unittest.main()
