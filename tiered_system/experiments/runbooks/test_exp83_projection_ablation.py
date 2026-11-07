from pathlib import Path
import tempfile
import unittest

import pyarrow as pa
import pyarrow.ipc as ipc

from exp83_projection_ablation import MODES,response_digests,validate_cost


class TestProjectionAblation(unittest.TestCase):
    def test_masks_check_every_retained_field_and_preserve_timestamp_order(self):
        table=pa.table({'timestamp':pa.array([1740787200000000194,1740787200000000195],type=pa.timestamp('ns')),
                        'name':['a','b'],'value':['9007199254740993','"123"'],'meta':['{"vsn":"A"}','{}']})
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'body'
            def write(t):
                with path.open('wb') as stream,ipc.new_stream(stream,t.schema) as writer:writer.write_table(t,max_chunksize=1)
            write(table);expected=response_digests(path,MODES)
            both=table.set_column(2,table.schema.field(2),pa.array(['null']*2)).set_column(3,table.schema.field(3),pa.array(['{}']*2))
            write(both);self.assertEqual(response_digests(path,('full',))['full'],expected['without_both'])
            changed=both.set_column(1,both.schema.field(1),pa.array(['wrong','b']))
            write(changed);self.assertNotEqual(response_digests(path,('full',))['full'],expected['without_both'])
            write(table.take(pa.array([1,0])))
            with self.assertRaisesRegex(ValueError,'out of order'):response_digests(path,('full',))

    def test_checksum_modes_require_same_raw_inputs_and_unchanged_retained_json(self):
        baseline=dict(rows=10,source_hash_sum='100',source_hash_xor='20',value_hash_sum='30',meta_hash_sum='40')
        validate_cost(dict(baseline,meta_hash_sum='other'),baseline,'without_meta',10)
        with self.assertRaisesRegex(ValueError,'source rows'):validate_cost(dict(baseline,source_hash_sum='wrong'),baseline,'without_both',10)
        with self.assertRaisesRegex(ValueError,'retained'):validate_cost(dict(baseline,value_hash_sum='wrong'),baseline,'without_meta',10)
        with self.assertRaisesRegex(ValueError,'retained'):validate_cost(dict(baseline,meta_hash_sum='wrong'),baseline,'without_value',10)


if __name__=='__main__':unittest.main()
