import tempfile
from pathlib import Path
import unittest

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from exp72_plugin_layout import day_files,info,eligible_groups


class TestPluginLayoutSelection(unittest.TestCase):
    def test_multi_month_source_selects_only_complete_requested_day(self):
        start=pd.Timestamp('2025-03-01',tz='UTC').value
        end=start+86400*10**9
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            def write(name,stamps,stats=True):
                path=root/name
                pq.write_table(pa.table({'time':pa.array(stamps,type=pa.timestamp('ns',tz='UTC')),
                    'plugin':['p']*len(stamps),'vsn':['W0A0']*len(stamps)}),path,write_statistics=stats)
                return path
            march=write('march.parquet',[start,start+194,end-1])
            april=write('april.parquet',[end,end+1])
            self.assertEqual(day_files([march,april],start,end),[march])
            self.assertEqual(eligible_groups([march],{'plugin':'other'},start,end)['groups_not_excluded_by_minmax'],0)
            self.assertEqual(info([march])['mixed_plugin_groups'],0)
            crossing=write('crossing.parquet',[end-1,end])
            with self.assertRaisesRegex(ValueError,'spans'):day_files([crossing],start,end)
            missing=write('missing.parquet',[start],False)
            with self.assertRaisesRegex(ValueError,'statistics'):day_files([missing],start,end)


if __name__=='__main__':unittest.main()
