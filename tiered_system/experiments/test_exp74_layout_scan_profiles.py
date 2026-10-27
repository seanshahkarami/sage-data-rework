from pathlib import Path
import tempfile
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

from exp74_layout_scan_profiles import inventory,geometry,DAY_LOW,DAY_HIGH,time_bounds,query_cases,rotated_layouts


class TestLayoutGeometry(unittest.TestCase):
    def test_nine_candidates_visit_early_middle_and_late_positions(self):
        labels=list(range(9));orders=[rotated_layouts(labels,rep,3) for rep in range(3)]
        for label in labels:self.assertEqual({order.index(label)//3 for order in orders},{0,1,2})

    def test_plugin_only_case_does_not_add_a_vsn_or_name_filter(self):
        queries=query_cases('test-plugin')
        self.assertEqual(queries['plugin_hour']['filter'],{'plugin':'test-plugin'})
        self.assertEqual(queries['plugin_hour']['end'],'2025-03-01T01:00:00+00:00')
        self.assertEqual(queries['plugin_vsn_day']['filter'],{'plugin':'test-plugin','vsn':'W0A0'})

    def test_same_day_rows_have_different_time_pruning(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);time=root/'time';plugin=root/'plugin';time.mkdir();plugin.mkdir()
            stamps=[DAY_LOW+194,DAY_LOW+195,DAY_LOW+3_600_000_000_194,DAY_LOW+3_600_000_000_195]
            table=pa.table({'time':pa.array(stamps,type=pa.timestamp('ns',tz='UTC')),
                            'plugin':['p0','p1','p0','p1'],'vsn':['W0A0']*4})
            pq.write_table(table,time/'file.parquet',row_group_size=2)
            pq.write_table(table.take(pa.array([0,2,1,3])),plugin/'file.parquet',row_group_size=2)
            first,one=inventory(time);second,two=inventory(plugin)
            self.assertEqual(one['rows'],two['rows'])
            self.assertEqual(time_bounds(pq.ParquetFile(first[0]).metadata.row_group(0)),(DAY_LOW+194,DAY_LOW+195))
            query=dict(start='2025-03-01T00:00:00Z',end='2025-03-01T00:01:00Z',filter={'vsn':'W0A0'})
            self.assertEqual(geometry(first,query)['rows_in_those_groups'],2)
            self.assertEqual(geometry(second,query)['rows_in_those_groups'],4)
            self.assertEqual(geometry(second,dict(query,filter={'plugin':'missing'}))['groups_not_excluded_by_footer'],0)

    def test_boundary_spanning_file_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            pq.write_table(pa.table({'time':pa.array([DAY_HIGH-1,DAY_HIGH+1],type=pa.timestamp('ns',tz='UTC'))}),Path(directory)/'cross.parquet')
            with self.assertRaisesRegex(ValueError,'boundary'):inventory(directory)


if __name__=='__main__':unittest.main()
