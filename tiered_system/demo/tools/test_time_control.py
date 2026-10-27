from pathlib import Path
import shutil
import tempfile
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

from seed_time_control import select_day,digest


class TestTimeControl(unittest.TestCase):
    def test_select_complete_files_and_copy_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);first=root/'first.parquet';next_day=root/'next.parquet'
            low=1740787200000000000
            for path,stamp in ((first,low+194),(next_day,low+86_400_000_000_000)):
                pq.write_table(pa.table({'time':pa.array([stamp],type=pa.timestamp('ns',tz='UTC'))}),path)
            selected=select_day({first:1,next_day:1},'2025-03-01')
            self.assertEqual(selected,{first:1})
            copied=root/'copy.parquet';shutil.copyfile(first,copied)
            self.assertEqual(digest(first),digest(copied))
            self.assertEqual(pq.ParquetFile(copied).metadata.row_group(0).column(0).statistics.min_raw,low+194)

    def test_cross_day_file_is_not_silently_clipped(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'cross.parquet'
            pq.write_table(pa.table({'time':pa.array([1740787200000000000,1740873600000000000],type=pa.timestamp('ns',tz='UTC'))}),path)
            with self.assertRaisesRegex(ValueError,'crosses'):select_day({path:2},'2025-03-01')


if __name__=='__main__':unittest.main()
