import unittest
from math import ceil
from pathlib import Path
import numpy as np
import xarray as xr

from xcube.core.mldataset import MultiLevelDataset
from xcube_smos.mldataset.newdgg import NUM_LEVELS
from xcube_smos.mldataset.newdgg import MAX_WIDTH
from xcube_smos.mldataset.newdgg import MAX_HEIGHT
from xcube_smos.mldataset.newdgg import new_dgg
from xcube_smos.mldataset.newdgg import get_package_path


class NewDggTest(unittest.TestCase):
    def test_new_dgg_ok(self):
        dgg = new_dgg()
        self.assertIsInstance(dgg, MultiLevelDataset)
        self.assertEqual(NUM_LEVELS, dgg.num_levels)
        self.assertIsInstance(dgg.resolutions, (list, tuple))
        self.assertEqual(NUM_LEVELS, len(dgg.resolutions))

        for i in range(0, dgg.num_levels):
            scale = 1 << i
            expected_w = MAX_WIDTH // scale
            expected_h = MAX_HEIGHT // scale

            ds = dgg.get_dataset(i)
            self.assertIsInstance(ds, xr.Dataset)
            self.assertEqual({'lat': expected_h, 'lon': expected_w}, ds.dims)
            self.assertIn("seqnum", ds)
            self.assertIsInstance(ds.seqnum, xr.DataArray)
            self.assertEqual(np.dtype("uint32"), ds.seqnum.dtype)
            self.assertEqual(('lat', 'lon'), ds.seqnum.dims)
            self.assertEqual((expected_h, expected_w), ds.seqnum.shape)
            self.assertEqual(((expected_h,), (expected_w,)), ds.seqnum.chunks)

    def test_new_dgg_creates_new_instances(self):
        dgg1 = new_dgg()
        dgg2 = new_dgg()
        self.assertIsInstance(dgg1, MultiLevelDataset)
        self.assertIsInstance(dgg2, MultiLevelDataset)
        self.assertIsNot(dgg1, dgg2)

    def test_get_package_path(self):
        expected_path = (Path(__file__).parent / ".." / ".." /
                         "xcube_smos" / "mldataset"
                         ).absolute().resolve()
        self.assertEqual(expected_path, get_package_path())
