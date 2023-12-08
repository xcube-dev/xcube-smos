import os
import unittest
from pathlib import Path
import xarray as xr

from xcube_smos.catalog import SmosSimpleCatalog


def new_simple_catalog() -> SmosSimpleCatalog:
    path = Path(__file__).parent / ".." / ".." / "testdata" / "SM"
    smos_l2_sm_paths = [
        str(path / name)
        for name in path.resolve().iterdir()
        if name.suffix == ".nc"
    ]
    return SmosSimpleCatalog(
        smos_l2_sm_paths=smos_l2_sm_paths,
        smos_l2_os_paths=[]
    )


class SmosSimpleCatalogTest(unittest.TestCase):

    def test_1_find_datasets(self):
        catalog = new_simple_catalog()
        files = catalog.find_datasets("SM", (None, None))
        self.assertIsInstance(files, list)
        self.assertEqual(5, len(files))
        for file in files:
            self.assertIsInstance(file, tuple)
            self.assertEqual(3, len(file))
            path, start, end = file
            self.assertIsInstance(path, str)
            self.assertIsInstance(start, str)
            self.assertIsInstance(end, str)
            self.assertIn(os.path.join("SM", "SM_OPER_MIR_SMUDP2_"), path)
            self.assertTrue(path.endswith("_700_001_1.nc"))
            self.assertEqual(14, len(start))
            self.assertEqual(14, len(end))

    def test_2_dataset_opener(self):
        catalog = new_simple_catalog()
        files = catalog.find_datasets("SM", (None, None))
        path, _, _ = files[0]

        path = catalog.resolve_path(path)
        open_dataset = catalog.dataset_opener

        self.assertTrue(callable(open_dataset))

        ds = open_dataset(path,
                          protocol=catalog.source_protocol,
                          storage_options=catalog.source_storage_options)

        self.assertIsInstance(ds, xr.Dataset)
        self.assertIn("Altitude", ds)
        self.assertIn("Grid_Point_ID", ds)
        self.assertIn("Soil_Moisture", ds)
        self.assertIn("Surface_Temperature", ds)
