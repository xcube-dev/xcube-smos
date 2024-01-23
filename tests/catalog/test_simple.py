import os
import unittest
from pathlib import Path
import xarray as xr

from xcube_smos.catalog.base import DatasetRecord
from xcube_smos.constants import SM_VAR_NAMES
from .simple import SmosSimpleCatalog


def new_simple_catalog() -> SmosSimpleCatalog:
    path = Path(__file__).parent / ".." / ".." / "testdata" / "SM"
    smos_l2_sm_paths = [
        str(path / name) for name in path.resolve().iterdir() if name.suffix == ".nc"
    ]
    return SmosSimpleCatalog(smos_l2_sm_paths=smos_l2_sm_paths, smos_l2_os_paths=[])


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

    def test_1_find_datasets_ascending(self):
        catalog = new_simple_catalog()

        key = "Ascending_Flag"

        def filter_ascending(record: DatasetRecord) -> bool:
            attrs = catalog.get_dataset_attrs(record[0])
            print(f"filter_ascending: {key} = {attrs.get(key)}")
            return attrs.get(key) == "A"

        ascending_files = catalog.find_datasets(
            "SM", (None, None), accept_record=filter_ascending
        )
        self.assertIsInstance(ascending_files, list)
        self.assertEqual(1, len(ascending_files))

    def test_1_find_datasets_descending(self):
        catalog = new_simple_catalog()

        key = "Ascending_Flag"

        def filter_descending(record: DatasetRecord) -> bool:
            attrs = catalog.get_dataset_attrs(record[0])
            print(f"filter_descending: {key} = {attrs.get(key)}")
            return attrs.get(key) == "D"

        descending_files = catalog.find_datasets(
            "SM", (None, None), accept_record=filter_descending
        )
        self.assertIsInstance(descending_files, list)
        self.assertEqual(4, len(descending_files))

    def test_2_dataset_opener(self):
        catalog = new_simple_catalog()
        files = catalog.find_datasets("SM", (None, None))
        path, _, _ = files[0]

        path = catalog.resolve_path(path)
        open_dataset = catalog.get_dataset_opener()

        self.assertTrue(callable(open_dataset))

        ds = open_dataset(path)

        self.assertIsInstance(ds, xr.Dataset)
        self.assertEquals(
            SM_VAR_NAMES.difference({"X_swath", "Mean_acq_time"}).union(
                {"Grid_Point_ID"}
            ),
            set(ds.data_vars.keys()),
        )
