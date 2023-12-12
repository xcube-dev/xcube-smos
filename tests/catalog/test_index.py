import os
import unittest
import xarray as xr

from xcube_smos.constants import INDEX_ENV_VAR_NAME
from xcube_smos.catalog import SmosIndexCatalog

index_path = os.environ.get(INDEX_ENV_VAR_NAME)

if not index_path:
    reason = f"env var {INDEX_ENV_VAR_NAME!r} not set {index_path}"
else:
    reason = f"index {index_path} not found"


@unittest.skipUnless(index_path and os.path.exists(index_path), reason)
class SmosIndexCatalogTest(unittest.TestCase):

    def test_1_find_datasets(self):
        catalog = SmosIndexCatalog(index_path)

        files = catalog.find_datasets("SM", ("2021-05-01", "2021-05-03"))
        self.assert_files_ok(files, "SMOS/L2SM/MIR_SMUDP2/", (20, 30))

        files = catalog.find_datasets("OS", ("2021-05-01", "2021-05-03"))
        self.assert_files_ok(files, "SMOS/L2OS/MIR_OSUDP2/", (20, 30))

    def test_1_find_datasets_on_day_without_data(self):
        catalog = SmosIndexCatalog(index_path)

        files = catalog.find_datasets("SM", ("2022-10-21", "2022-10-21"))
        self.assert_files_ok(files, "SMOS/L2SM/MIR_SMUDP2/", 0)

        files = catalog.find_datasets("SM", ("2023-01-01", "2023-01-03"))
        self.assert_files_ok(files, "SMOS/L2SM/MIR_SMUDP2/", 0)

        files = catalog.find_datasets("SM", ("2023-01-01", "2023-01-05"))
        self.assert_files_ok(files, "SMOS/L2SM/MIR_SMUDP2/", 12)

    def assert_files_ok(self,
                        files, expected_prefix: str,
                        expected_count_range: int | tuple[int, int]):
        self.assertIsInstance(files, list)
        if isinstance(expected_count_range, int):
            self.assertEqual(expected_count_range, len(files))
        else:
            self.assertGreaterEqual(len(files), expected_count_range[0])
            self.assertLessEqual(len(files), expected_count_range[1])
        for file in files:
            self.assertIsInstance(file, tuple)
            self.assertEqual(3, len(file))
            path, start, end = file
            self.assertIsInstance(path, str)
            self.assertIsInstance(start, str)
            self.assertIsInstance(end, str)
            self.assertEqual(expected_prefix, path[:len(expected_prefix)])
            self.assertEqual(14, len(start))
            self.assertEqual(14, len(end))

    def test_2_dataset_opener(self):
        catalog = SmosIndexCatalog(index_path)
        files = catalog.find_datasets("SM", ("2021-05-01", "2021-05-01"))
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
