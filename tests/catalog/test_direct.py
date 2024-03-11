import os
import unittest
from typing import Tuple, Union
import tempfile

import dask.array
import numpy as np
import pandas as pd
import xarray as xr

from xcube_smos.catalog import SmosDirectCatalog
from xcube_smos.utils import normalize_time_range

_TEST_ENABLED = True

s3_storage_options = None
if "CREODIAS_S3_KEY" in os.environ and "CREODIAS_S3_SECRET" in os.environ:
    s3_storage_options = dict(
        endpoint_url="https://s3.cloudferro.com",
        anon=False,
        key=os.environ["CREODIAS_S3_KEY"],
        secret=os.environ["CREODIAS_S3_SECRET"],
    )

reason = "Set env vars CREODIAS_S3_KEY and CREODIAS_S3_SECRET to enable test"


@unittest.skipUnless(_TEST_ENABLED and s3_storage_options is not None, reason)
class SmosDirectCatalogTest(unittest.TestCase):
    def test_1_find_datasets(self):
        catalog = SmosDirectCatalog(
            source_path="EODATA",
            source_protocol="s3",
            source_storage_options=s3_storage_options,
        )

        files = catalog.find_datasets(
            "SM", normalize_time_range(("2021-05-01", "2021-05-03"))
        )
        self.assert_files_ok(files, "EODATA/SMOS/L2SM/MIR_SMUDP2/", (25, 30))

        files = catalog.find_datasets(
            "OS", normalize_time_range(("2021-05-01", "2021-05-03"))
        )
        self.assert_files_ok(files, "EODATA/SMOS/L2OS/MIR_OSUDP2/", (25, 30))

    # def test_1_find_datasets_ascending(self):
    #     catalog = SmosIndexCatalog(index_path)
    #
    #     key = "VH:SPH:MI:TI:Ascending_Flag"
    #
    #     def filter_ascending(record: DatasetRecord, attrs: dict) -> bool:
    #         print(f"filter_ascending: {key} = {attrs.get(key)}")
    #         return attrs.get(key) == "A"
    #
    #     ascending_files = catalog.find_datasets(
    #         "OS", ("2021-05-01", "2021-05-03"),
    #         predicate=filter_ascending
    #     )
    #     self.assert_files_ok(ascending_files, "SMOS/L2OS/MIR_OSUDP2/",
    #                          (10, 15))
    #
    # def test_1_find_datasets_descending(self):
    #     catalog = SmosIndexCatalog(index_path)
    #
    #     key = "VH:SPH:MI:TI:Ascending_Flag"
    #
    #     def filter_descending(record: DatasetRecord, attrs: dict) -> bool:
    #         print(f"filter_descending: {key} = {attrs.get(key)}")
    #         return attrs.get(key) == "D"
    #
    #     descending_files = catalog.find_datasets(
    #         "OS", ("2021-05-01", "2021-05-03"),
    #         predicate=filter_descending
    #     )
    #     self.assert_files_ok(descending_files, "SMOS/L2OS/MIR_OSUDP2/",
    #                          (10, 15))

    def assert_files_ok(
        self, files, expected_prefix: str, expected_count: Union[int, Tuple[int, int]]
    ):
        self.assertIsInstance(files, list)
        actual_count = len(files)
        if isinstance(expected_count, int):
            self.assertEqual(expected_count, actual_count)
        else:
            self.assertGreaterEqual(actual_count, expected_count[0])
            self.assertLessEqual(actual_count, expected_count[1])
        for file in files:
            self.assertIsInstance(file, tuple)
            self.assertEqual(3, len(file))
            path, start, end = file
            self.assertIsInstance(path, str)
            self.assertIsInstance(start, pd.Timestamp)
            self.assertIsInstance(end, pd.Timestamp)
            self.assertEqual(expected_prefix, path[: len(expected_prefix)])

    def test_2_dataset_opener_no_cache(self):
        catalog = SmosDirectCatalog(
            source_path="EODATA",
            source_protocol="s3",
            source_storage_options=s3_storage_options,
            cache_path=None,
            xarray_kwargs=dict(engine="netcdf4"),
        )

        files = catalog.find_datasets(
            "SM", normalize_time_range(("2021-05-01", "2021-05-01"))
        )
        path, _, _ = files[0]

        path = catalog.resolve_path(path)
        open_dataset_kwargs = catalog.get_dataset_opener_kwargs()
        open_dataset = catalog.get_dataset_opener()

        self.assertTrue(callable(open_dataset))

        ds = open_dataset(path, **open_dataset_kwargs)

        self.assert_dataset_ok(ds)

    def test_2_dataset_opener_with_cache(self):
        cache_dir = tempfile.TemporaryDirectory().name

        catalog = SmosDirectCatalog(
            source_path="EODATA",
            source_protocol="s3",
            source_storage_options=s3_storage_options,
            cache_path=cache_dir,
        )

        files = catalog.find_datasets(
            "SM", normalize_time_range(("2021-05-01", "2021-05-01"))
        )
        path, _, _ = files[0]

        path = catalog.resolve_path(path)
        open_dataset_kwargs = catalog.get_dataset_opener_kwargs()
        open_dataset = catalog.get_dataset_opener()

        self.assertTrue(callable(open_dataset))

        ds = open_dataset(path, **open_dataset_kwargs)

        self.assert_dataset_ok(ds)

    def assert_dataset_ok(self, ds):
        self.assertIsInstance(ds, xr.Dataset)
        self.assertIn("Grid_Point_ID", ds)
        self.assertEqual(np.dtype("uint32"), ds.Grid_Point_ID.dtype)
        self.assertIn("Soil_Moisture", ds)
        self.assertEqual(np.dtype("float32"), ds.Soil_Moisture.dtype)
        self.assertIn("Soil_Moisture_DQX", ds)
        self.assertEqual(np.dtype("float32"), ds.Soil_Moisture_DQX.dtype)

        data = ds.Grid_Point_ID.data
        self.assertIsInstance(data, dask.array.Array)
        values = ds.Grid_Point_ID.values
        self.assertIsInstance(values, np.ndarray)
