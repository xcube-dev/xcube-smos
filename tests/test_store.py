import unittest
from typing import Any, Type

import dask.array as da
import jsonschema
import numpy as np
import pytest
import xarray as xr

from xcube.core.store import DatasetDescriptor
from xcube.core.store import MultiLevelDatasetDescriptor
from xcube.util.jsonschema import JsonObjectSchema

from tests.catalog.test_simple import new_simple_catalog
from xcube_smos.dsiter import DatasetIterator
from xcube_smos.schema import DATASET_OPEN_PARAMS_SCHEMA
from xcube_smos.schema import ML_DATASET_OPEN_PARAMS_SCHEMA
from xcube_smos.schema import STORE_PARAMS_SCHEMA
from xcube_smos.store import SmosDataStore
from xcube_smos.store import DATASET_OPENER_ID
from xcube_smos.store import DATASET_ITERATOR_OPENER_ID
from xcube_smos.store import ML_DATASET_OPENER_ID


class SmosDataStoreTest(unittest.TestCase):
    def test_get_data_store_params_schema(self):
        self.assertIs(STORE_PARAMS_SCHEMA, SmosDataStore.get_data_store_params_schema())

    def test_get_data_types(self):
        self.assertEqual(("dataset", "mldataset"), SmosDataStore.get_data_types())

    def test_get_data_types_for_data(self):
        store = SmosDataStore()
        self.assertEqual(
            ("dataset", "mldataset"), store.get_data_types_for_data("SMOS-L2C-SM")
        )
        self.assertEqual(
            ("dataset", "mldataset"), store.get_data_types_for_data("SMOS-L2C-OS")
        )
        with pytest.raises(ValueError, match="Unknown dataset identifier 'SMOS-L3-OS'"):
            store.get_data_types_for_data("SMOS-L3-OS")

    def test_get_data_ids(self):
        store = SmosDataStore()
        for data_type in ("dataset", "mldataset"):
            self.assertEqual(
                ["SMOS-L2C-SM", "SMOS-L2C-OS"], list(store.get_data_ids(data_type))
            )
            self.assertEqual(
                [
                    ("SMOS-L2C-SM", {"title": "SMOS Level-2 Soil Moisture"}),
                    ("SMOS-L2C-OS", {"title": "SMOS Level-2 Ocean Salinity"}),
                ],
                list(store.get_data_ids(data_type, include_attrs=["title"])),
            )
            self.assertEqual(
                [("SMOS-L2C-SM", {}), ("SMOS-L2C-OS", {})],
                list(store.get_data_ids(data_type, include_attrs=["color"])),
            )

    def test_has_data(self):
        store = SmosDataStore()
        self.assertEqual(True, store.has_data("SMOS-L2C-SM"))
        self.assertEqual(True, store.has_data("SMOS-L2C-OS"))
        self.assertEqual(False, store.has_data("SMOS-L3-OS"))

        self.assertEqual(True, store.has_data("SMOS-L2C-SM", data_type="dataset"))
        self.assertEqual(True, store.has_data("SMOS-L2C-SM", data_type="mldataset"))
        self.assertEqual(False, store.has_data("SMOS-L2C-SM", data_type="geodataframe"))

    def test_get_search_params_schema(self):
        empty_object_schema = JsonObjectSchema(
            properties={}, additional_properties=False
        )
        for data_type in ("dataset", "mldataset"):
            schema = SmosDataStore.get_search_params_schema(data_type)
            self.assertEqual(empty_object_schema.to_dict(), schema.to_dict())

        with pytest.raises(ValueError, match="Invalid dataset type 'geodataframe'"):
            SmosDataStore.get_search_params_schema("geodataframe")

    def test_search_data(self):
        store = SmosDataStore()

        expected_ml_ds_descriptors = [
            {
                "data_id": "SMOS-L2C-SM",
                "data_type": "mldataset",
                "num_levels": 5,
                "spatial_res": 0.0439453125,
                "bbox": [-180.0, -88.59375, 180.0, 88.59375],
                "time_range": ["2010-01-01", None],
            },
            {
                "data_id": "SMOS-L2C-OS",
                "data_type": "mldataset",
                "num_levels": 5,
                "spatial_res": 0.0439453125,
                "bbox": [-180.0, -88.59375, 180.0, 88.59375],
                "time_range": ["2010-01-01", None],
            },
        ]

        expected_ds_descriptors = [
            {
                "data_id": "SMOS-L2C-SM",
                "data_type": "dataset",
                "spatial_res": 0.0439453125,
                "bbox": [-180.0, -88.59375, 180.0, 88.59375],
                "time_range": ["2010-01-01", None],
            },
            {
                "data_id": "SMOS-L2C-OS",
                "data_type": "dataset",
                "spatial_res": 0.0439453125,
                "bbox": [-180.0, -88.59375, 180.0, 88.59375],
                "time_range": ["2010-01-01", None],
            },
        ]

        descriptors = list(store.search_data())
        self.assertEqual(2, len(descriptors))
        for d in descriptors:
            self.assertIsInstance(d, MultiLevelDatasetDescriptor)
        self.assertEqual(expected_ml_ds_descriptors, [d.to_dict() for d in descriptors])

        descriptors = list(store.search_data(data_type="mldataset"))
        self.assertEqual(2, len(descriptors))
        for d in descriptors:
            self.assertIsInstance(d, MultiLevelDatasetDescriptor)
        self.assertEqual(expected_ml_ds_descriptors, [d.to_dict() for d in descriptors])

        descriptors = list(store.search_data(data_type="dataset"))
        self.assertEqual(2, len(descriptors))
        for d in descriptors:
            self.assertIsInstance(d, DatasetDescriptor)
        self.assertEqual(expected_ds_descriptors, [d.to_dict() for d in descriptors])

        with pytest.raises(ValueError, match="Invalid dataset type 'geodataframe'"):
            next(store.search_data(data_type="geodataframe"))

    def test_get_data_opener_ids(self):
        store = SmosDataStore()

        self.assertEqual(
            ("dataset:zarr:smos", "mldataset:zarr:smos", "dsiter:zarr:smos"),
            store.get_data_opener_ids(),
        )
        self.assertEqual(
            ("dataset:zarr:smos", "mldataset:zarr:smos", "dsiter:zarr:smos"),
            store.get_data_opener_ids(data_id="SMOS-L2C-OS"),
        )
        self.assertEqual(
            ("dataset:zarr:smos",), store.get_data_opener_ids(data_type="dataset")
        )
        self.assertEqual(
            ("mldataset:zarr:smos",), store.get_data_opener_ids(data_type="mldataset")
        )
        self.assertEqual(
            ("dsiter:zarr:smos",), store.get_data_opener_ids(data_type="dsiter")
        )

        with pytest.raises(ValueError, match="Unknown dataset identifier 'SMOS-L3-OS'"):
            store.get_data_types_for_data("SMOS-L3-OS")

        with pytest.raises(ValueError, match="Invalid dataset type 'geodataframe'"):
            store.get_data_opener_ids(data_type="geodataframe")

    def test_get_open_data_params_schema(self):
        store = SmosDataStore()

        self.assertIs(DATASET_OPEN_PARAMS_SCHEMA, store.get_open_data_params_schema())
        self.assertIs(
            DATASET_OPEN_PARAMS_SCHEMA,
            store.get_open_data_params_schema(opener_id=DATASET_OPENER_ID),
        )
        self.assertIs(
            DATASET_OPEN_PARAMS_SCHEMA,
            store.get_open_data_params_schema(opener_id=DATASET_ITERATOR_OPENER_ID),
        )
        self.assertIs(
            ML_DATASET_OPEN_PARAMS_SCHEMA,
            store.get_open_data_params_schema(opener_id=ML_DATASET_OPENER_ID),
        )

        with pytest.raises(ValueError, match="Unknown dataset identifier 'SMOS-L3-OS'"):
            store.get_open_data_params_schema(data_id="SMOS-L3-OS")

        with pytest.raises(
            ValueError, match="Invalid opener identifier 'dataset:zarr:s3'"
        ):
            store.get_open_data_params_schema(opener_id="dataset:zarr:s3")

    # noinspection PyMethodMayBeStatic
    def test_open_data_param_validation(self):
        store = SmosDataStore()

        time_range = ("2022-05-10", "2022-05-12")

        with pytest.raises(ValueError, match="Unknown dataset identifier 'SMOS-L3-OS'"):
            store.open_data("SMOS-L3-OS", time_range=time_range)

        with pytest.raises(
            jsonschema.exceptions.ValidationError,
            match="8 is not one of \\[0, 1, 2, 3, 4\\]",
        ):
            store.open_data(
                "SMOS-L2C-SM",
                time_range=time_range,
                res_level=8,
                opener_id="dataset:zarr:smos",
            )

        with pytest.raises(
            ValueError, match="Invalid opener identifier 'dataset:zarr:s3'"
        ):
            store.open_data(
                "SMOS-L2C-SM", time_range=time_range, opener_id="dataset:zarr:s3"
            )

        with pytest.raises(
            jsonschema.exceptions.ValidationError,
            match="10 is not of type 'string', 'null'",
        ):
            store.open_data("SMOS-L2C-SM", time_range=[10, 20])

        with pytest.raises(
            jsonschema.exceptions.ValidationError,
            match="Additional properties are not allowed"
            " \\('time_period' was unexpected\\)",
        ):
            store.open_data("SMOS-L2C-SM", time_range=time_range, time_period="2D")

    def test_open_dataset_iterator_no_res_level(self):
        self._test_open_dataset_iterator(None, (1, 4032, 8192))

    def test_open_dataset_iterator_res_level_0(self):
        self._test_open_dataset_iterator(0, (1, 4032, 8192))

    def test_open_dataset_iterator_res_level_4(self):
        self._test_open_dataset_iterator(4, (1, 252, 512))

    def _test_open_dataset_iterator(
        self, res_level: int | None, expected_shape: tuple[int, int, int]
    ):
        store = SmosDataStore(_catalog=new_simple_catalog())
        kwargs = dict(time_range=("2022-05-05", "2022-05-07"))
        if res_level is not None:
            kwargs.update(res_level=res_level)
        ds_iter = store.open_data("SMOS-L2C-SM", opener_id="dsiter:zarr:smos", **kwargs)
        self.assertIsInstance(ds_iter, DatasetIterator)
        self.assertEqual(5, len(ds_iter))
        dataset = next(ds_iter)
        t_size, y_size, x_size = expected_shape
        expected_sm_attrs = {"_FillValue": -999.0, "units": "m3 m-3"}
        expected_sm_encoding = {
            "dtype": np.dtype("float32"),
            "chunks": (1, y_size, x_size),
            "preferred_chunks": {"time": 1, "lat": y_size, "lon": x_size},
        }
        self.assert_dataset_ok(
            dataset,
            expected_shape,
            None,
            expected_sm_attrs,
            expected_sm_encoding,
            np.ndarray,
        )

    def test_open_dataset_no_res_level(self):
        self._test_open_dataset(None, (5, 4032, 8192))

    def test_open_dataset_no_res_level_0(self):
        self._test_open_dataset(0, (5, 4032, 8192))

    def test_open_dataset_no_res_level_4(self):
        self._test_open_dataset(4, (5, 252, 512))

    def _test_open_dataset(
        self, res_level: int | None, expected_shape: tuple[int, int, int]
    ):
        store = SmosDataStore(_catalog=new_simple_catalog())
        kwargs = dict(time_range=("2022-05-05", "2022-05-07"))
        if res_level is not None:
            kwargs.update(res_level=res_level)
        dataset = store.open_data("SMOS-L2C-SM", **kwargs)
        t_size, y_size, x_size = expected_shape
        expected_chunks = (
            t_size * (1,),
            (y_size,),
            (x_size,),
        )
        expected_sm_attrs = {"units": "m3 m-3"}
        expected_sm_encoding = {
            "_FillValue": -999.0,
            "chunks": (1, y_size, x_size),
            "compressor": None,
            "dtype": np.dtype("float32"),
            "filters": None,
            "preferred_chunks": {"time": 1, "lat": y_size, "lon": x_size},
        }
        self.assert_dataset_ok(
            dataset,
            expected_shape,
            expected_chunks,
            expected_sm_attrs,
            expected_sm_encoding,
            da.Array,
        )

    def assert_dataset_ok(
        self,
        dataset: xr.Dataset,
        expected_shape: tuple[int, int, int],
        expected_chunks: tuple[tuple[int, ...], tuple[int, ...], tuple[int, ...]]
        | None,
        expected_sm_attrs: dict[str, Any],
        expected_sm_encoding: dict[str, Any],
        expected_sm_array_type: Type,
    ):
        t_size, y_size, x_size = expected_shape

        self.assertIsInstance(dataset, xr.Dataset)
        self.assertEqual(
            {"lon": x_size, "lat": y_size, "time": t_size, "bnds": 2}, dataset.dims
        )
        self.assertEqual({"lon", "lat", "time", "time_bnds"}, set(dataset.coords))
        self.assertEqual(
            {
                "Chi_2",
                "Chi_2_P",
                "N_RFI_X",
                "N_RFI_Y",
                "RFI_Prob",
                "Soil_Moisture",
                "Soil_Moisture_DQX",
            },
            set(dataset.data_vars),
        )

        sm_var = dataset.Soil_Moisture
        self.assertEqual(expected_shape, sm_var.shape)
        self.assertEqual(expected_chunks, sm_var.chunks)
        self.assertEqual(("time", "lat", "lon"), sm_var.dims)
        self.assertEqual(np.float32, sm_var.dtype)
        self.assertEqual(expected_sm_attrs, sm_var.attrs)
        self.assertEqual(expected_sm_encoding, sm_var.encoding)

        self.assertIsInstance(sm_var.data, expected_sm_array_type)
        sm_data = dataset.Soil_Moisture.values
        self.assertIsInstance(sm_data, np.ndarray)

    def test_open_dataset_with_bbox(self):
        store = SmosDataStore(_catalog=new_simple_catalog())
        #  The bounding box for Germany
        expected_bbox = (5.87, 47.27, 15.03, 55.06)
        dataset = store.open_data(
            "SMOS-L2C-SM", time_range=("2022-05-05", "2022-05-07"), bbox=expected_bbox
        )
        self.assertEqual({"lon": 208, "lat": 177, "time": 5, "bnds": 2}, dataset.sizes)
        actual_bbox = tuple(
            map(
                float,
                (dataset.lon[0], dataset.lat[-1], dataset.lon[-1], dataset.lat[0]),
            )
        )
        places = 1
        self.assertAlmostEqual(expected_bbox[0], actual_bbox[0], places=places)
        self.assertAlmostEqual(expected_bbox[1], actual_bbox[1], places=places)
        self.assertAlmostEqual(expected_bbox[2], actual_bbox[2], places=places)
        self.assertAlmostEqual(expected_bbox[3], actual_bbox[3], places=places)


class SmosDistributedDataStoreTest(unittest.TestCase):
    def setUp(self) -> None:
        import dask.distributed

        self._client = dask.distributed.Client(processes=True)

    def tearDown(self) -> None:
        self._client.cluster.close()
        self._client.close()

    def test_open_data(self):
        store = SmosDataStore(_catalog=new_simple_catalog())

        dataset = store.open_data(
            "SMOS-L2C-SM", time_range=("2022-05-05", "2022-05-07")
        )
        self.assertIsInstance(dataset, xr.Dataset)
        self.assertIsInstance(dataset.Soil_Moisture, xr.DataArray)
        sm_var: xr.DataArray = dataset.Soil_Moisture
        self.assertIsInstance(sm_var.data, da.Array)
        self.assertIsInstance(sm_var.values, np.ndarray)  # trigger compute()
