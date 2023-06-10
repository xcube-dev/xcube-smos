import os
import os.path
import re
import unittest
from typing import Tuple, Optional, List

import jsonschema
import pytest
import xarray as xr

from xcube.core.store import DatasetDescriptor
from xcube.core.store import MultiLevelDatasetDescriptor
from xcube.util.jsonschema import JsonObjectSchema
from xcube_smos.catalog import INDEX_ENV_VAR_NAME, AbstractSmosCatalog
from xcube_smos.nckcindex.producttype import ProductTypeLike, \
    COMMON_NAME_PATTERN
from xcube_smos.schema import OPEN_PARAMS_SCHEMA
from xcube_smos.schema import STORE_PARAMS_SCHEMA
from xcube_smos.store import SmosDataStore

INDEX_PATH = os.environ.get(INDEX_ENV_VAR_NAME)

if not INDEX_PATH:
    reason = f"env var {INDEX_ENV_VAR_NAME!r} not set {INDEX_PATH}"
else:
    reason = f"index {INDEX_PATH} not found"


class SmosTestCatalog(AbstractSmosCatalog):

    def open_dataset(self, dataset_path: str) -> xr.Dataset:
        return xr.open_dataset(dataset_path, decode_cf=False)

    def find_datasets(self,
                      product_type: ProductTypeLike,
                      time_range: Tuple[Optional[str], Optional[str]]) \
            -> List[Tuple[str, str, str]]:
        path = os.path.normpath(
            os.path.join(os.path.dirname(__file__),
                         "..",
                         "testdata",
                         "SM")
        )
        filenames = os.listdir(path)
        result = []
        for filename in filenames:
            name, _ = os.path.splitext(filename)
            name_pattern = f"SM_OPER_MIR_SMUDP2_{COMMON_NAME_PATTERN}"
            m = re.match(name_pattern, name)
            start = m.group("sd") + m.group("st")
            end = m.group("ed") + m.group("et")
            result.append((os.path.join(path, filename), start, end))
        return result


class SmosDataStoreTest(unittest.TestCase):

    def test_get_data_store_params_schema(self):
        self.assertIs(
            STORE_PARAMS_SCHEMA,
            SmosDataStore.get_data_store_params_schema()
        )

    def test_get_data_types(self):
        self.assertEqual(('dataset', 'mldataset'),
                         SmosDataStore.get_data_types())

    def test_get_data_types_for_data(self):
        store = SmosDataStore()
        self.assertEqual(('dataset', 'mldataset'),
                         store.get_data_types_for_data('SMOS-L2C-SM'))
        self.assertEqual(('dataset', 'mldataset'),
                         store.get_data_types_for_data('SMOS-L2C-OS'))
        with pytest.raises(ValueError,
                           match="Unknown dataset identifier 'SMOS-L3-OS'"):
            store.get_data_types_for_data('SMOS-L3-OS')

    def test_get_data_ids(self):
        store = SmosDataStore()
        for data_type in ('dataset', 'mldataset'):
            self.assertEqual(
                [
                    'SMOS-L2C-SM',
                    'SMOS-L2C-OS'
                ],
                list(store.get_data_ids(data_type))
            )
            self.assertEqual(
                [
                    ('SMOS-L2C-SM', {'title': 'SMOS Level-2 Soil Moisture'}),
                    ('SMOS-L2C-OS', {'title': 'SMOS Level-2 Ocean Salinity'})
                ],
                list(store.get_data_ids(data_type,
                                        include_attrs=['title'])))
            self.assertEqual(
                [
                    ('SMOS-L2C-SM', {}),
                    ('SMOS-L2C-OS', {})
                ],
                list(store.get_data_ids(data_type,
                                        include_attrs=['color'])))

    def test_has_data(self):
        store = SmosDataStore()
        self.assertEqual(True, store.has_data('SMOS-L2C-SM'))
        self.assertEqual(True, store.has_data('SMOS-L2C-OS'))
        self.assertEqual(False, store.has_data('SMOS-L3-OS'))

        self.assertEqual(True, store.has_data('SMOS-L2C-SM',
                                              data_type='dataset'))
        self.assertEqual(True, store.has_data('SMOS-L2C-SM',
                                              data_type='mldataset'))
        self.assertEqual(False, store.has_data('SMOS-L2C-SM',
                                               data_type='geodataframe'))

    def test_get_search_params_schema(self):
        empty_object_schema = JsonObjectSchema(properties={},
                                               additional_properties=False)
        for data_type in ('dataset', 'mldataset'):
            schema = SmosDataStore.get_search_params_schema(data_type)
            self.assertEqual(empty_object_schema.to_dict(), schema.to_dict())

        with pytest.raises(ValueError,
                           match="Invalid dataset type 'geodataframe'"):
            SmosDataStore.get_search_params_schema('geodataframe')

    def test_search_data(self):
        store = SmosDataStore()

        expected_ml_ds_descriptors = [
            {
                'data_id': 'SMOS-L2C-SM',
                'data_type': 'mldataset',
                'num_levels': 6
            },
            {
                'data_id': 'SMOS-L2C-OS',
                'data_type': 'mldataset',
                'num_levels': 6
            }
        ]

        expected_ds_descriptors = [
            {
                'data_id': 'SMOS-L2C-SM',
                'data_type': 'dataset',
            },
            {
                'data_id': 'SMOS-L2C-OS',
                'data_type': 'dataset',
            },
        ]

        descriptors = list(store.search_data())
        self.assertEqual(2, len(descriptors))
        for d in descriptors:
            self.assertIsInstance(d, MultiLevelDatasetDescriptor)
        self.assertEqual(expected_ml_ds_descriptors,
                         [d.to_dict() for d in descriptors])

        descriptors = list(store.search_data(data_type='mldataset'))
        self.assertEqual(2, len(descriptors))
        for d in descriptors:
            self.assertIsInstance(d, MultiLevelDatasetDescriptor)
        self.assertEqual(expected_ml_ds_descriptors,
                         [d.to_dict() for d in descriptors])

        descriptors = list(store.search_data(data_type='dataset'))
        self.assertEqual(2, len(descriptors))
        for d in descriptors:
            self.assertIsInstance(d, DatasetDescriptor)
        self.assertEqual(expected_ds_descriptors, [d.to_dict()
                                                   for d in descriptors])

        with pytest.raises(ValueError,
                           match="Invalid dataset type 'geodataframe'"):
            next(store.search_data(data_type='geodataframe'))

    def test_get_data_opener_ids(self):
        store = SmosDataStore()

        self.assertEqual(
            ('dataset:zarr:smos', 'mldataset:zarr:smos'),
            store.get_data_opener_ids()
        )
        self.assertEqual(
            ('dataset:zarr:smos', 'mldataset:zarr:smos'),
            store.get_data_opener_ids(data_id='SMOS-L2C-OS')
        )
        self.assertEqual(
            ('dataset:zarr:smos',),
            store.get_data_opener_ids(data_type='dataset')
        )
        self.assertEqual(
            ('mldataset:zarr:smos',),
            store.get_data_opener_ids(data_type='mldataset')
        )

        with pytest.raises(ValueError,
                           match="Unknown dataset identifier 'SMOS-L3-OS'"):
            store.get_data_types_for_data('SMOS-L3-OS')

        with pytest.raises(ValueError,
                           match="Invalid dataset type 'geodataframe'"):
            store.get_data_opener_ids(data_type='geodataframe')

    def test_get_open_data_params_schema(self):
        store = SmosDataStore()

        self.assertIs(
            OPEN_PARAMS_SCHEMA,
            store.get_open_data_params_schema()
        )

        with pytest.raises(ValueError,
                           match="Unknown dataset identifier 'SMOS-L3-OS'"):
            store.get_open_data_params_schema(data_id='SMOS-L3-OS')

        with pytest.raises(ValueError,
                           match="Invalid opener identifier 'dataset:zarr:s3'"):
            store.get_open_data_params_schema(opener_id='dataset:zarr:s3')

    # noinspection PyMethodMayBeStatic
    def test_open_data_param_validation(self):
        store = SmosDataStore()

        time_range = ("2022-05-10", "2022-05-12")

        with pytest.raises(ValueError,
                           match="Unknown dataset identifier 'SMOS-L3-OS'"):
            store.open_data('SMOS-L3-OS',
                            time_range=time_range)

        with pytest.raises(ValueError,
                           match="Invalid opener identifier 'dataset:zarr:s3'"):
            store.open_data('SMOS-L2C-SM',
                            time_range=time_range,
                            opener_id='dataset:zarr:s3')

        with pytest.raises(jsonschema.exceptions.ValidationError,
                           match="'time_range' is a required property"):
            store.open_data('SMOS-L2C-SM',
                            bbox="10, 20, 30, 40")

        with pytest.raises(jsonschema.exceptions.ValidationError,
                           match="10 is not of type 'string', 'null'"):
            store.open_data('SMOS-L2C-SM', time_range=[10, 20])

        with pytest.raises(jsonschema.exceptions.ValidationError,
                           match="'10, 20, 30, 40' is not of type 'array'"):
            store.open_data('SMOS-L2C-SM',
                            time_range=time_range,
                            bbox="10, 20, 30, 40")

        with pytest.raises(jsonschema.exceptions.ValidationError,
                           match="Additional properties are not allowed"
                                 " \\('time_period' was unexpected\\)"):
            store.open_data('SMOS-L2C-SM',
                            time_range=time_range,
                            time_period="2D")

    def test_open_data(self):
        store = SmosDataStore(catalog=SmosTestCatalog())

        dataset = store.open_data('SMOS-L2C-SM',
                                  time_range=("2022-05-05", "2022-05-07"))
        self.assertIsInstance(dataset, xr.Dataset)

        self.assertEqual({'lon': 8192, 'lat': 4032, 'time': 5, 'bnds': 2},
                         dataset.dims)

        self.assertEqual({'lon', 'lat', 'time', 'time_bnds'},
                         set(dataset.coords))

        self.assertEqual(
            {
                'Chi_2',
                'Chi_2_P',
                'N_RFI_X',
                'N_RFI_Y',
                'RFI_Prob',
                'Soil_Moisture',
                'Soil_Moisture_DQX',
            },
            set(dataset.data_vars)
        )

        sm_data = dataset.Soil_Moisture.values


        # TODO (forman): more tests
