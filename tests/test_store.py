import unittest

import pytest

from xcube.core.store import DatasetDescriptor
from xcube.core.store import MultiLevelDatasetDescriptor
from xcube.util.jsonschema import JsonObjectSchema
from xcube_smos.store import SmosStore
from xcube_smos.schema import STORE_PARAMS_SCHEMA
from xcube_smos.schema import OPEN_PARAMS_SCHEMA


class SmosStoreTest(unittest.TestCase):

    def test_get_data_store_params_schema(self):
        self.assertIs(
            STORE_PARAMS_SCHEMA,
            SmosStore.get_data_store_params_schema()
        )

    def test_get_data_types(self):
        self.assertEqual(('dataset', 'mldataset'),
                         SmosStore.get_data_types())

    def test_get_data_types_for_data(self):
        store = SmosStore()
        self.assertEqual(('dataset', 'mldataset'),
                         store.get_data_types_for_data('SMOS-L2-SM'))
        self.assertEqual(('dataset', 'mldataset'),
                         store.get_data_types_for_data('SMOS-L2-OS'))
        with pytest.raises(ValueError,
                           match="Unknown dataset identifier 'SMOS-L3-OS'"):
            store.get_data_types_for_data('SMOS-L3-OS')

    def test_get_data_ids(self):
        store = SmosStore()
        for data_type in ('dataset', 'mldataset'):
            self.assertEqual(
                [
                    'SMOS-L2-SM',
                    'SMOS-L2-OS'
                ],
                list(store.get_data_ids(data_type))
            )
            self.assertEqual(
                [
                    ('SMOS-L2-SM', {'title': 'SMOS Level-2 Soil Moisture'}),
                    ('SMOS-L2-OS', {'title': 'SMOS Level-2 Ocean Salinity'})
                ],
                list(store.get_data_ids(data_type,
                                        include_attrs=['title'])))
            self.assertEqual(
                [
                    ('SMOS-L2-SM', {}),
                    ('SMOS-L2-OS', {})
                ],
                list(store.get_data_ids(data_type,
                                        include_attrs=['color'])))

    def test_has_data(self):
        store = SmosStore()
        self.assertEqual(True, store.has_data('SMOS-L2-SM'))
        self.assertEqual(True, store.has_data('SMOS-L2-OS'))
        self.assertEqual(False, store.has_data('SMOS-L3-OS'))

        self.assertEqual(True, store.has_data('SMOS-L2-SM',
                                              data_type='dataset'))
        self.assertEqual(True, store.has_data('SMOS-L2-SM',
                                              data_type='mldataset'))
        self.assertEqual(False, store.has_data('SMOS-L2-SM',
                                               data_type='geodataframe'))

    def test_get_search_params_schema(self):
        empty_object_schema = JsonObjectSchema(properties={},
                                               additional_properties=False)
        for data_type in ('dataset', 'mldataset'):
            schema = SmosStore.get_search_params_schema(data_type)
            self.assertEqual(empty_object_schema.to_dict(), schema.to_dict())

        with pytest.raises(ValueError,
                           match="Invalid dataset type 'geodataframe'"):
            SmosStore.get_search_params_schema('geodataframe')

    def test_search_data(self):
        store = SmosStore()

        expected_ml_ds_descriptors = [
            {
                'data_id': 'SMOS-L2-SM',
                'data_type': 'mldataset',
                'num_levels': 6
            },
            {
                'data_id': 'SMOS-L2-OS',
                'data_type': 'mldataset',
                'num_levels': 6
            }
        ]

        expected_ds_descriptors = [
            {
                'data_id': 'SMOS-L2-SM',
                'data_type': 'dataset',
            },
            {
                'data_id': 'SMOS-L2-OS',
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
        store = SmosStore()
        self.assertEqual(
            ('dataset:zarr:smos', 'mldataset:zarr:smos'),
            store.get_data_opener_ids()
        )
        self.assertEqual(
            ('dataset:zarr:smos', 'mldataset:zarr:smos'),
            store.get_data_opener_ids(data_id='SMOS-L2-OS')
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
        store = SmosStore()
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


    def test_open_data(self):
        store = SmosStore()

        # TODO (forman): implement me!

        with pytest.raises(ValueError,
                           match="Unknown dataset identifier 'SMOS-L3-OS'"):
            store.open_data('SMOS-L3-OS')
