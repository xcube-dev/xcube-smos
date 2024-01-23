import unittest

from xcube.util.jsonschema import JsonObjectSchema
from xcube_smos.schema import STORE_PARAMS_SCHEMA
from xcube_smos.schema import DATASET_OPEN_PARAMS_SCHEMA
from xcube_smos.schema import ML_DATASET_OPEN_PARAMS_SCHEMA


class SmosSchemaTest(unittest.TestCase):
    def test_store_params_schema(self):
        schema = STORE_PARAMS_SCHEMA
        self.assertIsInstance(schema, JsonObjectSchema)
        self.assertIn("source_path", schema.properties)
        self.assertIn("source_protocol", schema.properties)
        self.assertIn("source_storage_options", schema.properties)
        self.assertIn("cache_path", schema.properties)
        self.assertIn("xarray_kwargs", schema.properties)

    def test_dataset_open_params_schema(self):
        schema = DATASET_OPEN_PARAMS_SCHEMA
        self.assertIsInstance(schema, JsonObjectSchema)
        self.assertEqual(["time_range"], schema.required)
        self.assertIn("time_range", schema.properties)
        self.assertIn("res_level", schema.properties)
        self.assertNotIn("l2_product_cache_size", schema.properties)
        # TODO: support variable_names and bbox
        # self.assertIn("variable_names", DATASET_OPEN_PARAMS_SCHEMA.properties)
        # self.assertIn("bbox", DATASET_OPEN_PARAMS_SCHEMA.properties)

    def test_ml_dataset_open_params_schema(self):
        schema = ML_DATASET_OPEN_PARAMS_SCHEMA
        self.assertIsInstance(schema, JsonObjectSchema)
        self.assertEqual(["time_range"], schema.required)
        self.assertIn("time_range", schema.properties)
        self.assertIn("l2_product_cache_size", schema.properties)
        self.assertNotIn("res_level", schema.properties)
        # TODO: support variable_names and bbox
        # self.assertIn("variable_names", DATASET_OPEN_PARAMS_SCHEMA.properties)
        # self.assertIn("bbox", DATASET_OPEN_PARAMS_SCHEMA.properties)
