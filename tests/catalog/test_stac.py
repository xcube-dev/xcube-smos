import json
import unittest
from pathlib import Path
from unittest.mock import Mock
from unittest.mock import patch

import pandas as pd
import pytest
import requests

from xcube_smos.catalog.stac import SmosStacCatalog
from xcube_smos.catalog.stac import create_request_params
from xcube_smos.catalog.stac import fetch_features
from xcube_smos.utils import normalize_time_range


# noinspection PyMethodMayBeStatic
class SmosStacCatalogTest(unittest.TestCase):

    expected = None

    @classmethod
    def setUpClass(cls):
        with open(Path(__file__).parent / "stac-response.json") as fp:
            cls.expected = json.load(fp)

    def setUp(self):
        self.assertIsInstance(self.expected.get("url"), str)
        self.assertIsInstance(self.expected.get("params"), list)
        self.assertIsInstance(self.expected.get("response"), dict)

        self.response_mock = unittest.mock.Mock(requests.Response)
        self.response_mock.status_code = 200
        self.response_mock.ok = True
        self.response_mock.json = lambda: self.expected["response"]

    @unittest.mock.patch("requests.get")
    def test_requests_mock(self, requests_get_mock):
        """Test that `unittest.mock` works as expected with `requests` module."""
        requests_get_mock.return_value = self.response_mock

        response = requests.get(self.expected["url"], params=self.expected["params"])

        self.assertEqual(True, response.ok)
        self.assertEqual(200, response.status_code)
        feature_collection = response.json()
        self.assertIsInstance(feature_collection, dict)
        self.assertEqual("FeatureCollection", feature_collection.get("type"))
        self.assertIsInstance(feature_collection.get("features"), list)
        self.assertEqual(20, len(feature_collection.get("features")))

    @unittest.mock.patch("requests.get")
    def test_find_datasets(self, requests_get_mock):
        requests_get_mock.return_value = self.response_mock

        catalog = SmosStacCatalog()
        dataset_records = catalog.find_datasets(
            "MIR_SMUDP2",
            time_range=normalize_time_range(("2023-05-01", "2023-05-01")),
            bbox=(0, 40, 20, 60),
        )
        self.assertEqual(2, len(dataset_records))
        prefix = "/eodata/SMOS/L2SM/MIR_SMUDP2/2023/05/01/SM_OPER_MIR_SMUDP2"
        self.assertEqual(
            [
                (
                    f"{prefix}_20230501T162850_20230501T172204_700_001_1",
                    pd.Timestamp("2023-05-01 16:28:49.927", tz="UTC"),
                    pd.Timestamp("2023-05-01 17:22:04.372", tz="UTC"),
                ),
                (
                    f"{prefix}_20230501T180855_20230501T190209_700_001_1",
                    pd.Timestamp("2023-05-01 18:08:54.811", tz="UTC"),
                    pd.Timestamp("2023-05-01 19:02:09.256", tz="UTC"),
                ),
            ],
            dataset_records,
        )

    @patch("requests.get")
    def test_fetch_sm_features(self, requests_get_mock):
        requests_get_mock.return_value = self.response_mock

        features = fetch_features(
            product_type_id="MIR_SMUDP2",
            time_range=normalize_time_range(("2023-05-01", "2023-05-01")),
            bbox=(0, 40, 20, 60),
        )
        self.assertEqual(2, len(features))
        self.assertEqual(
            [
                "SM_OPER_MIR_SMUDP2_20230501T162850_20230501T172204_700_001_1",
                "SM_OPER_MIR_SMUDP2_20230501T180855_20230501T190209_700_001_1",
            ],
            [f.get("id") for f in features],
        )
        # print(json.dumps(features[0], indent=2))

    @patch("requests.get")
    def test_fetch_os_features(self, requests_get_mock):
        requests_get_mock.return_value = self.response_mock

        features = fetch_features(
            product_type_id="MIR_OSUDP2",
            time_range=normalize_time_range(("2023-05-01", "2023-05-02")),
            bbox=(0, 40, 20, 60),
        )
        self.assertEqual(2, len(features))
        self.assertEqual(
            [
                "SM_OPER_MIR_OSUDP2_20230501T162850_20230501T172204_700_001_1",
                "SM_OPER_MIR_OSUDP2_20230501T180855_20230501T190209_700_001_1",
            ],
            [f.get("id") for f in features],
        )
        # print(json.dumps(features[0], indent=2))

    @patch("requests.get")
    def test_fetch_features_fails_ok(self, requests_get_mock):
        self.response_mock.status_code = 400
        self.response_mock.ok = False
        self.response_mock.json = lambda: {}
        requests_get_mock.return_value = self.response_mock

        with pytest.raises(
            ValueError,
            match=(
                "Encountered error from"
                " https://datahub.creodias.eu/stac/collections/SMOS/items"
            ),
        ):
            fetch_features(
                product_type_id="MIR_SMUDP2",
                time_range=normalize_time_range(("2023-05-01", "2023-05-02")),
                bbox=(0, 40, 20, 60),
                limit=-1,  # !
            )

    def test_create_request_params(self):
        params = create_request_params(
            time_range=normalize_time_range(("2023-05-01", "2023-05-30")),
            bbox=None,
            limit=1000,
        )
        self.assertEqual(
            [
                ("limit", "1000"),
                ("datetime", "2023-05-01T00:00:00.000Z/2023-05-30T23:59:59.999Z"),
            ],
            params,
        )

        params = create_request_params(
            time_range=normalize_time_range(("2023-05-01", "2023-05-30")),
            bbox=(0, 40, 20, 60),
            limit=1000,
        )
        self.assertEqual(
            [
                ("limit", "1000"),
                ("datetime", "2023-05-01T00:00:00.000Z/2023-05-30T23:59:59.999Z"),
                ("bbox", "0,40,20,60"),
            ],
            params,
        )
