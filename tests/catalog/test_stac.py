import unittest

import pytest

from xcube_smos.catalog.stac import SmosStacCatalog
from xcube_smos.catalog.stac import create_request_params
from xcube_smos.catalog.stac import fetch_features
from xcube_smos.utils import normalize_time_range

SUPPRESS_TEST = True

properties = {
    "authority": "ESA",
    "orbitNumber": 74013,
    "orbitDirection": "DESCENDING",
    "acquisitionType": "NOMINAL",
    "operationalMode": "FULL",
    "processingLevel": "1B",
    "processingCenter": "ESAC",
    "processorVersion": "724",
    "wrsLongitudeGrid": "01029",
    "platformShortName": "SMOS",
    "spatialResolution": 50,
    "instrumentShortName": "MIRAS",
    "datetime": "2023-12-02T16:58:33.177Z",
    "end_datetime": "2023-12-02T17:52:32.019Z",
    "start_datetime": "2023-12-02T16:58:33.177Z",
    "productType": "MIR_SC_F1B",
}

assets = {
    "PRODUCT": {
        "href": "https://datahub.creodias.eu/odata/v1/Products(1b3d7d9d-aecd-4fa4-927e-98ba36f5ccd5)/$value",
        "title": "Product",
        "type": "application/octet-stream",
        "alternate": {
            "s3": {
                "href": "/eodata/SMOS/L1B/MIR_SC_F1B/2023/12/02/SM_OPER_MIR_SC_F1B_20231202T165834_20231202T175232_724_001_1",
                "storage:platform": "CLOUDFERRO",
                "storage:region": "waw",
                "storage:requester_pays": False,
                "storage:tier": "Online",
            }
        },
    }
}


# noinspection PyMethodMayBeStatic
class SmosStacCatalogTest(unittest.TestCase):
    @unittest.skipIf(SUPPRESS_TEST, reason="Test has been suppressed by intention")
    def test_find_datasets(self):
        catalog = SmosStacCatalog()
        dataset_records = catalog.find_datasets(
            "MIR_SMUDP2", time_range=("2023-05-01", "2023-05-02"), bbox=(0, 40, 20, 60)
        )
        self.assertEqual([], dataset_records)

    @unittest.skipIf(SUPPRESS_TEST, reason="Test has been suppressed by intention")
    def test_fetch_features(self):
        features = fetch_features(
            product_type_id="MIR_SMUDP2",
            date_range=("2023-05-01", "2023-05-02"),
            bbox=(0, 40, 20, 60),
            limit=100,
        )
        self.assertEqual(5, len(features))
        self.assertEqual(
            [
                "SM_OPER_MIR_SMUDP2_20230501T162850_20230501T172204_700_001_1",
                "SM_OPER_MIR_SMUDP2_20230501T180855_20230501T190209_700_001_1",
                "SM_OPER_MIR_SMUDP2_20230502T045919_20230502T055238_700_001_1",
                "SM_OPER_MIR_SMUDP2_20230502T172958_20230502T182311_700_001_1",
                "SM_OPER_MIR_SMUDP2_20230502T191004_20230502T200316_700_001_1",
            ],
            [f.get("id") for f in features],
        )
        # print(json.dumps(features[0], indent=2))

        features = fetch_features(
            product_type_id="MIR_OSUDP2",
            date_range=("2023-05-01", "2023-05-02"),
            bbox=(0, 40, 20, 60),
            limit=100,
        )
        self.assertEqual(5, len(features))
        self.assertEqual(
            [
                "SM_OPER_MIR_OSUDP2_20230501T162850_20230501T172204_700_001_1",
                "SM_OPER_MIR_OSUDP2_20230501T180855_20230501T190209_700_001_1",
                "SM_OPER_MIR_OSUDP2_20230502T045919_20230502T055238_700_001_1",
                "SM_OPER_MIR_OSUDP2_20230502T172958_20230502T182311_700_001_1",
                "SM_OPER_MIR_OSUDP2_20230502T191004_20230502T200316_700_001_1",
            ],
            [f.get("id") for f in features],
        )
        # print(json.dumps(features[0], indent=2))

    @unittest.skipIf(SUPPRESS_TEST, reason="Test has been suppressed by intention")
    def test_fetch_features_fails_ok(self):
        with pytest.raises(
            ValueError,
            match=(
                "Encountered error from"
                " https://datahub.creodias.eu/stac/collections/SMOS/items"
            ),
        ):
            fetch_features(
                product_type_id="MIR_SMUDP2",
                date_range=("2023-05-01", "2023-05-02"),
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
