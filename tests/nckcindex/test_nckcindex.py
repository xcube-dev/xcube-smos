import json
import unittest
import fsspec
import os.path
import shutil

from xcube_smos.nckcindex import NcKcIndex
from xcube_smos.nckcindex.constants import INDEX_CONFIG_FILENAME

local_path = os.path.dirname(__file__)
index_path = os.path.join(local_path, "test-index")
index_config_path = os.path.join(index_path, INDEX_CONFIG_FILENAME)


class NcKcIndexTest(unittest.TestCase):

    def setUp(self) -> None:
        shutil.rmtree(index_path, ignore_errors=True)

    def tearDown(self) -> None:
        shutil.rmtree(index_path, ignore_errors=True)

    def test_create(self):
        index = NcKcIndex.create(index_path=index_path)
        self.assertEqual("EODATA",
                         index.source_path)
        self.assertEqual({'endpoint_url': 'https://s3.cloudferro.com'},
                         index.source_storage_options)
        self.assertEqual({'OS': 'SMOS/L2OS/MIR_OSUDP2/',
                          'SM': 'SMOS/L2SM/MIR_SMUDP2/'},
                         index.prefixes)
        self.assertIsInstance(index.source_fs, fsspec.AbstractFileSystem)
        self.assertEqual(('s3', 's3a'), index.source_fs.protocol)

        with open(index_config_path) as fp:
            config = json.load(fp)
        self.assertEqual(
            {
                'version': 2,
                'prefixes': {'OS': 'SMOS/L2OS/MIR_OSUDP2/',
                             'SM': 'SMOS/L2SM/MIR_SMUDP2/'},
                'source_path': 'EODATA',
                'source_protocol': 's3',
                'source_storage_options': {
                    'endpoint_url': 'https://s3.cloudferro.com'
                },
            },
            config
        )

    def test_open(self):
        NcKcIndex.create(index_path=index_path)
        index = NcKcIndex.open(index_path=index_path)
        self.assertEqual("EODATA",
                         index.source_path)
        self.assertEqual({'endpoint_url': 'https://s3.cloudferro.com'},
                         index.source_storage_options)
        self.assertEqual({'OS': 'SMOS/L2OS/MIR_OSUDP2/',
                          'SM': 'SMOS/L2SM/MIR_SMUDP2/'},
                         index.prefixes)
        self.assertIsInstance(index.source_fs, fsspec.AbstractFileSystem)
        self.assertEqual(('s3', 's3a'), index.source_fs.protocol)

