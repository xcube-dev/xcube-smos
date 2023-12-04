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

source_path = os.path.realpath(os.path.join(local_path, "..", ".."))


class NcKcIndexTest(unittest.TestCase):

    def setUp(self) -> None:
        shutil.rmtree(index_path, ignore_errors=True)

    def tearDown(self) -> None:
        shutil.rmtree(index_path, ignore_errors=True)

    def test_create(self):
        index = NcKcIndex.create(index_path=index_path,
                                 source_path=source_path)
        self.assert_local_index_ok(index)
        with open(index_config_path) as fp:
            config = json.load(fp)
            self.assertEqual(
                {
                    'version': 2,
                    'source_path': source_path,
                    'source_protocol': 'file',
                    'source_storage_options': {},
                },
                config
            )

    def test_open(self):
        NcKcIndex.create(index_path=index_path,
                         source_path=source_path)
        index = NcKcIndex.open(index_path=index_path)
        self.assert_local_index_ok(index)

    def assert_local_index_ok(self, index):
        self.assertEqual(source_path, index.source_path)
        self.assertEqual("file", index.source_protocol)
        self.assertEqual({}, index.source_storage_options)
        self.assertIsInstance(index.source_fs, fsspec.AbstractFileSystem)
        self.assertEqual(('file', 'local'), index.source_fs.protocol)

