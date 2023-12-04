import json
import unittest
import fsspec
import os.path
import shutil

from xcube_smos.nckcindex import NcKcIndex
from xcube_smos.nckcindex import INDEX_CONFIG_FILENAME

local_path = os.path.dirname(__file__)
index_path = os.path.join(local_path, "test-index")
index_config_path = os.path.join(index_path, INDEX_CONFIG_FILENAME)

source_path = os.path.realpath(
    os.path.join(local_path, "..", "..", "testdata")
).replace("\\", "/")


class NcKcIndexTest(unittest.TestCase):

    def setUp(self) -> None:
        shutil.rmtree(index_path, ignore_errors=True)

    def tearDown(self) -> None:
        shutil.rmtree(index_path, ignore_errors=True)

    def test_create(self):
        index = NcKcIndex.create(index_path=index_path,
                                 source_path=source_path)
        self.assert_local_index_ok(index)

    def test_open(self):
        NcKcIndex.create(index_path=index_path,
                         source_path=source_path)
        index = NcKcIndex.open(index_path=index_path)
        self.assert_local_index_ok(index)

    def test_sync_dry_run(self):
        index = NcKcIndex.create(index_path=index_path,
                                 source_path=source_path)
        sync_count, problems = index.sync("SM", dry_run=True)
        self.assertEqual(5, sync_count)
        self.assertEqual([], problems)
        self.assertFalse(os.path.exists(index_path + "/SM"))

    def test_sync(self):
        index = NcKcIndex.create(index_path=index_path,
                                 source_path=source_path)
        sync_count, problems = index.sync("SM")
        self.assertEqual(5, sync_count)
        self.assertEqual([], problems)
        self.assertTrue(os.path.isdir(index_path + "/SM"))
        self.assertTrue([
            f"SM_OPER_MIR_SMUDP2_{dt}_700_001_1.nc.json"
            for dt in (
                '20230401T150613_20230401T155931',
                '20230401T155619_20230401T164933',
                '20230401T173625_20230401T182938',
                '20230401T191629_20230401T200942',
                '20230401T205632_20230401T214947'
            )
        ], os.listdir(index_path + "/SM"))


    def assert_local_index_ok(self, index):
        self.assertEqual(source_path, index.source_path)
        self.assertEqual("file", index.source_protocol)
        self.assertEqual({}, index.source_storage_options)
        self.assertIsInstance(index.source_fs, fsspec.AbstractFileSystem)
        self.assertEqual(('file', 'local'), index.source_fs.protocol)
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

