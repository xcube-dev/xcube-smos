import json
import unittest
import zipfile

import fsspec
import os.path
import shutil

import pytest

from xcube_smos.nckcindex import NcKcIndex
from xcube_smos.nckcindex import INDEX_CONFIG_FILENAME

local_path = os.path.dirname(__file__)
index_path = os.path.join(local_path, "test-index")
index_zip_path = os.path.join(local_path, "test-index.zip")

source_path = os.path.realpath(
    os.path.join(local_path, "..", "..", "testdata")
).replace("\\", "/")


# TODO: have common base class for NcKcIndexDirTest and NcKcIndexZipTest
#   that contains all tests.


class NcKcIndexDirTest(unittest.TestCase):
    def setUp(self) -> None:
        shutil.rmtree(index_path, ignore_errors=True)

    def tearDown(self) -> None:
        shutil.rmtree(index_path, ignore_errors=True)

    def test_create(self):
        with NcKcIndex.create(
            index_path=index_path, source_path=source_path, replace=True
        ) as index:
            self.assert_local_index_ok(index)

    def test_create_exists(self):
        with NcKcIndex.create(
            index_path=index_path, source_path=source_path, replace=True
        ) as index:
            self.assert_local_index_ok(index)
        with pytest.raises(OSError, match=f"Index exists: *."):
            NcKcIndex.create(
                index_path=index_path, source_path=source_path, replace=False
            )

    def test_open(self):
        with NcKcIndex.create(
            index_path=index_path, source_path=source_path, replace=True
        ):
            pass
        index = NcKcIndex.open(index_path=index_path)
        self.assert_local_index_ok(index)

    # noinspection PyMethodMayBeStatic
    def test_open_not_found(self):
        with pytest.raises(FileNotFoundError, match=f"Index not found: *."):
            NcKcIndex.open(index_path=index_path)

    def test_sync_dry_run(self):
        with NcKcIndex.create(
            index_path=index_path, source_path=source_path, replace=True
        ) as index:
            sync_count, problems = index.sync("SM", dry_run=True)
        self.assertEqual(5, sync_count)
        self.assertEqual([], problems)
        self.assertFalse(os.path.exists(index_path + "/SM"))

    def test_sync(self):
        with NcKcIndex.create(
            index_path=index_path, source_path=source_path, replace=True
        ) as index:
            sync_count, problems = index.sync("SM")
        self.assertEqual(5, sync_count)
        self.assertEqual([], problems)
        self.assertTrue(os.path.isdir(index_path + "/SM"))
        self.assertTrue(
            [
                f"SM_OPER_MIR_SMUDP2_{dt}_700_001_1.nc.json"
                for dt in (
                    "20230401T150613_20230401T155931",
                    "20230401T155619_20230401T164933",
                    "20230401T173625_20230401T182938",
                    "20230401T191629_20230401T200942",
                    "20230401T205632_20230401T214947",
                )
            ],
            os.listdir(index_path + "/SM"),
        )

    def assert_local_index_ok(self, index):
        self.assertEqual(source_path, index.source_path)
        self.assertEqual("file", index.source_protocol)
        self.assertEqual({}, index.source_storage_options)
        self.assertIsInstance(index.source_fs, fsspec.AbstractFileSystem)
        self.assertEqual(("file", "local"), index.source_fs.protocol)
        with open(os.path.join(index_path, INDEX_CONFIG_FILENAME)) as fp:
            config = json.load(fp)
        self.assertEqual(
            {
                "version": 2,
                "source_path": source_path,
                "source_protocol": "file",
                "source_storage_options": {},
            },
            config,
        )

    def test_open_with_env_subst(self):
        os.environ["TEST_KEY"] = "12345"
        os.environ["TEST_SECRET"] = "ABCDE"
        config = {
            "source_path": "s3://SMOS",
            "source_storage_options": {
                "anon": False,
                "key": "${TEST_KEY}",
                "secret": "${TEST_SECRET}",
            },
        }
        os.mkdir(index_path)
        with open(os.path.join(index_path, INDEX_CONFIG_FILENAME), "w") as f:
            json.dump(config, f)
        index = NcKcIndex.open(index_path=index_path)
        self.assertEqual("s3://SMOS", index.source_path)
        self.assertEqual("s3", index.source_protocol)
        self.assertEqual(
            {
                "anon": False,
                "key": "12345",
                "secret": "ABCDE",
            },
            index.source_storage_options,
        )


class NcKcIndexZipTest(unittest.TestCase):
    def setUp(self) -> None:
        if os.path.exists(index_zip_path):
            os.remove(index_zip_path)

    def tearDown(self) -> None:
        if os.path.exists(index_zip_path):
            os.remove(index_zip_path)

    def test_create(self):
        with NcKcIndex.create(
            index_path=index_zip_path, source_path=source_path, replace=True
        ) as index:
            self.assertTrue(os.path.isfile(index_zip_path))
            self.assert_local_index_ok(index)

    def test_open(self):
        with NcKcIndex.create(
            index_path=index_zip_path, source_path=source_path, replace=True
        ) as index:
            index.close()
            index = NcKcIndex.open(index_path=index_zip_path)
            self.assert_local_index_ok(index)

    def test_sync_dry_run(self):
        with NcKcIndex.create(
            index_path=index_zip_path, source_path=source_path, replace=True
        ) as index:
            sync_count, problems = index.sync("SM", dry_run=True)
        self.assertEqual(5, sync_count)
        self.assertEqual([], problems)
        with zipfile.ZipFile(index_zip_path) as zf:
            self.assertEqual([INDEX_CONFIG_FILENAME], zf.namelist())

    def test_sync(self):
        with NcKcIndex.create(
            index_path=index_zip_path, source_path=source_path, replace=True
        ) as index:
            sync_count, problems = index.sync("SM")
        self.assertEqual(5, sync_count)
        self.assertEqual([], problems)
        with zipfile.ZipFile(index_zip_path) as zf:
            self.assertEqual(
                {
                    INDEX_CONFIG_FILENAME,
                    "SM/SM_OPER_MIR_SMUDP2_20230401T173625_20230401T182938_700_001_1.nc.json",
                    "SM/SM_OPER_MIR_SMUDP2_20230401T205632_20230401T214947_700_001_1.nc.json",
                    "SM/SM_OPER_MIR_SMUDP2_20230401T155619_20230401T164933_700_001_1.nc.json",
                    "SM/SM_OPER_MIR_SMUDP2_20230401T150613_20230401T155931_700_001_1.nc.json",
                    "SM/SM_OPER_MIR_SMUDP2_20230401T191629_20230401T200942_700_001_1.nc.json",
                },
                set(zf.namelist()),
            )

    def assert_local_index_ok(self, index):
        self.assertEqual(source_path, index.source_path)
        self.assertEqual("file", index.source_protocol)
        self.assertEqual({}, index.source_storage_options)
        self.assertIsInstance(index.source_fs, fsspec.AbstractFileSystem)
        self.assertEqual(("file", "local"), index.source_fs.protocol)
        with zipfile.ZipFile(index_zip_path, "r") as zf:
            with zf.open(INDEX_CONFIG_FILENAME) as fp:
                config = json.load(fp)
        self.assertEqual(
            {
                "version": 2,
                "source_path": source_path,
                "source_protocol": "file",
                "source_storage_options": {},
            },
            config,
        )
