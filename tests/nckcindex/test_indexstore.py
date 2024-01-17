import unittest
from abc import ABC

import pytest
import os.path
import shutil
from xcube_smos.nckcindex import IndexStore

local_path = os.path.dirname(__file__)
index_dir_path = os.path.join(local_path, "test-index")
index_zip_path = os.path.join(local_path, "test-index.zip")


# noinspection PyUnresolvedReferences
class IndexStoreTestMixin:
    def test_new_x(self):
        self.assertFalse(os.path.exists(self.path))
        store = IndexStore.new(self.path, mode="x")
        self.assertTrue(os.path.exists(self.path))
        self.assertEqual(self.path, store.path)

    def test_new_x_exist(self):
        IndexStore.new(self.path, mode="x")
        with pytest.raises(OSError, match=f"Index exists: "):
            IndexStore.new(self.path, mode="x")

    def test_new_x_replace(self):
        IndexStore.new(self.path, mode="x")
        IndexStore.new(self.path, mode="x", replace=True)

    def test_list(self):
        store = IndexStore.new(self.path, mode="x")
        self._write_test_data(store)
        store.close()
        store = IndexStore.new(self.path, mode="r")
        self.assertEqual(
            {
                "README.md",
                "SMOS/SM/2022/05/07/data-07-1.nc.json",
                "SMOS/SM/2022/05/08/data-08-1.nc.json",
                "SMOS/SM/2022/05/08/data-08-2.nc.json",
                "SMOS/SM/2022/05/09/data-09-1.nc.json",
                "SMOS/SM/2022/05/09/data-09-2.nc.json",
                "SMOS/SM/2022/05/09/data-09-3.nc.json",
                "data/SMOS/SM/2022/05/07/data-07-1.nc",
                "data/SMOS/SM/2022/05/08/data-08-1.nc",
                "data/SMOS/SM/2022/05/08/data-08-2.nc",
                "data/SMOS/SM/2022/05/09/data-09-1.nc",
                "data/SMOS/SM/2022/05/09/data-09-2.nc",
                "data/SMOS/SM/2022/05/09/data-09-3.nc",
            },
            set(store.list()),
        )

    def test_list_with_prefix(self):
        store = IndexStore.new(self.path, mode="x")
        self._write_test_data(store)
        store.close()
        store = IndexStore.new(self.path, mode="r")
        self.assertEqual(
            {
                "SMOS/SM/2022/05/09/data-09-1.nc.json",
                "SMOS/SM/2022/05/09/data-09-2.nc.json",
                "SMOS/SM/2022/05/09/data-09-3.nc.json",
            },
            set(store.list(prefix="SMOS/SM/2022/05/09/")),
        )

    @staticmethod
    def _write_test_data(store: IndexStore):
        store.write("SMOS/SM/2022/05/07/data-07-1.nc.json", {})
        store.write("SMOS/SM/2022/05/08/data-08-1.nc.json", {})
        store.write("SMOS/SM/2022/05/08/data-08-2.nc.json", {})
        store.write("SMOS/SM/2022/05/09/data-09-1.nc.json", {})
        store.write("SMOS/SM/2022/05/09/data-09-2.nc.json", {})
        store.write("SMOS/SM/2022/05/09/data-09-3.nc.json", {})
        store.write("data/SMOS/SM/2022/05/07/data-07-1.nc", bytes([1, 2, 3]))
        store.write("data/SMOS/SM/2022/05/08/data-08-1.nc", bytes([1, 2, 3]))
        store.write("data/SMOS/SM/2022/05/08/data-08-2.nc", bytes([1, 2, 3]))
        store.write("data/SMOS/SM/2022/05/09/data-09-1.nc", bytes([1, 2, 3]))
        store.write("data/SMOS/SM/2022/05/09/data-09-2.nc", bytes([1, 2, 3]))
        store.write("data/SMOS/SM/2022/05/09/data-09-3.nc", bytes([1, 2, 3]))
        store.write("README.md", "# Hallo!")


class DirIndexStoreTest(unittest.TestCase, IndexStoreTestMixin):
    def setUp(self) -> None:
        self.path = index_dir_path
        shutil.rmtree(self.path, ignore_errors=True)

    def tearDown(self) -> None:
        shutil.rmtree(self.path, ignore_errors=True)
        self.path = None


class ZipIndexStoreTest(unittest.TestCase, IndexStoreTestMixin):
    def setUp(self) -> None:
        self.path = index_zip_path
        if os.path.exists(self.path):
            os.remove(self.path)

    def tearDown(self) -> None:
        if os.path.exists(self.path):
            os.remove(self.path)
        self.path = None
