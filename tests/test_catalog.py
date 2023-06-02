import os
import unittest

from xcube_smos.catalog import INDEX_ENV_VAR_NAME
from xcube_smos.catalog import SmosCatalog

index_path = os.environ.get(INDEX_ENV_VAR_NAME)

if not index_path:
    reason = f"env var {INDEX_ENV_VAR_NAME!r} not set {index_path}"
else:
    reason = f"index {index_path} not found"


@unittest.skipUnless(index_path and os.path.exists(index_path), reason)
class SmosArchiveTest(unittest.TestCase):

    def test_find_files(self):
        archive = SmosCatalog(index_path)

        files = archive.find_files("SM", ("2021-05-01", "2021-05-03"))
        self.assertIsInstance(files, list)
        self.assertTrue(len(files) >= 20)

        files = archive.find_files("OS", ("2021-05-01", "2021-05-03"))
        self.assertIsInstance(files, list)
        self.assertTrue(len(files) >= 20)
