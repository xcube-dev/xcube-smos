import os
import unittest

from xcube_smos.constants import INDEX_ENV_VAR_NAME
from xcube_smos.catalog import SmosIndexCatalog

index_path = os.environ.get(INDEX_ENV_VAR_NAME)

if not index_path:
    reason = f"env var {INDEX_ENV_VAR_NAME!r} not set {index_path}"
else:
    reason = f"index {index_path} not found"


@unittest.skipUnless(index_path and os.path.exists(index_path), reason)
class SmosCatalogTest(unittest.TestCase):

    def test_find_files(self):
        archive = SmosIndexCatalog(index_path)

        files = archive.find_datasets("SM", ("2021-05-01", "2021-05-03"))
        self.assertIsInstance(files, list)
        self.assertTrue(len(files) >= 20)

        files = archive.find_datasets("OS", ("2021-05-01", "2021-05-03"))
        self.assertIsInstance(files, list)
        self.assertTrue(len(files) >= 20)
