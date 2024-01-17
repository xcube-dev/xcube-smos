import unittest

import pytest

from xcube_smos.catalog.producttype import ProductType


class ProductTypeTest(unittest.TestCase):
    def test_normalize(self):
        self.assertIs(ProductType.SM, ProductType.normalize(ProductType.SM))
        self.assertIs(ProductType.SM, ProductType.normalize("SM"))
        self.assertIs(ProductType.OS, ProductType.normalize(ProductType.OS))
        self.assertIs(ProductType.OS, ProductType.normalize("OS"))

        with pytest.raises(ValueError, match="invalid product_type 'OZ'"):
            ProductType.normalize("OZ")

        with pytest.raises(TypeError, match="invalid product_type type <class 'int'>"):
            # noinspection PyTypeChecker
            ProductType.normalize(2)
