# The MIT License (MIT)
# Copyright (c) 2023-2024 by the xcube development team and contributors
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

import unittest

import pytest

from xcube_smos.catalog.producttype import ProductType


class ProductTypeTest(unittest.TestCase):
    def test_normalize(self):
        # Level 2 Soil Moisture
        self.assertIs(
            ProductType.MIR_SMUDP2, ProductType.normalize(ProductType.MIR_SMUDP2)
        )
        self.assertIs(ProductType.MIR_SMUDP2, ProductType.normalize("MIR_SMUDP2"))
        self.assertIs(ProductType.MIR_SMUDP2, ProductType.normalize("L2SM"))
        self.assertIs(ProductType.MIR_SMUDP2, ProductType.normalize("SM"))

        # Level 2 Ocean Salinity
        self.assertIs(
            ProductType.MIR_OSUDP2, ProductType.normalize(ProductType.MIR_OSUDP2)
        )
        self.assertIs(ProductType.MIR_OSUDP2, ProductType.normalize("MIR_OSUDP2"))
        self.assertIs(ProductType.MIR_OSUDP2, ProductType.normalize("L2OS"))
        self.assertIs(ProductType.MIR_OSUDP2, ProductType.normalize("OS"))

        with pytest.raises(ValueError, match="invalid product_type 'OZ'"):
            ProductType.normalize("OZ")

        with pytest.raises(TypeError, match="invalid product_type type <class 'int'>"):
            # noinspection PyTypeChecker
            ProductType.normalize(2)
