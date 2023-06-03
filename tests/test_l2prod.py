# The MIT License (MIT)
# Copyright (c) 2023 by the xcube development team and contributors
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

import os.path
import os.path
import unittest

from xcube_smos.dgg import SmosDiscreteGlobalGrid
from xcube_smos.l2prod import SmosMappedL2Product

dgg_path = os.path.expanduser("~/.snap/auxdata/smos-dgg/grid-tiles")


@unittest.skipUnless(os.path.isdir(dgg_path),
                     f"cannot find {dgg_path}")
class SmosMappedL2ProductTest(unittest.TestCase):
    def test_it(self):
        dgg = SmosDiscreteGlobalGrid()

        l2_product_path = os.path.join(
            os.path.dirname(__file__),
            "../testdata/SM/"
            "SM_OPER_MIR_SMUDP2_20230401T150613_20230401T155931_700_001_1.nc"
        )

        mapped_l2 = SmosMappedL2Product.open(l2_product_path, dgg)
        mapped_l2_level_0 = mapped_l2.get_dataset(0)

        encodings = {
            var_name: {**var.encoding, "write_empty_chunks": False}
            for var_name, var in mapped_l2_level_0.data_vars.items()
        }

        # TODO (forman): complete test

        # The above doesn't work:
        # ValueError: unexpected encoding parameters for zarr backend:
        # ['write_empty_chunks']:
        encodings = None

        mapped_l2_zarr_path = os.path.splitext(l2_product_path)[0] + ".zarr"

        # mapped_l2_level_0.to_zarr(mapped_l2_zarr_path,
        #                           mode="w",
        #                           encoding=encodings)
