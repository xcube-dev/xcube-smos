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
class SmosDiscreteGlobalGridTest(unittest.TestCase):
    def test_default(self):
        dgg = SmosDiscreteGlobalGrid()

        self.assertEqual(7, dgg.num_levels)

        ds0 = dgg.get_dataset(0)
        self.assertIn("seqnum", ds0)
        self.assertEqual((8064, 16384), ds0.seqnum.shape)
        self.assertEqual((16 * (504,), 32 * (512,)), ds0.seqnum.chunks)

        ds4 = dgg.get_dataset(4)
        self.assertIn("seqnum", ds4)
        self.assertEqual((504, 1024), ds4.seqnum.shape)
        self.assertEqual(((504,), (512, 512)), ds4.seqnum.chunks)

    def test_load(self):
        dgg = SmosDiscreteGlobalGrid(load=True)

        self.assertEqual(7, dgg.num_levels)

        ds0 = dgg.get_dataset(0)
        self.assertIn("seqnum", ds0)
        self.assertEqual((8064, 16384), ds0.seqnum.shape)
        self.assertEqual(None, ds0.seqnum.chunks)

        ds4 = dgg.get_dataset(4)
        self.assertIn("seqnum", ds4)
        self.assertEqual((504, 1024), ds4.seqnum.shape)
        self.assertEqual(None, ds4.seqnum.chunks)

        ds6 = dgg.get_dataset(6)
        self.assertIn("seqnum", ds6)
        self.assertEqual((126, 256), ds6.seqnum.shape)
        self.assertEqual(None, ds6.seqnum.chunks)

    def test_load_and_level0(self):
        dgg = SmosDiscreteGlobalGrid(load=True, level0=1)

        self.assertEqual(6, dgg.num_levels)

        ds0 = dgg.get_dataset(0)
        self.assertIn("seqnum", ds0)
        self.assertEqual((4032, 8192), ds0.seqnum.shape)
        self.assertEqual(None, ds0.seqnum.chunks)

        ds4 = dgg.get_dataset(3)
        self.assertIn("seqnum", ds4)
        self.assertEqual((504, 1024), ds4.seqnum.shape)
        self.assertEqual(None, ds4.seqnum.chunks)

        ds5 = dgg.get_dataset(5)
        self.assertIn("seqnum", ds5)
        self.assertEqual((126, 256), ds5.seqnum.shape)
        self.assertEqual(None, ds5.seqnum.chunks)
