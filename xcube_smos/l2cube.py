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

from typing import Dict, Any, Sequence, Tuple, Optional

import numpy as np
import xarray as xr

from xcube.core.gridmapping import GridMapping
from xcube.core.mldataset import LazyMultiLevelDataset
from xcube.util.assertions import assert_given
from .dgg import SmosDiscreteGlobalGrid
from .l2prod import SmosMappedL2Product


class SmosMappedL2Cube(LazyMultiLevelDataset):
    """
    A multi-level dataset that represents a SMOS Level 2 data cube
    using a geographic projection.

    Use :meth:open to create instances of this class.

    :param l2_products: The mapped SMOS level 2 products.
    """

    WIDTH = SmosDiscreteGlobalGrid.WIDTH
    HEIGHT = SmosDiscreteGlobalGrid.HEIGHT

    TILE_WIDTH = SmosDiscreteGlobalGrid.TILE_WIDTH
    TILE_HEIGHT = SmosDiscreteGlobalGrid.TILE_HEIGHT

    def __init__(self, l2_products: Sequence[SmosMappedL2Product]):
        super().__init__()
        assert_given(l2_products, "l2_products")
        self._l2_products = tuple(l2_products)

        start_times = self._parse_times(l2_products,
                                        'FH:Validity_Period:Validity_Start')
        stop_times = self._parse_times(l2_products,
                                       'FH:Validity_Period:Validity_Stop')

        self.time = xr.DataArray(
            start_times + (stop_times - start_times) / 2,
            dims=('time',)
        )
        self.time_bnds = xr.DataArray(
            np.stack([start_times, stop_times], axis=1),
            dims=('time', 'bnds')
        )

    @classmethod
    def _parse_times(cls,
                     l2_products: Sequence[SmosMappedL2Product],
                     key: str) -> np.ndarray:
        return np.array(
            [cls._normalize_time_text(p.get_dataset(0).attrs.get(key))
             for p in l2_products],
            dtype=np.datetime64
        )

    @classmethod
    def _normalize_time_text(cls, time_str: Optional[str]):
        if not time_str:
            return ""
        if time_str.startswith("UTC="):
            return time_str[4:]
        return time_str

    @classmethod
    def open(cls,
             l2_product_paths: Sequence[str],
             dgg: SmosDiscreteGlobalGrid) \
            -> "SmosMappedL2Cube":
        """
        Open a multi-level data cube that represents the given
        SMOS Level 2 products using a geographic projection.

        The newly created multi-level cube has the same
        spatial layout and CRS as the given *dgg* but also
        has a time dimension.

        :param l2_product_paths: The SMOS Level 2 product paths
        :param dgg: The SMOS DGG
        :return: The mapped SMOS Level 2 product
        """
        return SmosMappedL2Cube([SmosMappedL2Product.open(p, dgg)
                                 for p in l2_product_paths])

    @property
    def l2_products(self) -> Tuple[SmosMappedL2Product]:
        """The SMOS level 2 products"""
        return self._l2_products

    @property
    def _first_l2_product(self) -> SmosMappedL2Product:
        """The first SMOS level 2 product"""
        return self._l2_products[0]

    def _get_num_levels_lazily(self) -> int:
        return self._first_l2_product.num_levels

    def _get_grid_mapping_lazily(self) -> GridMapping:
        return self._first_l2_product.grid_mapping

    def _get_dataset_lazily(self,
                            level: int,
                            parameters: Dict[str, Any]) -> xr.Dataset:
        time_slices = [p.get_dataset(level) for p in self.l2_products]
        dataset = xr.concat(time_slices, "time")
        dataset.coords["time"] = self.time
        dataset.coords["time_bnds"] = self.time_bnds
        return dataset
