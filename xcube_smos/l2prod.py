# The MIT License (MIT)
# Copyright (c) 2022 by the xcube development team and contributors
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

from typing import Dict, Any

import dask.array as da
import numpy as np
import xarray as xr

from xcube.core.gridmapping import GridMapping
from xcube.core.mldataset import LazyMultiLevelDataset
from .dgg import SmosDiscreteGlobalGrid
from .l2index import SmosL2Index


class SmosMappedL2Product(LazyMultiLevelDataset):
    WIDTH = SmosDiscreteGlobalGrid.WIDTH
    HEIGHT = SmosDiscreteGlobalGrid.HEIGHT

    TILE_WIDTH = SmosDiscreteGlobalGrid.TILE_WIDTH
    TILE_HEIGHT = SmosDiscreteGlobalGrid.TILE_HEIGHT

    @classmethod
    def open(cls, l2_product_path: str, dgg: SmosDiscreteGlobalGrid) \
            -> "SmosMappedL2Product":
        # Note, decode_cf=False is important!
        l2_product = xr.open_dataset(l2_product_path, decode_cf=False)
        l2_index = SmosL2Index(l2_product.Grid_Point_ID, dgg)
        return SmosMappedL2Product(
            l2_product,
            l2_index
        )

    def __init__(self,
                 l2_product: xr.Dataset,
                 l2_index: SmosL2Index):
        super().__init__()
        self._l2_product = l2_product
        self._l2_index = l2_index

    @property
    def l2_product(self) -> xr.Dataset:
        return self._l2_product

    @property
    def l2_index(self) -> SmosL2Index:
        return self._l2_index

    def _get_num_levels_lazily(self) -> int:
        return self._l2_index.num_levels

    def _get_grid_mapping_lazily(self) -> GridMapping:
        return self._l2_index.grid_mapping

    def _get_dataset_lazily(self,
                            level: int,
                            parameters: Dict[str, Any]) -> xr.Dataset:
        l2_index_ds = self._l2_index.get_dataset(level)
        l2_index = l2_index_ds.l2_index

        l2_product = self._l2_product

        data_vars = {}
        for l2_var_name, l2_var in l2_product.data_vars.items():
            assert isinstance(l2_index.data, da.Array)
            assert isinstance(l2_var.data, np.ndarray)

            mapped_l2_data = l2_index.data.map_blocks(
                map_l2_values,
                dtype=l2_var.dtype,
                chunks=l2_index.chunks,
                l2_values=l2_var.data
            )

            data_vars[l2_var_name] = xr.DataArray(
                mapped_l2_data,
                dims=l2_index.dims,
                # coords=l2_index.coords,
                attrs=l2_var.attrs,
                name=l2_var_name,
            )

        return xr.Dataset(data_vars=data_vars,
                          coords=l2_index_ds.coords,
                          attrs=l2_product.attrs)


def map_l2_values(l2_index: np.ndarray,
                  l2_values: np.ndarray = None) -> np.ndarray:
    # print("Computing ", l2_index_block.shape)
    if l2_index.size == 0:
        return l2_index.astype(l2_values.dtype)
    return l2_values[l2_index]
