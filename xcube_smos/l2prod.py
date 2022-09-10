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

import xarray as xr

from xcube.core.gridmapping import GridMapping
from xcube.core.mldataset import LazyMultiLevelDataset
from .dgg import SmosDiscreteGlobalGrid
from .l2index import SmosL2Index


class SmosL2Product(LazyMultiLevelDataset):
    WIDTH = SmosDiscreteGlobalGrid.WIDTH
    HEIGHT = SmosDiscreteGlobalGrid.HEIGHT

    TILE_WIDTH = SmosDiscreteGlobalGrid.TILE_WIDTH
    TILE_HEIGHT = SmosDiscreteGlobalGrid.TILE_HEIGHT

    def __init__(self,
                 l2_ds: xr.Dataset,
                 dgg: SmosDiscreteGlobalGrid):
        super().__init__()
        self._l2_ds = l2_ds
        self._dgg = dgg
        self._l2_index = SmosL2Index(l2_ds.Grid_Point_ID, dgg)

    def _get_num_levels_lazily(self) -> int:
        return self._dgg.num_levels

    def _get_grid_mapping_lazily(self) -> GridMapping:
        return self._dgg.grid_mapping

    def _get_dataset_lazily(self,
                            level: int,
                            parameters: Dict[str, Any]) -> xr.Dataset:
        l2_index_ds = self._l2_index.get_dataset(level)
        l2_index = l2_index_ds.l2_index

        l2_ds = self._l2_ds

        data_vars = {}
        for l2_var_name, l2_var in l2_ds.data_vars.items():
            reprojected_var = l2_index.map_blocks(_reproject_var,
                                                  args=[l2_var])
            reprojected_var.attrs.update(l2_var.attrs)
            data_vars[l2_var_name] = reprojected_var
            # print(f"Created {l2_var_name} of type {l2_var.dtype}")

        return xr.Dataset(data_vars=data_vars)


def _reproject_var(l2_index_block: xr.DataArray,
                   l2_var: xr.DataArray) -> xr.DataArray:
    # print("Computing ", l2_index_block.shape)
    if l2_index_block.size == 0:
        return l2_index_block.astype(l2_var.dtype)
    return xr.DataArray(l2_var.data[l2_index_block.data],
                        dims=l2_index_block.dims,
                        coords=l2_index_block.coords)
