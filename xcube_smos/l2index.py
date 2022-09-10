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

import numba
import numpy as np
import xarray as xr

from xcube.core.gridmapping import GridMapping
from xcube.core.mldataset import LazyMultiLevelDataset
from .dgg import SmosDiscreteGlobalGrid


class SmosL2Index(LazyMultiLevelDataset):
    WIDTH = SmosDiscreteGlobalGrid.WIDTH
    HEIGHT = SmosDiscreteGlobalGrid.HEIGHT

    TILE_WIDTH = SmosDiscreteGlobalGrid.TILE_WIDTH
    TILE_HEIGHT = SmosDiscreteGlobalGrid.TILE_HEIGHT

    def __init__(self,
                 grid_point_id_var: xr.DataArray,
                 dgg: SmosDiscreteGlobalGrid):
        super().__init__()
        self._dgg = dgg

        if not np.issubdtype(grid_point_id_var.dtype, np.uint):
            raise ValueError()

        seqnums = SmosDiscreteGlobalGrid.grid_point_id_to_seqnum(
            grid_point_id_var.values
        )
        min_seqnum = np.min(seqnums)
        max_seqnum = np.max(seqnums)
        if not (SmosDiscreteGlobalGrid.MIN_SEQNUM
                <= min_seqnum
                <= SmosDiscreteGlobalGrid.MAX_SEQNUM):
            raise ValueError()
        if not (SmosDiscreteGlobalGrid.MIN_SEQNUM
                <= max_seqnum
                <= SmosDiscreteGlobalGrid.MAX_SEQNUM):
            raise ValueError()

        # TODO (forman): numba!
        seqnum_to_index = np.zeros(SmosDiscreteGlobalGrid.MAX_SEQNUM + 1,
                                   dtype=np.uint32)
        for i in range(len(seqnums)):
            j = seqnums[i]
            seqnum_to_index[j] = i

        self.seqnum_to_index = seqnum_to_index

    def _get_num_levels_lazily(self) -> int:
        return self._dgg.num_levels

    def _get_grid_mapping_lazily(self) -> GridMapping:
        return self._dgg.grid_mapping

    def _get_dataset_lazily(self,
                            level: int,
                            parameters: Dict[str, Any]) -> xr.Dataset:

        dgg_level_ds = self._dgg.get_dataset(level)
        dgg_seqnum_var = dgg_level_ds.seqnum

        l2_index_var = dgg_seqnum_var.map_blocks(
            _map_dgg_seqnum_to_l2_index,
            args=[self.seqnum_to_index],
            template=dgg_seqnum_var
        )

        return xr.Dataset(dict(l2_index=l2_index_var))


def _map_dgg_seqnum_to_l2_index(dgg_seqnum_block: xr.DataArray,
                                seqnum_to_index: np.ndarray) -> xr.DataArray:
    dgg_seqnum = dgg_seqnum_block.values.flatten()
    l2_index = __map_dgg_seqnum_to_l2_index(dgg_seqnum,
                                            seqnum_to_index)
    return xr.DataArray(l2_index.reshape(dgg_seqnum_block.shape),
                        dims=dgg_seqnum_block.dims,
                        coords=dgg_seqnum_block.coords)


@numba.jit(nopython=True)
def __map_dgg_seqnum_to_l2_index(smos_seqnum_values: np.ndarray,
                                 seqnum_to_index: np.ndarray) -> np.ndarray:
    smos_index_values = np.zeros(smos_seqnum_values.size, dtype=np.uint32)
    for i in range(smos_index_values.size):
        seqnum = smos_seqnum_values[i]
        smos_index_values[i] = seqnum_to_index[seqnum]
    return smos_index_values
