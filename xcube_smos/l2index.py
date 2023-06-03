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

from typing import Dict, Any

import dask.array as da
import numba as nb
import numpy as np
import xarray as xr

from xcube.core.gridmapping import GridMapping
from xcube.core.mldataset import LazyMultiLevelDataset
from .dgg import SmosDiscreteGlobalGrid


class SmosL2Index(LazyMultiLevelDataset):
    """
    A multi-level dataset that provides variable "l2_index"
    which is used to map a SMOS Level 2 product on a geographic grid
    by the means of its "grid_point_id" variable.

    The newly created multi-level dataset has exactly the same
    layout and CRS as the given *dgg*.

    :param grid_point_id_var: The variable "grid_point_id" from a
        SMOS level 2 product
    :param dgg: The SMOS DGG
    """

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
            raise ValueError(f'expected variable {grid_point_id_var.name!r}'
                             f' to be a sub type of np.uint,'
                             f' but was {grid_point_id_var.dtype}.'
                             f' Note that SMOS L2 products should be'
                             f' opened with decode_cf=False.')

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
        missing_index = len(grid_point_id_var)
        seqnum_to_index = np.full(SmosDiscreteGlobalGrid.MAX_SEQNUM + 1,
                                  missing_index,
                                  dtype=np.uint32)
        for i in range(len(seqnums)):
            j = seqnums[i]
            seqnum_to_index[j] = i

        self._seqnum_to_index = seqnum_to_index
        self._missing_index = missing_index

    @property
    def seqnum_to_index(self) -> np.ndarray:
        return self._seqnum_to_index

    @property
    def missing_index(self) -> int:
        return self._missing_index

    def _get_num_levels_lazily(self) -> int:
        return self._dgg.num_levels

    def _get_grid_mapping_lazily(self) -> GridMapping:
        return self._dgg.grid_mapping

    def _get_dataset_lazily(self,
                            level: int,
                            parameters: Dict[str, Any]) -> xr.Dataset:

        dgg_level_ds = self._dgg.get_dataset(level)
        seqnum_var = dgg_level_ds.seqnum

        assert isinstance(seqnum_var.data, da.Array)
        assert isinstance(self._seqnum_to_index, np.ndarray)

        # Note, da.map_blocks (dask 2022.6.0) is much faster
        # than xr.map_blocks (xarray 2022.3.0)!
        l2_index_data = seqnum_var.data.map_blocks(
            map_seqnum_to_l2_index,
            dtype=seqnum_var.dtype,
            chunks=seqnum_var.chunks,
            seqnum_to_index=self._seqnum_to_index,
        )

        l2_index_var = xr.DataArray(
            l2_index_data,
            dims=seqnum_var.dims,
            attrs={"missing_index": self._missing_index}
        )

        return xr.Dataset(data_vars=dict(l2_index=l2_index_var),
                          coords=dgg_level_ds.coords)


@nb.jit(nopython=True)
def map_seqnum_to_l2_index(seqnum_values_2d: np.ndarray,
                           seqnum_to_index: np.ndarray) -> np.ndarray:
    seqnum_values_1d = seqnum_values_2d.flatten()
    index_values_1d = np.zeros(seqnum_values_1d.size,
                               dtype=np.uint32)
    for i in range(index_values_1d.size):
        seqnum = seqnum_values_1d[i]
        index_values_1d[i] = seqnum_to_index[seqnum]
    return index_values_1d.reshape(seqnum_values_2d.shape)
