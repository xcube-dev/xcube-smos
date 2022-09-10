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

import zipfile
from typing import Dict, Any

import numpy as np
import xarray as xr

from xcube.core.mldataset import LazyMultiLevelDataset
from xcube.core.store.zarrstore import GenericArray
from xcube.core.store.zarrstore import GenericZarrStore


class SmosDiscreteGlobalGrid(LazyMultiLevelDataset):
    MIN_SEQNUM = 1
    MAX_SEQNUM = 2621442

    WIDTH = 16384
    HEIGHT = 8064

    TILE_WIDTH = 512
    TILE_HEIGHT = 504

    DTYPE: np.dtype = np.dtype(np.uint32).newbyteorder('>')

    def __init__(self, path: str):
        super().__init__()
        self._path = path

    def _get_num_levels_lazily(self) -> int:
        return 5

    def _get_dataset_lazily(self,
                            level: int,
                            parameters: Dict[str, Any]) -> xr.Dataset:
        spatial_res = (1 << level) * 360. / self.WIDTH

        width = self.WIDTH >> level
        height = self.HEIGHT >> level

        zarr_store = GenericZarrStore(
            GenericArray(
                name="seqnum",
                dtype=self.DTYPE.str,
                dims=("lat", "lon"),
                shape=(height, width),
                chunks=(self.TILE_HEIGHT, self.TILE_WIDTH),
                get_data=self._load_smos_dgg_tile,
                get_data_params=dict(level=level),
                chunk_encoding="ndarray"
            ),
            GenericArray(
                name="lon",
                dims="lon",
                data=np.linspace(-180 + spatial_res / 2,
                                 +180 - spatial_res / 2,
                                 width),
            ),
            GenericArray(
                name="lat",
                dims="lat",
                data=np.linspace(+height * spatial_res / 2 - spatial_res / 2,
                                 -height * spatial_res / 2 + spatial_res / 2,
                                 height),
            ),
        )
        dataset = xr.open_zarr(zarr_store)
        dataset.zarr_store.set(zarr_store)
        return dataset

    def _load_smos_dgg_tile(self,
                            chunk_info: Dict[str, Any],
                            level: int = 0) -> np.ndarray:
        y_index, x_index = chunk_info["index"]
        shape = chunk_info["shape"]
        path = self._path + f"/{level}/{x_index}-{y_index}.raw.zip"
        with zipfile.ZipFile(path) as zf:
            name = zf.namelist()[0]
            with zf.open(name) as fp:
                buffer = fp.read()
                return np.frombuffer(buffer, dtype=self.DTYPE).reshape(shape)

    @staticmethod
    def grid_point_id_to_seqnum(grid_point_id: np.ndarray) -> np.ndarray:
        return np.where(
            grid_point_id < 1000000,
            grid_point_id,
            grid_point_id - 737856 * ((grid_point_id - 1) // 1000000) + 1
        )

