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

import os
import os.path
import zipfile
from typing import Dict, Any, Optional, Tuple

import numpy as np
import xarray as xr

from xcube.core.mldataset import LazyMultiLevelDataset
from xcube.core.zarrstore import GenericArray
from xcube.core.zarrstore import GenericZarrStore
from xcube.util.assertions import assert_true
from xcube_smos.constants import DEFAULT_SMOS_DGG_PATH
from xcube_smos.constants import DGG_ENV_VAR_NAME


class SmosDiscreteGlobalGrid(LazyMultiLevelDataset):
    """
    A multi-level dataset that represents the SMOS discrete global grid (DGG)
    in geographic projection.

    :param path: Path to the DGG as a SNAP image pyramid.
        If not given, the value of the environment variable named
        "XCUBE_SMOS_DGG_PATH" is used.
        If this isn't given as well, *path* defaults to
        "~/.snap/auxdata/smos-dgg/grid-tiles", which is installed
        by the SNAP SMOS-Box plugin.
    """

    MIN_SEQNUM = 1
    MAX_SEQNUM = 2621442

    # TODO: Rename into MAX_WIDTH, MAX_HEIGHT
    WIDTH = 16384
    HEIGHT = 8064

    TILE_WIDTH = 512
    TILE_HEIGHT = 504

    # TODO: Rename into MAX_NUM_LEVELS
    NUM_LEVELS = 5

    # TODO: Rename into MAX_SPATIAL_RES
    SPATIAL_RES = 360. / WIDTH

    DTYPE: np.dtype = np.dtype(np.uint32).newbyteorder('>')

    def __init__(self, path: Optional[str] = None,
                 load: bool = False,
                 level0: int = 0):
        super().__init__()
        path = os.path.expanduser(path
                                  or os.environ.get(DGG_ENV_VAR_NAME)
                                  or DEFAULT_SMOS_DGG_PATH)
        assert_true(os.path.exists(path),
                    message=f'SMOS DDG not found: {path}')
        assert_true(0 <= level0 < self.NUM_LEVELS,
                    message=f'Invalid level0: {level0}')
        self._path = os.path.expanduser(path)
        self._load = load
        self._level0 = level0

    def _get_num_levels_lazily(self) -> int:
        return self.NUM_LEVELS - self._level0

    def get_level_geom(self, level: int) -> Tuple[int, int, float]:
        level = level + self._level0
        width = self.WIDTH >> level
        height = self.HEIGHT >> level
        spatial_res = (1 << level) * self.SPATIAL_RES
        return width, height, spatial_res

    def _get_dataset_lazily(self,
                            level: int,
                            parameters: Dict[str, Any]) -> xr.Dataset:

        width, height, spatial_res = self.get_level_geom(level)

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
        dataset: xr.Dataset = xr.open_zarr(zarr_store)
        if self._load:
            dataset.load()
        else:
            dataset.zarr_store.set(zarr_store)
        return dataset

    def _load_smos_dgg_tile(self,
                            chunk_info: Dict[str, Any],
                            level: int = 0) -> np.ndarray:
        level = level + self._level0
        y_index, x_index = chunk_info["index"]
        shape = chunk_info["shape"]
        path = self._path + f"/{level}/{x_index}-{y_index}.raw.zip"
        with zipfile.ZipFile(path) as zf:
            name = zf.namelist()[0]
            with zf.open(name) as fp:
                buffer = fp.read()
                return np.frombuffer(buffer, dtype=self.DTYPE).reshape(shape)

    # check if numba.jit can significantly improve speed
    @staticmethod
    def grid_point_id_to_seqnum(grid_point_id: np.ndarray) -> np.ndarray:
        # TODO (forman): add link to SMOS DGG docs here to
        #   explain magic numbers
        return np.where(
            grid_point_id < 1000000,
            grid_point_id,
            grid_point_id - 737856 * ((grid_point_id - 1) // 1000000) + 1
        )

