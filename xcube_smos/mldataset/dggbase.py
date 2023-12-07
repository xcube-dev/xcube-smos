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
from typing import Dict, Any, Tuple

import fsspec.core
import numpy as np
import xarray as xr

from xcube.core.mldataset import LazyMultiLevelDataset
from xcube.core.zarrstore import GenericArray
from xcube.core.zarrstore import GenericZarrStore
from xcube.util.assertions import assert_given
from xcube.util.assertions import assert_true
from xcube_smos.utils import NotSerializable


class SmosDiscreteGlobalGrid(NotSerializable, LazyMultiLevelDataset):
    """
    A multi-level dataset that represents the SMOS discrete global grid (DGG)
    in geographic projection.

    :param urlpath: Path or URL to the DGG as a SNAP image pyramid.
    :param level0: The level that will become level zero.
        Default is zero.
    :param compute: Whether to compute and entirely load
        the DGG level datasets. If True, data will not be chunked.
        Default is False.
    """

    # Constant data type of "seqnum"
    DTYPE: np.dtype = np.dtype(np.uint32).newbyteorder('>')

    # Smallest possible "seqnum" value
    MIN_SEQNUM = 1
    # Greatest possible "seqnum" value
    MAX_SEQNUM = 2621442

    # Total number of resolution levels
    MAX_NUM_LEVELS = 7

    # Width in pixels at level zero
    MAX_WIDTH = 16384
    # Height in pixels at level zero
    MAX_HEIGHT = 8064

    # Constant tile width in pixels
    TILE_WIDTH = 512
    # Constant tile height in pixels
    TILE_HEIGHT = 504

    # Pixel size in degrees at lever zero (= highest spatial resolution)
    MIN_PIXEL_SIZE = 360. / MAX_WIDTH

    def __init__(self,
                 urlpath: str,
                 level0: int = 0,
                 compute: bool = False):
        super().__init__()
        assert_given(urlpath, name="urlpath")
        protocol, path = fsspec.core.split_protocol(urlpath)
        protocol = protocol or "file"
        fs: fsspec.AbstractFileSystem = fsspec.filesystem(protocol)
        if protocol == "file":
            path = os.path.expanduser(path)
            urlpath = path
        assert_true(fs.exists(path),
                    message=f'SMOS DDG not found: {urlpath}')
        assert_true(0 <= level0 < self.MAX_NUM_LEVELS,
                    message=f'Invalid level0: {level0}')
        self._urlpath = urlpath
        self._compute = compute
        self._level0 = level0

    @property
    def urlpath(self) -> str:
        return self._urlpath

    @property
    def compute(self) -> bool:
        return self._compute

    @property
    def level0(self) -> int:
        return self._level0

    def _get_num_levels_lazily(self) -> int:
        return self.MAX_NUM_LEVELS - self._level0

    def get_level_geom(self, level: int) -> Tuple[int, int, float]:
        level = level + self._level0
        width = self.MAX_WIDTH >> level
        height = self.MAX_HEIGHT >> level
        spatial_res = (1 << level) * self.MIN_PIXEL_SIZE
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
                get_data=SmosDiscreteGlobalGrid.load_smos_dgg_tile,
                get_data_params=dict(
                    level=level + self._level0,
                    base_path=self._urlpath,
                ),
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
        if self._compute:
            dataset.load()
        else:
            dataset.zarr_store.set(zarr_store)
        return dataset

    # It is very important that this method is static.
    # Otherwise, the current object will be serialized
    # to Dask workers! This must not happen.
    @staticmethod
    def load_smos_dgg_tile(chunk_info: Dict[str, Any],
                           array_info: Dict[str, Any],
                           level: int,
                           base_path: str) -> np.ndarray:
        y_index, x_index = chunk_info["index"]
        shape = chunk_info["shape"]
        dtype = array_info["dtype"]
        # TODO (forman): this currently works for local base_path only.
        #   Fix code to also load tiles from S3
        path = f"{base_path}/{level}/{x_index}-{y_index}.raw.zip"
        with zipfile.ZipFile(path) as zf:
            name = zf.namelist()[0]
            with zf.open(name) as fp:
                buffer = fp.read()
                return np.frombuffer(buffer, dtype=dtype).reshape(shape)

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

