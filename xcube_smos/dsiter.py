# The MIT License (MIT)
# Copyright (c) 2024 by the xcube development team and contributors
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
from abc import ABC
from collections.abc import Sized, Iterator
from typing import List, Dict, Any, Callable

import numpy as np
import xarray as xr

from xcube.core.mldataset import MultiLevelDataset
from xcube_smos.mldataset.l2cube import SmosL2Product
from xcube_smos.mldataset.l2cube import get_dataset_spatial_subset


# TODO: Replace by new xcube "dsiter" type.
#   See https://github.com/xcube-dev/xcube/issues/919
class DatasetIterator(Iterator, Sized, ABC):
    """Interface representing a dataset interator."""

    def __next__(self) -> xr.Dataset:
        """Yield the next dataset."""


class SmosDatasetIterator(DatasetIterator):
    def __init__(
        self,
        dgg: MultiLevelDataset,
        dataset_opener: Callable,
        dataset_opener_kwargs: Dict[str, Any],
        dataset_paths: List[str],
        time_bounds: np.array,
        bbox: tuple[float, float, float, float] | None,
        res_level: int,
        var_names: set[str],
    ):
        self._dgg = dgg
        self._dataset_opener = dataset_opener
        self._dataset_opener_kwargs = dataset_opener_kwargs
        self._dataset_paths = dataset_paths
        self._time_bounds = time_bounds
        self._bbox = bbox
        self._res_level = res_level
        self._var_names = var_names
        self._current_index = 0

    @property
    def current_index(self):
        return self._current_index

    def __len__(self) -> int:
        return len(self._time_bounds)

    def __next__(self) -> xr.Dataset:
        index = self._current_index
        if index >= len(self._time_bounds):
            raise StopIteration()

        dgg = self._dgg
        dataset_path = self._dataset_paths[index]
        start, stop = self._time_bounds[index]
        bbox = self._bbox
        res_level = self._res_level

        l2_dataset = self._dataset_opener(dataset_path, **self._dataset_opener_kwargs)
        l2_product = SmosL2Product(dgg, l2_dataset)

        mapped_l2_product = l2_product.get_mapped_s2_product(res_level)

        mapped_dims = "time", "lat", "lon"

        dgg_ds = dgg.get_dataset(self._res_level)
        h, w = dgg_ds.seqnum.shape
        mapped_chunks = 1, h, w

        mapped_data_vars = {}
        for var_name, var in l2_dataset.data_vars.items():
            if var_name in self._var_names:
                mapped_var_data = mapped_l2_product.map_l2_var(var_name)
                mapped_var = xr.DataArray(
                    mapped_var_data, dims=mapped_dims, attrs=var.attrs
                )
                mapped_var.encoding = {
                    "dtype": var.dtype,
                    "chunks": mapped_chunks,
                    "preferred_chunks": dict(zip(mapped_dims, mapped_chunks)),
                }
                mapped_data_vars[var_name] = mapped_var

        time_encoding = {
            "calendar": "proleptic_gregorian",
            "units": "milliseconds since 2010-01-01 00:00:00.000000",
        }

        time_bnds_data = np.array([[start, stop]], dtype=self._time_bounds.dtype)
        time_bnds = xr.DataArray(time_bnds_data, dims=("time", "bnds"))

        time_data = np.array(
            [start + (stop - start) / 2], dtype=self._time_bounds.dtype
        )
        time = xr.DataArray(
            time_data,
            dims=("time",),
            attrs={
                "long_name": "time",
                "standard_name": "time",
                "bounds": "time_bnds",
            },
        )
        time.encoding.update(time_encoding)

        mapped_l2_dataset = xr.Dataset(
            mapped_data_vars,
            coords={**dgg_ds.coords, "time": time, "time_bnds": time_bnds},
            attrs=l2_dataset.attrs,
        )

        mapped_l2_dataset = (
            mapped_l2_dataset
            if bbox is None
            else get_dataset_spatial_subset(mapped_l2_dataset, bbox, dgg.grid_mapping)
        )

        self._current_index += 1

        return mapped_l2_dataset
