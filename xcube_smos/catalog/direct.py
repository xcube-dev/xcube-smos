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

import atexit
from functools import cached_property
import os
from pathlib import Path
import shutil
import tempfile
from typing import Union, Dict, Any, Optional, Tuple, List, Set, Iterable

import fsspec
import xarray as xr

from ..constants import OS_VAR_NAMES
from ..constants import SM_VAR_NAMES
from .base import AbstractSmosCatalog
from .producttype import ProductType
from .producttype import ProductTypeLike
from .types import DatasetOpener
from .types import DatasetRecord
from .types import AcceptRecord


class SmosDirectCatalog(AbstractSmosCatalog):
    """A SMOS L2 dataset catalog that directly accesses the source filesystem.
    """

    def __init__(self,
                 source_path: Optional[Union[str, Path]] = None,
                 source_protocol: Optional[str] = None,
                 source_storage_options: Optional[Dict[str, Any]] = None,
                 cache_path: Optional[str] = None,
                 xarray_kwargs: Dict[str, Any] = None):
        source_path = str(source_path or "EODATA")
        _protocol, source_path = fsspec.core.split_protocol(source_path)
        source_protocol = source_protocol or _protocol or "file"
        if source_protocol == "file":
            source_path = os.path.expanduser(source_path)
        self._source_path = source_path
        self._source_protocol = source_protocol
        self._source_storage_options = source_storage_options or {}
        self._cache_path = os.path.expanduser(cache_path) \
            if cache_path else None
        self._xarray_kwargs = xarray_kwargs or {}

    @cached_property
    def source_fs(self) -> fsspec.AbstractFileSystem:
        return fsspec.filesystem(self._source_protocol,
                                 **self._source_storage_options)

    def get_dataset_opener_kwargs(self) -> DatasetOpener:
        return dict(source_protocol=self._source_protocol,
                    source_storage_options=self._source_storage_options,
                    cache_path=self._cache_path,
                    xarray_kwargs=self._xarray_kwargs)

    def get_dataset_opener(self) -> DatasetOpener:
        return open_dataset

    def find_datasets(self,
                      product_type: ProductTypeLike,
                      time_range: Tuple[Optional[str], Optional[str]],
                      accept_record: Optional[AcceptRecord] = None) \
            -> List[DatasetRecord]:
        product_type = ProductType.normalize(product_type)
        return product_type.find_files_for_time_range(
            time_range,
            self._get_files_for_path,
            accept_record=accept_record
        )

    def _get_files_for_path(self, path: str) -> Iterable[str]:
        source_path = self._source_path + "/" + path
        for root, _, files in self.source_fs.walk(source_path):
            for file in files:
                if file:
                    yield root + "/" + file


class TempNcDir:
    def __init__(self):
        self._dir = tempfile.mkdtemp(prefix="xcube-smos-")

    def __del__(self):
        self.close()

    def new_file(self) -> str:
        return tempfile.mktemp(suffix=".nc", dir=self._dir)

    def close(self):
        shutil.rmtree(self._dir, ignore_errors=True)

    _instance = None

    @staticmethod
    def get_instance() -> 'TempNcDir':
        if TempNcDir._instance is None:
            TempNcDir._instance = TempNcDir()
            atexit.register(TempNcDir.dispose_instance)
        return TempNcDir._instance

    @staticmethod
    def dispose_instance():
        if TempNcDir._instance is not None:
            TempNcDir._instance.close()
            TempNcDir._instance = None


def open_dataset(source_file: str,
                 source_protocol: str = None,
                 source_storage_options: Dict[str, Any] = None,
                 cache_path: Optional[str] = None,
                 xarray_kwargs: Dict[str, Any] = None) \
        -> xr.Dataset:
    open_dataset_kwargs = dict(xarray_kwargs or {})
    open_dataset_kwargs.update(decode_cf=False, chunks={})

    if "/SMOS/L2OS/" in source_file:
        var_names = OS_VAR_NAMES
    elif "/SMOS/L2SM/" in source_file:
        var_names = SM_VAR_NAMES
    else:
        var_names = None

    remote_fs = fsspec.filesystem(source_protocol,
                                  **source_storage_options)
    if not cache_path:
        if open_dataset_kwargs.get("engine") == "h5netcdf":
            fp = remote_fs.open(source_file, "rb")
            ds = xr.open_dataset(fp, **open_dataset_kwargs)
        else:
            local_file = TempNcDir.get_instance().new_file()
            remote_fs.get(source_file, local_file)
            ds = xr.open_dataset(local_file, **open_dataset_kwargs)
        return filter_dataset(ds, var_names)
    else:
        local_file = f"{cache_path}/{source_file}"
        if not os.path.isfile(local_file):
            os.makedirs(os.path.dirname(local_file), exist_ok=True)
            temp_file = local_file + ".temp"
            remote_fs.get(source_file, temp_file)
            with xr.open_dataset(temp_file, **open_dataset_kwargs) as ds:
                dataset = filter_dataset(ds, var_names)
                dataset.to_netcdf(local_file)
            os.remove(temp_file)
        return xr.open_dataset(local_file, **open_dataset_kwargs)


def filter_dataset(ds: xr.Dataset, var_names: Set[str]) -> xr.Dataset:
    key_prefix = "VH:SPH:MI:TI:"
    ds = ds.drop_vars([v for v in ds.data_vars
                       if var_names is None
                       or not (v in var_names or v == "Grid_Point_ID")])
    ds.attrs = {k[len(key_prefix):]: v
                for k, v in ds.attrs.items()
                if k.startswith(key_prefix)}
    return ds
