# The MIT License (MIT)
# Copyright (c) 2023-2024 by the xcube development team and contributors
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
import warnings
from functools import cached_property
import logging
import os
from pathlib import Path
import re
import shutil
import tempfile
from typing import Dict, Any, Set, Iterable, Union, Tuple, Optional, List, Callable

import fsspec
import pandas as pd
import xarray as xr

from ..constants import DEFAULT_ARCHIVE_URL
from ..constants import DEFAULT_STORAGE_OPTIONS
from ..constants import OS_VAR_NAMES
from ..constants import SM_VAR_NAMES
from ..timeinfo import normalize_time_range
from ..timeinfo import to_compact_time
from .base import AbstractSmosCatalog
from .producttype import ProductType
from .producttype import ProductTypeLike
from .types import DatasetOpener
from .types import DatasetRecord
from .types import DatasetFilter


GetFilesForPath = Callable[[str], Iterable[str]]

_ONE_DAY = pd.Timedelta(1, unit="days")

LOG = logging.getLogger("xcube-smos")


class SmosDirectCatalog(AbstractSmosCatalog):
    """A SMOS L2 dataset catalog that directly accesses the source filesystem."""

    def __init__(
        self,
        source_path: Optional[Union[str, Path]] = None,
        source_protocol: Optional[str] = None,
        source_storage_options: Optional[Dict[str, Any]] = None,
        cache_path: Optional[str] = None,
        xarray_kwargs: Dict[str, Any] = None,
        **extra_source_storage_options,
    ):
        source_path = str(source_path or DEFAULT_ARCHIVE_URL)
        _protocol, source_path = fsspec.core.split_protocol(source_path)
        source_protocol = source_protocol or _protocol or "file"
        if source_protocol == "file":
            source_path = os.path.expanduser(source_path)
        if source_storage_options is None:
            source_storage_options = dict(DEFAULT_STORAGE_OPTIONS)
        if extra_source_storage_options:
            source_storage_options = (
                dict(source_storage_options or {}) | extra_source_storage_options
            )
        self._source_path = source_path
        self._source_protocol = source_protocol
        self._source_storage_options = source_storage_options or {}
        self._cache_path = os.path.expanduser(cache_path) if cache_path else None
        self._xarray_kwargs = xarray_kwargs or {}

    @cached_property
    def source_fs(self) -> fsspec.AbstractFileSystem:
        return fsspec.filesystem(self._source_protocol, **self._source_storage_options)

    def get_dataset_opener_kwargs(self) -> Dict[str, Any]:
        return dict(
            source_protocol=self._source_protocol,
            source_storage_options=self._source_storage_options,
            cache_path=self._cache_path,
            xarray_kwargs=self._xarray_kwargs,
        )

    def get_dataset_opener(self) -> DatasetOpener:
        return open_dataset

    def find_datasets(
        self,
        product_type: ProductTypeLike,
        time_range: Tuple[Optional[str], Optional[str]],
        dataset_filter: Optional[DatasetFilter] = None,
        **query_parameters,
    ) -> List[DatasetRecord]:
        if query_parameters:
            warnings.warn(
                f"Additional parameter(s) not understood:"
                f" {', '.join(query_parameters.keys())}"
            )
        product_type = ProductType.normalize(product_type)
        return find_files_for_time_range(
            product_type,
            time_range,
            self._get_files_for_path,
            dataset_filter=dataset_filter,
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
    def get_instance() -> "TempNcDir":
        if TempNcDir._instance is None:
            TempNcDir._instance = TempNcDir()
            atexit.register(TempNcDir.dispose_instance)
        return TempNcDir._instance

    @staticmethod
    def dispose_instance():
        if TempNcDir._instance is not None:
            TempNcDir._instance.close()
            TempNcDir._instance = None


def open_dataset(
    source_file: str,
    source_protocol: str = None,
    source_storage_options: Dict[str, Any] = None,
    cache_path: Optional[str] = None,
    xarray_kwargs: Dict[str, Any] = None,
) -> xr.Dataset:
    open_dataset_kwargs = dict(xarray_kwargs or {})
    open_dataset_kwargs.update(decode_cf=False, chunks={})

    if "/SMOS/L2OS/" in source_file:
        var_names = OS_VAR_NAMES
    elif "/SMOS/L2SM/" in source_file:
        var_names = SM_VAR_NAMES
    else:
        var_names = None

    remote_fs = fsspec.filesystem(source_protocol, **source_storage_options)
    if not cache_path:
        if open_dataset_kwargs.get("engine") == "h5netcdf":
            LOG.debug("Opening dataset directly from %s", source_file)
            fp = remote_fs.open(source_file, "rb")
            ds = xr.open_dataset(fp, **open_dataset_kwargs)
        else:
            local_file = TempNcDir.get_instance().new_file()
            LOG.debug("Downloading %s to %s", source_file, local_file)
            remote_fs.get(source_file, local_file)
            LOG.debug("Opening dataset from %s", local_file)
            ds = xr.open_dataset(local_file, **open_dataset_kwargs)
        return filter_dataset(ds, var_names)
    else:
        local_file = f"{cache_path}/{source_file}"
        if not os.path.isfile(local_file):
            os.makedirs(os.path.dirname(local_file), exist_ok=True)
            temp_file = local_file + ".temp"
            LOG.debug("Downloading %s to %s", source_file, temp_file)
            remote_fs.get(source_file, temp_file)
            LOG.debug("Opening dataset from %s", temp_file)
            with xr.open_dataset(temp_file, **open_dataset_kwargs) as ds:
                dataset = filter_dataset(ds, var_names)
                LOG.debug("Writing %s", local_file)
                dataset.to_netcdf(local_file)
            os.remove(temp_file)
        LOG.debug("Opening dataset from %s", local_file)
        return xr.open_dataset(local_file, **open_dataset_kwargs)


def filter_dataset(ds: xr.Dataset, var_names: Set[str]) -> xr.Dataset:
    key_prefix = "VH:SPH:MI:TI:"
    ds = ds.drop_vars(
        [
            v
            for v in ds.data_vars
            if var_names is None or not (v in var_names or v == "Grid_Point_ID")
        ]
    )
    ds.attrs = {
        k[len(key_prefix) :]: v for k, v in ds.attrs.items() if k.startswith(key_prefix)
    }
    return ds


def find_files_for_time_range(
    product_type: ProductType,
    time_range: Tuple[Optional[str], Optional[str]],
    get_files_for_path: GetFilesForPath,
    dataset_filter: Optional[DatasetFilter] = None,
) -> List[DatasetRecord]:
    start, end = normalize_time_range(time_range)

    start_times = find_files_for_date(
        product_type, start, get_files_for_path, dataset_filter
    )
    end_times = find_files_for_date(
        product_type, end, get_files_for_path, dataset_filter
    )

    start_str = to_compact_time(start)
    end_str = to_compact_time(end)

    start_index = -1
    for index, (_, _, start_end_str) in enumerate(start_times):
        if start_end_str >= start_str:
            start_index = index
            break

    end_index = -1
    for index, (_, end_start_str, _) in enumerate(end_times):
        if end_start_str >= end_str:
            end_index = index
            break

    start_names = []
    if start_index >= 0:
        start_names.extend(start_times[start_index:])

    # Add everything between start + start.day and end - end.day

    start_p1d = (
        pd.Timestamp(year=start.year, month=start.month, day=start.day) + _ONE_DAY
    )
    end_m1d = pd.Timestamp(year=end.year, month=end.month, day=end.day) - _ONE_DAY

    in_between_names = []
    if end_m1d > start_p1d:
        time = start_p1d
        while time <= end_m1d:
            in_between_names.extend(
                find_files_for_date(
                    product_type, time, get_files_for_path, dataset_filter
                )
            )
            time += _ONE_DAY

    end_names = []
    if end_index >= 0:
        end_names.extend(end_times[:end_index])

    return start_names + in_between_names + end_names


def find_files_for_date(
    product_type: ProductType,
    date: pd.Timestamp,
    get_files_for_path: GetFilesForPath,
    accept_record: Optional[DatasetFilter] = None,
) -> List[DatasetRecord]:
    path_pattern = product_type.path_pattern
    name_pattern = product_type.name_pattern

    year = date.year
    month = date.month
    day = date.day

    prefix_path = path_pattern.format(
        year=year,
        month=f"0{month}" if month < 10 else month,
        day=f"0{day}" if day < 10 else day,
    )

    records = []
    for file_path in get_files_for_path(prefix_path):
        parent_and_filename = file_path.rsplit("/", 1)
        filename = (
            parent_and_filename[1] if len(parent_and_filename) == 2 else file_path
        )
        m = re.match(name_pattern, filename)
        if m is not None:
            start = m.group("sd") + m.group("st")
            end = m.group("ed") + m.group("et")

            record = file_path, start, end
            if accept_record is None or accept_record(record):
                records.append(record)

    return sorted(records)
