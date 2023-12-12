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
import re
from functools import cached_property
from pathlib import Path
import tempfile
from typing import Union, Dict, Any, Optional, Tuple, List, Set

import fsspec
import pandas as pd
import xarray as xr

from ..constants import OS_VAR_NAMES
from ..constants import SM_VAR_NAMES
from ..nckcindex.producttype import ProductType
from ..nckcindex.producttype import ProductTypeLike
from ..timeinfo import to_compact_time
from .base import AbstractSmosCatalog, DatasetPredicate, DatasetRecord
from .base import DatasetOpener

_ONE_DAY = pd.Timedelta(1, unit="days")


class SmosDirectCatalog(AbstractSmosCatalog):
    """A SMOS L2 dataset catalog that directly accesses the source filesystem.
    """

    def __init__(self,
                 source_path: Optional[Union[str, Path]] = None,
                 source_protocol: Optional[str] = None,
                 source_storage_options: Optional[Dict[str, Any]] = None,
                 cache_path: Optional[str] = None):
        source_path = source_path or "EODATA"
        source_path = os.path.expanduser(str(source_path))
        _protocol, source_path = fsspec.core.split_protocol(source_path)
        source_protocol = source_protocol or _protocol or "file"
        source_protocol = source_protocol or "s3"
        self._source_path = source_path
        self._source_protocol = source_protocol
        self._source_storage_options = source_storage_options or {}
        self._cache_path = cache_path

    @cached_property
    def source_fs(self) -> fsspec.AbstractFileSystem:
        return fsspec.filesystem(self._source_protocol,
                                 **self._source_storage_options)

    def get_dataset_opener_kwargs(self) -> DatasetOpener:
        return dict(source_protocol=self._source_protocol,
                    source_storage_options=self._source_storage_options,
                    cache_path=self._cache_path)

    def get_dataset_opener(self) -> DatasetOpener:
        return open_dataset

    def find_datasets(self,
                      product_type: ProductTypeLike,
                      time_range: Tuple[Optional[str], Optional[str]],
                      predicate: Optional[DatasetPredicate] = None) \
            -> List[DatasetRecord]:
        product_type = ProductType.normalize(product_type)
        start, end = self._normalize_time_range(time_range)

        start_times = self._find_files_for_date(product_type,
                                                start.year,
                                                start.month,
                                                start.day,
                                                predicate)
        end_times = self._find_files_for_date(product_type,
                                              end.year,
                                              end.month,
                                              end.day,
                                              predicate)

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

        start_p1d = pd.Timestamp(year=start.year,
                                 month=start.month,
                                 day=start.day) + _ONE_DAY
        end_m1d = pd.Timestamp(year=end.year,
                               month=end.month,
                               day=end.day) - _ONE_DAY

        in_between_names = []
        if end_m1d > start_p1d:
            time = start_p1d
            while time <= end_m1d:
                in_between_names.extend(
                    self._find_files_for_date(product_type,
                                              time.year,
                                              time.month,
                                              time.day,
                                              predicate)
                )
                time += _ONE_DAY

        end_names = []
        if end_index >= 0:
            end_names.extend(end_times[:end_index])

        return start_names + in_between_names + end_names

    def _find_files_for_date(self,
                             product_type: ProductTypeLike,
                             year: int,
                             month: int,
                             day: int,
                             predicate: Optional[DatasetPredicate]) \
            -> List[Tuple[str, str, str]]:
        product_type = ProductType.normalize(product_type)
        path_pattern = product_type.path_pattern
        name_pattern = product_type.name_pattern

        prefix = path_pattern.format(
            year=year,
            month=f'0{month}' if month < 10 else month,
            day=f'0{day}' if day < 10 else day
        )

        source_path = f"{self._source_path}/{prefix}"

        records = []
        for root, _, files in self.source_fs.walk(source_path):
            for file in files:
                parent_and_filename = file.rsplit("/", 1)
                filename = parent_and_filename[1] \
                    if len(parent_and_filename) == 2 else file
                m = re.match(name_pattern, filename)
                if m is not None:
                    start = m.group("sd") + m.group("st")
                    end = m.group("ed") + m.group("et")

                    record = f"{root}/{file}", start, end
                    records.append(record)

        return sorted(records)

    @staticmethod
    def _normalize_time_range(time_range):
        start, end = time_range
        if start is None:
            start = "2000-01-01 00:00:00"
        if end is None:
            end = "2050-01-01 00:00:00"
        start, end = pd.to_datetime((start, end))
        return start, end


def open_dataset(source_file: str,
                 source_protocol: str = None,
                 source_storage_options: Dict[str, Any] = None,
                 cache_path: Optional[str] = None) \
        -> xr.Dataset:
    remote_fs = fsspec.filesystem(source_protocol,
                                  **source_storage_options)
    if not cache_path:
        local_file = tempfile.TemporaryFile(prefix="xcube-smos-",
                                            suffix=".nc").name
        remote_fs.get(source_file, local_file)
    else:
        local_file = f"{cache_path}/{source_file}"
        if not os.path.isfile(local_file):
            key_prefix = "VH:SPH:MI:TI:"
            if "/SMOS/L2OS/" in source_file:
                var_names = OS_VAR_NAMES
            elif "/SMOS/L2SM/" in source_file:
                var_names = SM_VAR_NAMES
            else:
                var_names = None
            os.makedirs(os.path.dirname(local_file), exist_ok=True)
            temp_file = local_file + ".temp"
            remote_fs.get(source_file, temp_file)
            with xr.open_dataset(temp_file, decode_cf=False,
                                 chunks={}) as ds:
                dataset = include_vars(ds, var_names)
                dataset.attrs = {k[len(key_prefix):]: v
                                 for k, v in dataset.attrs.items()
                                 if k.startswith(key_prefix)}
                dataset.to_netcdf(local_file)
            os.remove(temp_file)

    return xr.open_dataset(local_file, decode_cf=False, chunks={})


def include_vars(ds: xr.Dataset, var_names: Set[str]) -> xr.Dataset:
    return ds.drop_vars([v for v in ds.data_vars
                         if var_names is None
                         or not (v in var_names or v == "Grid_Point_ID")])
