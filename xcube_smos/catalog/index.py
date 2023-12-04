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
from pathlib import Path
from typing import Union, Dict, Any, Optional, Tuple, List

import pandas as pd
import xarray as xr

from xcube.util.assertions import assert_given
from ..constants import INDEX_ENV_VAR_NAME
from ..nckcindex.nckcindex import NcKcIndex
from ..nckcindex.producttype import ProductType
from ..nckcindex.producttype import ProductTypeLike
from xcube_smos.timeinfo import to_compact_time
from .base import AbstractSmosCatalog
from .base import DatasetOpener

_ONE_DAY = pd.Timedelta(1, unit="days")


class SmosIndexCatalog(AbstractSmosCatalog):
    """A SMOS L2 dataset catalog that uses a Kerchunk index (NcKcIndex).

    :param index_path: Path or URL to the root directory
    :param index_storage_options: Storage options to access *index_urlpath*.
    """

    def __init__(self,
                 index_path: Optional[Union[str, Path]] = None,
                 index_storage_options: Optional[Dict[str, Any]] = None):
        index_path = index_path or os.environ.get(INDEX_ENV_VAR_NAME)
        assert_given(index_path, name='index_path')
        index_path = os.path.expanduser(str(index_path))
        self._nc_kc_index = NcKcIndex.open(
            index_path=index_path,
            index_storage_options=index_storage_options
        )

    @property
    def dataset_opener(self) -> DatasetOpener:
        return SmosIndexCatalog.open_dataset

    @staticmethod
    def open_dataset(dataset_path: str, source_storage_options: dict) \
            -> xr.Dataset:
        index_filename = dataset_path.rsplit("/", maxsplit=1)[-1]
        index_json_path = f"{dataset_path}/{index_filename}.nc.json"
        return xr.open_dataset(
            "reference://",
            engine="zarr",
            backend_kwargs={
                "storage_options": {
                    "fo": index_json_path,
                    "remote_protocol": "s3",
                    "remote_options": source_storage_options or {}
                },
                "consolidated": False
            },
            decode_cf=False  # IMPORTANT!
        )

    @property
    def source_storage_options(self) -> Optional[Dict[str, Any]]:
        return self._nc_kc_index.source_storage_options

    def find_datasets(self,
                      product_type: ProductTypeLike,
                      time_range: Tuple[Optional[str], Optional[str]]) \
            -> List[Tuple[str, str, str]]:
        product_type = ProductType.normalize(product_type)
        start, end = self._normalize_time_range(time_range)

        start_times = self._find_files_for_date(product_type,
                                                start.year,
                                                start.month,
                                                start.day)
        end_times = self._find_files_for_date(product_type,
                                              end.year,
                                              end.month,
                                              end.day)

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
                                              time.day)
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
                             day: int) -> List[Tuple[str, str, str]]:
        product_type = ProductType.normalize(product_type)
        path_pattern = product_type.path_pattern
        name_pattern = product_type.name_pattern

        path = self._nc_kc_index.index_path + "/" + path_pattern.format(
            year=year,
            month=f'0{month}' if month < 10 else month,
            day=f'0{day}' if day < 10 else day
        )

        result = []
        for item in self._nc_kc_index.index_fs.listdir(path, detail=True):
            # display(item)
            if item["type"] == "directory":
                name = item["name"][len(path) + 1:]
                m = re.match(name_pattern, name)
                if m is not None:
                    start = m.group("sd") + m.group("st")
                    end = m.group("ed") + m.group("et")
                    result.append((path + "/" + name, start, end))

        return sorted(result, key=lambda item: item[1])

    @staticmethod
    def _normalize_time_range(time_range):
        start, end = time_range
        if start is None:
            start = "2000-01-01 00:00:00"
        if end is None:
            end = "2050-01-01 00:00:00"
        start, end = pd.to_datetime((start, end))
        return start, end
