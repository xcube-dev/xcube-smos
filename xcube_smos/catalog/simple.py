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
import re
import warnings
from typing import Tuple, Optional, List

import xarray as xr

from ..nckcindex.producttype import ProductType
from ..nckcindex.producttype import ProductTypeLike
from .base import AbstractSmosCatalog, DatasetPredicate, DatasetRecord
from .base import DatasetOpener


class SmosSimpleCatalog(AbstractSmosCatalog):
    """A simple SMOS L2 dataset catalog for testing only.

    :param smos_l2_sm_paths: SMOS L2 SM file paths
    :param smos_l2_os_paths: SMOS L2 SM file paths
    """

    def __init__(self,
                 smos_l2_sm_paths: List[str],
                 smos_l2_os_paths: List[str]):
        self.smos_l2_sm_paths = smos_l2_sm_paths or []
        self.smos_l2_os_paths = smos_l2_os_paths or []

    @property
    def dataset_opener(self) -> DatasetOpener:
        return SmosSimpleCatalog.open_dataset

    # noinspection PyUnusedLocal
    @staticmethod
    def open_dataset(dataset_path: str,
                     protocol: Optional[str] = None,
                     storage_options: Optional[dict] = None) \
            -> xr.Dataset:
        return xr.open_dataset(dataset_path, decode_cf=False)

    def find_datasets(self,
                      product_type: ProductTypeLike,
                      time_range: Tuple[Optional[str], Optional[str]],
                      predicate: Optional[DatasetPredicate] = None) \
            -> List[DatasetRecord]:
        product_type = ProductType.normalize(product_type)
        if product_type.id == "SM":
            paths = self.smos_l2_sm_paths
        else:
            paths = self.smos_l2_os_paths
        result = []
        for path in paths:
            name = os.path.basename(path)
            name, _ = os.path.splitext(name)
            name_pattern = product_type.name_pattern
            m = re.match(name_pattern, name)
            if m is None:
                warnings.warn(f"path {path} does not match"
                              f" pattern {name_pattern!r}")
                continue
            start = m.group("sd") + m.group("st")
            end = m.group("ed") + m.group("et")
            record = path, start, end
            if not predicate:
                result.append(record)
            else:
                with xr.open_dataset(path) as ds:
                    if predicate(record, ds.attrs):
                        result.append(record)

        return result
