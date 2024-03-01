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

from xcube_smos.catalog.base import AbstractSmosCatalog
from xcube_smos.catalog.direct import filter_dataset
from xcube_smos.catalog.types import ProductFilter
from xcube_smos.catalog.types import DatasetRecord
from xcube_smos.catalog.types import DatasetOpener
from xcube_smos.catalog.producttype import ProductType
from xcube_smos.catalog.producttype import ProductTypeLike
from xcube_smos.constants import OS_VAR_NAMES
from xcube_smos.constants import SM_VAR_NAMES


class SmosSimpleCatalog(AbstractSmosCatalog):
    """A simple SMOS L2 dataset catalog for testing only.

    :param smos_l2_sm_paths: SMOS L2 SM file paths
    :param smos_l2_os_paths: SMOS L2 SM file paths
    """

    def __init__(self, smos_l2_sm_paths: List[str], smos_l2_os_paths: List[str]):
        self.smos_l2_sm_paths = smos_l2_sm_paths or []
        self.smos_l2_os_paths = smos_l2_os_paths or []

    def get_dataset_opener(self) -> DatasetOpener:
        return SmosSimpleCatalog.open_dataset

    @staticmethod
    def open_dataset(dataset_path: str) -> xr.Dataset:
        ds = xr.open_dataset(dataset_path, engine="h5netcdf", decode_cf=False)
        return filter_dataset(
            ds, SM_VAR_NAMES if "_SMUDP2_" in dataset_path else OS_VAR_NAMES
        )

    def find_datasets(
        self,
        product_type: ProductTypeLike,
        time_range: Tuple[Optional[str], Optional[str]],
        accept_record: Optional[ProductFilter] = None,
    ) -> List[DatasetRecord]:
        product_type = ProductType.normalize(product_type)
        if product_type.type_id == "SM":
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
                warnings.warn(
                    f"path {path} does not match" f" pattern {name_pattern!r}"
                )
                continue
            start = m.group("sd") + m.group("st")
            end = m.group("ed") + m.group("et")
            record = path, start, end
            if accept_record is None or accept_record(record):
                result.append(record)

        return result
