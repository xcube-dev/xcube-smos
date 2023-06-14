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

import abc
from typing import Dict, Any, Optional, Tuple, List, Callable

import xarray as xr

from xcube_smos.nckcindex.producttype import ProductTypeLike
from xcube_smos.utils import NotSerializable

DatasetOpener = Callable[[str, Optional[Dict[str, Any]]], xr.Dataset]


class AbstractSmosCatalog(NotSerializable, abc.ABC):
    """Abstract SMOS L2 dataset catalog.

    A catalog is used to find datasets and provides the means to
    open a found dataset.
    """

    @property
    @abc.abstractmethod
    def dataset_opener(self) -> DatasetOpener:
        """Get a function that opens a dataset. The function
        must have the signature:::

            open(dataset_path: str, remote_storage_options: dict)

        and must return a xarray dataset. The data set path is one
        of the returned paths from :meth:find_datasets() and
        *remote_storage_options* are the catalog's remote storage options.

        It is important that the dataset is opened using `decode_cf=False`
        if xarray is used.
        Otherwise, a given _FillValue attribute will turn Grid_Point_ID
        and other variables from integers into floating point.
        """

    @property
    def remote_storage_options(self) -> Optional[Dict[str, Any]]:
        """Get options for the remote data storage."""
        return None

    @abc.abstractmethod
    def find_datasets(self,
                      product_type: ProductTypeLike,
                      time_range: Tuple[Optional[str], Optional[str]]) \
            -> List[Tuple[str, str, str]]:
        """Find SMOS L2 datasets in the given *time_range*.

        :param product_type: SMOS product type
        :param time_range: Time range (from, to) ISO format, UTC
        :return: List of tuples of the form (dataset_path, start, stop), where
            start and stop represent the observation time range
            using "compact" datetime format, e.g., "20230503103546".
        """
