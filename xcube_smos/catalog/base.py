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
from typing import Dict, Any, Optional, Tuple, List

from xcube_smos.catalog.producttype import ProductTypeLike
from ..utils import NotSerializable
from .types import DatasetOpener
from .types import DatasetRecord
from .types import AcceptRecord


class AbstractSmosCatalog(NotSerializable, abc.ABC):
    """Abstract SMOS L2 dataset catalog.

    A catalog is used to find datasets and provides the means to
    open a found dataset.
    """

    def get_dataset_attrs(self, dataset_path: str) -> Optional[Dict[str, Any]]:
        """Get the global attributes for the dataset given by *dataset_path*.

        The default implementation resolves the given *dataset_path*
        and returns the attributes of the dataset opened using the function
        returned by ``get_dataset_opener()``.

        :param dataset_path: Unresolved dataset path as returned by
            ``find_datasets()``.
        :return: The dictionary of dataset attributes
            or ``None``, if they cannot be retrieved.
        """
        resolved_path = self.resolve_path(dataset_path)
        open_dataset = self.get_dataset_opener()
        try:
            with open_dataset(
                resolved_path, **(self.get_dataset_opener_kwargs() or {})
            ) as ds:
                return dict(ds.attrs)
        except OSError:
            return None

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def get_dataset_opener_kwargs(self) -> Dict[str, Any]:
        """Get keyword arguments passed to the function returned by
        ``get_dataset_opener()``."""
        return {}

    @abc.abstractmethod
    def get_dataset_opener(self) -> DatasetOpener:
        """Get a function that opens a dataset. The function
        must have the signature:::

            open_dataset(resolved_path: str,
                         **kwargs) -> xr.Dataset

        and must return a xarray dataset.

        The optional *kwargs* must handle the ones contained in the
        dictionary returned by ``get_open_dataset_kwargs()``.

        The **resolved_path** passed to ``open_dataset`` is a resolved
        dataset path, that is, a path returned by ``find_datasets()``
        and transformed by the ``resolve_path()`` method.

        It is important that the dataset is opened using `decode_cf=False`
        if ``xarray.open_dataset()`` is used to open the dataset.
        Otherwise, a given _FillValue attribute will turn Grid_Point_ID
        and other variables from integers into floating point, because
        they use the `_FillValue` attribute.

        In addition, the returned dataset should contain only chunked arrays,
        which can be forced by passing ``chunks={}`` to
        ``xarray.open_dataset()``.
        """

    # noinspection PyMethodMayBeStatic
    def resolve_path(self, dataset_path: str) -> str:
        """Resolve the given path returned by ``find_datasets()``.

        :param dataset_path: Unresolved dataset path as returned by
            ``find_datasets()``.
        :return: A resolved dataset path that is passed to the
            (static) ``open_dataset`` function returned by the
            ``get_dataset_opener()`` method.
        """
        return dataset_path

    @abc.abstractmethod
    def find_datasets(
        self,
        product_type: ProductTypeLike,
        time_range: Tuple[Optional[str], Optional[str]],
        accept_record: Optional[AcceptRecord] = None,
    ) -> List[DatasetRecord]:
        """Find SMOS L2 datasets in the given *time_range*.

        :param product_type: SMOS product type
        :param time_range: Time range (from, to) ISO format, UTC
        :param accept_record: An optional dataset filter function,
            that receives a dataset record.
        :return: List of dataset records of the form
            (*dataset_path*, *start*, *stop*), where *dataset_path*
            is yet unresolved and *start*, *stop* represent the observation
            time range using "compact" datetime format,
            e.g., ``"20230503103546"``.
        """
