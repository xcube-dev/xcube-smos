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

import json
import os
from pathlib import Path
from typing import Union, Dict, Any, Optional, Tuple, List, Iterable

import fsspec
import pandas as pd
import xarray as xr

from xcube.util.assertions import assert_given

from ..constants import INDEX_ENV_VAR_NAME
from ..nckcindex.nckcindex import NcKcIndex
from .base import AbstractSmosCatalog
from .producttype import ProductType
from .producttype import ProductTypeLike
from .types import AcceptRecord
from .types import DatasetOpener
from .types import DatasetRecord

_ONE_DAY = pd.Timedelta(1, unit="days")


class SmosIndexCatalog(AbstractSmosCatalog):
    """A SMOS L2 dataset catalog that uses a Kerchunk index (NcKcIndex).

    :param index_path: Path or URL to the root directory
    """

    def __init__(self, index_path: Optional[Union[str, Path]] = None):
        index_path = index_path or os.environ.get(INDEX_ENV_VAR_NAME)
        assert_given(index_path, name='index_path')
        index_path = os.path.expanduser(str(index_path))
        self._nc_kc_index = NcKcIndex.open(index_path=index_path)

    @property
    def source_protocol(self) -> Optional[str]:
        return self._nc_kc_index.source_protocol

    @property
    def source_storage_options(self) -> Optional[Dict[str, Any]]:
        return self._nc_kc_index.source_storage_options

    def get_dataset_opener_kwargs(self) -> DatasetOpener:
        return dict(protocol=self.source_protocol,
                    storage_options=self.source_storage_options)

    def get_dataset_opener(self) -> DatasetOpener:
        return open_dataset

    def resolve_path(self, path: str) -> str:
        index_path = self._nc_kc_index.index_path
        index_url = f"file://{index_path}"
        if index_path.endswith(".zip"):
            return f'zip://{path}::{index_url}'
        else:
            return f'{index_url}/{path}'

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

    def _get_files_for_path(self, source_path: str) -> Iterable[str]:
        index_store = self._nc_kc_index.index_store
        for item in index_store.list(prefix=source_path + "/"):
            yield item

    def get_dataset_attrs(self, dataset_path: str) \
            -> Optional[Dict[str, Any]]:
        resolved_path = self.resolve_path(dataset_path)
        try:
            refs_dict = load_json(resolved_path)
        except OSError:
            # Warn
            return None
        if not isinstance(refs_dict, dict):
            return None
        refs = refs_dict.get("refs")
        if not isinstance(refs, dict):
            return None
        attrs_json = refs.get(".zattrs")
        if not isinstance(attrs_json, str):
            return None
        try:
            attrs = json.loads(attrs_json)
        except ValueError:
            # Warn
            return None
        if not isinstance(attrs, dict):
            return None
        return attrs


def open_dataset(path: str,
                 protocol: Optional[str] = None,
                 storage_options: Optional[dict] = None) \
        -> xr.Dataset:
    # Preload reference JSON
    # See https://github.com/fsspec/filesystem_spec/issues/1455
    refs = load_json(path)
    return xr.open_dataset(
        "reference://",
        engine="zarr",
        backend_kwargs={
            "storage_options": {
                "fo": refs,
                "remote_protocol": protocol,
                "remote_options": storage_options
            },
            "consolidated": False
        },
        # decode_cf=False is important!
        # Otherwise, fill-values will be replaced by NaN,
        # which will convert variable "seqnum" from dtype uint32 to
        # dtype float64.
        decode_cf=False
    )


def load_json(path):
    with fsspec.open(path) as f:
        return json.load(f)
