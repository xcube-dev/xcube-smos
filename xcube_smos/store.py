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
from functools import cached_property
from typing import Iterator, Any, Tuple, Container, Union, Dict, Optional

import xarray as xr

from xcube.core.mldataset import MultiLevelDataset
from xcube.core.store import DATASET_TYPE
from xcube.core.store import DataDescriptor
from xcube.core.store import DataStore
from xcube.core.store import DataType
from xcube.core.store import DataTypeLike
from xcube.core.store import DatasetDescriptor
from xcube.core.store import MULTI_LEVEL_DATASET_TYPE
from xcube.core.store import MultiLevelDatasetDescriptor
from xcube.util.jsonschema import JsonObjectSchema
from .catalog import SmosCatalog
from .dgg import SmosDiscreteGlobalGrid
from .l2cube import SmosMappedL2Cube
from .l2index import SmosL2Index
from .l2prod import SmosMappedL2Product
from .schema import OPEN_PARAMS_SCHEMA
from .schema import STORE_PARAMS_SCHEMA
from .timeinfo import parse_time_ranges

_DATASETS = {
    'SMOS-L2-SM': {
        'title': 'SMOS Level-2 Soil Moisture'
    },
    'SMOS-L2-OS': {
        'title': 'SMOS Level-2 Ocean Salinity'
    }
}

DATASET_OPENER_ID = 'dataset:zarr:smos'
ML_DATASET_OPENER_ID = 'mldataset:zarr:smos'

DEFAULT_OPENER_ID = DATASET_OPENER_ID


class SmosDataStore(DataStore):
    def __init__(self,
                 dgg_path: Optional[str] = None,
                 index_urlpath: Optional[str] = None,
                 index_options: Optional[Dict[str, Any]] = None):
        self._dgg_path = dgg_path
        self._index_urlpath = index_urlpath
        self._index_options = index_options

    @classmethod
    def get_data_store_params_schema(cls) -> JsonObjectSchema:
        return STORE_PARAMS_SCHEMA

    @classmethod
    def get_data_types(cls) -> Tuple[str, ...]:
        return DATASET_TYPE.alias, MULTI_LEVEL_DATASET_TYPE.alias

    def get_data_types_for_data(self, data_id: str) -> Tuple[str, ...]:
        self._assert_valid_data_id(data_id)
        return self.get_data_types()

    def get_data_ids(self,
                     data_type: DataTypeLike = None,
                     include_attrs: Container[str] = None) -> \
            Union[Iterator[str], Iterator[Tuple[str, Dict[str, Any]]]]:
        if self._is_valid_data_type(data_type):
            for data_id, data_attrs in _DATASETS.items():
                if include_attrs:
                    yield (data_id,
                           {k: v
                            for k, v in data_attrs.items()
                            if k in include_attrs})
                else:
                    yield data_id

    def has_data(self, data_id: str, data_type: DataTypeLike = None) -> bool:
        if not self._is_valid_data_type(data_type):
            return False
        return data_id in _DATASETS

    @classmethod
    def get_search_params_schema(
            cls,
            data_type: DataTypeLike = None
    ) -> JsonObjectSchema:
        cls._assert_valid_data_type(data_type)
        return JsonObjectSchema(properties={},
                                additional_properties=False)

    def search_data(self,
                    data_type: DataTypeLike = None,
                    **search_params) -> Iterator[DataDescriptor]:
        data_type = self._assert_valid_data_type(data_type)
        for data_id, data_attrs in _DATASETS.items():
            yield self.describe_data(data_id, data_type=data_type)

    def get_data_opener_ids(
            self,
            data_id: str = None,
            data_type: DataTypeLike = None
    ) -> Tuple[str, ...]:
        if data_id is not None:
            self._assert_valid_data_id(data_id)
        if data_type is not None:
            data_type = self._assert_valid_data_type(data_type)
        if data_type is None:
            return DATASET_OPENER_ID, ML_DATASET_OPENER_ID
        return f'{data_type.alias}:zarr:smos',

    def describe_data(self,
                      data_id: str,
                      data_type: DataTypeLike = None) -> DataDescriptor:
        self._assert_valid_data_id(data_id)
        data_type = self._assert_valid_data_type(data_type)
        if data_type.is_sub_type_of(MULTI_LEVEL_DATASET_TYPE):
            return MultiLevelDatasetDescriptor(data_id, num_levels=6)
        else:
            return DatasetDescriptor(data_id)

    def get_open_data_params_schema(
            self,
            data_id: str = None,
            opener_id: str = None
    ) -> JsonObjectSchema:
        if data_id is not None:
            self._assert_valid_data_id(data_id)
        self._assert_valid_opener_id(opener_id)
        return OPEN_PARAMS_SCHEMA

    @cached_property
    def catalog(self) -> SmosCatalog:
        return SmosCatalog(index_urlpath=self._index_urlpath,
                           index_options=self._index_options)

    @cached_property
    def dgg(self) -> SmosDiscreteGlobalGrid:
        return SmosDiscreteGlobalGrid(path=self._dgg_path)

    def open_data(self,
                  data_id: str,
                  opener_id: str = None,
                  **open_params) -> Union[xr.Dataset, MultiLevelDataset]:
        OPEN_PARAMS_SCHEMA.validate_instance(open_params)
        self._assert_valid_data_id(data_id)
        product_type = data_id.rsplit('-', maxsplit=1)[-1]
        opener_id = self._assert_valid_opener_id(opener_id)
        data_type = DataType.normalize(opener_id.split(":")[0])

        # Required parameter time_range:
        time_range = open_params["time_range"]
        # Output debugging info to stdout
        debug = open_params.get("debug", False)
        # Force lazy loading of variable data from SMOS L2 products
        lazy_load = open_params.get("lazy_load", False)

        files = self.catalog.find_files(product_type, time_range)
        mapped_l2_products = []
        time_ranges = []
        for index_path, start, stop in files:
            index_filename = index_path.rsplit("/", maxsplit=1)[-1]
            index_json_path = f"{index_path}/{index_filename}.nc.json"
            self._debug_print(debug, f"Opening L2 product {index_json_path}")
            l2_product = xr.open_dataset(
                "reference://",
                engine="zarr",
                backend_kwargs={
                    "storage_options": {
                        "fo": index_json_path,
                        "remote_protocol": "s3",
                        "remote_options": self.catalog.s3_options
                    },
                    "consolidated": False
                },
                decode_cf=False  # IMPORTANT!
            )
            self._debug_print(debug, "Creating L2 index")
            l2_index = SmosL2Index(l2_product.Grid_Point_ID, self.dgg)
            self._debug_print(debug, "Mapping L2 product")
            mapped_l2_product = SmosMappedL2Product(l2_product,
                                                    l2_index,
                                                    lazy_load=lazy_load)
            mapped_l2_products.append(mapped_l2_product)
            time_ranges.append((start, stop))

        self._debug_print(debug, "Creating L2 cube")
        ml_dataset = SmosMappedL2Cube(
            mapped_l2_products,
            time_bounds=parse_time_ranges(time_ranges, is_compact=True)
        )
        if data_type.is_sub_type_of(MULTI_LEVEL_DATASET_TYPE):
            return ml_dataset
        else:
            return ml_dataset.get_dataset(0)

    @staticmethod
    def _debug_print(debug: bool, msg: str):
        if debug:
            print(msg)

    @classmethod
    def _assert_valid_data_id(cls, data_id: str):
        if data_id not in _DATASETS:
            raise ValueError(f'Unknown dataset identifier {data_id!r}')

    @classmethod
    def _assert_valid_opener_id(cls, opener_id: Optional[str]) -> str:
        if opener_id is None:
            return DEFAULT_OPENER_ID
        if opener_id not in (DATASET_OPENER_ID, ML_DATASET_OPENER_ID):
            raise ValueError(f'Invalid opener identifier {opener_id!r}')
        return opener_id

    @classmethod
    def _assert_valid_data_type(cls, data_type: Optional[DataTypeLike]) \
            -> DataType:
        data_type = cls._normalize_data_type(data_type)
        if not cls._is_valid_data_type(data_type):
            raise ValueError(f'Invalid dataset type {data_type!r}')
        return data_type

    @classmethod
    def _normalize_data_type(cls, data_type: Optional[DataTypeLike]) \
            -> DataType:
        if data_type is None:
            return MULTI_LEVEL_DATASET_TYPE
        return DataType.normalize(data_type)

    @classmethod
    def _is_valid_data_type(cls, data_type: Optional[DataTypeLike]) -> bool:
        data_type = cls._normalize_data_type(data_type)
        return data_type.is_sub_type_of(DATASET_TYPE) or \
               data_type.is_sub_type_of(MULTI_LEVEL_DATASET_TYPE)
