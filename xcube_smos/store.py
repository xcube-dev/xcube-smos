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
from .catalog import AbstractSmosCatalog
from .catalog import SmosDirectCatalog
from .dsiter import DatasetIterator
from .mldataset.newdgg import MAX_HEIGHT
from .mldataset.newdgg import MIN_PIXEL_SIZE
from .mldataset.newdgg import new_dgg
from .mldataset.l2cube import SmosL2Cube
from .mldataset.l2cube import SmosTimeStepLoader
from .schema import OPEN_PARAMS_SCHEMA
from .schema import STORE_PARAMS_SCHEMA
from .timeinfo import parse_time_ranges
from .utils import NotSerializable

_DATASETS = {
    'SMOS-L2C-SM': {
        'title': 'SMOS Level-2 Soil Moisture'
    },
    'SMOS-L2C-OS': {
        'title': 'SMOS Level-2 Ocean Salinity'
    }
}

DATASET_ITERATOR_TYPE = DataType(
    DatasetIterator,
    ['dsiter', 'xcube_smos.dsiter.DatasetIterator']
)
DataType.register_data_type(DATASET_ITERATOR_TYPE)

DATASET_OPENER_ID = 'dataset:zarr:smos'
ML_DATASET_OPENER_ID = 'mldataset:zarr:smos'
DATASET_ITERATOR_OPENER_ID = 'dsiter:zarr:smos'

DEFAULT_OPENER_ID = DATASET_OPENER_ID


class SmosDataStore(NotSerializable, DataStore):
    """Data store for SMOS L2C data cubes.

    :param source_path: Path or URL to the SMOS Kerchunk index
    :param source_protocol: Optional filesystem protocol for accessing
        *index_path*. Overwrites the protocol parsed from *index_path*,
        if any.
    :param source_storage_options: Storage options for accessing *index_path*.
    :param cache_path: Path to local cache directory.
        Must be given, if file caching is desired.
    :param xarray_kwargs: Extra keyword arguments accepted by
        ``xarray.open_dataset``.
    :param _catalog: Catalog (mock) instance used for testing only.
        If given, all other arguments are ignored.
    """

    def __init__(self,
                 source_path: Optional[str] = None,
                 source_protocol: Optional[str] = None,
                 source_storage_options: Optional[Dict[str, Any]] = None,
                 cache_path: Optional[str] = None,
                 xarray_kwargs: Optional[Dict[str, Any]] = None,
                 _catalog: Optional[AbstractSmosCatalog] = None):
        if _catalog is None:
            self.catalog = SmosDirectCatalog(
                source_path=source_path,
                source_protocol=source_protocol,
                source_storage_options=source_storage_options,
                cache_path=cache_path,
                xarray_kwargs=xarray_kwargs
            )
        else:
            self.catalog = _catalog

    @cached_property
    def dgg(self) -> MultiLevelDataset:
        return new_dgg()

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
            return (DATASET_OPENER_ID,
                    ML_DATASET_OPENER_ID,
                    DATASET_ITERATOR_OPENER_ID)
        return f'{data_type.alias}:zarr:smos',

    def describe_data(self,
                      data_id: str,
                      data_type: DataTypeLike = None) -> DataDescriptor:
        self._assert_valid_data_id(data_id)
        data_type = self._assert_valid_data_type(data_type)
        # TODO (forman): implement descriptors.
        #   Implementation note: It should be possible to provide
        #   all/more required metadata statically from the DGG and
        #   other sources such as the SMOS Kerchunk index.
        lat_max = MAX_HEIGHT * MIN_PIXEL_SIZE / 2
        metadata = dict(
            bbox=[-180., -lat_max, 180., lat_max],
            spatial_res=MIN_PIXEL_SIZE,
            time_range=["2010-01-01", None],  # TODO (forman): adjust start!
        )
        if data_type.is_sub_type_of(MULTI_LEVEL_DATASET_TYPE):
            return MultiLevelDatasetDescriptor(data_id,
                                               num_levels=6,
                                               **metadata)
        else:
            return DatasetDescriptor(data_id, **metadata)

    def get_open_data_params_schema(
            self,
            data_id: str = None,
            opener_id: str = None
    ) -> JsonObjectSchema:
        if data_id is not None:
            self._assert_valid_data_id(data_id)
        self._assert_valid_opener_id(opener_id)
        return OPEN_PARAMS_SCHEMA

    def open_data(
            self,
            data_id: str,
            opener_id: str = None,
            **open_params
    ) -> Union[xr.Dataset, MultiLevelDataset, DatasetIterator]:
        OPEN_PARAMS_SCHEMA.validate_instance(open_params)
        self._assert_valid_data_id(data_id)
        product_type = data_id.rsplit('-', maxsplit=1)[-1]
        opener_id = self._assert_valid_opener_id(opener_id)
        data_type = DataType.normalize(opener_id.split(":")[0])

        time_range = open_params["time_range"]  # required
        l2_product_cache_size = open_params.get("l2_product_cache_size", 0)

        # TODO (forman): respect other parameter from open_params here

        dataset_records = self.catalog.find_datasets(product_type, time_range)
        if not dataset_records:
            raise ValueError(f"No SMOS datasets of type {product_type!r}"
                             f" found for time range {time_range!r}")
        dataset_paths = list(map(self.catalog.resolve_path,
                                 [path for path, _, _ in dataset_records]))
        time_ranges = [(start, stop) for _, start, stop in dataset_records]
        time_bounds = parse_time_ranges(time_ranges, is_compact=True)

        if data_type.is_sub_type_of(DATASET_ITERATOR_TYPE):
            return DatasetIterator(self.dgg,
                                   self.catalog.get_dataset_opener(),
                                   self.catalog.get_dataset_opener_kwargs(),
                                   dataset_paths,
                                   time_bounds)

        time_step_loader = SmosTimeStepLoader(
            self.dgg,
            self.catalog.get_dataset_opener(),
            self.catalog.get_dataset_opener_kwargs(),
            dataset_paths,
            l2_product_cache_size
        )

        ml_dataset = SmosL2Cube(
            self.dgg,
            data_id,
            time_bounds,
            time_step_loader,
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
        if opener_id not in (DATASET_OPENER_ID,
                             ML_DATASET_OPENER_ID,
                             DATASET_ITERATOR_OPENER_ID):
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
            data_type.is_sub_type_of(MULTI_LEVEL_DATASET_TYPE) or \
            data_type.is_sub_type_of(DATASET_ITERATOR_TYPE)
