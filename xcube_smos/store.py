from typing import Iterator, Any, Tuple, Container, Union, Dict, Optional
import xarray as xr
from xcube.core.store import DATASET_TYPE
from xcube.core.store import DataDescriptor
from xcube.core.store import DataStore
from xcube.core.store import DataType
from xcube.core.store import DataTypeLike
from xcube.core.store import DatasetDescriptor
from xcube.core.store import MULTI_LEVEL_DATASET_TYPE
from xcube.core.store import MultiLevelDatasetDescriptor
from xcube.util.jsonschema import JsonObjectSchema
from .schema import OPEN_PARAMS_SCHEMA
from .schema import STORE_PARAMS_SCHEMA
from .catalog import SmosCatalog

_DATASETS = {
    'SMOS-L2-SM': {
        'title': 'SMOS Level-2 Soil Moisture'
    },
    'SMOS-L2-OS': {
        'title': 'SMOS Level-2 Ocean Salinity'
    }
}


class SmosStore(DataStore):
    def __init__(self,
                 index_urlpath: Optional[str] = None,
                 index_options: Optional[Dict[str, Any]] = None):
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
            return 'dataset:zarr:smos', 'mldataset:zarr:smos'
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

    def open_data(self,
                  data_id: str,
                  opener_id: str = None,
                  **open_params) -> Any:
        OPEN_PARAMS_SCHEMA.validate_instance(open_params)
        self._assert_valid_data_id(data_id)
        self._assert_valid_opener_id(opener_id)
        catalog = SmosCatalog(index_urlpath=self._index_urlpath,
                              index_options=self._index_options)
        product_type = data_id.rsplit('-', maxsplit=1)[-1]
        time_range = open_params["time_range"]
        index_paths = catalog.find_files(product_type, time_range)
        for index_path in index_paths:
            index_filename = index_path.rsplit("/", maxsplit=1)[-1]
            index_json_path = f"{index_path}/{index_filename}.nc.json"
            ds = xr.open_dataset(
                "reference://",
                engine="zarr",
                backend_kwargs={
                    "storage_options": {
                        "fo": index_json_path,
                        "remote_protocol": "s3",
                        "remote_options": catalog.s3_options
                    },
                    "consolidated": False
                }
            )
            # TODO (forman): continue here
            print(ds)

    @classmethod
    def _assert_valid_data_id(cls, data_id: str):
        if data_id not in _DATASETS:
            raise ValueError(f'Unknown dataset identifier {data_id!r}')

    def _assert_valid_opener_id(self, opener_id: Optional[str]):
        if opener_id is not None \
                and opener_id not in self.get_data_opener_ids():
            raise ValueError(f'Invalid opener identifier {opener_id!r}')

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
