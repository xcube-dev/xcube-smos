import logging
import warnings
from typing import Dict, Any, Callable, List
from typing import Hashable, Union

import numba as nb
import numpy as np
import xarray as xr
from xcube.core.gridmapping import GridMapping
from xcube.core.mldataset import LazyMultiLevelDataset
from xcube.core.mldataset import MultiLevelDataset
from xcube.core.zarrstore import GenericArray
from xcube.core.zarrstore import GenericZarrStore

from .dgg import SmosDiscreteGlobalGrid
from .newdgg import MAX_HEIGHT
from .newdgg import MAX_WIDTH
from .newdgg import MIN_PIXEL_SIZE
from .newdgg import new_dgg
from ..constants import OS_VAR_NAMES
from ..constants import SM_VAR_NAMES
from ..utils import LruCache
from ..utils import NotSerializable

LOG = logging.getLogger("xcube-smos")

# Cube visualisation:
#   - chunks: spatial DGG chunking, time=1
#   - load L2 data per level and time step
#   - parallelize computation of spatial tiles per var
#   - l2 var data needed to compute spatial tiles of a time step of a var
#   - cache l2 var data for time step tiles
#
# Cube writing:
#   - no spatial chunking, time=1
#   - load L2 data per level and time step
#   - parallelize computation of time steps for all vars at once
#   - l2 var data only needed to compute entire time steps of a var
#   - don't cache l2 var data, used only temporarily

DATASET_VAR_NAMES = {"SMOS-L2C-OS": OS_VAR_NAMES, "SMOS-L2C-SM": SM_VAR_NAMES}


class SmosL2Cube(NotSerializable, LazyMultiLevelDataset):
    """
    A multi-level dataset that represents a SMOS Level 2C data cube
    using a geographic projection.

    :param dataset_id: Dataset identifier.
    :param time_bounds: Time bounds with dimensions (time, 2), where
        time has the same length as *dataset_paths*.
    :param time_step_loader: Serializable class that can load data
        for time steps.
    """

    def __init__(
        self,
        dgg: MultiLevelDataset,
        dataset_id: str,
        time_bounds: np.array,
        bbox: tuple[float, float, float, float] | None,
        time_step_loader: "SmosTimeStepLoader",
    ):
        super().__init__()
        self.dgg = dgg
        self.dataset_id = dataset_id
        self.time_bounds = time_bounds
        self.bbox = bbox
        self.time_step_loader = time_step_loader

    def _get_num_levels_lazily(self) -> int:
        return self.dgg.num_levels

    def _get_grid_mapping_lazily(self) -> GridMapping:
        if self.bbox is None:
            return self.dgg.grid_mapping
        dataset = self.dgg.get_dataset(0)
        dataset_subset = self._get_dataset_spatial_subset(dataset)
        return GridMapping.from_dataset(dataset_subset)

    def _get_dataset_lazily(self, level: int, parameters: Dict[str, Any]) -> xr.Dataset:
        scale = 1 << level
        width = MAX_WIDTH // scale
        height = MAX_HEIGHT // scale
        spatial_res = MIN_PIXEL_SIZE * scale

        # Load prototype product (cached)
        l2_product = self.time_step_loader.load_l2_product(0)

        time_bounds = self.time_bounds
        time_start = time_bounds[:, 0]
        time_stop = time_bounds[:, 1]
        time = time_start + (time_stop - time_start) / 2

        # Note, it is important that the get_data function and its parameters
        # get_data_params is serializable and slim when serialized.
        # Therefore, we pack the stuff that we need to fetch L2 data into
        # a separate, serializable data class TimeStepLoader.

        global_l2_vars = [
            GenericArray(
                name=var_name,
                dtype=var.dtype.str,
                dims=("time", "lat", "lon"),
                shape=(len(time), height, width),
                chunks=(1, height, width),
                get_data=self.time_step_loader.load_time_step,
                get_data_params=dict(level=level),
                fill_value=_sanitize_attr_value(l2_product.l2_fill_values[var_name]),
                chunk_encoding="ndarray",
                attrs=_sanitize_attrs(var.attrs),
            )
            for var_name, var in l2_product.l2_dataset.data_vars.items()
            if var_name in DATASET_VAR_NAMES[self.dataset_id]
        ]

        zarr_store = GenericZarrStore(
            GenericArray(
                name="time",
                dims="time",
                data=time,
                attrs={
                    "long_name": "time",
                    "standard_name": "time",
                    "bounds": "time_bnds",
                },
            ),
            GenericArray(
                name="time_bnds",
                dims=("time", "bnds"),
                data=time_bounds,
            ),
            GenericArray(
                name="lon",
                dims="lon",
                data=np.linspace(-180 + spatial_res / 2, +180 - spatial_res / 2, width),
                attrs={
                    "long_name": "longitude",
                    "standard_name": "longitude",
                    "units": "degrees_east",
                },
            ),
            GenericArray(
                name="lat",
                dims="lat",
                data=np.linspace(
                    +height * spatial_res / 2 - spatial_res / 2,
                    -height * spatial_res / 2 + spatial_res / 2,
                    height,
                ),
                attrs={
                    "long_name": "latitude",
                    "standard_name": "latitude",
                    "units": "degrees_north",
                },
            ),
            *global_l2_vars,
            attrs={
                "coordinates": "lon lat time time_bnds",
            },
        )

        dataset = xr.open_zarr(zarr_store)
        dataset.zarr_store.set(zarr_store)
        return (
            dataset if self.bbox is None else self._get_dataset_spatial_subset(dataset)
        )

    def _get_dataset_spatial_subset(self, dataset: xr.Dataset) -> xr.Dataset:
        return get_dataset_spatial_subset(dataset, self.bbox, self.dgg.grid_mapping)


def get_dataset_spatial_subset(
    dataset: xr.Dataset, bbox: tuple[float, float, float, float], global_gm: GridMapping
) -> xr.Dataset:
    assert isinstance(bbox, (tuple, list))
    assert len(bbox) == 4
    x_min, y_min, x_max, y_max = bbox
    eps = MIN_PIXEL_SIZE
    if (
        x_min < global_gm.x_min + eps
        and x_max > global_gm.x_max - eps
        and y_min < global_gm.y_min + eps
        and y_max > global_gm.y_max - eps
    ):
        return dataset
    return dataset.sel(lon=slice(x_min, x_max), lat=slice(y_max, y_min))


class SmosTimeStepLoader:
    """
    Helper class for loading single SMOS time steps.
    Instances must be serializable because the :meth:load_time_step
    is executed on dask workers.

    For serialization, we exclude the DGG.
    When deserialized, we load a new DGG.

    :param dgg: SMOS discrete global grid.
    :param dataset_opener: Function that can open one of *dataset_paths*.
    :param dataset_opener_kwargs: Keyword arguments
        passed to *dataset_opener*.
    :param dataset_paths: SMOS L2 dataset paths (from catalog).
    :param l2_product_cache_size: Product cache size for L2 products.
    """

    def __init__(
        self,
        dgg: MultiLevelDataset,
        dataset_opener: Callable,
        dataset_opener_kwargs: Dict[str, Any],
        dataset_paths: List[str],
        l2_product_cache_size: int,
    ):
        self.dgg = dgg
        self.dataset_paths = dataset_paths
        self.dataset_opener = dataset_opener
        self.dataset_opener_kwargs = dataset_opener_kwargs or {}
        self.l2_product_cache_size = l2_product_cache_size
        self.l2_product_cache = self.new_l2_product_cache()

    def new_l2_product_cache(self):
        return LruCache[int, SmosL2Product](
            max_size=self.l2_product_cache_size, dispose_value=self.dispose_l2_product
        )

    @classmethod
    def dispose_l2_product(cls, l2_product: "SmosL2Product"):
        l2_product.dispose()

    def __getstate__(self) -> Dict[str, Any]:
        LOG.debug("Serializing {}", self._class_name)

        state = self.__dict__.copy()
        del state["l2_product_cache"]
        del state["dgg"]

        return state

    def __setstate__(self, state: Dict[str, Any]):
        LOG.debug("Deserializing {}", self._class_name)

        self.__dict__.update(state)

        self.dgg = new_dgg()
        self.l2_product_cache = self.new_l2_product_cache()

    @property
    def _class_name(self):
        return self.__class__.__name__

    def load_time_step(
        self, level: int, array_info: Dict[str, Any], chunk_info: Dict[str, Any]
    ) -> np.ndarray:
        var_name = array_info["name"]
        time_idx, lat_idx, lon_idx = chunk_info["index"]
        assert (
            lat_idx == 0 and lon_idx == 0
        ), "should not be chunked in spatial dimensions"
        l2_product = self.load_l2_product(time_idx)
        mapped_l2_product = l2_product.get_mapped_s2_product(level)
        return mapped_l2_product.map_l2_var(var_name)

    def load_l2_product(self, time_idx: int) -> "SmosL2Product":
        """Load the SMOS L2 product for the given *time_idx*."""
        l2_product = self.l2_product_cache.get(time_idx)
        if l2_product is not None:
            return l2_product
        dataset_path = self.dataset_paths[time_idx]
        l2_dataset = self.dataset_opener(dataset_path, **self.dataset_opener_kwargs)
        l2_dataset = l2_dataset.chunk()  # Wrap numpy arrays into dask arrays
        LOG.debug("Opening L2 product %s for time index %d", dataset_path, time_idx)
        l2_product = SmosL2Product(self.dgg, l2_dataset)
        self.l2_product_cache.put(time_idx, l2_product)
        return l2_product


class SmosL2Product:
    def __init__(self, dgg: MultiLevelDataset, l2_dataset: xr.Dataset):
        grid_point_id = l2_dataset.Grid_Point_ID.values
        l2_seqnum = SmosDiscreteGlobalGrid.grid_point_id_to_seqnum(grid_point_id)
        l2_min_seqnum = np.min(l2_seqnum)
        l2_max_seqnum = np.max(l2_seqnum)
        if not (
            SmosDiscreteGlobalGrid.MIN_SEQNUM
            <= l2_min_seqnum
            <= SmosDiscreteGlobalGrid.MAX_SEQNUM
        ):
            raise ValueError("internal error: min. seqnum out of valid range")
        if not (
            SmosDiscreteGlobalGrid.MIN_SEQNUM
            <= l2_max_seqnum
            <= SmosDiscreteGlobalGrid.MAX_SEQNUM
        ):
            raise ValueError("internal error: max. seqnum out of valid range")

        l2_missing_index = len(grid_point_id)
        l2_seqnum_to_index = seqnum_to_index(
            l2_seqnum, SmosDiscreteGlobalGrid.MAX_SEQNUM + 1, l2_missing_index
        )

        l2_fill_values = {}
        for l2_var_name, l2_var in l2_dataset.data_vars.items():
            fill_value = l2_var.attrs.get("_FillValue")
            if fill_value is None:
                if np.issubdtype(l2_var.dtype, np.floating):
                    fill_value = float(np.nan)
                else:
                    fill_value = 0
                warnings.warn(
                    f"Variable {l2_var_name!r}"
                    f" is missing a fill value,"
                    f" using {fill_value} instead."
                )
            l2_fill_values[l2_var_name] = fill_value

        self.dgg = dgg
        self.l2_dataset = l2_dataset
        self.l2_fill_values = l2_fill_values
        self.l2_seqnum_to_index = l2_seqnum_to_index
        self.l2_missing_index = l2_missing_index
        self.mapped_l2_product_cache = LruCache[int, SmosMappedL2Product](
            max_size=dgg.num_levels, dispose_value=self.dispose_mapped_l2_product
        )

    @classmethod
    def dispose_mapped_l2_product(cls, mapped_l2_product: "SmosMappedL2Product"):
        mapped_l2_product.dispose()

    def dispose(self):
        self.l2_dataset.close()
        self.mapped_l2_product_cache.clear()

    def get_mapped_s2_product(self, level: int) -> "SmosMappedL2Product":
        """Get the global, mapped L2 product for given *level*
        LRU-cached access w.r.t. *level*.
        """
        mapped_l2_product = self.mapped_l2_product_cache.get(level)
        if mapped_l2_product is not None:
            return mapped_l2_product
        dgg_dataset = self.dgg.get_dataset(level)
        seqnum = dgg_dataset.seqnum
        # from dask.distributed import print
        # print(f'creating global L2 product for level={level}', flush=True)
        mapped_l2_product = SmosMappedL2Product(self, seqnum.values)
        self.mapped_l2_product_cache.put(level, mapped_l2_product)
        return mapped_l2_product


class SmosMappedL2Product:
    def __init__(self, l2_product: SmosL2Product, mapped_seqnum: np.ndarray):
        self.l2_product = l2_product
        self.mapped_l2_index = map_seqnum_to_l2_index(
            mapped_seqnum, self.l2_product.l2_seqnum_to_index
        )
        # We could make the result LRU-cached with *l2_var_name* as key,
        # but most likely every L2 variable will only be read once, when
        # we write SMOS data cubes. This would look different when
        # visualising dynamic SMOS data cubes. Therefore, we set max_size=0.
        self.mapped_l2_values_cache = LruCache[Hashable, np.ndarray](max_size=0)

    def dispose(self):
        self.mapped_l2_values_cache.clear()

    def map_l2_var(self, l2_var_name: Hashable) -> np.ndarray:
        """Reproject L2 variable to global grid.

        :param l2_var_name: The L2 variable name
        :return: 3D array of shape (1, height, width)
        """
        mapped_l2_values = self.mapped_l2_values_cache.get(l2_var_name)
        if mapped_l2_values is not None:
            return mapped_l2_values
        l2_var = self.l2_product.l2_dataset[l2_var_name]
        l2_values = l2_var.values  # effectively read data from L2 variable
        l2_fill_value = self.l2_product.l2_fill_values[l2_var_name]
        l2_missing_index = self.l2_product.l2_missing_index
        mapped_l2_values = map_l2_values(
            self.mapped_l2_index, l2_values, l2_missing_index, l2_fill_value
        )
        mapped_l2_values = np.expand_dims(mapped_l2_values, axis=0)
        self.mapped_l2_values_cache.put(l2_var_name, mapped_l2_values)
        return mapped_l2_values


@nb.jit(nopython=True)
def seqnum_to_index(
    seqnum: np.ndarray, size: int, fill_value: Union[int, float]
) -> np.ndarray:
    index = np.full(size, fill_value, dtype=np.uint32)
    for i in range(len(seqnum)):
        j = seqnum[i]
        index[j] = i
    return index


@nb.jit(nopython=True)
def map_seqnum_to_l2_index(
    seqnum_values_2d: np.ndarray, seqnum_to_index: np.ndarray
) -> np.ndarray:
    seqnum_values_1d = seqnum_values_2d.flatten()
    index_values_1d = np.zeros(seqnum_values_1d.size, dtype=np.uint32)
    for i in range(index_values_1d.size):
        seqnum = seqnum_values_1d[i]
        index_values_1d[i] = seqnum_to_index[seqnum]
    return index_values_1d.reshape(seqnum_values_2d.shape)


# TODO (forman): fix new numba error
#   No implementation of function Function(<built-in function getitem>)
#   found for signature:
#   >>> getitem(array(float32, 1d, C), array(int64, 2d, C))
#
# @nb.jit(nopython=True)
def map_l2_values(
    index_2d: np.ndarray,
    var_data: np.ndarray,
    missing_index: int,
    fill_value: Union[int, float],
) -> np.ndarray:
    if index_2d.size == 0:
        return index_2d.astype(var_data.dtype)
    l2_mask_2d = index_2d != missing_index
    l2_index_ok = np.where(l2_mask_2d, index_2d, 0)
    mapped_l2_value = var_data[l2_index_ok]
    return np.where(l2_mask_2d, mapped_l2_value, fill_value)


def _sanitize_attrs(attrs: Dict[str, Any]):
    return {k: _sanitize_attr_value(v) for k, v in attrs.items()}


def _sanitize_attr_value(value):
    if hasattr(value, "dtype"):
        if np.issubdtype(value.dtype, np.floating):
            return float(value)
        if np.issubdtype(value.dtype, np.integer):
            return int(value)
    return value
