import warnings
from functools import cached_property
from typing import Dict, Any, Callable, List, Optional
from typing import Hashable, Union

import numba as nb
import numpy as np
import xarray as xr

from xcube.core.gridmapping import GridMapping
from xcube.core.mldataset import LazyMultiLevelDataset
from xcube.core.zarrstore import GenericArray
from xcube.core.zarrstore import GenericZarrStore
from .dgg import SmosDiscreteGlobalGrid

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

DATASETS = {
    "SMOS-L2C-OS": {
        "Mean_acq_time",
        "SSS_corr",
        "Sigma_SSS_corr",
        "SSS_anom",
        "Sigma_SSS_anom",
        "Dg_chi2_corr",
        "Dg_quality_SSS_corr",
        "Dg_quality_SSS_anom",
        "Coast_distance",
        "Dg_RFI_X",
        "Dg_RFI_Y",
        "X_swath",
    },
    "SMOS-L2C-SM": {
        "Mean_acq_time",
        "Soil_Moisture",
        "Soil_Moisture_DQX",
        "Chi_2",
        "Chi_2_P",
        "N_RFI_X",
        "N_RFI_Y",
        "RFI_Prob",
        "X_swath",
    }
}


def new_dgg():
    """Create a 4-level DGG that is not chunked"""
    return SmosDiscreteGlobalGrid(level0=1, load=True)


class SmosGlobalL2Cube(LazyMultiLevelDataset):
    """
    A multi-level dataset that represents a SMOS Level 2C data cube
    using a geographic projection.

    :param dataset_id: Dataset identifier.
    :param time_bounds: Time bounds with dimensions (time, 2), where
        time has the same length as *dataset_paths*.
    :param time_step_loader: Serializable class that can load data
        for time steps.
    """

    def __init__(self,
                 dataset_id: str,
                 time_bounds: np.array,
                 time_step_loader: "TimeStepLoader"):
        super().__init__()
        self.dataset_id = dataset_id
        self.time_bounds = time_bounds
        self.time_step_loader = time_step_loader

    @cached_property
    def dgg(self) -> SmosDiscreteGlobalGrid:
        return new_dgg()

    def _get_num_levels_lazily(self) -> int:
        return self.dgg.num_levels

    def _get_grid_mapping_lazily(self) -> GridMapping:
        return self.dgg.grid_mapping

    def _get_dataset_lazily(self,
                            level: int,
                            parameters: Dict[str, Any]) -> xr.Dataset:
        width, height, spatial_res = self.dgg.get_level_geom(level)

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
                fill_value=self._sanitize_attr_value(
                    l2_product.l2_fill_values[var_name]
                ),
                chunk_encoding="ndarray",
                attrs=self._sanitize_attrs(var.attrs)
            )
            for var_name, var in l2_product.l2_dataset.data_vars.items()
            if var_name in DATASETS[self.dataset_id]
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
                data=np.linspace(-180 + spatial_res / 2,
                                 +180 - spatial_res / 2,
                                 width),
                attrs={
                    "long_name": "longitude",
                    "standard_name": "longitude",
                    "units": "degrees_east",
                },
            ),
            GenericArray(
                name="lat",
                dims="lat",
                data=np.linspace(+height * spatial_res / 2 - spatial_res / 2,
                                 -height * spatial_res / 2 + spatial_res / 2,
                                 height),
                attrs={
                    "long_name": "latitude",
                    "standard_name": "latitude",
                    "units": "degrees_north",
                }
            ),
            *global_l2_vars,
            attrs={
                "coordinates": "lon lat time time_bnds",
            }
        )

        dataset = xr.open_zarr(zarr_store)
        dataset.zarr_store.set(zarr_store)
        return dataset

    @classmethod
    def _sanitize_attrs(cls, attrs: Dict[str, Any]):
        return {k: cls._sanitize_attr_value(v) for k, v in attrs.items()}

    @classmethod
    def _sanitize_attr_value(cls, value):
        if hasattr(value, 'dtype'):
            if np.issubdtype(value.dtype, np.floating):
                return float(value)
            if np.issubdtype(value.dtype, np.integer):
                return int(value)
        return value


class DggMixin:
    """A mixin that provides a DGG either from an attribute "_dgg"
    or generates a new, cached one.
    """

    @property
    def dgg(self):
        if self._dgg is not None:
            return self._dgg
        self._dgg = new_dgg()
        return self._dgg

    def __getstate__(self):
        state = self.__dict__.copy()
        state.pop('_dgg', None)
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._dgg = None


class TimeStepLoader(DggMixin):
    def __init__(self,
                 dataset_paths: List[str],
                 dataset_opener: Callable,
                 storage_options: Optional[Dict[str, Any]],
                 dgg: Optional[SmosDiscreteGlobalGrid] = None):
        self.dataset_paths = dataset_paths
        self.dataset_opener = dataset_opener
        self.storage_options = storage_options
        self._dgg = dgg

    def load_time_step(self,
                       level: int,
                       array_info: Dict[str, Any],
                       chunk_info: Dict[str, Any]) -> np.ndarray:
        var_name = array_info["name"]
        time_idx, lat_idx, lon_idx = chunk_info["index"]
        assert lat_idx == 0 and lon_idx == 0, \
            "should not be chunked in spatial dimensions"
        l2_product = self.load_l2_product(time_idx)
        global_l2_product = l2_product.get_global_s2_product(level)
        return global_l2_product.map_l2_var(var_name)

    # @lru_cache()
    def load_l2_product(self, time_idx: int) -> 'SmosL2Product':
        """Load the SMOS L2 product for the given *time_idx*.
        LRU-cached access w.r.t. *time_idx*.
        """
        dataset_path = self.dataset_paths[time_idx]
        l2_dataset = self.dataset_opener(dataset_path, self.storage_options)
        l2_dataset = l2_dataset.chunk()  # Wrap numpy arrays into dask arrays
        return SmosL2Product(l2_dataset,
                             dgg=self._dgg if self._dgg is not None else None)


class SmosL2Product(DggMixin):

    def __init__(self,
                 l2_dataset: xr.Dataset,
                 dgg: Optional[SmosDiscreteGlobalGrid] = None):

        grid_point_id = l2_dataset.Grid_Point_ID.values
        l2_seqnum = SmosDiscreteGlobalGrid.grid_point_id_to_seqnum(
            grid_point_id
        )
        l2_min_seqnum = np.min(l2_seqnum)
        l2_max_seqnum = np.max(l2_seqnum)
        if not (SmosDiscreteGlobalGrid.MIN_SEQNUM
                <= l2_min_seqnum
                <= SmosDiscreteGlobalGrid.MAX_SEQNUM):
            raise ValueError()
        if not (SmosDiscreteGlobalGrid.MIN_SEQNUM
                <= l2_max_seqnum
                <= SmosDiscreteGlobalGrid.MAX_SEQNUM):
            raise ValueError()

        l2_missing_index = len(grid_point_id)
        l2_seqnum_to_index = seqnum_to_index(
            l2_seqnum,
            SmosDiscreteGlobalGrid.MAX_SEQNUM + 1,
            l2_missing_index
        )

        l2_fill_values = {}
        for l2_var_name, l2_var in l2_dataset.data_vars.items():
            fill_value = l2_var.attrs.get("_FillValue")
            if fill_value is None:
                if np.issubdtype(l2_var.dtype, np.floating):
                    fill_value = float(np.nan)
                else:
                    fill_value = 0
                warnings.warn(f"Variable {l2_var_name!r}"
                              f" is missing a fill value,"
                              f" using {fill_value} instead.")
            l2_fill_values[l2_var_name] = fill_value

        self.l2_dataset = l2_dataset
        self.l2_fill_values = l2_fill_values
        self.l2_seqnum_to_index = l2_seqnum_to_index
        self.l2_missing_index = l2_missing_index
        self._dgg = dgg

    #@lru_cache()
    def get_global_s2_product(self, level: int):
        """Get the global, mapped L2 product for given *level*
        LRU-cached access w.r.t. *level*.
        """
        dgg_dataset = self.dgg.get_dataset(level)
        seqnum = dgg_dataset.seqnum
        return SmosGlobalL2Product(self, seqnum.values)


class SmosGlobalL2Product:
    def __init__(self, l2_product: SmosL2Product, global_seqnum: np.ndarray):
        self.l2_product = l2_product
        self.global_l2_index = map_seqnum_to_l2_index(
            global_seqnum,
            self.l2_product.l2_seqnum_to_index
        )

    # Could make this LRU-cached, but most likely every L2 variable will
    # only be read once.
    # @lru_cache()
    def map_l2_var(self, l2_var_name: Hashable) -> np.ndarray:
        """Reproject L2 variable to global grid.

        :param l2_var_name: The L2 variable name
        :return: 3D array of shape (1, height, width)
        """
        l2_var = self.l2_product.l2_dataset[l2_var_name]
        l2_fill_value = self.l2_product.l2_fill_values[l2_var_name]
        l2_missing_index = self.l2_product.l2_missing_index
        mapped_l2_values = map_l2_values(self.global_l2_index,
                                         l2_var.values,  # reads L2 variable
                                         l2_missing_index,
                                         l2_fill_value)
        return np.expand_dims(mapped_l2_values, axis=0)


@nb.jit(nopython=True)
def seqnum_to_index(seqnum: np.ndarray,
                    size: int,
                    fill_value: Union[int, float]) -> np.ndarray:
    index = np.full(size, fill_value, dtype=np.uint32)
    for i in range(len(seqnum)):
        j = seqnum[i]
        index[j] = i
    return index


@nb.jit(nopython=True)
def map_seqnum_to_l2_index(seqnum_values_2d: np.ndarray,
                           seqnum_to_index: np.ndarray) -> np.ndarray:
    seqnum_values_1d = seqnum_values_2d.flatten()
    index_values_1d = np.zeros(seqnum_values_1d.size,
                               dtype=np.uint32)
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
def map_l2_values(index_2d: np.ndarray,
                  var_data: np.ndarray,
                  missing_index: int,
                  fill_value: Union[int, float]) -> np.ndarray:
    if index_2d.size == 0:
        return index_2d.astype(var_data.dtype)
    l2_mask_2d = index_2d != missing_index
    l2_index_ok = np.where(l2_mask_2d, index_2d, 0)
    mapped_l2_value = var_data[l2_index_ok]
    return np.where(l2_mask_2d, mapped_l2_value, fill_value)
