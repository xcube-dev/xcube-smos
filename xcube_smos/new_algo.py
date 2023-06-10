import warnings
from functools import cached_property, lru_cache
from typing import Dict, Any, Callable, Sequence
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

DatasetOpener = Callable[[str], xr.Dataset]


class SmosGlobalL2Cube(LazyMultiLevelDataset):
    """
    A multi-level dataset that represents a SMOS Level 2C data cube
    using a geographic projection.

    :param dataset_id: Dataset identifier.
    :param dataset_paths: Paths to SMOS Level 2 datasets.
    :param dataset_opener: Function that can open SMOS L2 datasets.
    :param time_bounds: Time bounds with dimensions (time, 2), where
        time has the same length as *dataset_paths*.
    """

    def __init__(self,
                 dataset_id: str,
                 dataset_paths: Sequence[str],
                 dataset_opener: DatasetOpener,
                 time_bounds: np.array):
        super().__init__()
        self.dataset_id = dataset_id
        self.dataset_paths = dataset_paths
        self.dataset_opener = dataset_opener
        self.time_bounds = time_bounds
        self.time = time_bounds[:, 0] + (
                time_bounds[:, 1] - time_bounds[:, 0]
        ) / 2
        self.dgg = SmosDiscreteGlobalGrid(level0=1)

    def _get_num_levels_lazily(self) -> int:
        return self.dgg.num_levels

    def _get_grid_mapping_lazily(self) -> GridMapping:
        return self.dgg.grid_mapping

    def _get_dataset_lazily(self,
                            level: int,
                            parameters: Dict[str, Any]) -> xr.Dataset:
        width, height, spatial_res = self.dgg.get_level_geom(level)

        l2_product = self.load_l2_product(0)

        global_l2_vars = [
            GenericArray(
                name=var_name,
                dtype=var.dtype.str,
                dims=("time", "lat", "lon"),
                shape=(len(self.time), height, width),
                chunks=(1, height, width),
                get_data=self.load_time_step,
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
                data=self.time,
                attrs={
                    "long_name": "time",
                    "standard_name": "time",
                    "bounds": "time_bnds",
                },
            ),
            GenericArray(
                name="time_bnds",
                dims=("time", "bnds"),
                data=self.time_bounds,
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

    def load_time_step(self,
                       level: int,
                       array_info: Dict[str, Any],
                       chunk_info: Dict[str, Any]):
        var_name = array_info["name"]
        time_idx, lat_idx, lon_idx = chunk_info["index"]
        assert lat_idx == 0 and lon_idx == 0, \
            "should not be chunked in spatial dimensions"
        l2_product = self.load_l2_product(time_idx)
        global_l2_product = l2_product.get_global_s2_product(level)
        return global_l2_product.map_l2_var(var_name)

    @lru_cache()
    def load_l2_product(self, time_idx: int) -> 'SmosL2Product':
        """Load the SMOS L2 product for the given *time_idx*.
        LRU-cached access.
        """
        dataset_path = self.dataset_paths[time_idx]
        l2_dataset = self.dataset_opener(dataset_path)
        l2_dataset = l2_dataset.chunk()  # Wrap numpy arrays into dask arrays
        return SmosL2Product(l2_dataset)

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


class SmosL2Product:

    def __init__(self, l2_dataset: xr.Dataset):

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

    @lru_cache()
    def get_global_s2_product(self, level: int):
        """Get the global, mapped L2 product for given *level*
        LRU-cached access.
        """
        return SmosGlobalL2Product(self, level)


class SmosGlobalL2Product:
    def __init__(self, l2_product: SmosL2Product, level: int):
        self.l2_product = l2_product
        self.global_l2_index = map_seqnum_to_l2_index(
            self.dgg.get_dataset(level).seqnum.values,
            self.l2_product.l2_seqnum_to_index
        )

    @cached_property
    def dgg(self) -> SmosDiscreteGlobalGrid:
        return SmosDiscreteGlobalGrid(load=True, level0=1)

    def map_l2_var(self, l2_var_name: Hashable) -> np.ndarray:
        l2_var = self.l2_product.l2_dataset[l2_var_name]
        l2_fill_value = self.l2_product.l2_fill_values[l2_var_name]
        l2_missing_index = self.l2_product.l2_missing_index
        mapped_l2_values = map_l2_values(self.global_l2_index,
                                         l2_var.values,
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
    # print("Computing ", l2_index_block.shape)
    if index_2d.size == 0:
        return index_2d.astype(var_data.dtype)
    l2_index_ok = np.where(index_2d != missing_index, index_2d, 0)
    mapped_l2_value = var_data[l2_index_ok]
    return np.where(index_2d != missing_index, mapped_l2_value, fill_value)
