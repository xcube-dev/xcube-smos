import warnings
from functools import cached_property
from typing import Hashable

import numpy as np
import xarray as xr

from .dgg import SmosDiscreteGlobalGrid
from .l2index import map_seqnum_to_l2_index
from .l2prod import map_l2_values


class SmosL2Mapper:

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

    # 1 Time Step x 1 L2 Product x N Vars x N Levels

    # Note: make sure l2_product is opened on distributed processes
    def __init__(self, level: int, l2_product: xr.Dataset):
        self._l2_product = l2_product

        global_seqnum = self.dgg.get_dataset(level).seqnum.values

        grid_point_id = self._l2_product.Grid_Point_ID.values
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

        # TODO (forman): numba!
        l2_missing_index = len(grid_point_id)
        l2_seqnum_to_index = np.full(SmosDiscreteGlobalGrid.MAX_SEQNUM + 1,
                                     l2_missing_index,
                                     dtype=np.uint32)
        for i in range(len(l2_seqnum)):
            j = l2_seqnum[i]
            l2_seqnum_to_index[j] = i

        self._l2_missing_index: int = l2_missing_index

        self._global_l2_index = map_seqnum_to_l2_index(
            global_seqnum,
            l2_seqnum_to_index
        )

        self._l2_fill_values = {}
        for l2_var_name, l2_var in l2_product.data_vars.items()
            fill_value = l2_var.attrs.get("_FillValue")
            if fill_value is None:
                if np.issubdtype(l2_var.dtype, np.floating):
                    fill_value = float(np.nan)
                else:
                    fill_value = 0
                warnings.warn(f"Variable {l2_var_name!r}"
                              f" is missing a fill value,"
                              f" using {fill_value} instead.")
            self._l2_fill_values[l2_var_name] = fill_value

    def map_l2_var(self, l2_var_name: Hashable) -> np.ndarray:
        l2_var = self._l2_product[l2_var_name]
        fill_value = self._l2_fill_values[l2_var_name]
        return map_l2_values(self._global_l2_index,
                             l2_var.values,
                             self._l2_missing_index,
                             fill_value)

    @cached_property
    def dgg(self) -> SmosDiscreteGlobalGrid:
        return SmosDiscreteGlobalGrid(load=True, level0=1)

