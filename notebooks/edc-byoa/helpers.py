import logging
import os
import shutil
from typing import Iterator

import numpy as np
import pandas as pd
import xarray as xr

from zappend.api import Context
from zappend.api import SliceSource
from zappend.api import to_slice_factory


def generate_slices(
    smos_store,
    product_type: str,
    time_ranges: list[tuple[str, str]],
    agg_interval: str | None,
    res_level: int,
):
    for time_range in time_ranges:
        ds_iterator = smos_store.open_data(
            data_id=product_type,
            opener_id="dsiter:zarr:smos",
            time_range=time_range,
            res_level=res_level,
        )
        if not agg_interval:
            # If we have no interval, we deliver the slices as provided.
            yield from ds_iterator
        else:
            # Otherwise we deliver a slice source that creates the
            # mean of slices in ds_iterator.
            yield to_slice_factory(MeanSliceSource, ds_iterator, time_range)


class MeanSliceSource(SliceSource):
    def __init__(
        self, ctx: Context, ds_iterator: Iterator, time_range: tuple[str, str]
    ):
        super().__init__(ctx)
        self.ds_iterator = ds_iterator
        self.time_range = time_range
        temp_path = f"./temp-{'-'.join(time_range)}"
        self.temp_path = temp_path
        self.slice_path = temp_path + ".zarr"
        self.ds_mean = None
        self.logger = logging.getLogger("notebook")

    def get_dataset(self) -> xr.Dataset:
        ds_iterator = self.ds_iterator
        time_range = self.time_range
        temp_path = self.temp_path
        slice_path = self.slice_path
        logger = self.logger

        if not os.path.exists(temp_path):
            os.mkdir(temp_path)

        num_datasets = len(ds_iterator)
        temp_slice_paths = []
        for index, ds in enumerate(ds_iterator):
            temp_slice_path = f"{temp_path}/slice-{index}.nc"
            logger.info(
                f"Writing slice %d of %d to %s",
                index + 1,
                num_datasets,
                temp_slice_path,
            )
            ds.to_netcdf(temp_slice_path, mode="w")
            temp_slice_paths.append(temp_slice_path)

        ds = xr.open_mfdataset(temp_slice_paths, combine="nested", concat_dim="time")

        ds_mean = ds.mean("time")

        # ds_mean has no time dimension, so we re-introduce it
        ds_mean = ds_mean.expand_dims("time", axis=0)
        start, stop = pd.to_datetime(time_range)
        ds_mean.coords["time"] = xr.DataArray(
            np.array([start + (stop - start) / 2]),
            dims="time",
        )
        ds_mean.coords["time_bnds"] = xr.DataArray(
            np.array([[start, stop]]),
            dims=("time", "bnds"),
        )

        # Align encoding and attributes
        for var_name, var in ds.variables.items():
            mean_var = ds_mean.get(var_name)
            if mean_var is not None:
                mean_var.encoding.update(var.encoding)
                mean_var.attrs.update(var.attrs)

        logger.info(f"Writing mean slice to %s", slice_path)
        ds_mean.to_zarr(slice_path, mode="w", write_empty_chunks=False)
        ds_mean.close()
        ds_mean = None
        ds.close()
        ds = None

        logger.info(f"Removing temporary %s", temp_path)
        shutil.rmtree(temp_path, ignore_errors=True)

        self.ds_mean = xr.open_zarr(slice_path)
        return self.ds_mean

    def dispose(self):
        if self.ds_mean is not None:
            self.ds_mean.close()
            self.ds_mean = None
        self.logger.info(f"Removing temporary %s", self.slice_path)
        shutil.rmtree(self.slice_path, ignore_errors=True)


def get_time_ranges(time_range: str, agg_interval: str | None) -> list[tuple[str, str]]:
    one_sec = pd.Timedelta("1s")
    one_day = pd.Timedelta("1d")
    one_week = pd.Timedelta("1w")

    interval_td = pd.Timedelta(agg_interval) if agg_interval else one_day
    if interval_td < one_day:
        raise ValueError("agg_interval must not be less than a day")

    date_range = pd.to_datetime(time_range.split("/", maxsplit=1))
    if len(date_range) == 2:
        start_date, stop_date = date_range
    else:
        start_date, stop_date = date_range[0], date_range[0]
    dates = pd.date_range(start_date, stop_date + interval_td, freq=interval_td)

    def to_date_str(date):
        return date.strftime("%Y-%m-%d")

    return [
        (to_date_str(dates[i]), to_date_str(dates[i + 1] - one_sec))
        for i in range(len(dates) - 1)
    ]
