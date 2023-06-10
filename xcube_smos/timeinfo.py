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

from typing import Tuple, Sequence

import numpy as np
import xarray as xr

from xcube.util.assertions import assert_true


def parse_smos_time_ranges(l2_products: Sequence[xr.Dataset]) -> np.array:
    time_ranges = [get_time_range(l2_product) for l2_product in l2_products]
    return parse_time_ranges(time_ranges)


def parse_time_ranges(time_ranges: Sequence[Tuple[str, str]],
                      is_compact: bool = False) -> np.array:
    if is_compact:
        time_ranges = compact_to_iso_time_ranges(time_ranges)
    return np.array(time_ranges, dtype="datetime64[us]")


def compact_to_iso_time_ranges(time_ranges: Sequence[Tuple[str, str]]) \
        -> Sequence[Tuple[str, str]]:
    return [(compact_to_iso_time(s), compact_to_iso_time(e))
            for s, e in time_ranges]


def compact_to_iso_time(compact_time: str) -> str:
    assert_true(len(compact_time) == 14, message='invalid compact time')
    return (f"{compact_time[0:4]}-"
            f"{compact_time[4:6]}-"
            f"{compact_time[6:8]}T"
            f"{compact_time[8:10]}:"
            f"{compact_time[10:12]}:"
            f"{compact_time[12:14]}")


def get_time_range(l2_product: xr.Dataset) -> Tuple[str, str]:
    start = get_raw_time(l2_product, 'FH:Validity_Period:Validity_Start')
    stop = get_raw_time(l2_product, 'FH:Validity_Period:Validity_Stop')
    return start, stop


def get_raw_time(l2_product: xr.Dataset, attr_name: str) -> str:
    time_str = l2_product.attrs.get(attr_name)
    if not time_str:
        raise ValueError(f'missing attribute {attr_name!r}')
    return time_str[4:] if time_str.startswith("UTC=") else time_str
