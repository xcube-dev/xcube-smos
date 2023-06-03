import unittest

import numpy as np
import numpy.testing
import pytest
import xarray as xr

from xcube_smos.timeinfo import parse_smos_time_ranges


class TimeInfoTest(unittest.TestCase):

    # noinspection PyMethodMayBeStatic
    def test_parse_smos_time_ranges(self):
        l2_products = [
            xr.Dataset(attrs={
                "FH:Validity_Period:Validity_Start":
                    f"UTC=2023-04-0{i}T1{i}:15:10",
                "FH:Validity_Period:Validity_Stop":
                    f"UTC=2023-04-0{i}T1{i + 1}:45:20",
            })
            for i in range(1, 6)
        ]
        time_ranges = parse_smos_time_ranges(l2_products)
        numpy.testing.assert_equal(
            time_ranges,
            np.array([['2023-04-01T11:15:10', '2023-04-01T12:45:20'],
                      ['2023-04-02T12:15:10', '2023-04-02T13:45:20'],
                      ['2023-04-03T13:15:10', '2023-04-03T14:45:20'],
                      ['2023-04-04T14:15:10', '2023-04-04T15:45:20'],
                      ['2023-04-05T15:15:10', '2023-04-05T16:45:20']],
                     dtype='datetime64[s]'),
        )

    # noinspection PyMethodMayBeStatic
    def test_parse_smos_time_ranges_validates(self):
        l2_products = [
            xr.Dataset(attrs={
                "Start":
                    f"UTC=2023-04-0{i}T1{i}:15:10",
                "Stop":
                    f"UTC=2023-04-0{i}T1{i + 1}:45:20",
            })
            for i in range(1, 6)
        ]
        with pytest.raises(ValueError,
                           match="missing attribute"
                                 " 'FH:Validity_Period:Validity_Start'"):
            parse_smos_time_ranges(l2_products)
