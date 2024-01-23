import unittest

import pytest

from helpers import get_time_ranges


class TimeRangeTest(unittest.TestCase):
    def test_get_time_ranges_from_date_range(self):
        time_ranges = get_time_ranges("2020-01-01", agg_interval="1d")
        self.assertEqual(
            [
                ("2020-01-01", "2020-01-01"),
            ],
            time_ranges,
        )

        time_ranges = get_time_ranges("2020-01-01/2020-01-01", agg_interval="1d")
        self.assertEqual(
            [
                ("2020-01-01", "2020-01-01"),
            ],
            time_ranges,
        )

        time_ranges = get_time_ranges("2020-01-01/2020-01-03", agg_interval="1d")
        self.assertEqual(
            [
                ("2020-01-01", "2020-01-01"),
                ("2020-01-02", "2020-01-02"),
                ("2020-01-03", "2020-01-03"),
            ],
            time_ranges,
        )

        time_ranges = get_time_ranges("2020-01-01/2020-01-10", agg_interval="2d")
        self.assertEqual(
            [
                ("2020-01-01", "2020-01-02"),
                ("2020-01-03", "2020-01-04"),
                ("2020-01-05", "2020-01-06"),
                ("2020-01-07", "2020-01-08"),
                ("2020-01-09", "2020-01-10"),
            ],
            time_ranges,
        )

    def test_get_time_ranges_from_date(self):
        time_ranges = get_time_ranges("2020-01-01", agg_interval="1d")
        self.assertEqual(
            [
                ("2020-01-01", "2020-01-01"),
            ],
            time_ranges,
        )

        time_ranges = get_time_ranges("2020-01-01", agg_interval="2d")
        self.assertEqual(
            [
                ("2020-01-01", "2020-01-02"),
            ],
            time_ranges,
        )

    # noinspection PyMethodMayBeStatic
    def test_it_raises_for_illegal_interval(self):
        with pytest.raises(
            ValueError, match="agg_interval must not be less than a day"
        ):
            get_time_ranges("2020-01-01", agg_interval="4h")

    # noinspection PyMethodMayBeStatic
    def test_it_raises_for_illegal_time_range(self):
        with pytest.raises(ValueError, match="time_range must not exceed 400 days"):
            get_time_ranges("2020-01-01/2021-03-01", agg_interval="8d")
