from typing import Union, Tuple, Optional, List
import re
import pandas as pd
from ..timeinfo import normalize_time_range
from ..timeinfo import to_compact_time
from .types import DatasetRecord
from .types import GetFilesForPath
from .types import AcceptRecord

COMMON_SUB_PATH_PATTERN = "{year}/{month}/{day}"

COMMON_NAME_PATTERN = r"(?P<sd>\d{8})T(?P<st>\d{6})_" \
                      r"(?P<ed>\d{8})T(?P<et>\d{6})_" \
                      r"\d{3}_\d{3}_\d{1}"

ProductTypeLike = Union[str, "ProductType"]

_ONE_DAY = pd.Timedelta(1, unit="days")


class ProductType:
    """SMOS product type."""

    OS: 'ProductType'
    SM: 'ProductType'

    def __init__(self,
                 type_id: str,
                 path_prefix: str,
                 name_prefix: str):
        if not path_prefix:
            raise ValueError("path_prefix must be given")
        if not path_prefix.endswith("/"):
            raise ValueError("path_prefix must end with '/'")
        self.type_id = type_id
        self.path_prefix = path_prefix
        self.path_pattern = path_prefix + COMMON_SUB_PATH_PATTERN
        self.name_pattern = name_prefix + COMMON_NAME_PATTERN

    @classmethod
    def normalize(cls, product_type: ProductTypeLike) -> "ProductType":
        if isinstance(product_type, ProductType):
            return product_type
        if isinstance(product_type, str):
            if product_type.upper() == 'SM':
                return cls.SM
            if product_type.upper() == 'OS':
                return cls.OS
            raise ValueError(f'invalid product_type {product_type!r}')
        raise TypeError(f'invalid product_type type {type(product_type)}')

    def find_files_for_time_range(
            self,
            time_range: Tuple[Optional[str], Optional[str]],
            get_files_for_path: GetFilesForPath,
            accept_record: Optional[AcceptRecord] = None,
    ) -> List[DatasetRecord]:
        start, end = normalize_time_range(time_range)

        start_times = self.find_files_for_date(start,
                                               get_files_for_path,
                                               accept_record)
        end_times = self.find_files_for_date(end,
                                             get_files_for_path,
                                             accept_record)

        start_str = to_compact_time(start)
        end_str = to_compact_time(end)

        start_index = -1
        for index, (_, _, start_end_str) in enumerate(start_times):
            if start_end_str >= start_str:
                start_index = index
                break

        end_index = -1
        for index, (_, end_start_str, _) in enumerate(end_times):
            if end_start_str >= end_str:
                end_index = index
                break

        start_names = []
        if start_index >= 0:
            start_names.extend(start_times[start_index:])

        # Add everything between start + start.day and end - end.day

        start_p1d = pd.Timestamp(year=start.year,
                                 month=start.month,
                                 day=start.day) + _ONE_DAY
        end_m1d = pd.Timestamp(year=end.year,
                               month=end.month,
                               day=end.day) - _ONE_DAY

        in_between_names = []
        if end_m1d > start_p1d:
            time = start_p1d
            while time <= end_m1d:
                in_between_names.extend(
                    self.find_files_for_date(time,
                                             get_files_for_path,
                                             accept_record)
                )
                time += _ONE_DAY

        end_names = []
        if end_index >= 0:
            end_names.extend(end_times[:end_index])

        return start_names + in_between_names + end_names

    def find_files_for_date(
            self,
            date: pd.Timestamp,
            get_files_for_path: GetFilesForPath,
            accept_record: Optional[AcceptRecord] = None) \
            -> List[DatasetRecord]:
        path_pattern = self.path_pattern
        name_pattern = self.name_pattern

        year = date.year
        month = date.month
        day = date.day

        prefix_path = path_pattern.format(
            year=year,
            month=f'0{month}' if month < 10 else month,
            day=f'0{day}' if day < 10 else day
        )

        records = []
        for file_path in get_files_for_path(prefix_path):
            parent_and_filename = file_path.rsplit("/", 1)
            filename = parent_and_filename[1] \
                if len(parent_and_filename) == 2 else file_path
            m = re.match(name_pattern, filename)
            if m is not None:
                start = m.group("sd") + m.group("st")
                end = m.group("ed") + m.group("et")

                record = file_path, start, end
                if accept_record is None or accept_record(record):
                    records.append(record)

        return sorted(records)


ProductType.OS = ProductType(
    "OS",
    "SMOS/L2OS/MIR_OSUDP2/",
    r"SM_(OPER|REPR)_MIR_OSUDP2_"
)

ProductType.SM = ProductType(
    "SM",
    "SMOS/L2SM/MIR_SMUDP2/",
    r"SM_(OPER|REPR)_MIR_SMUDP2_"
)
