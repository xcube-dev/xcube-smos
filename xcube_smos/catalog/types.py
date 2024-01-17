from typing import Dict, Any, Optional, Tuple, Callable, Iterator, Iterable

import xarray as xr

DatasetRecord = Tuple[
    str,  # relative path
    str,  # start date/time in compact format
    str,  # end date/time in compact format
]

DatasetOpener = Callable[
    [
        str,  # dataset_path
        {
            "protocol": Optional[str],
            "storage_options": Optional[Dict[str, Any]],
        },
    ],
    xr.Dataset,
]

AcceptRecord = Callable[[DatasetRecord], bool]
GetFilesForPath = Callable[[str], Iterable[str]]
