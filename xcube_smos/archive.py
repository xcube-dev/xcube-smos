from pathlib import Path
from typing import Union, Dict, Any, Optional

from xcube_smos.nckcindex.nckcindex import NcKcIndex


class SmosArchive:
    def __init__(self,
                 index_urlpath: Union[str, Path],
                 index_options: Optional[Dict[str, Any]] = None):
        self._nc_kc_index = NcKcIndex.open(index_urlpath,
                                           index_options=index_options)

