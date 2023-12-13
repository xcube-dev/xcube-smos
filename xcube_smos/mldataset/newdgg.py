import contextlib
from pathlib import Path
from typing import Optional

import importlib_resources
import atexit

from xcube.core.mldataset import MultiLevelDataset
from xcube.core.store import new_fs_data_store
from .dgg import SmosDiscreteGlobalGrid


# We don't use the first and the last level
NUM_LEVELS = SmosDiscreteGlobalGrid.MAX_NUM_LEVELS - 2
# Because we skip original level 0, resolution decreases by factor 2
MAX_WIDTH = SmosDiscreteGlobalGrid.MAX_WIDTH // 2
MAX_HEIGHT = SmosDiscreteGlobalGrid.MAX_HEIGHT // 2
MIN_PIXEL_SIZE = SmosDiscreteGlobalGrid.MIN_PIXEL_SIZE * 2

_PACKAGE_PATH: Optional[str] = None


def new_dgg() -> MultiLevelDataset:
    global _PACKAGE_PATH
    if _PACKAGE_PATH is None:
        _PACKAGE_PATH = str(get_package_path())

    store = new_fs_data_store("file", _PACKAGE_PATH)
    # noinspection PyTypeChecker
    return store.open_data("smos-dgg.levels")


def get_package_path() -> Path:
    file_manager = contextlib.ExitStack()
    atexit.register(file_manager.close)
    ref = importlib_resources.files("xcube_smos.mldataset")
    return file_manager.enter_context(
        importlib_resources.as_file(ref)
    )
