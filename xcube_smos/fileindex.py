import json
from pathlib import Path
from typing import Union, Dict, Any

import fsspec

from .producttype import ProductType

INDEX_CONFIG_VERSION = 1
INDEX_CONFIG_FILENAME = "index-config.json"


class FileIndex:

    def __init__(self,
                 index_path: str,
                 index_config: Dict[str, Any]):
        self._index_path = index_path
        self._index_config = index_config

    @property
    def index_path(self) -> str:
        return self._index_path

    @property
    def index_config(self) -> Dict[str, Any]:
        return self._index_config

    @property
    def lock_path(self) -> str:
        return f"{self.index_path}/lock.json"

    @classmethod
    def _index_config_path(cls, index_path: str) -> str:
        return f"{index_path}/{INDEX_CONFIG_FILENAME}"

    @classmethod
    def create(cls,
               index_path: Union[str, Path],
               remote_path: str,
               remote_protocol: str = "s3",
               remote_options: Dict[str, Any] = None) -> "FileIndex":
        index_path = str(index_path)
        remote_options = remote_options or {}

        index_fs: fsspec.AbstractFileSystem = fsspec.filesystem("file")
        remote_fs: fsspec.AbstractFileSystem = fsspec.filesystem(
            remote_protocol,
            **(remote_options or {})
        )

        for pt in ProductType.get_all():
            sub_paths = remote_fs.listdir(f"{remote_path}/{pt.path}",
                                          detail=False)
            for sub_path in sub_paths:
                index_sub_path = sub_path[len(remote_path):]
                index_fs.mkdirs(index_path + index_sub_path,
                                exist_ok=False)

        index_config = dict(
            version=INDEX_CONFIG_VERSION,
            remote_path=remote_path,
            remote_protocol=remote_protocol,
            remote_options=remote_options,
            product_types={pt.id: pt.path for pt in ProductType.get_all()}
        )
        with index_fs.open(cls._index_config_path(index_path), "w") as f:
            json.dump(index_config, f, indent=2)

        return cls.open(index_path)

    @classmethod
    def open(cls,
             index_path: Union[str, Path]) -> "FileIndex":
        index_fs: fsspec.AbstractFileSystem = fsspec.filesystem("file")
        index_path = str(index_path)
        with index_fs.open(cls._index_config_path(index_path), "r") as f:
            index_config = json.load(f)
        return FileIndex(
            index_path,
            index_config
        )

    def sync(self):
        # if self.lock_file.exists():
        #     raise RuntimeError(f"Already synchronizing: {self.lock_file}")
        # with open(self.lock_file, "w") as f:
        #     json.dump(dict(
        #         started=datetime.datetime.now().isoformat()
        #     ), f)
        pass
