import json
from functools import cached_property
from pathlib import Path
from typing import Union, Dict, Any, Optional, Iterator, List, Tuple

import fsspec
import kerchunk.hdf

from .constants import DEFAULT_BUCKET_NAME
from .constants import DEFAULT_INDEX_NAME
from .constants import INDEX_CONFIG_FILENAME
from .constants import INDEX_CONFIG_VERSION
from .producttype import ProductType
from .s3scanner import S3Scanner


class NcKcIndex:
    """
    Represents a NetCDF Kerchunk index.
    Index files are created for NetCDF files in some S3 bucket.
    """

    def __init__(self,
                 index_fs: fsspec.AbstractFileSystem,
                 index_path: str,
                 index_config: Dict[str, Any]):
        """
        Private constructor. Use :meth:create() or :meth:open() instead.

        :param index_fs: Index filesystem.
        :param index_path: Path to the index directory.
        :param index_config: Index configuration.
        """
        self.index_fs = index_fs
        self.index_path = index_path
        self.index_config = index_config

    @cached_property
    def s3_bucket(self) -> str:
        return self.index_config["s3_bucket"]

    @cached_property
    def s3_options(self) -> Dict[str, Any]:
        return self.index_config["s3_options"] or {}

    @cached_property
    def s3_fs(self) -> fsspec.AbstractFileSystem:
        return fsspec.filesystem("s3", **self.s3_options)

    @classmethod
    def create(
            cls,
            index_urlpath: Union[str, Path] = DEFAULT_INDEX_NAME,
            index_options: Optional[Dict[str, Any]] = None,
            s3_bucket: str = DEFAULT_BUCKET_NAME,
            s3_options: Optional[Dict[str, Any]] = None,
            replace_existing: bool = False,
    ) -> "NcKcIndex":
        """
        Create a new NetCDF Kerchunk index.

        :param index_urlpath: Local path or URL for the
        :param index_options: Optional options for accessing the
            filesystem of *index_urlpath*.
        :param s3_bucket: The source S3 bucket.
        :param s3_options: Storage options for the S3 filesystem.
        :param replace_existing: Whether to replace an existing
            NetCDF Kerchunk index.
        :return:
        """

        index_urlpath = str(index_urlpath)
        s3_options = s3_options or {}

        index_config = dict(
            version=INDEX_CONFIG_VERSION,
            s3_bucket=s3_bucket,
            s3_options=s3_options,
            product_types={pt.id: pt.path for pt in ProductType.get_all()}
        )

        index_fs, index_path, _ = cls._get_fs_path_protocol(
            index_urlpath,
            storage_options=index_options
        )
        if replace_existing and index_fs.isdir(index_path):
            index_fs.rm(index_path, recursive=True)
        index_fs.mkdirs(index_path)
        with index_fs.open(cls._index_config_path(index_path), "w") as f:
            json.dump(index_config, f, indent=2)
        return cls.open(index_urlpath, index_options=index_options)

    @classmethod
    def open(cls,
             index_urlpath: Union[str, Path] = DEFAULT_INDEX_NAME,
             index_options: Optional[Dict[str, Any]] = None) -> "NcKcIndex":
        """Open the given index at *index_urlpath*.

        :param index_urlpath: Local file path or URL.
        :param index_options: Optional storage options for the
            filesystem of *index_urlpath*.
        :return: A NetCDF file index.
        """
        index_fs, index_path, _ = cls._get_fs_path_protocol(
            index_urlpath,
            storage_options=index_options
        )
        with index_fs.open(cls._index_config_path(index_path), "r") as f:
            index_config = json.load(f)
        return NcKcIndex(
            index_fs,
            index_path,
            index_config
        )

    def sync(self,
             prefix: Optional[str] = None,
             num_workers: int = 8,
             block_size: int = 100,
             dry_run: bool = False) -> int:
        """Synchronize this index with the files.
        If *prefix* is given, only files that match the given prefix
        are processed. Otherwise, all SMOS L2 files are processed.

        :param prefix: Key prefix.
        :param num_workers: Number of parallel workers.
            Not used yet.
        :param block_size: Number of files processed by a single worker.
            Not used yet.
        :param dry_run: Does not write any files. For testing only.
        :return: Number of NetCDF files successfully indexed.
        """
        num_files = 0
        for nc_file_block in self.get_nc_file_blocks(prefix=prefix,
                                                     block_size=block_size):
            for nc_file in nc_file_block:
                success = self.index_nc_file(nc_file, dry_run=dry_run)
                num_files += int(success)
        return num_files

    def index_nc_file(self,
                      nc_path: str,
                      dry_run: bool = False) -> bool:
        """

        :param nc_path: NetCDF file S3 path
        :param dry_run: Does not write any files. For testing only.
        :return: True, if the NetCDF file has been successfully indexed.
        """
        nc_index_path = f"{self.index_path}/{nc_path}.json"

        if self.index_fs.exists(nc_index_path):
            print(f"Skipping {nc_path}, index exists")
            return False

        s3_url = f"s3://{self.s3_bucket}/{nc_path}"

        print(f"Indexing {s3_url}")
        if dry_run:
            return True

        with self.s3_fs.open(s3_url) as f:
            chunks = kerchunk.hdf.SingleHdf5ToZarr(
                f, s3_url, inline_threshold=100
            )
            chunks_object = chunks.translate()

        nc_index_dir, _ = nc_index_path.rsplit("/", maxsplit=1)
        self.index_fs.mkdirs(nc_index_dir, exist_ok=True)

        with self.index_fs.open(nc_index_path, "w") as f:
            json.dump(chunks_object, f)

        return True

    @classmethod
    def _index_config_path(cls, index_path: str) -> str:
        return f"{index_path}/{INDEX_CONFIG_FILENAME}"

    @classmethod
    def _get_fs_path_protocol(
            cls,
            urlpath: str,
            storage_options: Optional[Dict[str, Any]] = None
    ) -> Tuple[fsspec.AbstractFileSystem, str, str]:
        protocol, path = fsspec.core.split_protocol(urlpath)
        protocol = protocol or "file"
        fs = fsspec.filesystem(protocol, **(storage_options or {}))
        return fs, path, protocol

    def get_nc_files(self, prefix: Optional[str] = None) -> Iterator[str]:
        s3_bucket = self.index_config["s3_bucket"]
        s3_options = self.index_config["s3_options"]
        s3_scanner = S3Scanner(**s3_options)
        if prefix is not None:
            yield from s3_scanner.get_keys(s3_bucket,
                                           prefix=prefix,
                                           suffix=".nc")
        else:
            for pt in ProductType.get_all():
                yield from s3_scanner.get_keys(s3_bucket,
                                               prefix=pt.path,
                                               suffix=".nc")

    def get_nc_file_blocks(self,
                           prefix: Optional[str] = None,
                           block_size: int = 100) -> Iterator[List[str]]:
        block = []
        for nc_file in self.get_nc_files(prefix=prefix):
            if len(block) >= block_size:
                yield block
                block = []
            else:
                block.append(nc_file)
        if block:
            yield block
