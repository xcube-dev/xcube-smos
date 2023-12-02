from functools import cached_property
import json
from pathlib import Path
from typing import Union, Dict, Any, Optional, Iterator, List, \
    Tuple, TypeVar, Type
import warnings

import fsspec
from xcube.util.undefined import UNDEFINED
from .constants import DEFAULT_SOURCE_PROTOCOL
from .constants import DEFAULT_INDEX_NAME
from .constants import INDEX_CONFIG_FILENAME
from .constants import INDEX_CONFIG_VERSION
from .producttype import ProductType


class NcKcIndex:
    """
    Represents a NetCDF Kerchunk index.
    """

    def __init__(self,
                 index_fs: fsspec.AbstractFileSystem,
                 index_path: str,
                 index_config: Dict[str, Any]):
        """
        Private constructor. Use :meth:create() or :meth:open() instead.

        :param index_fs: Index filesystem.
        :param index_path: Path to the index directory.
        :param index_config: Optional storage options for accessing the
            filesystem of *index_path*.
            See fsspec for protocol given by *index_urlpath*.
        """
        self.index_fs = index_fs
        self.index_path = index_path
        self.index_config = index_config

        self.source_path = _get_config_param(index_config, "source_path")
        self._source_protocol = _get_config_param(
            self.index_config,
            "source_protocol", str, None
        )
        self.source_storage_options = _get_config_param(
            index_config,
            "source_storage_options", dict,
            {}
        )

    @cached_property
    def source_protocol(self) -> str:
        if self._source_protocol:
            return self._source_protocol
        protocol, path = fsspec.core.split_protocol(self.source_path)
        return protocol or "file"

    @cached_property
    def source_fs(self) -> fsspec.AbstractFileSystem:
        return fsspec.filesystem(self.source_protocol,
                                 **self.source_storage_options)

    @cached_property
    def prefixes(self) -> Dict[str, Any]:
        return self.index_config["prefixes"] or {}

    @classmethod
    def create(
        cls,
        index_path: Union[str, Path] = DEFAULT_INDEX_NAME,
        index_storage_options: Optional[Dict[str, Any]] = None,
        source_path: Optional[Union[str, Path]] = None,
        source_protocol: Optional[str] = DEFAULT_SOURCE_PROTOCOL,
        source_storage_options: Optional[Dict[str, Any]] = None,
        replace_existing: bool = False,
    ) -> "NcKcIndex":
        """
        Create a new NetCDF Kerchunk index.

        :param index_path: The index path or URL.
        :param index_storage_options: Optional storage options for accessing
            the filesystem of *index_path*.
            See fsspec for protocol given by *index_urlpath*.
        :param source_path: The source path or URL.
        :param source_protocol: Optional protocol for the source filesystem.
            If not provided, it will be derived from *index_path*.
        :param source_storage_options: Storage options for source
            NetCDF files, e.g., options for an S3 filesystem,
            see fsspec/s3fs.
        :param replace_existing: Whether to replace an existing
            NetCDF Kerchunk index.
        :return: A new NetCDF file index.
        """
        if not source_path:
            raise ValueError("Missing source_path")

        index_path = str(index_path)

        path_protocol, source_path = fsspec.core.split_protocol(source_path)
        source_protocol = source_protocol or path_protocol or "file"
        source_storage_options = source_storage_options or {}

        index_config = dict(
            version=INDEX_CONFIG_VERSION,
            source_path=source_path,
            source_protocol=source_protocol,
            source_storage_options=source_storage_options,
            prefixes={pt.id: pt.path_prefix
                      for pt in ProductType.get_all()}
        )

        index_fs, index_path, _ = cls._get_fs_path_protocol(
            index_path,
            storage_options=index_storage_options
        )
        if replace_existing and index_fs.isdir(index_path):
            index_fs.rm(index_path, recursive=True)
        index_fs.mkdirs(index_path)
        with index_fs.open(cls._index_config_path(index_path), "w") as f:
            json.dump(index_config, f, indent=2)
        return cls.open(index_path,
                        index_storage_options=index_storage_options)

    @classmethod
    def open(
        cls,
        index_path: Union[str, Path] = DEFAULT_INDEX_NAME,
        index_storage_options: Optional[Dict[str, Any]] = None
    ) -> "NcKcIndex":
        """Open the given index at *index_path*.

        :param index_path: Local file path or URL.
        :param index_storage_options: Optional storage options for the
            filesystem of *index_path*.
            See fsspec for protocol given by *index_path*.
        :return: A NetCDF file index.
        """
        index_fs, index_path, _ = cls._get_fs_path_protocol(
            index_path,
            storage_options=index_storage_options
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
             num_workers: int = 1,
             block_size: int = 100,
             force: bool = False,
             dry_run: bool = False) -> Tuple[int, List[str]]:
        """Synchronize this index with the files.
        If *prefix* is given, only files that match the given prefix
        are processed. Otherwise, all SMOS L2 files are processed.

        :param prefix: Key prefix.
        :param num_workers: Number of parallel workers.
            Not used yet.
        :param block_size: Number of files processed by a single worker.
            Ignored, if *num_workers* is less than two.
            Not used yet.
        :param force: Do not skip existing indexes.
        :param dry_run: Do not write any indexes. Useful for testing.
        :return: A tuple comprising the number of NetCDF files
            successfully indexed and a list of encountered problems.
        """
        problems = []
        num_files = 0
        if num_workers < 2:
            for nc_file in self.get_nc_files(prefix=prefix):
                problem = self.index_nc_file(
                    nc_file, force=force, dry_run=dry_run
                )
                if problem is None:
                    num_files += 1
                else:
                    problems.append(problem)
        else:
            # TODO: setup mult-threaded/-process (Dask) executor with
            #   num_workers and submit workload in blocks.
            warnings.warn(f'num_workers={num_workers}:'
                          f' parallel processing not implemented yet.')
            for nc_file_block in self.get_nc_file_blocks(prefix=prefix,
                                                         block_size=block_size):
                for nc_file in nc_file_block:
                    problem = self.index_nc_file(
                        nc_file, force=force, dry_run=dry_run
                    )
                    if problem is None:
                        num_files += 1
                    else:
                        problems.append(problem)
        return num_files, problems

    def index_nc_file(self,
                      nc_source_path: str,
                      force: bool = False,
                      dry_run: bool = False) -> Optional[str]:
        """
        Index a NetCDF file given by *nc_path* in S3.

        :param nc_source_path: NetCDF source file path.
        :param force: Do not skip existing indexes.
        :param dry_run: Do not write any indexes. Useful for testing.
        :return: None, if the NetCDF file has been successfully indexed.
            Otherwise, a message indicating the problem.
        """
        import kerchunk.hdf

        nc_index_path = f"{self.index_path}/{nc_source_path}.json"

        if not force and self.index_fs.exists(nc_index_path):
            print(f"Skipping {nc_source_path}, index exists")
            return None

        print(f"Indexing {nc_source_path}")

        try:
            with self.source_fs.open(nc_source_path, mode="rb") as f:
                chunks = kerchunk.hdf.SingleHdf5ToZarr(
                    f, nc_source_path, inline_threshold=100
                )
                chunks_object = chunks.translate()
        except OSError as e:
            problem = f"Error indexing {nc_source_path}:" \
                      f" {e.__class__.__name__}: {e}"
            print(problem)
            return problem

        if dry_run:
            return None

        nc_index_dir, _ = nc_index_path.rsplit("/", maxsplit=1)
        try:
            self.index_fs.mkdirs(nc_index_dir, exist_ok=True)
            with self.index_fs.open(nc_index_path, "w") as f:
                json.dump(chunks_object, f)
        except OSError as e:
            problem = f"Error writing index {nc_index_path}:" \
                      f" {e.__class__.__name__}: {e}"
            print(problem)
            return problem

        return None

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

    def get_nc_files(self,
                     prefix: Optional[str] = None) -> Iterator[str]:

        source_fs = self.source_fs
        source_path = self.source_path

        if prefix:
            source_path += "/" + prefix

        def handle_error(e: OSError):
            print(f"Error scanning source {source_path}:"
                  f" {e.__class__.__name__}: {e}")

        for path, _, files in source_fs.walk(source_path,
                                             on_error=handle_error):
            for file in files:
                if file.endswith(".nc"):
                    yield path + "/" + file

    def get_nc_file_blocks(self,
                           prefix: Optional[str] = None,
                           block_size: int = 100) -> Iterator[List[str]]:
        block = []
        for nc_file in self.get_nc_files(prefix=prefix):
            block.append(nc_file)
            if len(block) >= block_size:
                yield block
                block = []
        if block:
            yield block


T = TypeVar('T')


def _get_config_param(index_config: Dict[str, Any],
                      param_name: str,
                      param_type: Type[T] = str,
                      default_value: Any = UNDEFINED) -> T:
    if param_name not in index_config:
        if default_value is UNDEFINED:
            raise ValueError(f"Missing configuration "
                             f"parameter '{param_name}'")
        return default_value
    value = index_config.get(param_name)
    if not isinstance(value, param_type):
        raise ValueError(f"Configuration parameter '{param_name}' "
                         f"must be of type {param_type}, "
                         f"but was {type(value)}")
    return value
