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

import json
import os
import string
import sys
import traceback
import warnings
from pathlib import Path
from typing import Union, Dict, Any, Optional, Iterator, List, \
    Tuple, TypeVar, Type

import fsspec

from .constants import INDEX_CONFIG_FILENAME
from .constants import INDEX_CONFIG_VERSION
from .indexstore import IndexStore

AFS = fsspec.AbstractFileSystem


class NcKcIndex:
    """
    Represents a NetCDF Kerchunk index.
    """

    def __init__(self,
                 index_store: IndexStore,
                 index_config: Dict[str, Any]):
        """
        Private constructor. Use :meth:create() or :meth:open() instead.

        :param index_store: The index store.
        :param index_config: Optional storage options for accessing the
            filesystem of *index_path*.
            See fsspec for protocol given by *index_urlpath*.
        """
        self.index_store = index_store
        self.index_config = index_config

        self.source_path = _get_config_param(index_config, "source_path")
        self.source_protocol = _get_config_param(
            index_config,
            "source_protocol", str,
            fsspec.core.split_protocol(self.source_path)[0] or "file"
        )
        self.source_storage_options = _get_config_param(
            index_config,
            "source_storage_options", dict,
            {}
        )
        self.source_fs = fsspec.filesystem(self.source_protocol,
                                           **self.source_storage_options)
        self.is_closed = False

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __del__(self):
        self.close()

    def close(self):
        if not self.is_closed:
            self.index_store.close()
            self.close_fs(self.source_fs)
            self.is_closed = True

    @classmethod
    def close_fs(cls, fs: AFS):
        if hasattr(fs, "close"):
            # noinspection PyBroadException
            try:
                fs.close()
            except BaseException as e:
                traceback.print_exception(e, file=sys.stderr)

    @classmethod
    def create(
            cls,
            index_path: Union[str, Path],
            source_path: Optional[Union[str, Path]] = None,
            source_protocol: Optional[str] = None,
            source_storage_options: Optional[Dict[str, Any]] = None,
            replace: bool = False,
    ) -> "NcKcIndex":
        """
        Create a new NetCDF Kerchunk index.

        :param index_path: The index path or URL.
        :param source_path: The source path or URL.
        :param source_protocol: Optional protocol for the source filesystem.
            If not provided, it will be derived from *source_path*.
        :param source_storage_options: Storage options for source
            NetCDF files, e.g., options for an S3 filesystem,
            See Python fsspec package spec for the used source protocol.
        :param replace: Whether to replace an existing
            NetCDF Kerchunk index.
        :return: A new NetCDF file index.
        """
        if not source_path:
            raise ValueError("Missing source_path")

        source_storage_options = source_storage_options or {}
        source_path, source_protocol = _normalize_path_protocol(
            source_path,
            protocol=source_protocol
        )

        index_config = dict(
            version=INDEX_CONFIG_VERSION,
            source_path=source_path,
            source_protocol=source_protocol,
            source_storage_options=source_storage_options,
        )

        index_store = IndexStore.new(index_path, mode="x", replace=replace)
        index_store.write(INDEX_CONFIG_FILENAME,
                          json.dumps(index_config, indent=2))
        index_store.close()

        return cls.open(index_path, mode="a")

    @classmethod
    def open(cls, index_path: Union[str, Path], mode: str = "r") \
            -> "NcKcIndex":
        """Open the given index at *index_path*.

        :param index_path: The index path or URL.
        :param mode: Open mode, must be either "w" or "r".
            Defaults to "r".
        :return: A NetCDF file index.
        """
        if mode not in ("r", "w", "a"):
            raise ValueError("Invalid mode, must be either 'r', 'w', or 'a'")

        # Open with "r" mode, so we can read configuration
        index_store = IndexStore.new(index_path, mode="r")
        with index_store.open(INDEX_CONFIG_FILENAME, "r") as f:
            index_config = _substitute_json(json.load(f))

        if mode != "r":
            # Reopen using write mode
            index_store = IndexStore.new(index_path, mode=mode)

        return NcKcIndex(index_store, index_config)

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
            #   num_workers and submit workload in blocks. [#12]
            warnings.warn(f'num_workers={num_workers}:'
                          f' parallel processing not implemented yet.')
            for nc_file_block in self.get_nc_file_blocks(
                    prefix=prefix, block_size=block_size
            ):
                for nc_file in nc_file_block:
                    problem = self.index_nc_file(
                        nc_file, force=force, dry_run=dry_run
                    )
                    if problem is None:
                        num_files += 1
                    else:
                        problems.append(problem)
        return num_files, problems

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

        if nc_source_path.startswith(self.source_path + "/"):
            nc_source_rel_path = nc_source_path[(len(self.source_path) + 1):]
        else:
            nc_source_rel_path = nc_source_path

        nc_index_path = f"{nc_source_rel_path}.json"

        if not force and nc_index_path in self.index_store:
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

        nc_index_dir, _ = _split_parent_dir(nc_index_path)
        try:
            self.index_store.write(nc_index_path, chunks_object)
        except OSError as e:
            problem = f"Error writing index {nc_index_path}:" \
                      f" {e.__class__.__name__}: {e}"
            print(problem)
            return problem

        return None


T = TypeVar('T')
_UNDEFINED = "_UNDEFINED"


def _get_config_param(index_config: Dict[str, Any],
                      param_name: str,
                      param_type: Type[T] = str,
                      default_value: Any = _UNDEFINED) -> T:
    if param_name not in index_config:
        if default_value == _UNDEFINED:
            raise ValueError(f"Missing configuration "
                             f"parameter '{param_name}'")
        return default_value
    value = index_config.get(param_name)
    if not isinstance(value, param_type):
        raise ValueError(f"Configuration parameter '{param_name}' "
                         f"must be of type {param_type}, "
                         f"but was {type(value)}")
    return value


def _normalize_path_protocol(
        path: str | Path,
        protocol: Optional[str] = None,
) -> Tuple[str, str]:
    _protocol, path = fsspec.core.split_protocol(path)
    protocol = protocol or _protocol or "file"
    if os.name == "nt" and protocol in ("file", "local"):
        # Normalize a Windows path
        path = path.replace("\\", "/")
    return path, protocol


def _split_parent_dir(path: str) -> Tuple[str, str]:
    splits = path.rsplit("/", maxsplit=1)
    if len(splits) == 1:
        return "", path
    return splits[0], splits[1]


def _substitute_json(value: Any) -> Any:
    if isinstance(value, str):
        return _substitute_text(value)
    if isinstance(value, dict):
        return {_substitute_text(k): _substitute_json(v)
                for k, v in value.items()}
    if isinstance(value, list):
        return [_substitute_json(v) for v in value]
    return value


def _substitute_text(text: str) -> str:
    return string.Template(text).safe_substitute(os.environ)
