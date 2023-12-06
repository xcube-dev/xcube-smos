import json
import os
import shutil
from abc import ABC, abstractmethod
import zipfile
from typing import List, Optional


class IndexStore(ABC):
    """A file store used by the Kerchunk NetCDF index."""

    @classmethod
    def new(cls, path: str, mode: str = "r", replace: bool = False) \
            -> 'IndexStore':
        """Create a new index store instance for the given *path*
        and *mode*.

        :param path: The local store path. If the filename ends with ".zip"
            a zip archive file is assumed, otherwise a directory.
        :param mode: The store open mode. May be one of
            "x" (create), "w" (write), "a" (append), or "r" (read-only).
            Defaults to "r".
        :param replace: If set and mode is "x"
            an existing index store at *path* will be replaced by
            a new and empty one.
        :return: a new index store instance
        """
        if path.endswith(".zip"):
            return ZipIndexStore(path, mode, replace=replace)
        else:
            return DirIndexStore(path, mode, replace=replace)

    def __init__(self, path: str, mode: str):
        """Internal constructor."""
        if mode not in {"x", "a", "w", "r"}:
            raise ValueError(f"Unknown mode {mode}")
        self.path = os.path.realpath(path)
        self.mode = mode

    def __del__(self):
        """Closes this store."""
        self.close()

    def close(self):
        """Close this index store."""

    @abstractmethod
    def __contains__(self, file: str) -> bool:
        """Check if given *file* exists in this store."""

    @abstractmethod
    def list(self, prefix: Optional[str] = None) -> List[str]:
        """List the files in this store.
        The method will return relative paths to files only.
        The separator character is always a forward slash "/".
        """

    @abstractmethod
    def open(self, file: str, mode: str = "r"):
        """Open the given binary *file*.

        :param file: The filename.
        :param mode: The open mode. Must be either "w" or "r".
        :return: a file pointer that can be used to write or read binary data
        """

    @abstractmethod
    def write(self, file: str, data: str | dict | bytes):
        """Write *data* to the given *file*.

        :param file: The filename.
        :param data: The data to write. A dictionary is converted
            to JSON text. Text is converted to bytes using UTF-8 encoding.
        """


class DirIndexStore(IndexStore):
    def __init__(self, path: str, mode: str, replace: bool = False):
        super().__init__(path, mode)
        dir_exists = os.path.exists(path)
        if mode == "x":
            if dir_exists:
                if replace:
                    shutil.rmtree(path)
                    dir_exists = False
                else:
                    raise OSError(f"Index exists: {path}")
            if not dir_exists:
                os.makedirs(path, exist_ok=True)
        elif not dir_exists:
            raise FileNotFoundError(f"Index not found: {path}")

    def __contains__(self, file: str) -> bool:
        return os.path.exists(os.path.join(self.path, file))

    def list(self, prefix: Optional[str] = None) -> List[str]:
        files = []
        n = len(self.path)
        if prefix and prefix.endswith("/"):
            root_path = os.path.join(self.path, prefix[:-1])
            prefix = None
        else:
            root_path = self.path
        if not os.path.isdir(root_path):
            return []
        for abs_path, _, _filenames in os.walk(root_path):
            file_prefix = abs_path[n + 1:]
            for filename in _filenames:
                file = filename if not file_prefix \
                    else f"{file_prefix}/{filename}"
                if os.name == "nt":
                    file = file.replace("\\", "/")
                if not prefix or file.startswith(prefix):
                    files.append(file)
        return files

    def open(self, file: str, mode: str = "r"):
        if mode not in ("r", "w"):
            raise ValueError("mode must be either 'r' or 'w")
        path = os.path.join(self.path, file)
        parent = os.path.dirname(path)
        if not os.path.exists(parent):
            os.makedirs(parent, exist_ok=True)
        return open(os.path.join(self.path, file), mode=mode + "b")

    def write(self, file: str, data: str | dict | bytes):
        if isinstance(data, dict):
            data = json.dumps(data)
        if isinstance(data, str):
            data = data.encode(encoding='utf-8')
        with self.open(file, mode="w") as fp:
            # noinspection PyTypeChecker
            fp.write(data)


class ZipIndexStore(IndexStore):
    def __init__(self, path: str, mode: str, replace: bool = False):
        super().__init__(path, mode)
        self.zip_file = None
        zip_exists = os.path.exists(path)
        if mode == "x":
            if zip_exists:
                if replace:
                    os.remove(path)
                else:
                    raise OSError(f"Index exists: {path}")
            zip_file = zipfile.ZipFile(path, mode="x")
            entries = []
        else:
            if not zip_exists:
                raise FileNotFoundError(f"Index not found: {path}")
            # noinspection PyTypeChecker
            zip_file = zipfile.ZipFile(path, mode="r")
            entries = zip_file.namelist()
            if mode != "r":
                zip_file.close()
                # noinspection PyTypeChecker
                zip_file = zipfile.ZipFile(path, mode=mode)
        self.zip_file = zip_file
        self.files = entries
        self.file_set = set(entries)

    def __contains__(self, file: str) -> bool:
        return file in self.file_set

    def list(self, prefix: Optional[str] = None) -> List[str]:
        return [file for file in self.files
                if not prefix or file.startswith(prefix)]

    def open(self, file: str, mode: str = "r"):
        # noinspection PyTypeChecker
        return self.zip_file.open(file, mode=mode)

    def write(self, file: str, data: str | dict | bytes):
        if isinstance(data, dict):
            data = json.dumps(data)
        if isinstance(data, str):
            data = data.encode('utf-8')
        with self.open(file, mode="w") as fp:
            fp.write(data)

    def close(self):
        if self.zip_file:
            self.zip_file.close()
            self.zip_file = None
