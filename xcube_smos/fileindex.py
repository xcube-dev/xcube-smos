import calendar
import json
from pathlib import Path
from typing import Union, Dict, Any, Optional

import boto3
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
               s3_bucket: str,
               s3_options: Dict[str, Any] = None) -> "FileIndex":
        index_path = str(index_path)
        s3_options = s3_options or {}

        index_fs: fsspec.AbstractFileSystem = fsspec.filesystem("file")

        scanner = Scanner(**s3_options)
        for pt in ProductType.get_all():
            sub_paths = scanner.get_prefixes(s3_bucket, prefix=pt.path)
            for sub_path in sub_paths:
                index_fs.mkdirs(f'{index_path}/{sub_path}',
                                exist_ok=False)

        index_config = dict(
            version=INDEX_CONFIG_VERSION,
            s3_bucket=s3_bucket,
            s3_options=s3_options,
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

    def sync(self, num_files_max: int = -1):
        s3_bucket = self.index_config["s3_bucket"]
        s3_options = self.index_config["s3_options"]
        scanner = Scanner(**s3_options)

        for pt in ProductType.get_all():
            num_files = 0
            sub_paths = scanner.get_prefixes(s3_bucket, prefix=pt.path)
            for sub_path in sub_paths:
                # print(f'Scanning {sub_path}')
                splits = sub_path.rsplit("/")
                year = None
                if len(splits) > 3 and splits[-1] == "":
                    try:
                        year = int(splits[-2])
                    except (ValueError, TypeError):
                        pass
                if year is None:
                    continue
                for month in range(1, 13):
                    if num_files > num_files_max:
                        break
                    start_day, end_day = calendar.monthrange(year, month)
                    for day in range(start_day, end_day + 1):
                        if num_files > num_files_max:
                            break
                        day_path = f"{sub_path}{_2c(month)}/{_2c(day)}/"
                        # print(f'Scanning {day_path}')
                        file_paths = scanner.get_keys(s3_bucket,
                                                      prefix=day_path,
                                                      suffix=".nc")
                        for file_path in file_paths:
                            if num_files > num_files_max:
                                break
                            num_files += 1
                            index_path = f'{self.index_path}/{file_path}'
                            print(f'Syncing with {index_path}')
                        # index_fs.mkdirs(f'{index_path}/{sub_path}',
                        #                 exist_ok=False)

        # Much slower? No!

        # for pt in ProductType.get_all():
        #     file_paths = scanner.get_keys(s3_bucket,
        #                                  prefix=pt.path,
        #                                  suffix=".nc")
        #     for index, file_path in enumerate(file_paths):
        #         if index >= num_files_max:
        #             break
        #         index_path = f'{self.index_path}/{file_path}'
        #         print(f'Syncing with {index_path}')


def _2c(value: int) -> str:
    return str(value) if value >= 10 else '0' + str(value)


class Scanner:
    def __init__(self,
                 key: Optional[str] = None,
                 secret: Optional[str] = None,
                 endpoint_url: Optional[str] = None):
        self._client = boto3.client(
            's3',
            aws_access_key_id=key,
            aws_secret_access_key=secret,
            endpoint_url=endpoint_url,
        )
        self._paginator = self._client.get_paginator('list_objects_v2')

    def get_keys(self, bucket_name: str, prefix: str = "", suffix: str = ""):
        for page in self.get_pages(bucket_name, prefix=prefix):
            for common_prefix in page.get('CommonPrefixes', ()):
                yield from self.get_keys(bucket_name,
                                         prefix=common_prefix["Prefix"],
                                         suffix=suffix)
            for content in page.get('Contents', ()):
                key = content["Key"]
                if (suffix and key.endswith(suffix)) \
                        or not key.endswith('/'):
                    yield key

    def get_prefixes(self, bucket_name: str, prefix: str = ""):
        for page in self.get_pages(bucket_name, prefix=prefix):
            for common_prefix in page.get('CommonPrefixes', ()):
                yield common_prefix["Prefix"]

    def get_pages(self, bucket_name: str, prefix: str = ""):
        if prefix and not prefix.endswith("/"):
            prefix += "/"
        return self._paginator.paginate(
            Bucket=bucket_name,
            Prefix=prefix,
            Delimiter="/"
        )

# class Sync:
#     def __init__(self,
#                  remote_fs: fsspec.AbstractFileSystem,
#                  remote_path: str,
#                  index_fs: fsspec.AbstractFileSystem,
#                  index_path: str,
#                  num_workers: int = 8):
#         self._remote_fs = remote_fs
#         self._remote_path = remote_path
#         self._index_fs = index_fs
#         self._index_path = index_path
#         self._num_workers = num_workers
#         # self._thread_pool = concurrent.futures.ThreadPoolExecutor(
#         #     max_workers=num_workers
#         # )
#
#     def start(self, remote_path: str, index_path: str):
#         for entry in self._remote_fs.listdir(remote_path):
#             if (entry)
