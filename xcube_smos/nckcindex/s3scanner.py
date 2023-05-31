from typing import Optional, Iterator

import boto3
import botocore.paginate


class S3Scanner:
    """
    A low-level S3 scanner.
    Way more performant than listing keys with s3fs.S3FileSystem.
    """

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

    def get_keys(self,
                 bucket_name: str,
                 prefix: str = "",
                 suffix: str = "") -> Iterator[str]:
        """Get bucket object keys only. Will never return prefixes."""
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

    def get_prefixes(self,
                     bucket_name: str,
                     prefix: str = "") -> Iterator[str]:
        """Get bucket prefixes only. Will never return object keys."""
        for page in self.get_pages(bucket_name, prefix=prefix):
            for common_prefix in page.get('CommonPrefixes', ()):
                yield common_prefix["Prefix"]

    def get_pages(self,
                  bucket_name: str,
                  prefix: str = "") -> botocore.paginate.PageIterator:
        """Get bucket contents as pages."""
        if prefix and not prefix.endswith("/"):
            prefix += "/"
        return self._paginator.paginate(
            Bucket=bucket_name,
            Prefix=prefix,
            Delimiter="/"
        )
