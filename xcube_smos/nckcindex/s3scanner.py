from typing import Iterator

import boto3
import botocore.paginate


class S3Scanner:
    """
    A low-level S3 scanner.
    Implements a much faster listing of keys than using s3fs.S3FileSystem.

    See https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html#boto3.session.Session.client
    See https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    """

    def __init__(self, **s3_options):
        boto3_kwargs = self._to_boto3_kwargs(s3_options)
        self._client = boto3.client('s3', **boto3_kwargs)
        self._paginator = self._client.get_paginator('list_objects_v2')

    def get_keys(self,
                 bucket: str,
                 prefix: str = "",
                 suffix: str = "") -> Iterator[str]:
        """Get bucket object keys only. Will never return prefixes."""
        for page in self.get_pages(bucket, prefix=prefix):
            for common_prefix in page.get('CommonPrefixes', ()):
                yield from self.get_keys(bucket,
                                         prefix=common_prefix["Prefix"],
                                         suffix=suffix)
            for content in page.get('Contents', ()):
                key = content["Key"]
                if (suffix and key.endswith(suffix)) \
                        or not key.endswith('/'):
                    yield key

    def get_prefixes(self,
                     bucket: str,
                     prefix: str = "") -> Iterator[str]:
        """Get bucket prefixes only. Will never return object keys."""
        for page in self.get_pages(bucket, prefix=prefix):
            for common_prefix in page.get('CommonPrefixes', ()):
                yield common_prefix["Prefix"]

    def get_pages(self,
                  bucket: str,
                  prefix: str = "") -> botocore.paginate.PageIterator:
        """Get bucket contents as pages."""
        if prefix and not prefix.endswith("/"):
            prefix += "/"
        return self._paginator.paginate(
            Bucket=bucket,
            Prefix=prefix,
            Delimiter="/"
        )

    @classmethod
    def _to_boto3_kwargs(cls, s3_options):
        """Convert fsspec S3 storage options into boto3 client kwargs."""
        anon = s3_options.pop("anon", None)
        key = s3_options.pop("key", None)
        secret = s3_options.pop("secret", None)
        token = s3_options.pop("token", None)
        endpoint_url = s3_options.pop("endpoint_url", None)
        region_name = s3_options.pop("region_name", None)
        client_kwargs = s3_options.pop("client_kwargs", {})
        if endpoint_url is None:
            endpoint_url = client_kwargs.pop("endpoint_url", None)
        if region_name is None:
            region_name = client_kwargs.pop("region_name", None)
        return {
            k: v for k, v in dict(
                aws_access_key_id=key if not anon else None,
                aws_secret_access_key=secret if not anon else None,
                aws_session_token=token if not anon else None,
                endpoint_url=endpoint_url,
                region_name=region_name,
                **client_kwargs
            ).items()
            if v is not None
        }
