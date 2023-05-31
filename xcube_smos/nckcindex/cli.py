import os

import click

from xcube_smos.nckcindex.constants import DEFAULT_BUCKET_NAME
from xcube_smos.nckcindex.constants import DEFAULT_ENDPOINT_URL
from xcube_smos.nckcindex.constants import DEFAULT_INDEX_NAME


@click.group()
@click.option('--debug', is_flag=True,
              help='Output verbose debugging information.'
                   ' NOT IMPLEMENTED YET.')
@click.option('--traceback', is_flag=True,
              help='Output stack traceback on errors.'
                   ' NOT IMPLEMENTED YET.')
@click.pass_context
def cli(ctx, debug, traceback):
    ctx.ensure_object(dict)
    ctx.obj['DEBUG'] = debug
    ctx.obj['TRACEBACK'] = traceback


@cli.command()
@click.pass_context
@click.option('--index', nargs=1, metavar='<path>',
              default=DEFAULT_INDEX_NAME,
              help=f'Local index directory path. Must not exist.'
                   f' Defaults to "{DEFAULT_INDEX_NAME}".')
@click.option('--endpoint', nargs=1, metavar='<url>',
              default=DEFAULT_ENDPOINT_URL,
              help=f'S3 endpoint URL. Defaults to "{DEFAULT_ENDPOINT_URL}".')
@click.option('--bucket', nargs=1, metavar='<name>',
              default=DEFAULT_BUCKET_NAME,
              help=f'S3 bucket name. Defaults to "{DEFAULT_BUCKET_NAME}".')
@click.option('--key', nargs=1, metavar='<key>',
              help='S3 access key identifier')
@click.option('--secret', nargs=1, metavar='<secret>',
              help='S3 secret access key')
def create(ctx,
           index,
           endpoint,
           bucket,
           key,
           secret):
    """Create a NetCDF Kerchunk index."""
    from xcube_smos.nckcindex.nckcindex import NcKcIndex
    # click.echo(f"Debug is {'on' if ctx.obj['DEBUG'] else 'off'}")
    NcKcIndex.create(
        index_urlpath=index,
        s3_bucket=bucket,
        s3_options={
            "endpoint_url": endpoint,
            "key": key or os.environ.get("AWS_ACCESS_KEY_IDENTIFIER"),
            "secret": secret or os.environ.get("AWS_SECRET_ACCESS_KEY"),
        })


@cli.command()
@click.option('--index', nargs=1, metavar='<path>',
              default=DEFAULT_INDEX_NAME,
              help=f'Local index directory path. Must exist.'
                   f' Defaults to "{DEFAULT_INDEX_NAME}".')
@click.option('--prefix', metavar='<path>',
              help='S3 key prefix')
@click.option('--dry-run', is_flag=True,
              help='Do not write any files.')
@click.pass_context
def sync(ctx,
         index,
         prefix,
         dry_run):
    """Synchronize a NetCDF Kerchunk index."""
    from xcube_smos.nckcindex.nckcindex import NcKcIndex
    # click.echo(f"Debug is {'on' if ctx.obj['DEBUG'] else 'off'}")
    nc_file_index = NcKcIndex.open(index_urlpath=index)
    nc_file_index.sync(prefix=prefix, dry_run=dry_run)


if __name__ == '__main__':
    cli(obj={})
