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
    nc_kc_index = NcKcIndex.create(
        index_urlpath=index,
        s3_bucket=bucket,
        s3_options={
            "endpoint_url": endpoint,
            "key": key or os.environ.get("AWS_ACCESS_KEY_IDENTIFIER"),
            "secret": secret or os.environ.get("AWS_SECRET_ACCESS_KEY"),
        })
    print(f"Created {nc_kc_index.index_path}")


@cli.command()
@click.option('--index', nargs=1, metavar='<path>',
              default=DEFAULT_INDEX_NAME,
              help=f'Local index directory path. Must exist.'
                   f' Defaults to "{DEFAULT_INDEX_NAME}".')
@click.option('--prefix', metavar='<path>',
              help='S3 key prefix')
@click.option('--force', is_flag=True,
              help='Do not skip existing indexes.')
@click.option('--dry-run', is_flag=True,
              help='Do not create any indexes.')
@click.pass_context
def sync(ctx,
         index,
         prefix,
         force,
         dry_run):
    """Synchronize a NetCDF Kerchunk index."""
    from xcube_smos.nckcindex.nckcindex import NcKcIndex
    # click.echo(f"Debug is {'on' if ctx.obj['DEBUG'] else 'off'}")
    nc_kc_index = NcKcIndex.open(index_urlpath=index)
    num_files = nc_kc_index.sync(prefix=prefix, force=force, dry_run=dry_run)
    print(f"{num_files} file(s) synchronized in {nc_kc_index.index_path}")


@cli.command()
@click.option('--index', nargs=1, metavar='<path>',
              default=DEFAULT_INDEX_NAME,
              help=f'Local index directory path. Must exist.'
                   f' Defaults to "{DEFAULT_INDEX_NAME}".')
@click.pass_context
def info(ctx, index):
    """Inform about a NetCDF Kerchunk index."""
    from xcube_smos.nckcindex.nckcindex import NcKcIndex
    # click.echo(f"Debug is {'on' if ctx.obj['DEBUG'] else 'off'}")
    nc_kc_index = NcKcIndex.open(index_urlpath=index)
    print(f"Index path: {os.path.abspath(nc_kc_index.index_path)}")
    print(f"S3 endpoint URL: {nc_kc_index.s3_endpoint_url}")
    print(f"S3 bucket: {nc_kc_index.s3_bucket}")
    if nc_kc_index.s3_prefixes:
        print("S3 prefixes:")
        for k, v in nc_kc_index.s3_prefixes.items():
            print(f"  {k}: {v}")
    else:
        print("No defined S3 prefixes.")


if __name__ == '__main__':
    cli(obj={})
