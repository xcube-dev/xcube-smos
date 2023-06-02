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
    """Manage NetCDF Kerchunk indexes.

    NetCDF Kerchunk index is a directory that contains references to
    NetCDF files stored in S3. It stores a Kerchunk JSON file for
    a given NetCDF file using a directory tree that corresponds
    to the NetCDF file's S3 key prefix (relative to bucket name).

    This form of the NetCDF Kerchunk index is useful for NetCDF files
    that represent datasets that cannot easily be concatenated along
    a given or new dimension, e.g., SMOS Level-2 products.
    """
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
              help=f'S3 endpoint URL.'
                   f' Defaults to "{DEFAULT_ENDPOINT_URL}".')
@click.option('--bucket', nargs=1, metavar='<name>',
              default=DEFAULT_BUCKET_NAME,
              help=f'S3 bucket name.'
                   f' Defaults to "{DEFAULT_BUCKET_NAME}".')
@click.option('--key', nargs=1, metavar='<key>',
              help='S3 access key identifier')
@click.option('--secret', nargs=1, metavar='<secret>',
              help='S3 secret access key')
@click.option('--anon', is_flag=True, default=None,
              help='Force anonymous S3 access.')
def create(ctx,
           index,
           endpoint,
           bucket,
           key,
           secret,
           anon):
    """Create a NetCDF Kerchunk index."""
    from xcube_smos.nckcindex.nckcindex import NcKcIndex
    # click.echo(f"Debug is {'on' if ctx.obj['DEBUG'] else 'off'}")
    s3_options = {
        "anon": anon,
        "key": key,
        "secret": secret,
        "endpoint_url": endpoint,
    }
    nc_kc_index = NcKcIndex.create(
        index_urlpath=index,
        s3_bucket=bucket,
        s3_options={k: v for k, v in s3_options.items() if v is not None}
    )
    print(f"Created empty index {os.path.abspath(nc_kc_index.index_path)}")


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
    num_files, problems = nc_kc_index.sync(prefix=prefix,
                                           force=force,
                                           dry_run=dry_run)
    print(f"{num_files} file(s) synchronized"
          f" in {os.path.abspath(nc_kc_index.index_path)}")
    if problems:
        print(f"{len(problems)} problem(s) encountered:")
        for problem in problems:
            print("  " + problem)


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
