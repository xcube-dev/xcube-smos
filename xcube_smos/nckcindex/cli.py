import os

import click

from xcube_smos.nckcindex.constants import S3_PROTOCOL
from xcube_smos.nckcindex.constants import DEFAULT_SOURCE_PROTOCOL
from xcube_smos.nckcindex.constants import EXAMPLE_S3_BUCKET
from xcube_smos.nckcindex.constants import EXAMPLE_S3_ENDPOINT_URL
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

    A NetCDF Kerchunk index is a directory that contains references to
    NetCDF files stored in some archive. For every NetCDF file, a
    Kerchunk JSON file is created and saved in a corresponding
    directory tree relative to a given prefix.

    This form of the NetCDF Kerchunk index is useful for NetCDF files
    that represent datasets that cannot easily be concatenated along
    a given or new dimension, e.g., SMOS Level-2 products.
    """
    ctx.ensure_object(dict)
    ctx.obj['DEBUG'] = debug
    ctx.obj['TRACEBACK'] = traceback


@cli.command()
@click.pass_context
@click.option('--index', 'index_path', nargs=1, metavar='<path>',
              default=DEFAULT_INDEX_NAME,
              help=f'Local index directory path. Must not exist.'
                   f' Defaults to "{DEFAULT_INDEX_NAME}".')
@click.option('--source', 'source_path', nargs=1, metavar='<path>',
              help=f'Source directory path or URL.')
@click.option('--source-protocol', nargs=1, metavar='<path>',
              help=f'Source protocol,'
                   f' for example "{S3_PROTOCOL}".'
                   f' If not given, derived if source is a URL,'
                   f' otherwise it defaults to'
                   f' "{DEFAULT_SOURCE_PROTOCOL}".')
@click.option('--s3-endpoint', nargs=1, metavar='<url>',
              help=f'S3 endpoint URL if source protocol is'
                   f' "{S3_PROTOCOL}",'
                   f' for example "{EXAMPLE_S3_ENDPOINT_URL}".')
@click.option('--s3-bucket', nargs=1, metavar='<name>',
              help=f'S3 bucket name if source protocol is'
                   f' "{S3_PROTOCOL}",'
                   f' for example "{EXAMPLE_S3_BUCKET}".')
@click.option('--s3-key', nargs=1, metavar='<key>',
              help='S3 access key identifier if source protocol is'
                   f' "{S3_PROTOCOL}".')
@click.option('--s3-secret', nargs=1, metavar='<secret>',
              help='S3 secret access key if source protocol is'
                   f' "{S3_PROTOCOL}".')
@click.option('--s3-anon', is_flag=True, default=None,
              help='Force anonymous S3 access if source protocol is'
                   f' "{S3_PROTOCOL}".')
def create(ctx,
           index_path,
           source_path,
           source_protocol,
           s3_endpoint,
           s3_bucket,
           s3_key,
           s3_secret,
           s3_anon):
    """Create a NetCDF Kerchunk index."""
    from xcube_smos.nckcindex.nckcindex import NcKcIndex
    if not source_protocol and source_path:
        import fsspec
        source_protocol, _ = fsspec.core.split_protocol(source_path)
    # click.echo(f"Debug is {'on' if ctx.obj['DEBUG'] else 'off'}")
    if source_protocol == S3_PROTOCOL:
        s3_storage_options = {
            "anon": s3_anon,
            "key": s3_key,
            "secret": s3_secret,
            "endpoint_url": s3_endpoint,
        }
        source_path = \
            f"{source_path}/{s3_bucket}" if source_path and s3_bucket\
                else source_path if source_path \
                else s3_bucket
        source_storage_options = {k: v for k, v in s3_storage_options.items()
                                  if v is not None}
    else:
        source_storage_options = {}
    if not source_path:
        raise click.UsageError('Option --source must be given')
    index = NcKcIndex.create(
        index_path=index_path,
        source_path=source_path,
        source_storage_options=source_storage_options
    )
    print(f"Created empty index {os.path.abspath(index.index_path)}")


@cli.command()
@click.option('--index', 'index_path', nargs=1, metavar='<path>',
              default=DEFAULT_INDEX_NAME,
              help=f'Local index directory path. Must exist.'
                   f' Defaults to "{DEFAULT_INDEX_NAME}".')
@click.option('--prefix', 'prefix_path', metavar='<path>',
              help='Source prefix path')
@click.option('--force', is_flag=True,
              help='Do not skip existing indexes.')
@click.option('--dry-run', is_flag=True,
              help='Do not create any indexes.')
@click.pass_context
def sync(ctx,
         index_path,
         prefix_path,
         force,
         dry_run):
    """Synchronize a NetCDF Kerchunk index."""
    from xcube_smos.nckcindex.nckcindex import NcKcIndex
    # click.echo(f"Debug is {'on' if ctx.obj['DEBUG'] else 'off'}")
    nc_kc_index = NcKcIndex.open(index_path=index_path)
    num_files, problems = nc_kc_index.sync(prefix=prefix_path,
                                           force=force,
                                           dry_run=dry_run)
    print(f"{num_files} file(s) synchronized"
          f" in {os.path.abspath(nc_kc_index.index_path)}")
    if problems:
        print(f"{len(problems)} problem(s) encountered:")
        for problem in problems:
            print("  " + problem)


@cli.command()
@click.option('--index', 'index_path',
              nargs=1, metavar='<path>',
              default=DEFAULT_INDEX_NAME,
              help=f'Local index directory path. Must exist.'
                   f' Defaults to "{DEFAULT_INDEX_NAME}".')
@click.pass_context
def info(ctx, index_path):
    """Inform about a NetCDF Kerchunk index."""
    from xcube_smos.nckcindex.nckcindex import NcKcIndex
    # click.echo(f"Debug is {'on' if ctx.obj['DEBUG'] else 'off'}")
    index = NcKcIndex.open(index_path=index_path)
    print(f"Index path: {os.path.abspath(index.index_path)}")
    print(f"Source path: {index.source_path}")
    print(f"Source protocol: {index.source_protocol}")
    if index.source_storage_options:
        print(f"Source storage options:")
        for k, v in index.source_storage_options.items():
            print(f"  {k}: {'*****' if v in ('key', 'secret') else v}")
    else:
        print(f"Source storage options: <none>")
    if index.prefixes:
        print("Prefixes:")
        for k, v in index.prefixes.items():
            print(f"  {k}: {v}")
    else:
        print("Prefixes: <none>")


if __name__ == '__main__':
    cli(obj={})
