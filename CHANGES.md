## Version 0.2.0 (in development)

* Storage credentials are no longer stored in configuration file. 
  Instead, they can be provided by environment variables `X` any `Y`,
  or passed to the functions as keyword arguments `x` and `y`. 
  The latter take precedence over environment variables. [#6]
* Source NetCDF files to be indexed can now also be provided in the local 
  filesystem instead of S3 only. [#4]
  - Changed JSON format of the index configuration to allow for any 
    source filesystem. Its version number is now 2.
  - Changed interface of `nckcindex` CLI tool to allow for local
    source NetCDF files.
* CLI `nckcidx info` has been renamed to `nckcidx describe`.
* Argument `index_path` for `NcKcIndex.create()` and `NcKcIndex.open()`
  is now mandatory.

## Version 0.1.0

Initial release featuring

* Access to SMOS data via xcube data store interface.
* `nckcindex` CLI tool for management of SMOS Kerchunk indices.
