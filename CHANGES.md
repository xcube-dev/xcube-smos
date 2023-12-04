## Version 0.2.0 (in development)

* CLI `nckcidx info` has been renamed to `nckcidx describe`.
* `index_path` is now a mandatory argument for `NcKcIndex.create()` 
  and `NcKcIndex.open()`.
* Source NetCDF files to be index can now also be provided in the local 
  filesystem instead of S3 only. [#4]
  - Changed JSON format of the index configuration to allow for any 
    source filesystem. Its version number is now 2.
  - Changed interface of `nckcindex` CLI tool to allow for local
    source NetCDF files.

## Version 0.1.0

Initial release featuring

* Access to SMOS data via xcube data store interface.
* `nckcindex` CLI tool for management of SMOS Kerchunk indices.
