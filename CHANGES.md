## Version 0.2.0 (in development)

* Storage credentials and other text values in the JSON configuration 
  file of the NetCDF Kerchunk index can now contain template
  variables of the form `"$ENV"` or `"${ENV}"`. Such references 
  will be replaced by the corresponding value of the environment 
  variable named `ENV`. [#6]
* Source NetCDF files to be indexed can now also be provided in the local 
  filesystem instead of S3 only. [#4]
  - Changed JSON format of the index configuration to allow for any 
    source filesystem. Its version number is now 2.
  - Changed interface of `nckcindex` CLI tool to allow for local
    source NetCDF files.
* CLI `nckcidx info` has been renamed to `nckcidx describe` and now 
  supports a `--json` flag for JSON output.
* Argument `index_path` for `NcKcIndex.create()` and `NcKcIndex.open()`
  is now mandatory.

## Version 0.1.0

Initial release featuring

* Access to SMOS data via xcube data store interface.
* `nckcindex` CLI tool for management of SMOS Kerchunk indices.
