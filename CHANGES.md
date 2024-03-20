## Version 0.3.1 (in development)

* Updated the Jupyter Notebooks in `notebooks` folder and applied 
  [black](https://black.readthedocs.io/) default code style.

## Version 0.3.0

* Added optional geographical bounding box parameter `bbox`. 
  Using a STAC API to filter source files accordingly.
  `bbox` is `None` by default, which means global coverage. (#20)
* Added optional parameter `res_level` in the range from 0 (default) to 4 
  to let users choose their desired spatial resolution. 
  A parameter `spatial_res` is not suitable, 
  because resolutions are provided at fixed levels using the DGG. (#21)
* Added module `smos_box.catalog.stac` with new `SmosStacCatalog`
  that is now the default catalog used by the SMOS data store.
* Removed module `smos_box.catalog.index` including `SmosIndexCatalog` 
  and dropped package `smos_box.nckcindex` entirely.
  We are no longer using a Kerchunk reference file index.

## Version 0.2.2

* Fixed problem where package data in `xcube_smos/mldataset/smos-dgg.levels` 
  did not include any hidden files prefixed with a dot such as `.zattrs`.

## Version 0.2.1

* Adjusted dependencies in `environment.yml` to fix the `xcube-smos` 
  conda build.

## Version 0.2.0

* Added basic documentation. [#24]
* Introduced open parameter `bbox`. [#20]
* Introduced open parameter `res_level`, 
  an integer value in the range 0 to 4. [#21]
* The SMOS auxiliary dataset _Discrete Global Grid_ (DGG)
  has been added as package data in `xcube_smos/mldataset/smos-dgg.levels`.
  Hence, the store parameter `dgg_urlpath` has been removed. [#9]
* The NetCDF Kerchunk index can now also be a Zip archive. 
  To use a Zip archive, pass an index path to the `nckcidx` CLI
  or to the `NcKcIndex`class whose filename has a `.zip`
  extension. [#8]
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
