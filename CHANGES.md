## Version 0.2.0 (in development)

* Storage credentials are no longer stored in configuration file. 
  Instead, they can be provided by environment variables `X` any `Y`,
  or passed to the functions as keyword arguments `x` and `y`. 
  The latter take precedence over environment variables. [#6]

## Version 0.1.0

Initial release.

* Access to SMOS data via xcube data store interface

* nckcindex tool for management of SMOS Kerchunk indices
