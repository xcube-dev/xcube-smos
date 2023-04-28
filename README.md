# xcube-smos

Experimental support for ESA EE SMOS Level-2 multi-level dataset.

Next steps:

- setup MkDocs documentation and configure RTD
- setup conda-forge deployment
- for SMOS NetCDF files on S3, investigate
  - into Kerchunk 
  - into own virtual Zarr store 
- find best representation of SMOS data as datacubes, consider
  - existing SMOS Level 2C/3 data products
  - dynamically aggregated L3
  - multi-resolution datasets and/or individual spatial resolutions
- develop xcube data store "smos" as xcube plugin
- test xcube server with "smos" data store
- write documentation
- update example notebooks
