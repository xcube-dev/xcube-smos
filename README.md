# xcube-smos

Experimental support for ESA EE SMOS Level-2 multi-level dataset.

## Next steps

### Project / product setup

- [] setup CI
- [] setup MkDocs documentation and configure RTD
- [] setup conda-forge deployment
- [] write README.md

### Investigations

- [] For the SMOS data source, i.e., NetCDF L2 files on S3, investigate
  w.r.t. to maximum performance into Kerchunk and/or into own virtual 
  Zarr store. 
- [] Will we provide user-supplied spatial resolutions or are we fine
  providing multi-resolution datasets with fixed spatial resolutions?
- [] Find the best representation of SMOS discrete global grid,
  currently in `~/.snap/auxdata/smos-dgg`. Bundle as aux data in 
  Python package or put on S3? Using original or xcube levels/zarr format?
- [] Check if we can provide the DGG in different projections,
  e.g., LAEA for Europe AOI.
- [] Optional: Check if we should use existing SMOS Level 3 data products 
  (on Creodias?) or dynamically aggregate to Level 3.

### For the existing prototype classes in package `xcube_smos`:

- [] add doc strings 
- [] add unit tests  
- [] add parameter validation
- [] add exception handling
- [] add variable filter (not all are needed)
- [] add scaling/offset/fill value masking to variables 
     (because they must be opened using `decode_cf=False`)
- [x] add new prototype class to form a data cube incl. time dim 
- [] check why we now get numba error in `smos/l2prod.py`, 
     see `TODO` in there

### Develop the xcube data store "smos" 

- [] from investigations above specify mode of operation as well as 
  store, search, and open parameters
- [] derive new data store from `xcube.core.store.DataStore` that make use
  of existing prototype classes
- [] test xcube server with "smos" data store
- [] write documentation
- [] update example notebooks, also put one in `xcube/examples`
