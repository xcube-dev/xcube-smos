## Setup project / product

* setup CI
* setup MkDocs documentation and configure RTD
* setup conda-forge + PyPI deployment
* write README.md

### Investigate

- [x] For the SMOS data source, i.e., NetCDF L2 files on S3, investigate
  w.r.t. to maximum performance into Kerchunk and/or into own virtual 
  Zarr store.  **DONE: Using combination of a SMOS NetCDF Kerchunk index and 
  a virtual Zarr Store for the cube**
- [ ] Will we provide user-supplied spatial resolutions or are we fine
  providing multi-resolution datasets with fixed spatial resolutions?
  **Pending: Currently, the SMOS data store can provide multi-level datasets
  with fixed resolutions or a max. resolution xarray dataset (level 0)**
- [ ] Find the best representation of SMOS discrete global grid,
  currently in `~/.snap/auxdata/smos-dgg`. Bundle as aux data in 
  Python package or put on S3? Using original or xcube levels/zarr format?
  **Pending: see section below.**
- [ ] Check if we can provide the DGG in different projections,
  e.g., LAEA for Europe AOI.
  **Pending: that should be possible by transforming the DGG for the desired
  projection and AOI.**

## Fix concurrent processing

We currently cannot use `dask.distributed` at all neither using
`dask.distributed.Client(processes=True)` nor 
`dask.distributed.Client(processes=False)`. Once we compute the dataset 
returned from the data store (e.g. `dataset.compute()`, 
`dataset.to_zarr("smos.zarr")`, 
`dataset.Soil_Moisture.isel(time=0).plot.imshow()`) DAG tasks are being 
executed first but then quickly become idle and computation pauses.


## Update SMOS NetCDF Kerchunk index

The current index comprises 2020 to 2023-05.

Consider indexing on a Creodias VM.

1. Generate indexes for the years 2010 to 2019
2. Update 2023-05+. 


## Provide aux-data

We need way(s) to provide required aux-data for the xcube data store `smos`:

1. SMOS discrete global grid data. We currently rely on a version installed 
   by SNAP plugin *SMOS-Box* in `~/.snap/auxdata/smos-dgg/grid-tiles`.
   We currently set its path via an 
   environment variable `XCUBE_SMOS_DGG_PATH`. 
2. SMOS NetCDF Kerchunk index. We currently set its path via an 
   environment variable `XCUBE_SMOS_INDEX_PATH`. 

Here are a number of options. They could also be provided in combination. 

* Put aux-data in publicly available S3 bucket (slower).
* Provide installable aux-data package in conda/pip (faster).
* Bundle aux-data with container image.
* Put aux-data in publicly available FTP for download and configure store
  via env var `XCUBE_SMOS_INDEX_PATH`.


## Split repo

The `nckcidx` CLI has package dependencies that are not needed by the 
xcube data store `smos`. Therefore, consider splitting the two: 

* `smos-nckcidx`: Currently in `xcube_smos.nckcindex.*` with dependencies
   `kerchunk`, `h3netcdf`, `h5py`.
* `xcube-smos`: Currently `xcube_smos.*` with dependency: `xcube`.

It is also possible to have two packages build from one repo: Just have the 
two top level directories `smos-nckcidx`, `xcube-smos` with own setup info
each.

## Fix issues with the SMOS NetCDF Kerchunk index

* The index requires daily updating, need a nightly service that calls
  `nckcidx` tool.
* The index is a directory with 250,000+ files, several GB in size. 
  However, it compresses well, therefore consider a single Zip archive 
  or annual/monthly Zip archives. Note, a monthly Zip archive can be 
  updated more efficiently. 
* Currently, a SMOS NetCDF Kerchunk index contains a file `nckc-index.json`
  that also contains the Creodias S3 credentials for EDC user.
  **THIS IS INSECURE**. Switch to dedicated env vars or AWS profile instead.

### fsspec.exceptions.FSTimeoutError

I sometimes get S3 timeouts while indexing:

```
  ...
  File "d:\projects\xcube-smos\xcube_smos\nckcindex\nckcindex.py", line 204, in index_nc_file
    chunks = kerchunk.hdf.SingleHdf5ToZarr(
  File "C:\Users\Norman\mamba\envs\xcube\lib\site-packages\kerchunk\hdf.py", line 92, in __init__
    self._h5f = h5py.File(self.input_file, mode="r")
  File "C:\Users\Norman\mamba\envs\xcube\lib\site-packages\h5py\_hl\files.py", line 567, in __init__
    fid = make_fid(name, mode, userblock_size, fapl, fcpl, swmr=swmr)
  File "C:\Users\Norman\mamba\envs\xcube\lib\site-packages\h5py\_hl\files.py", line 231, in make_fid
    fid = h5f.open(name, flags, fapl=fapl)
  ...
  File "C:\Users\Norman\mamba\envs\xcube\lib\site-packages\s3fs\core.py", line 2156, in _fetch_range
    return _fetch_range(
  File "C:\Users\Norman\mamba\envs\xcube\lib\site-packages\s3fs\core.py", line 2329, in _fetch_range
    return sync(fs.loop, _inner_fetch, fs, bucket, key, version_id, start, end, req_kw)
  File "C:\Users\Norman\mamba\envs\xcube\lib\site-packages\fsspec\asyn.py", line 98, in sync
    raise FSTimeoutError from return_result
fsspec.exceptions.FSTimeoutError
```

If this becomes an issue, consider ``retry`` in advanced boto configuration
`botocore.config.Config`. See 
https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html

### OSError: Unable to open file (file signature not found)

Some files seem to be corrupt or h5py is broken:

```
  ...
  File "d:\projects\xcube-smos\xcube_smos\nckcindex\nckcindex.py", line 196, in index_nc_file
    chunks = kerchunk.hdf.SingleHdf5ToZarr(
  File "C:\Users\Norman\mamba\envs\xcube\lib\site-packages\kerchunk\hdf.py", line 92, in __init__
    self._h5f = h5py.File(self.input_file, mode="r")
  File "C:\Users\Norman\mamba\envs\xcube\lib\site-packages\h5py\_hl\files.py", line 567, in __init__
    fid = make_fid(name, mode, userblock_size, fapl, fcpl, swmr=swmr)
  File "C:\Users\Norman\mamba\envs\xcube\lib\site-packages\h5py\_hl\files.py", line 231, in make_fid
    fid = h5f.open(name, flags, fapl=fapl)
  File "h5py\_objects.pyx", line 54, in h5py._objects.with_phil.wrapper
  File "h5py\_objects.pyx", line 55, in h5py._objects.with_phil.wrapper
  File "h5py\h5f.pyx", line 106, in h5py.h5f.open
OSError: Unable to open file (file signature not found)
```

Sometimes syncing once more helps. Sometimes not, for example, the following 
are not recoverable:

```commandline
$ nckcidx sync --prefix SMOS/L2SM/MIR_SMUDP2/2021
...
9783 file(s) synchronized in D:\nckc-index
12 problem(s) encountered:
  Error creating index s3://EODATA/SMOS/L2SM/MIR_SMUDP2/2021/02/14/SM_OPER_MIR_SMUDP2_20210214T101242_20210214T110603_650_001_1/SM_OPER_MIR_SMUDP2_20210214T101242_20210214T110603_650_001_1.nc: Unable to open file (file signature not found)
  Error creating index s3://EODATA/SMOS/L2SM/MIR_SMUDP2/2021/02/18/SM_OPER_MIR_SMUDP2_20210218T082704_20210218T092019_650_001_1/SM_OPER_MIR_SMUDP2_20210218T082704_20210218T092019_650_001_1.nc: Unable to open file (file signature not found)
  Error creating index s3://EODATA/SMOS/L2SM/MIR_SMUDP2/2021/03/14/SM_OPER_MIR_SMUDP2_20210314T043259_20210314T052612_650_001_1/SM_OPER_MIR_SMUDP2_20210314T043259_20210314T052612_650_001_1.nc: Unable to open file (file signature not found)
  Error creating index s3://EODATA/SMOS/L2SM/MIR_SMUDP2/2021/03/14/SM_OPER_MIR_SMUDP2_20210314T052255_20210314T061615_650_002_1/SM_OPER_MIR_SMUDP2_20210314T052255_20210314T061615_650_002_1.nc: Unable to open file (file signature not found)
  Error creating index s3://EODATA/SMOS/L2SM/MIR_SMUDP2/2021/03/14/SM_OPER_MIR_SMUDP2_20210314T061304_20210314T070617_650_001_1/SM_OPER_MIR_SMUDP2_20210314T061304_20210314T070617_650_001_1.nc: Unable to open file (file signature not found)
  Error creating index s3://EODATA/SMOS/L2SM/MIR_SMUDP2/2021/03/21/SM_OPER_MIR_SMUDP2_20210321T141053_20210321T150412_650_001_1/SM_OPER_MIR_SMUDP2_20210321T141053_20210321T150412_650_001_1.nc: Unable to open file (file signature not found)
  Error creating index s3://EODATA/SMOS/L2SM/MIR_SMUDP2/2021/03/21/SM_OPER_MIR_SMUDP2_20210321T132056_20210321T141410_650_001_1/SM_OPER_MIR_SMUDP2_20210321T132056_20210321T141410_650_001_1.nc: Unable to open file (file signature not found)
  Error creating index s3://EODATA/SMOS/L2SM/MIR_SMUDP2/2021/03/21/SM_OPER_MIR_SMUDP2_20210321T155058_20210321T164417_650_001_1/SM_OPER_MIR_SMUDP2_20210321T155058_20210321T164417_650_001_1.nc: Unable to open file (file signature not found)
  Error creating index s3://EODATA/SMOS/L2SM/MIR_SMUDP2/2021/03/21/SM_OPER_MIR_SMUDP2_20210321T155058_20210321T164417_650_002_1/SM_OPER_MIR_SMUDP2_20210321T155058_20210321T164417_650_002_1.nc: Unable to open file (file signature not found)
  Error creating index s3://EODATA/SMOS/L2SM/MIR_SMUDP2/2021/03/21/SM_OPER_MIR_SMUDP2_20210321T141053_20210321T150412_650_002_1/SM_OPER_MIR_SMUDP2_20210321T141053_20210321T150412_650_002_1.nc: Unable to open file (file signature not found)
  Error creating index s3://EODATA/SMOS/L2SM/MIR_SMUDP2/2021/03/21/SM_OPER_MIR_SMUDP2_20210321T132056_20210321T141410_650_002_1/SM_OPER_MIR_SMUDP2_20210321T132056_20210321T141410_650_002_1.nc: Unable to open file (file signature not found)
  Error creating index s3://EODATA/SMOS/L2SM/MIR_SMUDP2/2021/03/21/SM_OPER_MIR_SMUDP2_20210321T150100_20210321T155415_650_001_1/SM_OPER_MIR_SMUDP2_20210321T150100_20210321T155415_650_001_1.nc: Unable to open file (file signature not found)
```

and

```commandline
$ nckcidx sync --prefix SMOS/L2OS/MIR_OSUDP2/2021
...
9777 file(s) synchronized in D:\nckc-index
12 problem(s) encountered:
  Error creating index s3://EODATA/SMOS/L2OS/MIR_OSUDP2/2021/02/14/SM_OPER_MIR_OSUDP2_20210214T101242_20210214T110603_662_001_1/SM_OPER_MIR_OSUDP2_20210214T101242_20210214T110603_662_001_1.nc: Unable to open file (file signature not found)
  Error creating index s3://EODATA/SMOS/L2OS/MIR_OSUDP2/2021/02/18/SM_OPER_MIR_OSUDP2_20210218T082704_20210218T092019_662_001_1/SM_OPER_MIR_OSUDP2_20210218T082704_20210218T092019_662_001_1.nc: Unable to open file (file signature not found)
  Error creating index s3://EODATA/SMOS/L2OS/MIR_OSUDP2/2021/03/14/SM_OPER_MIR_OSUDP2_20210314T052255_20210314T061615_662_002_1/SM_OPER_MIR_OSUDP2_20210314T052255_20210314T061615_662_002_1.nc: Unable to open file (file signature not found)
  Error creating index s3://EODATA/SMOS/L2OS/MIR_OSUDP2/2021/03/14/SM_OPER_MIR_OSUDP2_20210314T043259_20210314T052612_662_001_1/SM_OPER_MIR_OSUDP2_20210314T043259_20210314T052612_662_001_1.nc: Unable to open file (file signature not found)
  Error creating index s3://EODATA/SMOS/L2OS/MIR_OSUDP2/2021/03/14/SM_OPER_MIR_OSUDP2_20210314T061304_20210314T070617_662_001_1/SM_OPER_MIR_OSUDP2_20210314T061304_20210314T070617_662_001_1.nc: Unable to open file (file signature not found)
  Error creating index s3://EODATA/SMOS/L2OS/MIR_OSUDP2/2021/03/21/SM_OPER_MIR_OSUDP2_20210321T141053_20210321T150412_662_001_1/SM_OPER_MIR_OSUDP2_20210321T141053_20210321T150412_662_001_1.nc: Unable to open file (file signature not found)
  Error creating index s3://EODATA/SMOS/L2OS/MIR_OSUDP2/2021/03/21/SM_OPER_MIR_OSUDP2_20210321T132056_20210321T141410_662_001_1/SM_OPER_MIR_OSUDP2_20210321T132056_20210321T141410_662_001_1.nc: Unable to open file (file signature not found)
  Error creating index s3://EODATA/SMOS/L2OS/MIR_OSUDP2/2021/03/21/SM_OPER_MIR_OSUDP2_20210321T155058_20210321T164417_662_001_1/SM_OPER_MIR_OSUDP2_20210321T155058_20210321T164417_662_001_1.nc: Unable to open file (file signature not found)
  Error creating index s3://EODATA/SMOS/L2OS/MIR_OSUDP2/2021/03/21/SM_OPER_MIR_OSUDP2_20210321T150100_20210321T155415_662_001_1/SM_OPER_MIR_OSUDP2_20210321T150100_20210321T155415_662_001_1.nc: Unable to open file (file signature not found)
  Error creating index s3://EODATA/SMOS/L2OS/MIR_OSUDP2/2021/03/21/SM_OPER_MIR_OSUDP2_20210321T132056_20210321T141410_662_002_1/SM_OPER_MIR_OSUDP2_20210321T132056_20210321T141410_662_002_1.nc: Unable to open file (file signature not found)
  Error creating index s3://EODATA/SMOS/L2OS/MIR_OSUDP2/2021/03/21/SM_OPER_MIR_OSUDP2_20210321T155058_20210321T164417_662_002_1/SM_OPER_MIR_OSUDP2_20210321T155058_20210321T164417_662_002_1.nc: Unable to open file (file signature not found)
  Error creating index s3://EODATA/SMOS/L2OS/MIR_OSUDP2/2021/03/21/SM_OPER_MIR_OSUDP2_20210321T141053_20210321T150412_662_002_1/SM_OPER_MIR_OSUDP2_20210321T141053_20210321T150412_662_002_1.nc: Unable to open file (file signature not found)
```

