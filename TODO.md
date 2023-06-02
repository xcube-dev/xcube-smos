
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

Some files seems to be corrupt or h5py is broken:

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

