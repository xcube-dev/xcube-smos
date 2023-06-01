
* Some files seems to be corrupt or h5py is broken:
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
  Sometimes syncing once more helps. Sometimes not, for example
  - `SMOS/L2SM/MIR_SMUDP2/2021/02/14/.../SM_OPER_MIR_SMUDP2_20210214T101242_20210214T110603_650_001_1.nc`
  - `SMOS/L2OS/MIR_OSUDP2/2021/02/14/.../SM_OPER_MIR_OSUDP2_20210214T101242_20210214T110603_662_001_1.nc`
  We need to catch error and continue in such cases.

* I sometimes get S3 timeouts while indexing. If persists, consider
  ``retry`` in advanced boto configuration `botocore.config.Config`. 
  See https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html