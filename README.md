# xcube-smos

Experimental support for ESA EE SMOS Level-2 multi-level dataset.

## Installation

Currently, you need to install code and aux-data separately.

### Code

For just the data store

```bash
conda activate xcube
git clone https://github.com/dcs4cop/xcube-smos.git
cd xcube-smos
pip install -ve .
```

If you also need to generate or enhance an existing SMOS Kerchunk Index:

```bash
conda install -c conda-forge kerchunk h5py h5netcdf
nckcidx --help
```

See relevant section on package management in [TODO.md](./TODO.md).

### Aux-data

Aux-data is in the xcube Sharepoint, document folder 
[SMOS_onboarding](https://brockmannconsult.sharepoint.com/:f:/s/xcube/Etp9hOpeXupFt5CWiBnGA1wB3BJ7li1d8F-hDvdMGiKeXA?e=NnxuLx):

Unpack the following:

* `grid-tiles.zip` - SMOS discrete global grid (DGG)
* `nckc-index.zip` - SMOS NetCDF kerchunk index, 2020-01 - 2023-05 subset
 
The latter must be unpacked into a folder `nckc-index`.

Reflect both locations by setting environment variables to the respective 
paths:

* `XCUBE_SMOS_DGG_PATH`
* `XCUBE_SMOS_INDEX_PATH`

See relevant section on aux-data in [TODO.md](./TODO.md).