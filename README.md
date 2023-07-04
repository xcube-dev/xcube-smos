# xcube-smos data store

xcube-smos is a
[data store plug-in](https://xcube.readthedocs.io/en/latest/api.html#data-store-framework)
for [xcube](https://xcube.readthedocs.io/), providing experimental support for
[ESA EE SMOS](https://www.esa.int/Applications/Observing_the_Earth/FutureEO/SMOS)
Level-2 multi-level datasets.

## Installation

xcube-smos consists of two parts: the code itself and the auxiliary data
(aux-data). The auxiliary data comprises a mapping between the SMOS native
discrete global grid (DGG) and a standard WGS84 CRS, and a pre-generated
data index which enables the plug-in to fetch required subsets of the data
directly from the public SMOS data files.

Currently, you need to install code and aux-data separately.

### Code installation

#### For users

xcube-smos end users can install cxube-smos directly from its git repository
into an xcube environment created with
[mamba](https://mamba.readthedocs.io/en/latest/installation.html)
(recommended) or
[conda](https://docs.conda.io/en/latest/miniconda.html).

```bash
mamba create -n xcube -c conda-forge xcube
mamba activate xcube
git clone https://github.com/dcs4cop/xcube-smos.git
cd xcube-smos
pip install --verbose --no-deps --editable .
```

#### For index maintainers

For ordinary xcube-smos users, the instructions above are sufficient to
install the necessary software packages. Index maintainers who also need to
generate or enhance an existing SMOS Kerchunk index should also install
some additional packages:

```bash
mamba activate xcube
mamba install -c conda-forge kerchunk h5py h5netcdf
nckcidx --help
```

See [relevant section](./TODO.md#setup-project--product) on package management in [TODO.md](./TODO.md).

### Aux-data

Aux-data is in the xcube Sharepoint, document folder 
[SMOS_onboarding](https://brockmannconsult.sharepoint.com/:f:/s/xcube/Etp9hOpeXupFt5CWiBnGA1wB3BJ7li1d8F-hDvdMGiKeXA?e=NnxuLx):

Unpack the following:

* `grid-tiles.zip` – SMOS discrete global grid (DGG)
* `nckc-index.zip` – SMOS NetCDF kerchunk index, 2020-01 – 2023-05 subset
 
The latter must be unpacked into a folder `nckc-index`.

Set the following environments to the paths to which the zip files were
unpacked, in order to allow xcube-smos to find them:

* `XCUBE_SMOS_DGG_PATH`
* `XCUBE_SMOS_INDEX_PATH`

See [relevant section](./TODO.md#provide-aux-data) on aux-data in
[TODO.md](./TODO.md).
