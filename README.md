# xcube-smos

_User-defined datacubes from SMOS Level-2 data_

[![CI](https://github.com/dcs4cop/xcube-smos/actions/workflows/tests.yaml/badge.svg)](https://github.com/dcs4cop/xcube-smos/actions/workflows/tests.yaml)
[![codecov](https://codecov.io/gh/dcs4cop/xcube-smos/graph/badge.svg?token=ZJ0J4QT1UM)](https://codecov.io/gh/dcs4cop/xcube-smos)
[![conda](https://anaconda.org/conda-forge/xcube-smos/badges/version.svg)](https://anaconda.org/conda-forge/xcube-smos)

<!--- Align following section with docs/index.md -->

`xcube-smos` is a Python package and [xcube](https://xcube.readthedocs.io/)
plugin that adds a 
[data store](https://xcube.readthedocs.io/en/latest/api.html#data-store-framework) 
named `smos` to xcube. The data store is used to 
access [ESA SMOS](https://earth.esa.int/eogateway/missions/smos) Level-2 data 
in form of analysis-ready geospatial datacubes with the dimensions 
`time`, `lat`, and `lon`. The datacubes are computed on-the-fly from the SMOS 
data archive `s3://EODATA/SMOS` hosted on [CREODIAS](https://creodias.eu/).

## Usage

After installation, data access is as easy as follows:

```python
from xcube.core.store import new_data_store

store = new_data_store("smos", **credentials)
datacube = store.open_data(
    "SMOS-L2C-SM", 
    time_range=("2022-01-01", "2022-01-06")
)
```

Above, a datacube of type
[xarray.Dataset](https://docs.xarray.dev/en/stable/generated/xarray.Dataset.html)
for SMOS **Soil Moisture** has been obtained.  
To access SMOS **Ocean Salinity** data use the identifier `"SMOS-L2C-OS"`. 


More about `xcube-smos` can be found in its 
[documentation](https://dcs4cop.github.io/xcube-smos/).
