<!--- Align following section with README.md -->

# xcube-smos

_User-defined datacubes from SMOS Level-2 data_

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


## Features

The SMOS data is provided using a geographic projection. 
Users can choose from five spatial resolutions by specifying a resolution 
level ranging from zero to four. Zero refers to a resolution of 
360/8192 ~ 0.044 degrees ~ 4.88 km. Higher levels subsequently increase the 
resolution by a factor of two.

!!! note
    The native spatial resolution of SMOS data is roughly 25 km. 
    `xcube-smos` performs an oversampling of the data to ensure no information
    is lost during spatial projection. It therefore uses a nearest-neighbor 
    resampling at higher resolution involving pixel duplication. 


`xcube-smos` does not perform any aggregation in the time dimension. Data is 
provided as-is, that is, 29 SMOS Level-2 data products are included per day. 


