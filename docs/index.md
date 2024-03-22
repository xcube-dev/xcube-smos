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
    time_range=("2022-01-01", "2022-01-06"),
    bbox=(0, 40, 20, 60)
)
```

Above, a datacube of type
[xarray.Dataset](https://docs.xarray.dev/en/stable/generated/xarray.Dataset.html)
for SMOS **Soil Moisture** has been obtained.  
To access SMOS **Ocean Salinity** data use the identifier `"SMOS-L2C-OS"`. 

## Spatial coverage and resolution

The SMOS data is provided globally or for a custom area of interest
using a geographic projection (`EPSG:4326` aka WGS84, 
World Geodetic System 1984). 

You can choose from five spatial resolutions by specifying a resolution 
level ranging from zero to four. Zero refers to a resolution of 
360/8192 ~ 0.044 degrees ~ 4.88 km. Higher levels subsequently increase the 
resolution by a factor of two.

!!! note
    The native spatial resolution of SMOS data is roughly 25 km. 
    `xcube-smos` performs an oversampling of the data to ensure no information
    is lost during spatial projection. It therefore uses a nearest-neighbor 
    resampling at higher resolution involving pixel duplication. 

## Temporal coverage and resolution

The SMOS satellite completes one orbit approximately every 100.1 minutes.
Therefore, up to 29 (number of orbits times two) level-2 data products are 
provided per day and included in the datasets as individual time steps datasets.
`xcube-smos` does not perform any aggregation in the time dimension. 

## Variables

The output of the data store are a SMOS Level-2C raster datacubes comprising 
a number of geophysical data variables with the dimensions `time`, `lat`, 
and `lon`. The variables depend on the selected SMOS product type, either
Soil Moisture or Ocean Salinity.

Soil Moisture datacubes of type `SMOS-L2C-SM` contain the following variables:

| Variable          | Type    | Units  |
|-------------------|---------|--------|
| Soil_Moisture     | float32 | m3 m-3 |
| Soil_Moisture_DQX | float32 | m3 m-3 |
| Chi_2             | uint8   | -      |
| Chi_2_P           | uint8   | -      |
| N_RFI_Y           | uint16  | -      |
| N_RFI_X           | uint16  | -      |
| RFI_Prob          | uint8   | -      |

Ocean Salinity datacubes of type `SMOS-L2C-OC` contain the following variables:

| Variable            | Type     | Units |
|---------------------|----------|-------|
| SSS_anom            | float32  | psu   |
| SSS_corr            | float32  | psu   |
| Sigma_SSS_anom      | float32  | psu   |
| Sigma_SSS_corr      | float32  | psu   |
| Dg_quality_SSS_anom | uint16   | -     |
| Dg_quality_SSS_corr | uint16   | -     |
| Dg_chi2_corr        | uint16   | -     |
| Dg_RFI_X            | uint16   | -     |
| Dg_RFI_Y            | uint16   | -     |
| Coast_distance      | uint8    | -     |
| Mean_acq_time       | float32  | dd    |
| X_swath             | float32  | m     |



