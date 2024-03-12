# User Guide

_This user guide is currently being written._

## Obtaining the Data Store

After [installation](start.md#installation) and once you have your 
[credentials](start.md#credentials) at hand, you can use the SMOS data store 
using its class exported by the `xcube_smos.store` module:

```python
from xcube_smos.store import SmosDataStore

store = SmosDataStore(key="your access key", secret="your secret")
```

However, the preferred way to obtain the store object is by its name `"smos"` 
and using the xcube `new_data_store()` function, because many other xcube 
data stores can be used in the same way:

```python
from xcube.core.store import new_data_store

store = new_data_store("smos", key="your access key", secret="your secret")
```

!!! note
    You can avoid passing `key` and `secret` arguments if you set the environment 
    variables `CREODIAS_S3_KEY` and `CREODIAS_S3_SECRET` accordingly.

The `new_data_store()` function in its general form can take arbitrary keyword 
arguments. The store identifier, here `"smos"` determines the allowed keywords.
You can inspect the allowed data store keywords by using the xcube function
`get_data_store_params_schema()`, which outputs the allowed parameters as a 
JSON Schema object:

```python
from xcube.core.store import get_data_store_params_schema

get_data_store_params_schema("smos")
```

## Data Store Parameters

Using the data store's `get_open_data_params_schema()` method you can 
inspect the allowed parameters for the `store.open_data()` method, which is 
used to access the SMOS data in various forms.

```python
open_schema = store.get_open_data_params_schema()
print(open_schema)
```

## Data Access

All xcube data stores provide a `open_data()` method to access the data.
It has one required positional argument `data_id` which identifies the
data(set) to be opened. The SMOS store provides two datasets, they are

* `"SMOS-L2C-SM"` - SMOS Level-2C Soil Moisture
* `"SMOS-L2C-OS"` - SMOS Level-2C Ocean Salinity

In the xcube data store framework, the different data representations 
are provided by dedicated _data openers_. Hence, a common and optional 
argument is `opener_id`, which is used to control how the data is 
represented. The SMOS data store can currently provide three 
different representations of SMOS data addressing different use cases:

* `"dataset:zarr:smos"` (the default) - represent data as a datacube including 
   all observations in the given time range at a fixed spatial resolution;
* `"mldataset:zarr:smos"` - represent data as a multi-resolution datacube 
   including all observations in the given time range including 5 spatial 
   resolution levels;
* `"smosdsiter:zarr:smos"` - represent data as an iterator providing datasets 
   for all the observations in the given time range at a fixed spatial 
   resolution. 

### Common Access Parameters

### Datacube

### Multi-Resolution Datacube

### Dataset Iterators




