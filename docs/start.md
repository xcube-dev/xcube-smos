## Getting Started

You can install xcube-smos as a conda package:

```shell
conda install -c conda-forge xcube-smos
```

Or install it from its GitHub sources into a Python environment with
[xcube and its dependencies installed](https://xcube.readthedocs.io/en/latest/installation.html).

```shell
git clone https://github.com/dcs4cop/xcube-smos.git
cd xcube-smos
pip install -ve .
```

You can now use the new data store using its class imported from the `xcube_smos.store` 
module:

```python
from xcube_smos.store import SmosDataStore

store = SmosDataStore(...)
```

However, the preferred way is to use it by its by its name `"smos"` via the xcube 
`new_data_store()` function, because many other xcube data stores can be used this way:

```python
from xcube.core.store import new_data_store

store = new_data_store("smos", ...)
```

The `new_data_store()` function in its general form can take arbitrary keyword 
arguments. The store identifier, here `"smos"` determines the allowed keywords.
You can inspect the allowed data store keywords by using the xcube function
`get_data_store_params_schema()`, which outputs the allowed parameters as a 
JSON Schema object:

```python
import json
from xcube.core.store import get_data_store_params_schema

store_schema = get_data_store_params_schema("smos")
print(json.dumps(store_schema, indent=2))
```





