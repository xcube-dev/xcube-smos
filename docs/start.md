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

You can now use the new data store by its name `"smos"` and using
xcube's `new_data_store()` function:

```python
from xcube.core.store import new_data_store

store = new_data_store("smos", ...)
```

Or you use the class directly by importing it from the `xcube_smos` package:

```python
from xcube_smos.store import SmosDataStore

store = SmosDataStore(...)
```

Using `new_data_store()` function is the preferred method because this way
you can access many other xcube data stores in a generic way.


