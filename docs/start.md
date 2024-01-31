# Getting Started

## Credentials

The xcube SMOS data store directly accesses 
[SMOS data](https://creodias.eu/eodata/smos/) in its S3 archive on 
[CREODIAS](https://creodias.eu/). Therefore, the data store requires your 
credentials. If not already done, create an account on CREODIAS and follow 
the instructions to 
[generate your access key and secret](https://creodias.docs.cloudferro.com/en/latest/general/How-to-generate-ec2-credentials-on-Creodias.html).

Once you have received your credentials, you may consider setting the environment 
variables `CREODIAS_S3_KEY` and `CREODIAS_S3_SECRET`. If you do so,
you can omit the `key` and `secret` arguments passed to the data store.

## Installation

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

## Get the Data

You can now use the SMOS data store using the xcube `new_data_store()` 
function and passing the store identifier `"smos"` and using your credentials.
Then you use the store method `open_data()` to access the data:

```python
from xcube.core.store import new_data_store

store = new_data_store("smos", 
                       key="your access key", 
                       secret="your access key secret")

dataset = store.open_data(data_id="SMOS-L2C-SM",
                          time_range=("2022-01-01", "2022-01-05"), 
                          bbox=(5.87, 47.27, 15.03, 55.06),
                          res_level=0)
```

There are a couple of parameters that can be passed to the `new_data_store()`
function and the `open_data()` method. You can read more about it in the 
[user guide](guide.md).
