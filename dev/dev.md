# xcube-smos Internals

### Code installation

#### For users

xcube-smos end users can install xcube-smos directly from its git repository
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

