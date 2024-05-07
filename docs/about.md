# About xcube-smos

## Changelog

You can find the complete `xcube-smos` changelog 
[here](https://github.com/xcube-dev/xcube-smos/blob/main/CHANGES.md). 

## Reporting

If you have suggestions, ideas, feature requests, or if you have identified
a malfunction or error, then please 
[post an issue](https://github.com/xcube-dev/xcube-smos/issues). 

## Development

You can install `xcube-smos` directly from its GitHub repository
into a xcube environment created with
[mamba](https://mamba.readthedocs.io/en/latest/installation.html)
(recommended) or
[conda](https://docs.conda.io/en/latest/miniconda.html).

```shell
mamba create -n xcube -c conda-forge xcube
mamba activate xcube
git clone https://github.com/xcube-dev/xcube-smos.git
cd xcube-smos
pip install --verbose --no-deps --editable .
```

### Testing and Coverage

`xcube-smos` uses [pytest](https://docs.pytest.org/) for unit-level testing 
and code coverage analysis.

```bash
pytest --cov=xcube-smos tests
```

### Code Style

`xcube-smos` source code is formatted using the [black](https://black.readthedocs.io/) tool.

```bash
black xcube-smos
black tests
```

### Documentation

`xcube-smos` documentation is built using the [mkdocs](https://www.mkdocs.org/) tool.

```bash
pip install -r requirements-doc.txt

mkdocs build
mkdocs serve
mkdocs gh-deploy
```

## License

`xcube-smos` is open source made available under the terms and conditions of the 
[MIT License](https://github.com/xcube-dev/xcube-smos/blob/main/LICENSE).

Copyright © 2024 Brockmann Consult Development

