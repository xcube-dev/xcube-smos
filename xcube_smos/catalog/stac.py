# The MIT License (MIT)
# Copyright (c) 2023-2024 by the xcube development team and contributors
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

import json
import logging
from typing import Dict, Any, Optional, Tuple, List, Callable

import fsspec
import requests

from .producttype import ProductTypeLike, ProductType
from .types import DatasetOpener
from .types import DatasetRecord
from .types import DatasetFilter
from .direct import SmosDirectCatalog
from ..constants import DEFAULT_STAC_URL

FeatureFilter = Callable[[Dict[str, Any]], bool]

LOG = logging.getLogger("xcube-smos")


class SmosStacCatalog(SmosDirectCatalog):
    """A SMOS L2 dataset catalog that directly accesses the source filesystem
    including a STAC Catalog for filtering."""

    def __init__(self, **direct_catalog_kwargs):
        super().__init__()
        self._direct_catalog = SmosDirectCatalog(**direct_catalog_kwargs)

    @property
    def source_fs(self) -> fsspec.AbstractFileSystem:
        return self._direct_catalog.source_fs

    def get_dataset_opener_kwargs(self) -> Dict[str, Any]:
        return self._direct_catalog.get_dataset_opener_kwargs()

    def get_dataset_opener(self) -> DatasetOpener:
        return self._direct_catalog.get_dataset_opener()

    def find_datasets(
        self,
        product_type: ProductTypeLike,
        time_range: Tuple[Optional[str], Optional[str]],
        bbox: tuple[float, float, float, float] | None = None,
        dataset_filter: Optional[DatasetFilter] = None,
        feature_filter: Optional[FeatureFilter] = None,
    ) -> List[DatasetRecord]:
        product_type = ProductType.normalize(product_type)
        features = fetch_features(product_type.type_id, time_range, feature_filter)
        dataset_records = []
        for feature in features:
            s3_url = (
                feature.get("assets", {})
                .get("PRODUCT", {})
                .get("alternate", {})
                .get("s3", {})
                .get("href")
            )
            if s3_url:
                properties = feature.get("properties", {})
                start_datetime = properties.get("start_datetime")
                end_datetime = properties.get("end_datetime")
                if start_datetime and end_datetime:
                    dataset_record = s3_url, start_datetime, end_datetime
                    if dataset_filter is None or dataset_filter(dataset_record):
                        dataset_records.append(dataset_record)
        return dataset_records


def fetch_features(
    product_type_id: str,
    date_range: tuple[str, str],
    bbox: tuple[float, float, float, float] | None,
    feature_filter: Callable[[dict], bool] | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    params = create_request_params(
        date_range=date_range,
        bbox=bbox,
        limit=limit,
    )
    url = DEFAULT_STAC_URL + "/items"
    filtered_features = []
    while True:
        response = requests.get(url, params)
        feature_collection = response.json()
        try:
            features = feature_collection["features"]
        except KeyError:
            msg = f"Encountered error from {url}: {json.dumps(feature_collection)}"
            LOG.error(msg)
            raise ValueError(msg)

        links = feature_collection.get("links", [])
        next_url = None
        for link in links:
            rel = link.get("rel")
            if rel == "next":
                next_url = link.get("href")
                break
        if not next_url:
            break

        url = next_url
        params = {}

        for feature in features:
            _product_type_id = feature.get("properties", {}).get("productType")
            if _product_type_id == product_type_id and (
                feature_filter is None or feature_filter(feature)
            ):
                filtered_features.append(feature)

    return filtered_features


def create_request_params(
    date_range: tuple[str, str],
    bbox: tuple[float, float, float, float] | None,
    limit: int,
) -> list[tuple[str, str]]:
    start, stop = date_range
    start += "T00:00:00.000Z"
    stop += "T23:59:59.999Z"
    params = [("limit", str(limit)), ("datetime", f"{start}/{stop}")]
    if bbox:
        x1, y1, x2, y2 = bbox
        params.append(("bbox", f"{x1},{y1},{x2},{y2}"))
    return params
