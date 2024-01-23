# The MIT License (MIT)
# Copyright (c) 2023 by the xcube development team and contributors
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

from xcube.util.jsonschema import JsonArraySchema
from xcube.util.jsonschema import JsonIntegerSchema
from xcube.util.jsonschema import JsonDateSchema
from xcube.util.jsonschema import JsonObjectSchema
from xcube.util.jsonschema import JsonStringSchema
from .mldataset.newdgg import MIN_PIXEL_SIZE
from .mldataset.newdgg import NUM_LEVELS

STORE_PARAMS_SCHEMA = JsonObjectSchema(
    properties=dict(
        source_path=JsonStringSchema(
            min_length=1,
            title="Path or URL into SMOS archive filesystem.",
            examples=["EODATA"],
        ),
        source_protocol=JsonStringSchema(
            min_length=2,
            title="Protocol name for the SMOS archive filesystem.",
            examples=["s3", "file"],
        ),
        source_storage_options=JsonObjectSchema(
            additional_properties=True,
            title="Storage options for the SMOS NetCDF Kerchunk index.",
            description="See fsspec documentation for specific filesystems.",
            examples=[
                dict(
                    endpoint_url="https://s3.cloudferro.com",
                    anon=False,
                    key="******",
                    secret="******",
                )
            ],
        ),
        cache_path=JsonStringSchema(
            min_length=1,
            title="Path to local cache directory. "
            "Must be given, if file caching is desired.",
            examples=["~/.smos-nc-cache"],
        ),
        xarray_kwargs=JsonObjectSchema(
            additional_properties=True,
            title="Extra keyword arguments accepted by xarray.open_dataset.",
            description="See xarray documentation for allowed keywords.",
            examples=[dict(engine="netcdf4")],
        ),
    ),
    additional_properties=False,
)


_COMMON_OPEN_PARAMS_PROPS = dict(
    time_range=JsonArraySchema(
        items=[
            JsonDateSchema(nullable=True),
            JsonDateSchema(nullable=True),
        ],
        title="Time Range",
        description=(
            "Time range given as pair of start and stop dates."
            " Dates must be given using format 'YYYY-MM-DD'."
            " Start and stop are inclusive."
        ),
    ),
    # TODO: support variable_names
    # variable_names=JsonArraySchema(
    #     items=JsonStringSchema(), title="Names of variables to be included"
    # ),
    # TODO: support bbox
    # bbox=JsonArraySchema(
    #     items=(
    #         JsonNumberSchema(),
    #         JsonNumberSchema(),
    #         JsonNumberSchema(),
    #         JsonNumberSchema(),
    #     ),
    #     title="Bounding box [x1,y1, x2,y2] in geographical coordinates",
    # ),
)

_ML_DATASET_OPEN_PARAMS_PROPS = dict(
    **_COMMON_OPEN_PARAMS_PROPS,
    l2_product_cache_size=JsonIntegerSchema(
        title="Size of the SMOS L2 product cache",
        description="Maximum number of SMOS L2 products to be cached.",
        default=0,
        minimum=0,
    ),
)

_DATASET_OPEN_PARAMS_PROPS = dict(
    **_COMMON_OPEN_PARAMS_PROPS,
    res_level=JsonIntegerSchema(
        enum=list(range(NUM_LEVELS)),
        title="Spatial Resolution Level",
        description=(
            f"Spatial resolution level in the range 0 to {NUM_LEVELS-1}."
            f" Zero refers to the max. resolution of {MIN_PIXEL_SIZE} degrees."
        ),
        default=0,
    ),
)


ML_DATASET_OPEN_PARAMS_SCHEMA = JsonObjectSchema(
    required=["time_range"],
    properties=_ML_DATASET_OPEN_PARAMS_PROPS,
    additional_properties=False,
)

DATASET_OPEN_PARAMS_SCHEMA = JsonObjectSchema(
    required=["time_range"],
    properties=_DATASET_OPEN_PARAMS_PROPS,
    additional_properties=False,
)
