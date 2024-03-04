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

from typing import Union

COMMON_SUB_PATH_PATTERN = "{year}/{month}/{day}"

COMMON_NAME_PATTERN = (
    r"(?P<sd>\d{8})T(?P<st>\d{6})_(?P<ed>\d{8})T(?P<et>\d{6})_\d{3}_\d{3}_\d{1}"
)

ProductTypeLike = Union[str, "ProductType"]

TYPE_ID_SM = "MIR_SMUDP2"
TYPE_ID_OS = "MIR_OSUDP2"


# noinspection SpellCheckingInspection
class ProductType:
    """SMOS product type."""

    MIR_OSUDP2: "ProductType"
    MIR_SMUDP2: "ProductType"

    def __init__(self, type_id: str, path_prefix: str, name_prefix: str):
        if not path_prefix:
            raise ValueError("path_prefix must be given")
        if not path_prefix.endswith("/"):
            raise ValueError("path_prefix must end with '/'")
        self.type_id = type_id
        self.path_prefix = path_prefix
        self.path_pattern = path_prefix + COMMON_SUB_PATH_PATTERN
        self.name_pattern = name_prefix + COMMON_NAME_PATTERN

    @classmethod
    def normalize(cls, product_type: ProductTypeLike) -> "ProductType":
        if isinstance(product_type, ProductType):
            return product_type
        if isinstance(product_type, str):
            if product_type.upper() in ("SM", "L2SM", TYPE_ID_SM):
                return cls.MIR_SMUDP2
            if product_type.upper() in ("OS", "L2OS", TYPE_ID_OS):
                return cls.MIR_OSUDP2
            raise ValueError(f"invalid product_type {product_type!r}")
        raise TypeError(f"invalid product_type type {type(product_type)}")


ProductType.MIR_SMUDP2 = ProductType(
    TYPE_ID_SM, f"SMOS/L2SM/{TYPE_ID_SM}/", rf"SM_(OPER|REPR)_{TYPE_ID_SM}_"
)

ProductType.MIR_OSUDP2 = ProductType(
    TYPE_ID_OS, f"SMOS/L2OS/{TYPE_ID_OS}/", rf"SM_(OPER|REPR)_{TYPE_ID_OS}_"
)
