from typing import Union

COMMON_SUB_PATH_PATTERN = "{year}/{month}/{day}"

COMMON_NAME_PATTERN = (
    r"(?P<sd>\d{8})T(?P<st>\d{6})_" r"(?P<ed>\d{8})T(?P<et>\d{6})_" r"\d{3}_\d{3}_\d{1}"
)

ProductTypeLike = Union[str, "ProductType"]


class ProductType:
    """SMOS product type."""

    # TODO rename into MIR_OSUDP2
    OS: "ProductType"
    # TODO rename into MIR_SMUDP2
    SM: "ProductType"

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
            if product_type.upper() == "SM":
                return cls.SM
            if product_type.upper() == "OS":
                return cls.OS
            raise ValueError(f"invalid product_type {product_type!r}")
        raise TypeError(f"invalid product_type type {type(product_type)}")


ProductType.OS = ProductType(
    "OS", "SMOS/L2OS/MIR_OSUDP2/", r"SM_(OPER|REPR)_MIR_OSUDP2_"
)

ProductType.SM = ProductType(
    "SM", "SMOS/L2SM/MIR_SMUDP2/", r"SM_(OPER|REPR)_MIR_SMUDP2_"
)
