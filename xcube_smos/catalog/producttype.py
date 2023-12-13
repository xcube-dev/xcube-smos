from typing import Union


COMMON_SUB_PATH_PATTERN = "{year}/{month}/{day}"

COMMON_NAME_PATTERN = r"(?P<sd>\d{8})T(?P<st>\d{6})_" \
                      r"(?P<ed>\d{8})T(?P<et>\d{6})_" \
                      r"\d{3}_\d{3}_\d{1}"


ProductTypeLike = Union[str, "ProductType"]


class ProductType:
    """SMOS product type."""

    def __init__(self,
                 id: str,
                 path_prefix: str,
                 name_prefix: str):
        if not path_prefix:
            raise ValueError("path_prefix must be given")
        if not path_prefix.endswith("/"):
            raise ValueError("path_prefix must end with '/'")
        self.id = id
        self.path_prefix = path_prefix
        self.path_pattern = path_prefix + COMMON_SUB_PATH_PATTERN
        self.name_pattern = name_prefix + COMMON_NAME_PATTERN

    @classmethod
    def normalize(cls, product_type: ProductTypeLike) -> "ProductType":
        if isinstance(product_type, ProductType):
            return product_type
        if isinstance(product_type, str):
            if product_type.upper() == 'SM':
                return SM_PRODUCT_TYPE
            if product_type.upper() == 'OS':
                return OS_PRODUCT_TYPE
            raise ValueError(f'invalid product_type {product_type!r}')
        raise TypeError(f'invalid product_type type {type(product_type)}')

    @classmethod
    def get_all(cls):
        return SM_PRODUCT_TYPE, OS_PRODUCT_TYPE


SM_PRODUCT_TYPE = ProductType(
    "SM",
    "SMOS/L2SM/MIR_SMUDP2/",
    r"SM_(OPER|REPR)_MIR_SMUDP2_"
)

OS_PRODUCT_TYPE = ProductType(
    "OS",
    "SMOS/L2OS/MIR_OSUDP2/",
    r"SM_(OPER|REPR)_MIR_OSUDP2_"
)

ProductType.OS = OS_PRODUCT_TYPE
ProductType.SM = SM_PRODUCT_TYPE
