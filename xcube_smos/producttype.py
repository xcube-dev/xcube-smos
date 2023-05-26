from typing import Union

COMMON_PATH_PATTERN = "{year}/{month}/{day}"

COMMON_NAME_PATTERN = r"(?P<sd>\d{8})T(?P<st>\d{6})_" \
                      r"(?P<ed>\d{8})T(?P<et>\d{6})_" \
                      r"\d{3}_\d{3}_\d{1}"

ProductTypeLike = Union[str, "ProductType"]


class ProductType:
    def __init__(self,
                 id: str,
                 path: str,
                 name_pattern: str):
        self.id = id
        self.path = path
        self.path_pattern = self.path + "/" + COMMON_PATH_PATTERN
        self.name_pattern = name_pattern

    @classmethod
    def normalize(cls, product_type: ProductTypeLike) -> "ProductType":
        if isinstance(product_type, ProductType):
            return product_type
        if isinstance(product_type, str):
            if product_type.upper() == 'SM':
                return SM_PRODUCT_TYPE
            if product_type.lower() == 'OS':
                return OS_PRODUCT_TYPE
            raise ValueError('invalid product_type')
        raise TypeError('invalid product_type type')

    @classmethod
    def get_all(cls):
        return SM_PRODUCT_TYPE, OS_PRODUCT_TYPE


SM_PRODUCT_TYPE = ProductType(
    "SM",
    "SMOS/L2SM/MIR_SMUDP2",
    "SM_OPER_MIR_SMUDP2_" + COMMON_NAME_PATTERN
)

OS_PRODUCT_TYPE = ProductType(
    "OS",
    "SMOS/L2OS/MIR_OSUDP2",
    "SM_OPER_MIR_OSUDP2_" + COMMON_NAME_PATTERN
)
