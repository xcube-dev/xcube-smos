# SMOS Data Cubes as a service

This directory contains a headless notebook for generating SMOS data cubes
using the [xcube-smos](https://github.com/dcs4cop/xcube-smos) data store 
and the [zappend](https://bcdev.github.io/zappend/) tool.

It follows the repository structure required for 
[Euro Data Cube](https://www.eurodatacube.com/) - _Insights On Demand_ 
(aka BYOA Bring Your Own Algorithm). Please consult the 
[EDC documentation](https://eurodatacube.com/documentation/offer_algorithms_for_on_demand_data_generation) for more information.

### Parameters

This algorithm can be onboarded to the EDC platform using the following 
parameter specification:

```json
[
    {
        "name": "Product Type",
        "id": "product_type",
        "type": "string",
        "description": "The SMOS product type, must be 'SMOS-L2C-SM' or 'SMOS-L2C-OS'",
        "optional": false
    },
    {
        "name": "Date Range",
        "id": "date_range",
        "type": "daterange",
        "description": "Date Range given as '<first>/<last>' with first and last having format 'YYYY-MM-DD'. Both are inclusive.",
        "optional": true
    },
    {
        "name": "Interval",
        "id": "interval",
        "type": "string",
        "description": "The averaging interval, e.g., '1d', '2d', '1w'. Default is '1d' (one day).",
        "optional": true
    }
]
```
