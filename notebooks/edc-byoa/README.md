# SMOS Data Cubes as a service

This directory contains a headless notebook for generating SMOS data cubes
using the [xcube-smos](https://github.com/dcs4cop/xcube-smos) data store 
and the [zappend](https://bcdev.github.io/zappend/) tool.

It follows the repository structure required for 
[Euro Data Cube](https://www.eurodatacube.com/) - [_Insights On Demand_](https://eurodatacube.com/marketplace/data-products/on-demand) 
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
        "description": "The SMOS product type, must be 'SMOS-L2C-SM' or 'SMOS-L2C-OS'.",
        "optional": false
    },
    {
        "name": "Date Range",
        "id": "date_range",
        "type": "daterange",
        "description": "Date range given as closed interval '<first>/<last>' with first and last having format 'YYYY-MM-DD'.",
        "optional": false
    },
    {
        "name": "Aggregation Interval",
        "id": "agg_interval",
        "type": "string",
        "description": "The averaging interval such as '1d' (the default), '2d', '1w', or empty, which means no aggregation.",
        "optional": true
    },
        {
        "name": "Spatial Resolution Level",
        "id": "res_level",
        "type": "int",
        "description": "Spatial resolution level in the range 0 to 4. Actual resolution in degrees is 360/8192 * 2^res_level. Defaults to zero.",
        "optional": true
    }
]
```
