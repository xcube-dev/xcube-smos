from xcube.util.jsonschema import JsonObjectSchema
from xcube.util.jsonschema import JsonStringSchema
from xcube.util.jsonschema import JsonArraySchema
from xcube.util.jsonschema import JsonNumberSchema
from xcube.util.jsonschema import JsonDateSchema
from xcube_smos.dgg import SmosDiscreteGlobalGrid

STORE_PARAMS_SCHEMA = JsonObjectSchema(
    properties=dict(
        key=JsonStringSchema(
            min_length=1,
            title='AWS access key identifier.',
            description='Can also be set in profile section'
                        ' of ~/.aws/config, or by environment'
                        ' variable AWS_ACCESS_KEY_ID.'
        ),
        secret=JsonStringSchema(
            min_length=1,
            title='AWS secret access key.',
            description='Can also be set in profile section'
                        ' of ~/.aws/config, or by environment'
                        ' variable AWS_SECRET_ACCESS_KEY.'
        ),
    ),
    additional_properties=False
)

OPEN_PARAMS_SCHEMA = JsonObjectSchema(
    properties=dict(
        variable_names=JsonArraySchema(
            items=JsonStringSchema(),
            title='Names of variables to be included'
        ),
        bbox=JsonArraySchema(
            items=(JsonNumberSchema(),
                   JsonNumberSchema(),
                   JsonNumberSchema(),
                   JsonNumberSchema()),
            title='Bounding box [x1,y1, x2,y2] in geographical coordinates'
        ),
        spatial_res=JsonNumberSchema(
            enum=[(1 << level) * SmosDiscreteGlobalGrid.SPATIAL_RES
                  for level in range(SmosDiscreteGlobalGrid.NUM_LEVELS)],
            title='Spatial resolution in decimal degrees.',
        ),
        time_range=JsonArraySchema(
            items=[
                JsonDateSchema(nullable=True),
                JsonDateSchema(nullable=True),
            ],
            title='Time range [from, to]'
        ),
        # time_period=JsonStringSchema(
        #     enum=[*map(lambda n: f'{n}D', range(1, 14)),
        #           '1W', '2W'],
        #     title='Time aggregation period'
        # ),
        time_tolerance=JsonStringSchema(
            default='10m',  # 10 minutes
            format='^([1-9]*[0-9]*)[NULSTH]$',
            title='Time tolerance'
        ),
    ),
    additional_properties=False
)
