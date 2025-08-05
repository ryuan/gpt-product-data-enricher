single_dimension_schema = {
    'type': 'object',
    'properties': {
        'unit': {'enum': ['INCHES', 'FEET']},
        'value': {'type': 'number', 'minimum': 0}
    },
    'required': ['unit', 'value'],
    'additionalProperties': False
}
dimension_schema = {
    'type': ['object', 'null'],
    'properties': {
        'width': {'type': 'array', 'items': single_dimension_schema, 'minItems': 1, 'uniqueItems': True},
        'depth': {'type': 'array', 'items': single_dimension_schema, 'minItems': 1, 'uniqueItems': True},
        'height': {'type': 'array', 'items': single_dimension_schema, 'minItems': 1, 'uniqueItems': True}
    },
    'minProperties': 2,
    'additionalProperties': False
}
dimension_sets_schema = {
    'type': ['object', 'null'],
    'properties': {
        'name': {'type': 'string', 'pattern': '\\S+'},
        'dimension': {'type': 'array', 'items': dimension_schema, 'minItems': 1, 'uniqueItems': True}
    },
    'required': ['name', 'dimension'],
    'additionalProperties': False
}
weight_schema = {
    'type': ['object', 'null'],
    'properties': {
        'unit': {'enum': ['OUNCE', 'POUND']},
        'value': {'type': 'number', 'minimum': 0}
    },
    'required': ['unit', 'value'],
    'additionalProperties': False
}
package_measurement_schema = {
    'type': ['object', 'null'],
    'properties': {
        'dimension': dimension_schema,
        'weight': weight_schema
    },
    'required': ['dimension', 'weight'],
    'additionalProperties': False
}
object_schema_reference = {
    'dimension_sets': dimension_sets_schema,
    'dimension': dimension_schema,
    'height': single_dimension_schema,
    'length': single_dimension_schema,
    'weight': weight_schema,
    'package_measurement': package_measurement_schema
}