dimension_schema = {
    # Conforms to GraphQL format for 'Dimension' data type
    'dimension': {
        'type': ['object', 'null'],
        'properties': {
            'unit': {'enum': ['INCHES']},
            'value': {'type': 'number', 'minimum': 0}
        },
        'required': ['unit', 'value'],
        'additionalProperties': False
    }
}
dimensions_schema = {
    # Custom format referencing GraphQL format for 'Dimension' data type
    'dimensions': {
        'type': ['object', 'null'],
        'properties': {
            'width': {'type': ['array', 'null'], 'items': { '$ref': '#/$defs/dimension' }, 'minItems': 1},
            'depth': {'type': ['array', 'null'], 'items': { '$ref': '#/$defs/dimension' }, 'minItems': 1},
            'height': {'type': ['array', 'null'], 'items': { '$ref': '#/$defs/dimension' }, 'minItems': 1}
        },
        'required': ['width', 'depth', 'height'],
        'additionalProperties': False
    }
}
dimensions_sets_schema = {
    # Custom format referencing another custom format
    'dimensions_sets': {
        'type': ['object', 'null'],
        'properties': {
            'name': {'type': 'string', 'pattern': '\\S+'},
            'dimensions': {'type': 'array', 'items': { '$ref': '#/$defs/dimensions' }, 'minItems': 1}
        },
        'required': ['name', 'dimensions'],
        'additionalProperties': False
    }
}
weight_schema = {
    # Conforms to GraphQL format for 'Weight' data type
    'weight': {
        'type': ['object', 'null'],
        'properties': {
            'unit': {'enum': ['OUNCES', 'POUNDS']},
            'value': {'type': 'number', 'minimum': 0}
        },
        'required': ['unit', 'value'],
        'additionalProperties': False
    }
}
package_measurement_schema = {
    # Custom format referencing both another custom format and GraphQL format for 'Weight' data type
    'package_measurement': {
        'type': ['object', 'null'],
        'properties': {
            'dimensions': { '$ref': '#/$defs/dimensions' },
            'weight': { '$ref': '#/$defs/weight' }
        },
        'required': ['dimensions', 'weight'],
        'additionalProperties': False
    }
}
object_schema_reference = {
    'dimensions_sets': [dimensions_sets_schema, dimensions_schema, dimension_schema],
    'dimensions': [dimensions_schema, dimension_schema],
    'dimension': [dimension_schema],
    'weight': [weight_schema],
    'package_measurement': [package_measurement_schema, dimensions_schema, dimension_schema, weight_schema]
}