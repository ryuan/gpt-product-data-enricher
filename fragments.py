single_dimension_schema = {
    'single_dimension': {
        'type': ['object', 'null'],
        'properties': {
            'unit': {'enum': ['INCHES', 'FEET']},
            'value': {'type': ['array', 'null'], 'items': {'type': 'number', 'minimum': 0}, 'minItems': 1}
        },
        'required': ['unit', 'value'],
        'additionalProperties': False
    }
}
dimension_schema = {
    'dimension': {
        'type': ['object', 'null'],
        'properties': {
            'unit': {'enum': ['INCHES', 'FEET']},
            'width': {'type': ['array', 'null'], 'items': {'type': 'number', 'minimum': 0}, 'minItems': 1},
            'depth': {'type': ['array', 'null'], 'items': {'type': 'number', 'minimum': 0}, 'minItems': 1},
            'height': {'type': ['array', 'null'], 'items': {'type': 'number', 'minimum': 0}, 'minItems': 1}
        },
        'required': ['unit', 'width', 'depth', 'height'],
        'additionalProperties': False
    }
}
dimension_sets_schema = {
    'dimension_sets': {
        'type': ['object', 'null'],
        'properties': {
            'name': {'type': 'string', 'pattern': '\\S+'},
            'dimension': {'type': 'array', 'items': { "$ref": "#/$defs/dimension" }, 'minItems': 1}
        },
        'required': ['name', 'dimension'],
        'additionalProperties': False
    }
}
weight_schema = {
    'weight': {
        'type': ['object', 'null'],
        'properties': {
            'unit': {'enum': ['OUNCE', 'POUND']},
            'value': {'type': ['array', 'null'], 'items': {'type': 'number', 'minimum': 0}, 'minItems': 1}
        },
        'required': ['unit', 'value'],
        'additionalProperties': False
    }
}
package_measurement_schema = {
    'package_measurement': {
        'type': ['object', 'null'],
        'properties': {
            'dimension': { "$ref": "#/$defs/dimension" },
            'weight': { "$ref": "#/$defs/weight" }
        },
        'required': ['dimension', 'weight'],
        'additionalProperties': False
    }
}
object_schema_reference = {
    'dimension_sets': [dimension_sets_schema, dimension_schema],
    'dimension': [dimension_schema],
    'single_dimension': [single_dimension_schema],
    'weight': [weight_schema],
    'package_measurement': [package_measurement_schema, dimension_schema, weight_schema]
}