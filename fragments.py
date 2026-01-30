product_schema = {
    # Schema to define a product, with fields structured as input for GraphQL's productSet mutation
    'product': {
        'type': 'object',
        'properties': {
            'productType': {'enum': ['FILL']},
            'status': {'type': 'string', 'const': 'DRAFT'},
            'vendor': {'enum': ['Safavieh', 'SEI', 'Modway', 'TOV', 'Surya', 'HomArt', 'Adesso', 'Moe\'s']},
            'variants': {'type': 'array', 'items': { '$ref': '#/$defs/variant'}, 'minItems': 1 }
        }
    }
}
variant_schema = {
    # Complements product_schema to create variants as part of its input for GraphQL's productSet mutation
    'variant': {
        'type': 'object',
        'properties': {
            'sku': {'type': 'string'},
            'barcode': {'type': ['string', 'null']},
            'inventoryItem': {
                'type': 'object',
                'properties': {
                    'measurement': {
                        'type': ['object', 'null'], 
                        'properties': { '$ref': '#/$defs/weight'},
                        'required': ['weight'],
                        'additionalProperties': False
                    },
                    'tracked': {'type': 'boolean', "const": True}
                }
            }
        }
    }
}
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
    'product': [product_schema, variant_schema, weight_schema],
    'variant': [variant_schema, weight_schema],
    'dimensions_sets': [dimensions_sets_schema, dimensions_schema, dimension_schema],
    'dimensions': [dimensions_schema, dimension_schema],
    'dimension': [dimension_schema],
    'weight': [weight_schema],
    'package_measurement': [package_measurement_schema, dimensions_schema, dimension_schema, weight_schema]
}