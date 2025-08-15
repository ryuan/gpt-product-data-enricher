import json
import pandas as pd
from typing import List, Dict
from fragments import object_schema_reference
from encoder import Encoder
from manager import BatchManager
from crawler import WebSearchTool


class PayloadsGenerator:
    def __init__(self, crawler: WebSearchTool, encoder: Encoder, sku_col_name: str, 
                 supplier_data_df: pd.DataFrame, store_data_df: pd.DataFrame, fields_data_df: pd.DataFrame, product_ids_skus: Dict):
        self.crawler: WebSearchTool = crawler
        self.encoder: Encoder = encoder
        self.sku_col_name: str = sku_col_name
        # Inputs
        self.supplier_data_df: pd.DataFrame = supplier_data_df
        self.store_data_df: pd.DataFrame = store_data_df
        self.fields_data_df: pd.DataFrame = fields_data_df
        # References
        self.product_ids_skus: Dict = product_ids_skus
        self.dependency_results: Dict = {}
        # Per-process Variables
        self.process_order_number: int = None
        self.resource_type: str = None
        self.batch_manager: BatchManager = None

    def generate_batch_payloads(self, process_order_number: int, batch_manager: BatchManager):
        """
        Generate a list of request payloads for each SKU in the supplier CSV with product images.
        """

        self.process_order_number = process_order_number
        self.resource_type = self.fields_data_df.loc[self.fields_data_df['Process Order Number'] == self.process_order_number, 'Resource'].iloc[0]
        self.batch_manager = batch_manager
        product_ids: List[str] = self.store_data_df.loc[self.store_data_df['id'].str.startswith('gid://shopify/Product/'), 'id'].to_list()

        for product_id in product_ids:
            product_data: Dict = self.store_data_df.loc[
                self.store_data_df['id'] == product_id, 
                ['id', 'vendor', 'productType']
                ].iloc[0].to_dict()
            product_data['image/url'] = self.store_data_df.loc[
                (self.store_data_df['id'].str.startswith('gid://shopify/MediaImage/')) & 
                (self.store_data_df['__parentId'] == product_id), 
                'image/url'
            ].to_list()

            # Get fields to extract for this product's type, omitting any fields that failed dependency check
            fields_to_extract = self.__get_fields_to_extract(product_id, product_data)

            if not fields_to_extract.empty:
                # For each variant, collect supplier data, store data, and web search data
                variants_data = self.__get_variants_data(product_id)

                # Build the output JSON schema based on the extraction fields
                output_schema = self.__build_output_schema(fields_to_extract)

                # For product or each variant, generate the request payload, then write to line in batch payloads JSONL file
                if self.resource_type == 'Product' and len(variants_data) != 0:
                    self.__generate_single_payload(product_id, product_data, variants_data, fields_to_extract, output_schema)
                elif self.resource_type == 'Variant' and len(variants_data) != 0:
                    for variant_data in variants_data:
                        if variant_data['supplier_data'] is not None:      # Skip any variants without supplier data
                            variant_id = variant_data['id']
                            self.__generate_single_payload(variant_id, product_data, variants_data, fields_to_extract, output_schema)

        print(f"Estimated input tokens for batch {self.process_order_number} = {self.encoder.batch_tokens_estimate[self.process_order_number]}")

    def set_dependency_results(self):
        """
        For each product processed in the first sequence process, collect whether extracted required fields were True or False
        """

        self.dependency_results = {}
        dependency_fields = self.fields_data_df['Dependency'].dropna().unique()

        with open(self.batch_manager.batch_outputs_path, 'r', encoding='ascii') as f:
            for line in f:
                if line.strip():  # Skip empty lines
                    try:
                        output: Dict = json.loads(line)
                        product_id = output['id']
                        self.dependency_results[product_id] = {}
                        outputs: Dict = output['output']
                        
                        for dependency_field in dependency_fields:
                            if dependency_field in outputs.keys():
                                self.dependency_results[product_id][dependency_field] = outputs[dependency_field]['value']

                    except json.JSONDecodeError as e:
                        print(f"Error decoding JSON on line: {line.strip()}. Error: {e}")

    def __get_fields_to_extract(self, product_id: str, product_data: Dict) -> pd.DataFrame:
        """
        Get the fields for this product's type and this process order number, omitting any fields that did not pass dependency check
        """

        fields_to_extract = self.fields_data_df[self.fields_data_df['Process Order Number'] == self.process_order_number].dropna(subset=[product_data['productType']])

        if self.dependency_results:
            dependency_fields = fields_to_extract['Dependency'].dropna().unique()

            for dependency_field in dependency_fields:
                if dependency_field in self.dependency_results[product_id].keys():  # This ensures fields like Frame Color with dependency Framed is kept for furniture
                    if self.dependency_results[product_id][dependency_field] is not True:
                        fields_to_extract = fields_to_extract[fields_to_extract['Dependency'] != dependency_field]

        return fields_to_extract

    def __get_variants_data(self, product_id: str) -> List[Dict]:
        """
        Get the data for each variant of the product, including supplier data, store data, and web search data
        """

        # Get store data for the product's SKUs
        variants_data: List[Dict] = self.store_data_df.loc[
            (self.store_data_df['id'].str.startswith('gid://shopify/ProductVariant/')) & 
            (self.store_data_df['__parentId'] == product_id), 
            ['id', 'sku', 'image/url']
        ].to_dict(orient='records')

        # Get all the supplier and web search data for the SKUs beloging to the product, dropping any blank/NaN values
        for variant_data in variants_data:
            sku: str = variant_data['sku']
            supplier_sku_data = self.supplier_data_df[self.supplier_data_df[self.sku_col_name] == sku].iloc[0].dropna().to_dict()
            variant_data['supplier_data'] = supplier_sku_data
            variant_data['web_data'] = self.crawler.web_search_results[variant_data['id']] if variant_data['id'] in self.crawler.web_search_results.keys() else {}

        return variants_data

    def __build_output_schema(self, fields_to_extract: pd.DataFrame) -> Dict:
        """
        Compose custom schema for the structured JSON output tailored to a payload's extracted fields.
        """

        fields = fields_to_extract['Field'].to_list()
        schema_properties = {}
        definitions = {}

        for field in fields:
            field_value_structure = {}
            field_data = fields_to_extract[fields_to_extract['Field'] == field].iloc[0]
            field_type = field_data['JSON Type']
            field_enum_values = field_data['JSON Enum Values']
            field_array_items = field_data['JSON Array Items']
            field_object_type = field_data['JSON Object Type']

            if field_type in ['string', 'number', 'boolean']:
                field_value_structure['type'] = [field_type, 'null']
            elif field_type == 'enum':
                field_value_structure['type'] = ['string', 'null']
                field_value_structure['enum'] = json.loads(field_enum_values) + [None]
            elif field_type == 'object':
                field_value_structure = { "$ref": f"#/$defs/{field_object_type}" }
            elif field_type == 'array':
                field_value_structure['type'] = [field_type, 'null']

                if field_array_items in ['string', 'number', 'boolean']:
                    field_value_structure['items'] = {'type': field_array_items}
                elif field_array_items == 'enum':
                    field_value_structure['items'] = {'enum': json.loads(field_enum_values) + [None]}
                elif field_array_items == 'object':
                    field_value_structure['items'] = { "$ref": f"#/$defs/{field_object_type}" }

            schema_properties[field] = {
                'type': 'object',
                'properties': {
                    'reasoning': {'type': 'string'},
                    'confidence': {'type': ['string', 'null'], 'enum': ['low', 'medium', 'high', None]},
                    'warning': {'type': ['string', 'null']},
                    'source': {'type': ['string', 'null'], 'enum': ['supplier data', 'image', 'both', 'inferred', None]},
                    'value': field_value_structure,
                },
                'required': ['reasoning', 'confidence', 'warning', 'source', 'value'],
                'additionalProperties': False
            }

            if not pd.isna(field_object_type):
                for reference_schema in object_schema_reference[field_object_type]:
                    definitions.update(reference_schema)

        schema = {
            'type': 'object', 
            'properties': schema_properties, 
            '$defs': definitions,
            'required': fields, 
            'additionalProperties': False
        }

        return schema

    def __generate_single_payload(self, object_id: str, product_data: Dict, variants_data: List[Dict], fields_to_extract: pd.DataFrame, output_schema: Dict):
        """
        Built the prompt, generate the payload, then write to batch payloads JSONL file.
        """

        product_type = product_data['productType']
        product_vendor = product_data['vendor']
        img_urls: List = product_data['image/url']

        # If process is for variant, position variant-specific image in front of image URLs list
        if self.resource_type == 'Variant':
            for variant_data in variants_data:
                if variant_data['id'] == object_id and pd.notna(variant_data['image/url']):
                    variant_img_url = variant_data['image/url']
                    img_urls.pop(img_urls.index(variant_img_url))
                    img_urls.insert(0, variant_img_url)

        print(f"Generating payload for {object_id}")
        system_instructions = self.__compose_instructions(product_type, fields_to_extract)
        user_prompt = self.__compose_prompt(object_id, product_vendor, variants_data)
        payload = self.__build_payload(object_id, system_instructions, user_prompt, img_urls, output_schema)        
        self.batch_manager.write(payload)

        tokens = self.encoder.estimate_input_tokens(self.process_order_number, system_instructions, user_prompt, img_urls, output_schema)
        print(f"Estimated input payload tokens = {tokens}")

    def __compose_instructions(self, product_type: str, fields_to_extract: pd.DataFrame) -> str:
        """
        Compose system instructions for a product or variant payload.
        """

        system_instructions = (
            "# Role and Objective\n"
            "- Act as an expert product data analyst for a large home goods retailer, specializing in extracting standardized field values from supplier spreadsheets and product images.\n"
            "\n"
            "# Instructions\n"
            "- Extract every field listed below from the supplier data and images, adhering strictly to provided rules and definitions for each field.\n"
            "- Carefully evaluate each attribute in the supplier data, verifying against images to ensure accuracy.\n"
            "- Handle typos, inconsistencies, or contradictions by cross-checking between sources, prioritizing image data when supplier data is clearly incorrect or unsupported.\n"
            "- The supplier data and images are equally important for extracting data unless specifically noted in a field's notes.\n"
            "- Only return null for Required fields if no trustworthy data exists; in such cases, provide a warning message.\n"
            "- Optional fields may be left null if data is untrustworthy or you lack confidence in your extracted value, with a brief explanation when feasible.\n"
            "- All field outputs must exactly match the requested structured output schema in naming, structure, data type, and order.\n"
        )

        if self.resource_type == 'Variant':
            system_instructions += (
                "- For dimension fields, always convert measurements to inches and use width for side-to-side and depth for front-to-back; verify each dimension's orientation using images.\n"
                "- Do not estimate values unless confident enough to avoid potential customer complaints; when in doubt, return null and explain.\n"
                "- Reuse dimension values from supplier data for other fields only when logically justified and consistent with product attributes.\n"
            )

        # Fields to extract along with notes and requirement of each field
        system_instructions += "\n"
        system_instructions += "# Field Extraction Details\n"

        fields_data = fields_to_extract[['Field', 'Notes', product_type]].to_dict(orient='records')
        counter = 0

        for field_data in fields_data:
            field = field_data['Field']
            notes = field_data['Notes']
            requirement = field_data[product_type]
            counter += 1

            field_fragment = f"{counter}. **{field}** ({requirement})"

            if pd.notna(notes):
                field_fragment += f": {notes}"
                
            system_instructions += field_fragment + "\n"

        system_instructions += (
            "\n"
            "# Output Format\n"
            "For each field, output an object with:\n"
            "- `reasoning`: Brief explanation of your decision, validation, and data source\n"
            "- `confidence`: One of 'low', 'medium', or 'high'\n"
            "- `warning`: Required only if value is null and field is Required. Description of the issue\n"
            "- `source`: One of 'supplier data', 'image', 'both', or 'inferred', indicating the primary data source\n"
            "- `value`: Extracted number, string, array, object, or null (if unsure or insufficient data)\n"
            "\n"
            "# Stop Conditions\n"
            "- Complete all requested fields per schema and requirements before outputting results; escalate for clarification if critical schema or data is missing or ambiguous."
        )

        return system_instructions

    def __compose_prompt(self, object_id: str, product_vendor: str, variants_data: List[Dict]) -> str:
        """
        Compose user prompt for a product or variant payload.
        """

        user_prompt = (
            f"Extract the data as structured output for the fields specified in the request payload and system instructions.\n\n"
        )

        # Introduce the SKUs (and vendor) that the system will extract data for.
        if self.resource_type == 'Product' and len(variants_data) > 1:
            skus = [variant_data['sku'] for variant_data in variants_data]

            user_prompt += (
                f"The product you're reviewing consists of {len(variants_data)} variants. Their SKUs are: {skus}. "
                "The fields that you're expected to extract data for are at the product level and will be relevant to all of the SKUs.\n\n"
            )
        else:
            if self.resource_type == 'Product':
                sku = variants_data[0]['sku']
            else:
                for variant_data in variants_data:
                    if variant_data['id'] == object_id:
                        sku = variant_data['sku']

            user_prompt += (
                f"You will review data for SKU {sku} by our supplier {product_vendor}. \n\n"
            )

        # If the current process is for one of many variants, then warn system that not all images are for this variant. 
        if self.resource_type == 'Variant' and len(variants_data) > 1:
            user_prompt += (
                "Note: The images provided are for the product this SKU belongs to, and may or may not depict this specific SKU/variant. "
                "You should still review and make use of all the images - just make sure any data that you extract from an image is appropriate for the SKU.\n\n"
            )

        # Supplier and web search result data for the variant(s)
        user_prompt += "# Supplier Data"

        for variant_data in variants_data:
            if self.resource_type == 'Product' or (self.resource_type == 'Variant' and variant_data['id'] == object_id):
                user_prompt += f"## SKU: {variant_data['sku']}\n"
                if not pd.isna(variant_data['image/url']):
                    user_prompt += f"- **Specific image URL**: {variant_data['image/url']} \n"
                for key, value in variant_data['supplier_data'].items():
                    if key is not self.sku_col_name:
                        cleaned_key = key.replace(":", "")
                        user_prompt += f"- **{cleaned_key}**: {value}\n"
                for key, value in variant_data['web_data'].items():
                    if value is not variant_data['sku']:
                        cleaned_key = key.replace(":", "")
                        user_prompt += f"- **{cleaned_key}**: {value}\n"
                user_prompt += "\n"

        return user_prompt

    def __build_payload(self, object_id: str, system_instructions: str, user_prompt: str, img_urls: List[str], output_schema: Dict) -> Dict:
        """
        Construct a single structured API payload for a product or variant.
        """

        content = [{'type': 'input_text', 'text': user_prompt}]

        for img_url in img_urls:
            img_url_json_object = {
                'type': 'input_image',
                'image_url': img_url,
                'detail': 'low'
            }
            content.append(img_url_json_object)

        payload = {
            'custom_id': object_id,
            'method': 'POST',
            'url': self.batch_manager.endpoint,
            'body': {
                'model': self.batch_manager.model,
                'reasoning': {
                    'effort': 'medium'
                },
                'instructions': system_instructions,
                'input': [
                    {
                        'role': 'user',
                        'content': content
                    }
                ],
                'text': {
                    'format': {
                        'type': 'json_schema',
                        'name': 'fields_extracted_response',
                        'strict': True,
                        'schema': output_schema
                    },
                    'verbosity': 'low'
                }
            }
        }

        return payload