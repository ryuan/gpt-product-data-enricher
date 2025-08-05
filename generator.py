import json
import tiktoken
import pandas as pd
from typing import List, Dict, Union
from fragments import object_schema_reference
from manager import BatchManager
from crawler import WebSearchTool
from pprint import pprint


class PayloadsGenerator:
    def __init__(self, crawler: WebSearchTool, sku_col_name: str, 
                 supplier_data_df: pd.DataFrame, store_data_df: pd.DataFrame, fields_data_df: pd.DataFrame, product_ids_skus: Dict):
        self.crawler: WebSearchTool = crawler
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
        self.total_tokens = 0
        self.encoder: tiktoken.Encoding = None

    def generate_batch_payloads(self, process_order_number: int, batch_manager: BatchManager):
        """
        Generate a list of request payloads for each SKU in the supplier CSV with product images.
        """

        self.process_order_number = process_order_number
        self.resource_type = self.fields_data_df.loc[self.fields_data_df['Process Order Number'] == self.process_order_number, 'Resource'].iloc[0]
        self.batch_manager = batch_manager
        self.total_tokens = 0
        self.encoder = tiktoken.encoding_for_model(batch_manager.model)
        product_ids: List[str] = self.store_data_df.loc[self.store_data_df['id'].str.startswith('gid://shopify/Product/'), 'id'].to_list()

        for product_id in product_ids:
            product_type: str = self.store_data_df.loc[self.store_data_df['id'] == product_id, 'productType'].iloc[0]
            product_vendor: str = self.store_data_df.loc[self.store_data_df['id'] == product_id, 'vendor'].iloc[0]
            product_img_urls: List[str] = self.store_data_df.loc[
                (self.store_data_df['id'].str.startswith('gid://shopify/MediaImage/')) & 
                (self.store_data_df['__parentId'] == product_id), 
                'image/url'
            ].to_list()

            # Get fields to extract, filtered for product type and passed dependency conditions
            fields_to_extract = self.fields_data_df[self.fields_data_df['Process Order Number'] == process_order_number].dropna(subset=[product_type])

            if self.dependency_results:
                dependency_fields = fields_to_extract['Dependency'].dropna().unique()

                for dependency_field in dependency_fields:
                    if self.dependency_results[product_id][dependency_field] is not True:
                        fields_to_extract = fields_to_extract[fields_to_extract['Dependency'] != dependency_field]

            if not fields_to_extract.empty:
                # Get store data for the product's SKUs
                variants_data: List[Dict] = self.store_data_df.loc[
                    (self.store_data_df['id'].str.startswith('gid://shopify/ProductVariant/')) & 
                    (self.store_data_df['__parentId'] == product_id), 
                    ['id', 'sku', 'selectedOptions/0/name', 'image/url']
                ].to_dict(orient='records')

                # Get all the supplier and web search data for the SKUs beloging to the product, dropping any blank/NaN values
                for variant_data in variants_data:
                    sku: str = variant_data['sku']
                    supplier_sku_data = self.supplier_data_df[self.supplier_data_df[self.sku_col_name] == sku].iloc[0].dropna().to_dict()
                    variant_data['supplier_data'] = supplier_sku_data
                    variant_data['web_data'] = self.crawler.web_search_results[variant_data['id']] if variant_data['id'] in self.crawler.web_search_results.keys() else {}

                # Build the output JSON schema based on the extraction fields
                output_schema = self.__build_output_schema(fields_to_extract)

                # For product or each variant, generate the request payload, then write to line in batch payloads JSONL file
                if self.resource_type == 'Product' and len(variants_data) != 0:
                    print(f"Generating payload for Product ID: {product_id}")
                    self.__generate_write_payload(product_type, product_vendor, variants_data, fields_to_extract, product_id, product_img_urls, output_schema)
                elif self.resource_type == 'Variant' and len(variants_data) != 0:
                    for variant_data in variants_data:
                        if variant_data['supplier_data'] is not None:      # Skip any variants where the SKU is missing in the supplier data
                            variant_id = variant_data['id']
                            print(f"Generating payload for Variant ID: {variant_id}")
                            self.__generate_write_payload(product_type, product_vendor, variant_data, fields_to_extract, variant_id, product_img_urls, output_schema)

        print(f"Estimated total tokens in batch {self.process_order_number} = {self.total_tokens}")

    def set_dependency_results(self):
        """
        For each product processed in the first sequence process, collect whether extracted required fields were True or False
        """

        self.dependency_results = {}
        dependency_fields = self.fields_data_df['Dependency'].dropna().unique()

        with open(self.batch_manager.batch_results_path, 'r', encoding='ascii') as f:
            for line in f:
                if line.strip():  # Skip empty lines
                    try:
                        line = json.loads(line)
                        product_id = line['custom_id']
                        self.dependency_results[product_id] = {}
                        results: Dict = json.loads(line['response']['body']['output'][0]['content'][0]['text'])
                        
                        for dependency_field in dependency_fields:
                            if dependency_field in results.keys():
                                self.dependency_results[product_id][dependency_field] = results[dependency_field]['value']

                    except json.JSONDecodeError as e:
                        print(f"Error decoding JSON on line: {line.strip()}. Error: {e}")

    def __build_output_schema(self, fields_to_extract: pd.DataFrame) -> Dict:
        """
        Compose custom schema for the structured JSON output tailored to a payload's extracted fields.
        """

        fields = fields_to_extract['Field'].to_list()
        schema_properties = {}

        for field in fields:
            field_value_structure = {}
            field_type = fields_to_extract[fields_to_extract['Field'] == field]['JSON Type'].iloc[0]
            field_enum_values = fields_to_extract[fields_to_extract['Field'] == field]['JSON Enum Values'].iloc[0]
            field_array_items = fields_to_extract[fields_to_extract['Field'] == field]['JSON Array Items'].iloc[0]
            field_object_type = fields_to_extract[fields_to_extract['Field'] == field]['JSON Object Type'].iloc[0]

            if field_type in ['string', 'number', 'boolean']:
                field_value_structure['type'] = [field_type, 'null']
            elif field_type == 'enum':
                field_value_structure['type'] = ['string', 'null']
                field_value_structure['enum'] = json.loads(field_enum_values) + [None]
            elif field_type == 'object':
                field_value_structure = object_schema_reference[field_object_type]
            elif field_type == 'array':
                field_value_structure['type'] = [field_type, 'null']

                if field_array_items in ['string', 'number', 'boolean']:
                    field_value_structure['items'] = {'type': field_array_items}
                elif field_array_items == 'enum':
                    field_value_structure['items'] = {'enum': json.loads(field_enum_values)}
                elif field_array_items == 'object':
                    field_value_structure['items'] = object_schema_reference[field_object_type]

            schema_properties[field] = {
                'type': 'object',
                'properties': {
                    'reasoning': {'type': 'string'},
                    'confidence': {'enum': ['low', 'medium', 'high']},
                    'warning': {'type': 'string'},
                    'value': field_value_structure,
                },
                'required': ['value', 'confidence', 'reasoning', 'warning'],
                'additionalProperties': False
            }

        schema = {'type': 'object', 'properties': schema_properties, 'required': fields, 'additionalProperties': False}

        return schema

    def __generate_write_payload(self, product_type: str, product_vendor: str, variants_data: Union[List[Dict], Dict], fields_to_extract: pd.DataFrame, 
                                 object_id: str, product_img_urls: List[str], output_schema: Dict):
        """
        Built the prompt, generate the payload, then write to batch payloads JSONL file.
        """

        # Coerce variants_data to a list if dict (representing a single variant data) was used as arg
        if isinstance(variants_data, dict):
            variants_data = [variants_data]

        system_instructions = self.__build_instructions()
        user_prompt = self.__build_prompt(product_type, product_vendor, variants_data, fields_to_extract)
        payload = self.__generate_single_payload(object_id, system_instructions, user_prompt, product_img_urls, output_schema)        
        self.batch_manager.write(payload)

    def __build_instructions(self) -> str:
        """
        Compose system instructions for a product or variant payload.
        """

        system_instructions = (
            "You are an expert product data analyst for a large home goods retailer like Wayfair. "
            "Your job is to extract standardized field values from supplier spreadsheet data and product images. "
            "Every attribute from the supplier data should be carefully examined when evaluating each field. "
            "Specific instructions and rules may be provided for certain fields — follow these exactly. "
            "Each field will be labeled as either 'Required' or 'Optional'. "
            "'Required' fields must never be left null unless no reliable data exists — in such cases, include an appropriate warning. "
            "'Optional' fields may be left null if no trustworthy value can be extracted. "
            "Be aware that supplier data may include typos or errors. Cross-check all data sources to validate your decision. "
            "When a value cannot be determined confidently and estimation could result in customer complaints, return null. "
            "All output fields must match the schema specified in the request exactly — including naming, structure, and data type. "
            "If no value is available, return null under value, but still include confidence and reasoning. "
        )

        if self.resource_type == 'Variant':
            system_instructions += (
                "Never guess or create new dimension values based solely on image appearances. "
                "However, you may reuse dimension values from supplier data if the label clearly maps to the intended field. "
                "For example, the 'Clearance Height' of a coffee table may be used for the 'Leg Dimension' field if applicable."
            )

        return system_instructions

    def __build_prompt(self, product_type: str, product_vendor: str, variants_data: List[Dict], fields_to_extract: pd.DataFrame) -> str:
        """
        Compose user prompt for a product or variant payload.
        """

        user_prompt = (
            f"Extract the fields specified in the 'fields_to_extract' object (provided separately in the input). \n"
            f"Use all sources of data: the supplier-provided attributes (in bullet format) and images provided in the payload. \n"
        )

        # If the current process is for a variant and the product has multiple variants, then warn model about image use. 
        if self.resource_type == 'Variant' and variants_data[0]['selectedOptions/0/name'] is not 'Title':
            user_prompt += (
                "Note: The images provided are for the product family this SKU belongs to, and may or may not depict this specific SKU or variant. \n"
                "Always confirm that an image is relevant to this SKU before using it to extract data. \n"
            )

        # Supplier and web search result data for the product's variant(s)
        if len(variants_data) == 1:
            user_prompt += (
                f"You will review data for SKU {variants_data[0]['sku']} by our supplier {product_vendor}. \n"
                "Here is the supplier data and web search result data (if any): \n\n"
            )
        else:
            skus = [variant_data['sku'] for variant_data in variants_data]

            user_prompt += (
                f"The product you're reviewing consists of {len(variants_data)} variants. Their SKUs are: {skus}. \n"
                "The fields that you're expected to extract data for are at the product level and will apply to all of the SKUs. \n"
                "Here are the supplier data and web search result data (if any) for each SKU: \n\n"
            )

        for variant_data in variants_data:
            user_prompt += f"SKU: {variant_data['sku']} \n"
            if not pd.isna(variant_data['image/url']):
                user_prompt += f"SKU Specific Image URL: {variant_data['image/url']} \n"
            for key, value in variant_data['supplier_data'].items():
                if key is not self.sku_col_name:
                    user_prompt += f"{key}: {value} \n"
            for key, value in variant_data['web_data'].items():
                if value is not variant_data['sku']:
                    user_prompt += f"{key}: {value} \n"

            user_prompt += "\n"

        # Fields to extract along with notes and requirement of each field
        user_prompt += "Here are the fields you'll be extracting data to. Follow any notes if specified for a specific field: \n\n"

        fields_data = fields_to_extract[['Field', 'Notes', product_type]].to_dict(orient='records')

        for field_data in fields_data:
            field = field_data['Field']
            notes = field_data['Notes']
            requirement = field_data[product_type]

            prompt_fragment = (
                f"Field Name: {field} \n"
                f"Notes: {notes if not pd.isna(notes) else 'None'} \n"
                f"Required or Optional?: {requirement} \n\n"
            )

            user_prompt += prompt_fragment

        return user_prompt

    def __generate_single_payload(self, object_id: str, system_instructions: str, user_prompt: str, product_img_urls: List[str], output_schema: Dict) -> Dict:
        """
        Construct a single structured API payload for a product or variant.
        """

        input_img_json_objects = []

        for product_img_url in product_img_urls:
            product_img_json_object = {
                'type': 'input_image',
                'image_url': product_img_url,
                'detail': 'low'
            }
            input_img_json_objects.append(product_img_json_object)

        content = [{'type': 'input_text', 'text': user_prompt}] + input_img_json_objects

        payload = {
            'custom_id': object_id,
            'method': 'POST',
            'url': self.batch_manager.endpoint,
            'body': {
                'model': self.batch_manager.model,
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
                    }
                }
            }
        }

        tokens = self.__estimate_tokens(system_instructions, user_prompt, product_img_urls, output_schema)

        return payload
    
    def __estimate_tokens(self, system_instructions: str, user_prompt: str, product_img_urls: List[str], output_schema: Dict) -> int:
        tokens = 0

        tokens += len(self.encoder.encode(system_instructions))
        tokens += len(self.encoder.encode(user_prompt))
        tokens += 85 * len(product_img_urls)        # 85 tokens limit if image 'detail' is set to 'low'
        tokens += len(self.encoder.encode(str(output_schema)))

        print(f"Estimated input payload tokens = {tokens}")
        self.total_tokens += tokens

        return tokens