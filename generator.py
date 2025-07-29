import json
import pandas as pd
from typing import List, Tuple, Dict
from fragments import object_schema_reference
from manager import BatchManager


class PayloadsGenerator:
    def __init__(self, sku_col_name: str, supplier_data_df: pd.DataFrame, image_urls_df: pd.DataFrame, fields_df: pd.DataFrame, sku_to_model: Dict, model_to_skus: Dict):
        self.sku_col_name: str = sku_col_name
        # Inputs
        self.supplier_data_df: pd.DataFrame = supplier_data_df
        self.image_urls_df: pd.DataFrame = image_urls_df
        self.fields_df: pd.DataFrame = fields_df
        # References
        self.sku_to_model: str = sku_to_model
        self.model_to_skus: Dict = model_to_skus
        self.dependency_results: Dict = {}
        # Per-process Variables
        self.process_order_number: int = None
        self.batch_manager: BatchManager = None

    def generate_batch_payloads(self, process_order_number: int, batch_manager: BatchManager):
        """
        Generate a list of request payloads for each SKU in the supplier CSV with product images.
        """

        self.process_order_number = process_order_number
        self.batch_manager = batch_manager

        # Loop through each row of supplier data, processing only SKUs that have hosted product images
        for _, row in self.supplier_data_df.iterrows():
            sku = str(row[self.sku_col_name]).strip()

            if sku in self.image_urls_df['sku'].to_list():
                print(f"Generating payload for SKU {sku}...")
                variant_id = self.image_urls_df[self.image_urls_df['sku'] == sku]['id'].iloc[0]
                product_id = self.image_urls_df[self.image_urls_df['sku'] == sku]['__parentId'].iloc[0]
                product_type = self.image_urls_df[self.image_urls_df['id'] == product_id]['productType'].iloc[0]
                product_vendor = self.image_urls_df[self.image_urls_df['id'] == product_id]['vendor'].iloc[0]

                # Get all the supplier product data for this SKU, dropping any blank values
                supplier_row_data = row.dropna().to_dict()

                # Get the featured image URL for the product variant (NaN if not available) and image URLs for the product
                variant_img_url = self.image_urls_df[self.image_urls_df['sku'] == sku]['image/url'].iloc[0]
                product_img_urls = self.image_urls_df[self.image_urls_df['__parentId'] == product_id]['image/url'].drop_duplicates().to_list()

                # Get related SKUs data if possible
                related_skus_data = {}
                
                if sku in self.sku_to_model:
                    model = self.sku_to_model[sku]
                    related_skus = self.model_to_skus[model]
                    related_skus_data = []

                    for related_sku in related_skus:
                        related_sku_data = self.supplier_data_df[self.supplier_data_df[self.sku_col_name] == related_sku].iloc[0].dropna().to_dict()
                        related_skus_data.append(related_sku_data)

                # Get fields to extract for the SKU, dropping fields that are not relevant to its product type and failed dependency conditions
                fields_to_extract = self.fields_df[self.fields_df['Process Order Number'] == process_order_number].dropna(subset=[product_type])

                if self.dependency_results:
                    dependency_fields = fields_to_extract['Dependency'].dropna().unique().to_list()

                    for dependency_field in dependency_fields:
                        if self.dependency_results[variant_id][dependency_field] is not True:
                            fields_to_extract = fields_to_extract[fields_to_extract['Dependency'] != dependency_field]

                # Generate the request payload for this SKU
                system_instructions, user_prompt = self.__build_prompt(sku, product_type, product_vendor, supplier_row_data, related_skus_data, fields_to_extract)
                schema = self.__build_schema(fields_to_extract)
                payload = self.__generate_single_payload(variant_id, batch_manager, system_instructions, user_prompt, variant_img_url, product_img_urls, schema)

                # Write final payload for this SKU to batch payloads JSONL file
                print(f"Payload generated for SKU {sku}. Writing to JSONL file.")
                batch_manager.write(payload)

        # If this is the first process order sequence, read the results output to fetch dependency field booleans for each processed SKU
        if self.process_order_number == 1:
            self.set_dependency_results()

    def __build_prompt(self, sku: str, product_type: str, product_vendor: str, supplier_row_data: Dict, related_skus_data: Dict, fields_to_extract: pd.DataFrame) -> Tuple[str, str]:
        """
        Compose system instructions and user prompt for a single SKU.
        """

        system_instructions = (
            "You are an expert product data analyst for a large home goods retailer like Wayfair. "
            "Your job is to extract standardized field values from supplier spreadsheet data, product images, and crawled website data. "
            "The user will provide supplier data as a stringified JSON object, and product images as a list of image URLs. "
            "All key-value pairs in the JSON should be carefully examined when evaluating each field. "
            "Specific instructions and rules may be provided for certain fields — follow these exactly. "
            "Each field will be labeled as either 'Required' or 'Optional'. "
            "'Required' fields must never be left null unless no reliable data exists — in such cases, include an appropriate warning. "
            "'Optional' fields may be left null if no trustworthy value can be extracted. "
            "Images are provided at the product level and may include variants with different sizes from the main SKU. "
            "Never guess or create new dimension values based solely on image appearances. "
            "However, you may reuse dimension values from supplier data if the label clearly maps to the intended field. "
            "For example, the 'Clearance Height' of a coffee table may be used for the 'Leg Dimension' field if applicable. "
            "For web search, make sure to only use data you're able to find on the suppliers' official website (which often has the supplier's name in the URL). "
            "Supplier data for related SKUs may also be provided. These are similar in design to the main SKU but may differ in size, material, or color. "
            "You may use data from related SKUs to fill gaps in the main SKU. "
            "If a minority of related SKUs have dimension values that differ slightly from the majority, normalize to the most common value. "
            "Be aware that supplier data may include typos or errors. Cross-check all data sources to validate your decision. "
            "When a value cannot be determined confidently and estimation could result in customer complaints, return null. "
            "The priority order for sourcing data should be: (1) supplier data, (2) images, (3) website data (if provided), (4) related SKU data. "
            "Return a structured JSON object named 'fields_extracted_response' that complies with the schema provided in the request payload. "
            "Each field must be accompanied by a confidence rating ('Low', 'Medium', or 'High'), even if null. "
            "Additionally, include a clear reasoning explaining the extracted value or why the field is null."
        )

        user_prompt = f"Review the following data for SKU {sku} (in stringified JSON format) from our supplier {product_vendor}: {supplier_row_data} "

        if related_skus_data:
            user_prompt += (
                f"SKU {sku} also has related SKUs that share the same design and features, but with potentially different material, color, and size. "
                "Consider the following data (also in stringified JSON format) of the related SKUs to potentially fill gaps and fix inconsistencies/errors: "
            )

            for related_sku_data in related_skus_data:
                user_prompt += f"{related_sku_data} "

        user_prompt += "Try to match these data to the corresponding fields below, following their specific notes/instructions if available: "

        fields = fields_to_extract['Field'].to_list()

        for field in fields:
            notes = fields_to_extract[fields_to_extract['Field'] == field]['Notes'].iloc[0]
            requirement = fields_to_extract[fields_to_extract['Field'] == field][product_type].iloc[0]

            prompt_fragment = (
                f"Field Name: {field} "
                f"Required or Optional?: {requirement} "
                f"Notes: {notes if notes else 'None'} "
            )

            user_prompt += prompt_fragment

        return system_instructions, user_prompt

    def __build_schema(self, fields_to_extract: pd.DataFrame) -> Dict:
        """
        Compose custom schema for the structured JSON output tailored to a payload's extracted fields.
        """

        fields = fields_to_extract['Field']
        schema_properties = {}

        for field in fields:
            field_value_structure = {}
            field_type = fields_to_extract[fields_to_extract['Field'] == field]['JSON Type'].iloc[0]
            field_enum_values = fields_to_extract[fields_to_extract['Field'] == field]['JSON Enum Values'].iloc[0]
            field_array_items = fields_to_extract[fields_to_extract['Field'] == field]['JSON Array Items'].iloc[0]
            field_object_type = fields_to_extract[fields_to_extract['Field'] == field]['JSON Object Type'].iloc[0]

            if field_type in ['string', 'number', 'boolean']:
                field_value_structure['type'] = field_type
            elif field_type == 'enum':
                field_value_structure['enum'] = json.loads(field_enum_values)
            elif field_type == 'object':
                field_value_structure = object_schema_reference[field_object_type]
            elif field_type == 'array':
                field_value_structure['type'] = field_type

                if field_array_items in ['string', 'number', 'boolean']:
                    field_value_structure['items'] = {'type': field_array_items}
                elif field_array_items == 'enum':
                    field_value_structure['items'] = {'enum': json.loads(field_enum_values)}
                elif field_array_items == 'object':
                    field_value_structure['items'] = object_schema_reference[field_object_type]

            schema_properties[field] = {
                'type': 'object',
                'properties': {
                    'value': field_value_structure,
                    'confidence': {'enum': ['low', 'medium', 'high']},
                    'reasoning': {'type': 'string'},
                    'warning': {'type': 'string'}
                }
            }

        schema = {'type': 'object', 'properties': schema_properties}

        return schema

    def __generate_single_payload(self, variant_id: str, system_instructions: str, user_prompt: str, variant_img_url: str, product_img_urls: List[str], schema: Dict) -> Dict:
        """
        Construct a single structured API payload for an SKU.
        """

        input_img_json_objects = []

        if variant_img_url and not pd.isna(variant_img_url):
            variant_img_json_object = {
                'type': 'input_image',
                'image_url': variant_img_url
            }
            input_img_json_objects.append(variant_img_json_object)

        for product_img_url in product_img_urls:
            product_img_json_object = {
                'type': 'input_image',
                'image_url': product_img_url
            }
            input_img_json_objects.append(product_img_json_object)

        content = [{'type': 'input_text', 'text': user_prompt}] + input_img_json_objects

        payload = {
            'custom_id': variant_id,
            'method': 'POST',
            'url': self.batch_manager.endpoint,
            'body': {
                'model': self.batch_manager.model,
                'tools': [{ "type": "web_search_preview" }],
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
                        'schema': schema
                    }
                }
            }
        }

        return payload

    def set_dependency_results(self):
        self.dependency_results = {}
        dependency_fields = self.fields_df['Dependency'].dropna().unique()

        with open(self.batch_manager.batch_results_path, 'r', encoding='ascii') as f:
            for line in f:
                if line.strip():  # Skip empty lines
                    try:
                        line = json.loads(line)
                        variant_id = line['custom_id']
                        results = line['body']['messages'][1]['content']
                        
                        for dependency_field in dependency_fields:
                            result = results[dependency_field]['value']
                            self.dependency_results[variant_id] = {dependency_field: result}

                    except json.JSONDecodeError as e:
                        print(f"Error decoding JSON on line: {line.strip()}. Error: {e}")