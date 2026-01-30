import json
import os
import io
import base64
import mimetypes
import requests
import utils
import pandas as pd
from typing import List, Dict, Optional
from urllib.parse import urlparse
from fragments import object_schema_reference
from encoder import Encoder
from manager import BatchManager
from tag_parsor import optimize_notes


class PayloadsGenerator:
    def __init__(self, encoder: Encoder, batch_manager: BatchManager, supplier_data_df: pd.DataFrame, store_data_df: pd.DataFrame, fields_data_df: pd.DataFrame):
        self.encoder: Encoder = encoder
        self.batch_manager: BatchManager = batch_manager
        self.sku_col_name: str = self.__get_sku_col_name(supplier_data_df)
        self.group_by_title: bool = self.__set_product_processing_mode()
        # Inputs
        self.supplier_data_df: pd.DataFrame = supplier_data_df
        self.store_data_df: pd.DataFrame = store_data_df
        self.fields_data_df: pd.DataFrame = fields_data_df
        # References
        self.dependency_results: Dict = {}
        self.img_inputs_ref: Dict = {}
        self.resource_gid_base: Dict = {
            'Product': 'gid://shopify/Product/',
            'Variant': 'gid://shopify/ProductVariant/',
            'Media': 'gid://shopify/MediaImage/'
        }
        # Per-process Variables
        self.process_order_number: int = None
        self.resource_type: str = None

    def set_dependency_results(self):
        """
        For each product processed in the first sequence process, collect whether extracted required fields were True or False
        """

        self.dependency_results = {}
        dependency_fields: List[str] = self.fields_data_df['Dependency'].dropna().unique()

        with open(self.batch_manager.current_batch_files.batch_outputs_path, 'r', encoding='ascii') as f:
            for line in f:
                if line.strip():  # Skip empty lines
                    try:
                        output: Dict = json.loads(line)
                        product_id = output['id']
                        self.dependency_results[product_id] = {}
                        outputs: Dict = output['output']
                        
                        for dependency_field in dependency_fields:
                            if '&&' in dependency_field:
                                conj_dependency_fields = [field.strip() for field in dependency_field.split('&&')]
                                
                                for conj_dependency_field in conj_dependency_fields:
                                    if conj_dependency_field in outputs.keys():
                                        self.dependency_results[product_id][conj_dependency_field] = outputs[conj_dependency_field]['value']
                            elif dependency_field in outputs.keys():
                                self.dependency_results[product_id][dependency_field] = outputs[dependency_field]['value']
                    except json.JSONDecodeError as e:
                        print(f"Error decoding JSON on line: {line.strip()}. Error: {e}")

    def __get_sku_col_name(self, supplier_data_df: pd.DataFrame) -> str:
        # Get column name representing column data for SKU from the supplier data CSV/XLSX
        headers = list(supplier_data_df)
        utils.print_options(headers)
        sku_idx = int(input("Which name represents column data for SKU?: "))
        sku_col_name = headers[sku_idx]
        return sku_col_name
    
    def __set_product_processing_mode(self) -> bool:
        #Ask user if Product resource objects should be processed as groups of products sharing the same first word in title
        options = ["Product resource should be processed individually", "Product resource should be processed by groups sharing first word in title"]
        utils.print_options(options)
        print("Fields can be extracted either per-product, or together with other products sharing the same first word in their title.")
        group_by_title = bool(int(input("When extracting fields for resource type 'Product', how do you want them processed?: ")))
        return group_by_title

class DataExtractor(PayloadsGenerator):
    def __init__(self, encoder, batch_manager, supplier_data_df, store_data_df, fields_data_df, new_skus: List[str]):
        super().__init__(encoder, batch_manager, supplier_data_df, store_data_df, fields_data_df)
        # Inputs
        self.new_skus: List[str] = new_skus

    def generate_batch_payloads(self, process_order_number: int) -> None:
        self.process_order_number = process_order_number
        self.resource_type = self.fields_data_df.loc[self.fields_data_df['Process Order Number'] == self.process_order_number, 'Resource'].iloc[0]
        self.batch_manager.create_batch_files(self.process_order_number)
        filtered_store_data_df = self.__prepare_store_data_df()

        # Loop through each keyed group (either title first word or product ID), creating payload for its objects (products, variants, or images)
        for custom_id, grouped_store_data_df in filtered_store_data_df.groupby('GROUP_KEY'):
            schema_properties = {}
            definitions = {}
            required = []
            instructions_parts = []
            content = []

            for product_id in grouped_store_data_df['PRODUCT_ID'].unique():
                # Compile data for the product and its child objects (variants and media) from GraphQL and supplier data
                product_objects_data: Dict = self.__get_product_objects_data(product_id, grouped_store_data_df)

                # Get fields to extract for this resource based on product type
                fields_to_extract = self.__get_fields_to_extract(product_id, product_objects_data)
        
                if not fields_to_extract.empty:
                    # Prepare image inputs (uploading to Files API if needed) for the Batch API and build a ID-object reference
                    self.__set_img_inputs_ref(product_objects_data)

                    # Update output JSON schema parts based on the extraction fields
                    object_ids = grouped_store_data_df.loc[self.store_data_df['id'].str.startswith(self.resource_gid_base[self.resource_type]), 'id'].to_list()
                    self.__update_output_schema_parts(schema_properties, definitions, required, fields_to_extract, object_ids)

                    # Compose parts for system instructions and prompt/image content based on extraction fields
                    self.__update_instructions_parts(instructions_parts, product_objects_data, fields_to_extract)
                    self.__update_content(content, product_objects_data)

            instructions = '\n'.join(instructions_parts)
            output_schema = self.__build_output_schema(schema_properties, definitions, required)
            payload = self.__generate_single_payload(custom_id, instructions, content, output_schema)
            self.batch_manager.write(payload)

            tokens = self.encoder.estimate_input_tokens(self.process_order_number, instructions, content, output_schema)
            print(f"Estimated input payload tokens = {tokens}")

        self.batch_manager.close_batch_payloads_file()
        print(f"Estimated input tokens for batch {self.process_order_number} = {self.encoder.batch_tokens_estimate[self.process_order_number]}")
    
    def __prepare_store_data_df(self) -> pd.DataFrame:
        """
        Preprocess the store data to form processing groups, filtering out any that errored out in previous batch processes or do not include new SKU (if provided)
        """

        filtered_store_data_df: pd.DataFrame = self.store_data_df.copy()
        filtered_store_data_df['PRODUCT_ID'] = filtered_store_data_df['__parentId'].fillna(filtered_store_data_df['id'])
        filtered_store_data_df['TITLE_FIRST_WORD'] = filtered_store_data_df['title'].ffill().fillna('').str.split().str[0]

        # If the user wants grouped product processing context, process each group of products sharing the same title first word, otherwise process per-product
        if self.resource_type == 'Product' and self.group_by_title:
            filtered_store_data_df['GROUP_KEY'] = filtered_store_data_df['TITLE_FIRST_WORD']
        else:
            filtered_store_data_df['GROUP_KEY'] = filtered_store_data_df['PRODUCT_ID']

        # Filter the store data to remove any products that errored out during previous batch processing
        filtered_store_data_df = filtered_store_data_df[
            (~filtered_store_data_df["PRODUCT_ID"].isin(self.batch_manager.error_ids)) &
            (~filtered_store_data_df["TITLE_FIRST_WORD"].isin(self.batch_manager.error_ids))
        ]

        # If a list of `new_skus` was provided, filter out any keyed groups that doesn't include any new SKUs (otherwise default to processing all groups)
        if self.new_skus:
            filtered_store_data_df = filtered_store_data_df.groupby('GROUP_KEY').filter(
                lambda x: x['sku'].isin(self.new_skus).any()
            )

        return filtered_store_data_df

    def __get_product_objects_data(self, product_id: str, store_data_df: pd.DataFrame) -> Dict:
        """
        Extract store data rows related to a product and its variants and media objects, supplier data for the variants, and return a dictionary object
        """

        product_objects_data: Dict = store_data_df.loc[
            store_data_df['id'] == product_id, 
            ['id', 'vendor', 'productType']
        ].iloc[0].to_dict()

        # Get all the variants objects (and their featured media image object) for the product
        product_objects_data['variants'] = store_data_df.loc[
            (store_data_df['id'].str.startswith(self.resource_gid_base['Variant'])) & 
            (store_data_df['__parentId'] == product_id), 
            ['id', 'sku']
        ].to_dict(orient='records')

        for variant_object_data in product_objects_data['variants']:
            variant_id = variant_object_data['id']
            variant_media_object_data = store_data_df.loc[
                (store_data_df['id'].str.startswith(self.resource_gid_base['Media'])) & 
                (store_data_df['__parentId'] == variant_id), 
                ['id']
            ]
            variant_object_data['media'] = variant_media_object_data.iloc[0].to_dict() if not variant_media_object_data.empty else None

        # Get all the supplier data for the SKUs beloging to the product, dropping any blank/NaN values
        for variant_object_data in product_objects_data['variants']:
            sku: str = variant_object_data['sku']
            variant_object_data['supplier_data'] = self.supplier_data_df[self.supplier_data_df[self.sku_col_name] == sku].iloc[0].dropna().to_dict()

        # Get all the media image objects for the product
        product_objects_data['media'] = store_data_df.loc[
            (store_data_df['id'].str.startswith(self.resource_gid_base['Media'])) & 
            (store_data_df['__parentId'] == product_id), 
            ['id', 'image/url']
        ].to_dict(orient='records')

        return product_objects_data
    
    def __set_img_inputs_ref(self, product_objects_data: Dict, timeout: int = 10, detail: str = 'low'):
        """
        Options are (1) upload images with Files API and return their file IDs, (2) return their base64 encoded images, or (3) supply Shopify URLs
        """

        session = requests.Session()
        session.headers.update({'User-Agent': 'Mozilla/5.0'})
        media_objects_data = product_objects_data['media']
        num_images_in_batch = len(self.store_data_df['image/url'].notna())

        for media_object_data in media_objects_data:
            img_id = media_object_data['id']
            img_url = media_object_data['image/url']

            try:
                if img_id not in self.img_inputs_ref:
                    response = session.get(img_url, timeout=timeout)
                    response.raise_for_status()

                    # Responses API: upload images via Files API and get image file IDs
                    if 'responses' in self.batch_manager.endpoint:
                        image_file = io.BytesIO(response.content)
                        parsed_url = urlparse(img_url)
                        image_file.name = os.path.basename(parsed_url.path)
                        result = self.batch_manager.client.files.create(
                            file=image_file,
                            purpose='vision',
                            expires_after={
                                'anchor': 'created_at',
                                'seconds': 86400
                            }
                        )
                        self.img_inputs_ref[img_id] = {
                            'type': 'input_image',
                            'file_id': result.id,
                            'detail': detail
                        }
                    # Chat Completions API: encode images to base64 (max 300 images in batch to stay under batch 200 MB limit) or suppler Shopify URL
                    elif 'completions' in self.batch_manager.endpoint:
                        if num_images_in_batch < 300:
                            # Prefer server-declared Content-Type; fall back to URL-based guess; default to JPEG
                            mime = (response.headers.get('Content-Type', '') or '').split(';', 1)[0].lower()

                            if not mime.startswith('image/'):
                                mime = (mimetypes.guess_type(img_url)[0] or 'image/jpeg').lower()

                            b64_img = base64.b64encode(response.content).decode('utf-8')
                            input_url = f'data:{mime};base64,{b64_img}'
                        else:
                            input_url = img_url
                        
                        self.img_inputs_ref[img_id] = {
                            'type': 'image_url',
                            'image_url': {
                                'url': input_url,
                                'detail': detail
                            },
                        }
            except Exception:
                print(f"Could not crawl image URL: {img_url}")

    def __get_fields_to_extract(self, product_id: str, product_objects_data: Dict) -> pd.DataFrame:
        """
        Get the fields based on product type and process number, omitting any fields that did not pass dependency check, and update notes + enum values
        """

        product_type = product_objects_data['productType']
        fields_to_extract = self.fields_data_df[self.fields_data_df['Process Order Number'] == self.process_order_number].dropna(subset=[product_type])

        if self.dependency_results:
            dependency_fields: List[str] = fields_to_extract['Dependency'].dropna().unique()

            for dependency_field in dependency_fields:
                if '&&' in dependency_field:
                    conj_dependency_fields = [field.strip() for field in dependency_field.split('&&')]
                    dependency_result = True
                    
                    for conj_dependency_field in conj_dependency_fields:
                        if conj_dependency_field in self.dependency_results.get(product_id, {}):    # Falsability check for sometimes unused dependencies like Framed
                            if self.dependency_results[product_id][conj_dependency_field] is not True:
                                dependency_result = False
                                break

                    if dependency_result == False:
                        fields_to_extract = fields_to_extract[fields_to_extract['Dependency'] != dependency_field]
                elif dependency_field in self.dependency_results.get(product_id, {}):   # Falsability check for sometimes unused dependencies like Framed
                    if self.dependency_results[product_id][dependency_field] is not True:
                        fields_to_extract = fields_to_extract[fields_to_extract['Dependency'] != dependency_field]

        # Update Notes values for optimized notes and JSON Enum Values for type-specific enum values
        field_names = self.fields_data_df.loc[self.fields_data_df[product_type].notna(), 'Field'].to_list()

        fields_to_extract['Notes'] = fields_to_extract['Notes'].apply(
            lambda x: optimize_notes(x, field_names, product_type) if pd.notna(x) else x
        )
        fields_to_extract["JSON Enum Values"] = [
            enum_vals_str if not isinstance(enum_vals_str, str)
            else (enum_vals_str if isinstance(enum_vals_obj := json.loads(enum_vals_str), list) 
            else json.dumps(enum_vals_obj[product_type]))
            for enum_vals_str in fields_to_extract["JSON Enum Values"]
        ]

        return fields_to_extract

    def __update_output_schema_parts(self, schema_properties: Dict, definitions: Dict, required: List, fields_to_extract: pd.DataFrame, object_ids: List[str]):
        """
        Compose custom schema parts for the structured JSON output tailored to a payload's extracted fields.
        """

        fields = fields_to_extract['Field'].to_list()
        fields_schema_properties = {}

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
                field_value_structure = { '$ref': f'#/$defs/{field_object_type}' }
            elif field_type == 'array':
                field_value_structure['type'] = [field_type, 'null']

                if field_array_items in ['string', 'number', 'boolean']:
                    field_value_structure['items'] = { 'type': field_array_items }
                elif field_array_items == 'enum':
                    field_value_structure['items'] = { 'enum': json.loads(field_enum_values) + [None] }
                elif field_array_items == 'object':
                    field_value_structure['items'] = { '$ref': f'#/$defs/{field_object_type}' }

            fields_schema_properties[field] = {
                'type': 'object',
                'properties': {
                    'reasoning': {'type': 'string'},
                    'confidence': {'type': ['string', 'null'], 'enum': ['low', 'medium', 'high']},
                    'warning': {'type': ['string', 'null']},
                    'source': {'type': ['string', 'null'], 'enum': ['supplier data', 'image', 'both', 'inferred']},
                    'value': field_value_structure,
                },
                'required': ['reasoning', 'confidence', 'warning', 'source', 'value'],
                'additionalProperties': False
            }

            if not pd.isna(field_object_type):
                for reference_schema in object_schema_reference[field_object_type]:
                    definitions.update(reference_schema)

        for object_id in object_ids:
            schema_properties[object_id] = {
                'type': 'object',
                'properties': fields_schema_properties,
                'required': fields,
                'additionalProperties': False
            }
            required.append(object_id)
    
    def __build_output_schema(self, schema_properties: Dict, definitions: Dict, required: List) -> Dict:
        schema = {
            'type': 'object',
            'properties': schema_properties,
            '$defs': definitions,
            'required': required,
            'additionalProperties': False
        }

        return schema

    def __update_instructions_parts(self, instructions_parts: List, product_objects_data: Dict, fields_to_extract: pd.DataFrame):
        """
        Compose system instructions for a product or variant payload.
        """

        if not instructions_parts:
            base_instructions = (
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
                "- Always use the more popular American English grammar and spelling (e.g., 'gray' should be preferred over 'grey').\n"
                "- Format text fields with longer descriptive values (e.g., 'Description', 'Highlights', and 'Care Instruction') in APA-style sentence case.\n"
                "- Format text fields with shorter categorical/label-like values in APA-style title case.\n"
                "- For outputs in string format, always use ASCII characters only.\n"
            )

            if self.resource_type == 'Variant':
                base_instructions += (
                    "- For dimension fields, always convert measurements to inches.\n"
                    "- We define width as side-to-side measurement and depth as front-to-back measurement.\n"
                    "- Verify the supplier's orientation for width, depth, or length using images since they might differ from our definition.\n"
                    "- Do not estimate values unless confident enough to avoid potential customer complaints; when in doubt, return null and explain.\n"
                    "- Reuse dimension values from supplier data for other fields only when logically justified and consistent with product attributes.\n"
                )

            base_instructions += (
                "# Output Format\n"
                "For each field, output an object with:\n"
                "- `reasoning`: Brief explanation of your decision, validation, and data source\n"
                "- `confidence`: One of 'low', 'medium', or 'high'\n"
                "- `warning`: Required only if value is null and field is Required. Description of the issue\n"
                "- `source`: One of 'supplier data', 'image', 'both', or 'inferred', indicating the primary data source\n"
                "- `value`: Extracted number, string, array, object, or null (if unsure or insufficient data)\n"
                "\n"
                "# Stop Conditions\n"
                "- Complete all requested fields per schema and requirements before outputting results; escalate for clarification if critical schema or data is missing or ambiguous.\n"
            )

            base_instructions += "# Field Extraction Details\n"
            instructions_parts.append(base_instructions)

        # Fields to extract along with notes and requirement of each field
        product_type = product_objects_data['productType']
        fields_data = fields_to_extract[['Field', 'Notes', product_type]].to_dict(orient='records')
        counter = 0

        for field_data in fields_data:
            field_instructions = ""
            field = field_data['Field']
            notes = field_data['Notes']
            requirement = field_data[product_type]
            counter += 1

            field_fragment = f"{counter}. **{field}** ({requirement})"

            if pd.notna(notes):
                field_fragment += f": {notes}"
                
            field_instructions += field_fragment + "\n"

            if field_instructions not in instructions_parts:
                instructions_parts.append(field_instructions)

    def __update_content(self, content: List, product_objects_data: Dict):
        """
        Compose user prompt for a product or variant payload.
        """

        product_id = product_objects_data['id']
        vendor = product_objects_data['vendor']
        variant_objects_data = product_objects_data['variants']
        media_objects_data = product_objects_data['media']
        prompt_input_type = 'input_text' if 'responses' in self.batch_manager.endpoint else 'text'

        if not content:
            intro_prompt = (
                f"Extract the data as structured output for the fields specified in the request payload and system instructions.\n\n"
            )
            self.__append_prompt_input_to_content(content, prompt_input_type, intro_prompt)

        # Introduce the SKUs (and vendor) that the system will extract data for.
        skus = [variant_object_data['sku'] for variant_object_data in variant_objects_data]

        if self.resource_type == 'Product':
            skus_prompt = f"# Supplier Data & Images for Product ID: {product_id}\n"
        else:
            skus_prompt = ""

        if len(skus) > 1:
            if self.resource_type == 'Product':
                skus_prompt += (
                    f"This product by our supplier {vendor} consists of {len(variant_objects_data)} variants - their supplier data are provided below.\n"
                    f"Their SKUs are: {skus}.\n"
                    "All these variants belong to the product. Supplier data will include both data that's specific to each SKU and data that's applicable across all SKUs. "
                    "The fields that you're expected to extract data for are at the product level and will be relevant to all of the SKUs.\n\n"
                )
            elif self.resource_type == 'Variant':
                skus_prompt += (
                    f"You will review data for {len(variant_objects_data)} variants by our supplier {vendor}.\n"
                    f"Their SKUs are: {skus}.\n"
                    "Review the supplier data to make sure that the data extracted for each variant's field is specifically applicable to that SKU. "
                    "If supplier provides data for only one SKU while omitting it from other SKUs, but you believe that the data is applicable for all variants, extract it for all. "
                    "Note: The images provided are for the product this SKU belongs to, and may or may not depict all SKUs/variants.\n\n"
                )
            elif self.resource_type == 'Media':
                skus_prompt += (
                    f"Data for {len(variant_objects_data)} variants from our supplier {vendor} are provided below to assist your image data labeling.\n"
                    f"Their SKUs are: {skus}.\n"
                    f"The images that you're extracting data for are at the product level even though it has multiple variants. "
                    "That means some of the supplier data for SKUs may not be applicable to the images if images specific to those SKUs were never provided.\n\n"
                )
        elif len(skus) == 1:
            sku = skus[0]
            skus_prompt += (
                f"You will review data for SKU {sku} by our supplier {vendor}.\n\n"
            )

        self.__append_prompt_input_to_content(content, prompt_input_type, skus_prompt)

        # Supplier data for the variant(s)
        for variant_object_data in variant_objects_data:
            variant_id: str = variant_object_data['id']
            sku: str = variant_object_data['sku']
            supplier_data: Dict[str, str] = variant_object_data['supplier_data']
            featured_media: Optional[Dict] = variant_object_data['media']

            if supplier_data:
                if self.resource_type == 'Variant':
                    supplier_data_prompt = f"## Supplier Data for SKU {sku} with Variant ID: {variant_id}\n"
                else:
                    supplier_data_prompt = f"## Supplier Data for SKU: {sku}\n"

                for key, value in supplier_data.items():
                    if key != self.sku_col_name:
                        cleaned_key = key.replace(":", "")
                        supplier_data_prompt += f"- **{cleaned_key}**: {value}\n"

                if featured_media:
                    featured_media_img_id = featured_media['id']
                    img_input = self.img_inputs_ref[featured_media_img_id]
                    supplier_data_prompt += f"- **Featured Image for {sku}**:\n"
                    self.__append_prompt_input_to_content(content, prompt_input_type, supplier_data_prompt)
                    content.append(img_input)
                else:
                    self.__append_prompt_input_to_content(content, prompt_input_type, supplier_data_prompt)

        # Images for the product
        for media_object_data in media_objects_data:
            media_id: str = media_object_data['id']
            img_input = self.img_inputs_ref[media_id]

            if self.resource_type == 'Media':
                images_prompt = f"## Image for MediaImage ID: {media_id}\n"
                self.__append_prompt_input_to_content(content, prompt_input_type, images_prompt)

            content.append(img_input)

    def __append_prompt_input_to_content(self, content: List, prompt_input_type: str, prompt: str):
        prompt_input = {
            'type': prompt_input_type,
            'text': prompt
        }
        content.append(prompt_input)

    def __generate_single_payload(self, custom_id: str, instructions: str, content: List[Dict], output_schema: Dict) -> Dict:
        """
        Generate a single structured API payload within the batch, then write to batch payloads JSONL file.
        """

        print(f"Generating payload for '{self.resource_type}' resource belonging to process context '{custom_id}'")

        if 'responses' in self.batch_manager.endpoint:
            body = {
                'model': self.batch_manager.model,
                'reasoning': {
                    'effort': 'high'
                },
                # 'temperature': 0.5,
                'instructions': instructions,
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
        else:
            body = {
                'model': self.batch_manager.model,
                'reasoning_effort': 'high',
                # 'temperature': 0.5,
                'messages': [
                    {
                        'role': 'developer',
                        'content': instructions
                    },
                    {
                        'role': 'user',
                        'content': content
                    }
                ],
                'response_format': {
                    'type': 'json_schema',
                    'json_schema': {
                        'name': 'fields_extracted_response',
                        'strict': True,
                        'schema': output_schema
                    }
                },
                'verbosity': 'low'
            }

        payload = {
            'custom_id': custom_id,
            'method': 'POST',
            'url': self.batch_manager.endpoint,
            'body': body
        }

        return payload
