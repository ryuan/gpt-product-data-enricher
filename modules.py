import openai
import time
import json
from typing import List, Tuple, Dict, Optional
from enum import Enum
import pandas as pd
from dotenv import load_dotenv
import os
import sys
from pathlib import Path
from utils import print_options

def init():
    load_dotenv()

    if "OPENAI_API_KEY" in os.environ:
        openai.api_key = os.getenv('OPENAI_API_KEY')
    else:
        print(("OPENAI_API_KEY does not exist in the local or global environment."
               "Generate an OpenAI API key, then export it as an environment variable in terminal via:"
               "export OPENAI_API_KEY='your_api_key_here'"))
        sys.exit()

def get_source_paths() -> List[str]:
    """
    Get filepaths for all the required CSV/XLSX sources for data pre-processing and ingestion.
    """
    
    source_dir = Path("./input")
    data_files = [file for file in source_dir.iterdir() if file.suffix == '.csv' or file.suffix == '.xlsx']
    required_files = {'supplier data': None, 'image URLs': None, 'fields to extract': None}

    if len(data_files) < 3:
        print(("There needs to be 3 files in the ./source directory."
               "1. CSV/XLSX product data from a supplier."
               "2. CSV/XLSX with list of SKUs and hosted image URLs."
               "3. CSV/XLSX with list of fields to extract."
               "Refer to the README.md for required formatting for each file."))
        sys.exit()
    else:
        for required_file in required_files:
            print_options(data_files)
            file_idx = int(input(f"Which CSV/XLSX file has the data for {required_file}?: "))
            required_files[required_file] = data_files[file_idx]

    return required_files.values()

def generate_batch_payloads(supplier_data_path: Path, image_urls_path: Path, fields_path: Path) -> List[Dict]:
    """
    Generate a list of request payloads for each SKU in the supplier CSV with product images.
    """

    supplier_data_df = pd.read_csv(supplier_data_path) if supplier_data_path.suffix == '.csv' else pd.read_excel(supplier_data_path)
    image_urls_df = pd.read_csv(image_urls_path) if supplier_data_path.suffix == '.csv' else pd.read_excel(image_urls_path)
    fields_df = pd.read_csv(fields_path) if supplier_data_path.suffix == '.csv' else pd.read_excel(fields_path)
    process_order = sorted(fields_df['Process Order'].dropna().unique())
    payloads = []

    # Get column name representing column data for SKU from the supplier data CSV/XLSX
    headers = list(supplier_data_df)
    print_options(headers)
    sku_idx = int(input(f"Which name represents column data for SKU?: "))
    sku_col_name = headers[sku_idx]

    # Begin generating batch payloads for each SKU/row, processed in sequence based on shared context fragments
    for order_number in process_order:
        fields_segment: pd.Series = fields_df[fields_df['Process Order'] == order_number]

        # Loop through each row of supplier data, processing only SKUs that have hosted product images
        for _, row in supplier_data_df.iterrows():
            sku = str(row[sku_col_name]).strip()

            if sku in image_urls_df['sku']:
                product_id: str = image_urls_df[image_urls_df['sku'] == sku]['__parentId']
                product_type: str = image_urls_df[image_urls_df['id'] == product_id]['productType']

                # Get all the supplier product data for this SKU, dropping any blank values
                supplier_row_data = row.dropna().to_dict()

                # Get the featured image URL for the product variant (NaN if not available) and image URLs for the product
                variant_img_url = image_urls_df[image_urls_df['sku'] == sku]['image/url']
                product_img_urls = image_urls_df[image_urls_df['__parentId'] == product_id]['image/url'].drop_duplicates()

                # Get fields to extract for the SKU based on its product type
                fields_to_extract = fields_segment.dropna(subset=[product_type])

                system_instructions, user_prompt = build_prompt(sku, product_type, supplier_row_data, fields_to_extract)
                schema = build_schema(sku, fields_to_extract)
                payload = generate_single_payload(system_instructions, user_prompt, variant_img_url, product_img_urls, schema)

                payloads.append(payload)

    return payloads

def build_prompt(sku: str, product_type: str, supplier_row_data: Dict[str, str], fields_to_extract: pd.DataFrame) -> Tuple[str, str]:
    """
    Compose system instructions and user prompt for a single SKU.
    """

    system_instructions = (
        "You are an expert product data analyst for a large home goods retailer like Wayfair. "
        "Your job is to extract standardized field values from supplier spreadsheet data, product images, and crawled website data. "
        "The user will provide supplier spreadsheet data as a stringified JSON object, while the images will be provided as URLs. "
        "All key-value pairs in the JSON should be carefully read and considered when evaluating each field. "
        "You will also perform web search on the suppliers' website to find additional data and verify suspicions for each SKU. "
        "Specific instructions and rules will be provided for evaluating certain fields - follow them closely. "
        "Each field will also need to conform to their specified data type. "
        "Never guess or create dimension values on your own based on visual appearance. "
        "However, it is acceptable to reuse a dimension value from supplier data if the label clearly corresponds to the intended field. "
        "For example, 'Clearance Height' value of a coffee table may often be used for the 'Leg Dimension' field. "
        "You will sometimes be provided with supplier data for related SKUs along with the main SKU data. "
        "The related SKUs are similar to the main SKU in dimension and design, but can differ in color or material. "
        "It can sometimes include usable data that are missing from the main SKU's data. "
        "Since the related SKUs should share the same dimensions as the main SKU, use the dimension that appear the most often if there's an anomaly. "
        "The supplier data can sometimes be completely wrong due to typos - check all data sources available to you to make your decisions. "
        "When you're not sure and you believe estimating can lead to customer complaints, just leave the field null. "
        "The order of data checking should be supplier data, images, crawled website data, and then related SKU data. "
        "Return a structured JSON named 'fields_extracted_response', abiding by the output schema specified in the request payload. "
        "If any field could not be extracted for whatever reason, just assign its value as null. "
        "A null value must still be accompanied by a confidence level (e.g., the system is highly confident that no valid data was available). "
        "A null value must also be accompanied by reasoning that explains why the value was null (e.g., insufficient evidence)."
    )

    user_prompt = (
        f"Review the following data for SKU {sku} from our supplier in stringified JSON format: "
        f"{supplier_row_data}"
        "Try to match these data to the corresponding fields below, following their notes/instructions (if any): "
        "\n"
    )
    fields = fields_to_extract['Field']

    for field in fields:
        dependent_field = fields_to_extract[fields_to_extract['Field'] == field]['Dependency']      # To-do: check past output on conditional field truthiness
        shopify_resource = fields_to_extract[fields_to_extract['Field'] == field]['Resource']       # To-do: check related SKU data if field is Product resource
        notes = fields_to_extract[fields_to_extract['Field'] == field]['Notes']
        requirement = fields_to_extract[fields_to_extract['Field'] == field][product_type]
        is_required = True if requirement == 'Required' else False

        prompt_fragment = (
            f"Field Name: {field}"
            f"Notes/Instructions: {notes if notes else "None"}"
            f"Is Field Required?: {is_required}"
            "\n"
        )

        user_prompt += prompt_fragment

    return system_instructions, user_prompt

def build_schema(sku: str, fields_to_extract: pd.DataFrame) -> Dict:
    """
    Compose custom schema for the structured JSON output tailored to a payload's extracted fields.
    """

    fields = fields_to_extract['Field']
    schema_properties = {}

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
        'type': 'object',
        'properties': {
            'width': {'type': 'array', 'items': single_dimension_schema, 'minItems': 1, 'uniqueItems': True},
            'depth': {'type': 'array', 'items': single_dimension_schema, 'minItems': 1, 'uniqueItems': True},
            'height': {'type': 'array', 'items': single_dimension_schema, 'minItems': 1, 'uniqueItems': True}
        },
        'minProperties': 2,
        'additionalProperties': False
    }
    dimension_sets_schema = {
        'type': 'object',
        'properties': {
            'name': {'type': 'string', 'pattern': '\\S+'},
            'dimension': {'type': 'array', 'items': dimension_schema, 'minItems': 1, 'uniqueItems': True}
        },
        'required': ['name', 'dimension'],
        'additionalProperties': False
    }
    weight_schema = {
        'type': 'object',
        'properties': {
            'unit': {'enum': ['OUNCE', 'POUND']},
            'value': {'type': 'number', 'minimum': 0}
        },
        'required': ['unit', 'value'],
        'additionalProperties': False
    }
    package_measurement_schema = {
        'type': 'object',
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

    for field in fields:
        field_value_structure = {}
        field_type = fields_to_extract[fields_to_extract['Field'] == field]['JSON Type']
        field_enum_values = fields_to_extract[fields_to_extract['Field'] == field]['JSON Enum Values']
        field_array_items = fields_to_extract[fields_to_extract['Field'] == field]['JSON Array Items']
        field_object_type = fields_to_extract[fields_to_extract['Field'] == field]['JSON Object Type']

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

    schema = {'type': 'object', 'sku': sku, 'properties': schema_properties}

    return schema

def generate_single_payload(system_instructions: str, user_prompt: str, variant_img_url: str, product_img_urls: List[str], schema: Dict) -> Dict:
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

    return payload

def export_batch_to_jsonl(payloads: List[Dict], output_path: str):
    with open(output_path, 'w', encoding='utf-8') as f:
        for task in payloads:
            json.dump(task, f)
            f.write('\n')

def upload_batch_payloads(output_path: str) -> Dict:
    """
    Upload batch payloads JSONL file to OpenAI servers, returning the file upload confirmation object
    """
    file = openai.files.create(
        file=open(output_path, 'rb'),
        purpose='batch'
    )

    return file

def create_batch(file: Dict, endpoint: str, model: str) -> Dict:
    """
    Creates and executes a batch from an uploaded file of requests, returning the batch status object
    """
    batch = openai.batches.create(
        input_file_id=file.id,
        endpoint=endpoint,
        completion_window="24h",
        model=model,
        metadata={"task": "product_field_enrichment"}
    )

    return batch

def poll_batch_until_complete(batch_id: str, poll_interval: int = 30) -> Dict:
    """
    Poll the batch job until it reaches a terminal state.
    """
    print(f"Polling batch job {batch_id} every {poll_interval} seconds...")
    while True:
        batch_status = openai.batches.retrieve(batch_id)
        status = batch_status.status
        print(f"Status: {status}")
        if status in ["completed", "failed", "cancelled", "expired"]:
            return batch_status
        time.sleep(poll_interval)

def download_batch_result(batch_status: Dict, output_path: str):
    """
    Download the results of the completed batch job.
    """
    if batch_status.status != "completed":
        print(f"Batch job did not complete successfully. Status: {batch_status.status}")
        return

    result_url = batch_status.output_file.url
    print(f"Downloading result from: {result_url}")

    import requests
    response = requests.get(result_url)
    if response.status_code == 200:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(response.text)
        print(f"Results saved to {output_path}")
    else:
        print(f"Failed to download result: HTTP {response.status_code}")