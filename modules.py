import openai
import time
import json
import os
import sys
import re
import pandas as pd
from dotenv import load_dotenv
from pathlib import Path
from typing import List, Tuple, Dict
from utils import print_options
from fragments import object_schema_reference


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
    required_files = {'supplier data': None, 'Shopify data': None, 'fields to extract': None}

    if len(data_files) < 3:
        print(("There needs to be 3 files in the ./source directory."
               "1. CSV/XLSX product data from a supplier."
               "2. CSV/XLSX Shopify data including image URLs."
               "3. CSV/XLSX with list of fields to extract."
               "Refer to the README.md for detailed formatting required for each file."))
        sys.exit()
    else:
        for required_file in required_files:
            print_options(data_files)
            file_idx = int(input(f"Which CSV/XLSX file has the data for {required_file}?: "))
            required_files[required_file] = data_files[file_idx]

    return required_files.values()

def get_input_dfs(supplier_data_path: Path, image_urls_path: Path, fields_path: Path) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Read XLSX or XSV inputs files and return dataframes
    """

    supplier_data_df = pd.read_csv(supplier_data_path) if supplier_data_path.suffix == '.csv' else pd.read_excel(supplier_data_path)
    image_urls_df = pd.read_csv(image_urls_path) if supplier_data_path.suffix == '.csv' else pd.read_excel(image_urls_path)
    fields_df = pd.read_csv(fields_path) if supplier_data_path.suffix == '.csv' else pd.read_excel(fields_path)

    return supplier_data_df, image_urls_df, fields_df

def get_related_skus(sku_col_name: str, supplier_data_df: pd.DataFrame) -> Tuple[Dict, Dict]:
    """
    Use common SKU-model pattern recognition rules to group related SKUs together
    """

    sku_to_model = {}
    model_to_skus = {}

    for sku in supplier_data_df[sku_col_name]:
        model = sku.split('-')[0]
        match = re.match('([A-Za-z]+[0-9]+)', model)
        if match is not None:
            model = match.group()

        if model != sku:
            sku_to_model[sku] = model

            if model not in  model_to_skus.keys():
                model_to_skus[model] = [sku]
            else:
                model_to_skus[model].append(sku)

    return sku_to_model, model_to_skus

def sequence_batches(supplier_data_df: pd.DataFrame, fields_df: pd.DataFrame) -> List[Dict]:
    """
    Sequence batches into groups based on process order numbers
    """
    
    process_order_numbers = sorted(fields_df['Process Order Number'].dropna().unique())

    # Get column name representing column data for SKU from the supplier data CSV/XLSX
    headers = list(supplier_data_df)
    print_options(headers)
    sku_idx = int(input(f"Which name represents column data for SKU?: "))
    sku_col_name = headers[sku_idx]

    return sku_col_name, process_order_numbers

def generate_batch_payloads(process_order_number: int, batch_results_path: str, endpoint: str, model: str, sku_col_name: str, sku_to_model: Dict, model_to_skus: Dict,
                            supplier_data_df: pd.DataFrame, image_urls_df: pd.DataFrame, fields_df: pd.DataFrame) -> List[Dict]:
    """
    Generate a list of request payloads for each SKU in the supplier CSV with product images.
    """

    payloads = []
    dependency_results = {}

    # If processing order numbers 2 or greater, read past results output to fetch dependency field booleans for each SKU
    if process_order_number > 1:
        dependency_results = get_dependency_results(fields_df, batch_results_path)

    # Loop through each row of supplier data, processing only SKUs that have hosted product images
    for _, row in supplier_data_df.iterrows():
        sku = str(row[sku_col_name]).strip()

        if sku in image_urls_df['sku']:
            variant_id = image_urls_df[image_urls_df['sku'] == sku]['id']
            product_id = image_urls_df[image_urls_df['sku'] == sku]['__parentId']
            product_type = image_urls_df[image_urls_df['id'] == product_id]['productType']
            product_vendor = image_urls_df[image_urls_df['id'] == product_id]['vendor']

            # Get all the supplier product data for this SKU, dropping any blank values
            supplier_row_data = row.dropna().to_dict()

            # Get the featured image URL for the product variant (NaN if not available) and image URLs for the product
            variant_img_url = image_urls_df[image_urls_df['sku'] == sku]['image/url']
            product_img_urls = image_urls_df[image_urls_df['__parentId'] == product_id]['image/url'].drop_duplicates()

            # Get related SKUs data if possible
            related_skus_data = {}
            
            if sku in sku_to_model:
                model = sku_to_model[sku]
                related_skus = model_to_skus[model]
                related_skus_data = supplier_data_df[supplier_data_df[sku_col_name].isin(related_skus)].to_dict()

            # Get fields to extract for the SKU, dropping fields that are not relevant to its product type and failed dependency conditions
            fields_to_extract = fields_df[fields_df['Process Order Number'] == process_order_number].dropna(subset=[product_type])

            if dependency_results:
                dependency_fields = fields_to_extract['Dependency'].dropna().unique()

                for dependency_field in dependency_fields:
                    if dependency_results[variant_id][dependency_field] is not True:
                        fields_to_extract = fields_to_extract[fields_to_extract['Dependency'] != dependency_field]

            # Generate the request payload for this SKU
            system_instructions, user_prompt = build_prompt(sku, product_type, product_vendor, supplier_row_data, related_skus_data, fields_to_extract)
            schema = build_schema(fields_to_extract)
            payload = generate_single_payload(variant_id, endpoint, model, system_instructions, user_prompt, variant_img_url, product_img_urls, schema)

            payloads.append(payload)

    return payloads

def get_dependency_results(fields_df: pd.DataFrame, batch_results_path: str) -> Dict:
    past_results = {}
    dependency_fields = fields_df['Dependency'].dropna().unique()

    with open(batch_results_path, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():  # Skip empty lines
                try:
                    line = json.loads(line)
                    variant_id = line['custom_id']
                    results = line['body']['messages'][1]['content']
                    
                    for dependency_field in dependency_fields:
                        result = results[dependency_field]['value']
                        past_results[variant_id] = {dependency_field: result}

                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON on line: {line.strip()}. Error: {e}")

    return past_results

def build_prompt(sku: str, product_type: str, product_vendor: str, supplier_row_data: Dict, related_skus_data: Dict, fields_to_extract: pd.DataFrame) -> Tuple[str, str]:
    """
    Compose system instructions and user prompt for a single SKU.
    """

    system_instructions = (
        "You are an expert product data analyst for a large home goods retailer like Wayfair. "
        "Your job is to extract standardized field values from supplier spreadsheet data, product images, and crawled website data. "
        "The user will provide supplier data as a stringified JSON object, and product images as a list of image URLs. "
        "All key-value pairs in the JSON should be carefully examined when evaluating each field. "
        "Specific instructions and rules may be provided for certain fields—follow these exactly. "
        "Each field will be labeled as either 'Required' or 'Optional'. "
        "'Required' fields must never be left null unless no reliable data exists — in such cases, include an appropriate warning. "
        "'Optional' fields may be left null if no trustworthy value can be extracted. "
        "Images are provided at the product level and may include variants with different sizes from the main SKU. "
        "Never guess or create new dimension values based solely on image appearances. "
        "However, you may reuse dimension values from supplier data if the label clearly maps to the intended field. "
        "For example, the 'Clearance Height' of a coffee table may be reused for the 'Leg Dimension' field if applicable. "
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

    user_prompt = (
        f"Review the following data for SKU {sku} (in stringified JSON format) from our supplier {product_vendor}: "
        f"{supplier_row_data}"
    )

    if related_skus_data:
        user_prompt += (
            f"SKU {sku} also has related SKUs that share the same design and features, but with potentially different material, color, and size."
            "Consider the following data (also in stringified JSON format) of the related SKUs to potentially fill gaps and fix inconsistencies/errors: "
            f"{related_skus_data}"
        )

    user_prompt += (
        "Try to match these data to the corresponding fields below, following their specific notes/instructions if available: "
        "\n"
    )

    fields = fields_to_extract['Field']

    for field in fields:
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

def build_schema(fields_to_extract: pd.DataFrame) -> Dict:
    """
    Compose custom schema for the structured JSON output tailored to a payload's extracted fields.
    """

    fields = fields_to_extract['Field']
    schema_properties = {}

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

    schema = {'type': 'object', 'properties': schema_properties}

    return schema

def generate_single_payload(variant_id: str, endpoint: str, model: str, system_instructions: str, user_prompt: str, 
                            variant_img_url: str, product_img_urls: List[str], schema: Dict) -> Dict:
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
        'url': endpoint,
        'body': {
            'model': model,
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