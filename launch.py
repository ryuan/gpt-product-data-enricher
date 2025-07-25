import openai
import time
import json
from typing import List, Dict
import pandas as pd
from dotenv import load_dotenv
import os
import sys
from pathlib import Path
from utils import print_options

# Example metafield rule structure
default_metafield_rules = {
    "Frame Material": {"allow_estimation": False},
    "Top Material": {"allow_estimation": True},
    "Top Finish": {"allow_estimation": True},
    "Frame Finish": {"allow_estimation": True},
    "Frame Color": {"allow_estimation": True},
    "Top Color": {"allow_estimation": True},
    "Base or Leg": {"allow_estimation": True},
    "Glides": {"allow_estimation": True},
    "Base/Leg Material": {"allow_estimation": True},
    "Base/Leg Finish": {"allow_estimation": True},
    "Base/Leg Color": {"allow_estimation": True},
    "Base Type": {"allow_estimation": True},
    "Base/Leg Dimension": {"allow_estimation": False},
    "Base Bottom Dimension": {"allow_estimation": False},
    "Leg Spacing": {"allow_estimation": False}
}

def main():
    init()

    supplier_data_path, image_urls_path, metafields_path = get_source_paths()
    payloads = generate_batch_payload(supplier_data_path, image_urls_path, metafields_path)

    export_batch_to_json(payloads, output_path='output/batch_payloads.json')

    # Upload the JSONL file (one prompt per line)
    file = openai.files.create(
        file=open("output/batch_payloads.jsonl", "rb"),
        purpose="batch"
    )

def init():
    load_dotenv()

    if "OPENAI_API_KEY" in os.environ:
        openai.api_key = os.getenv('OPENAI_API_KEY')
    else:
        print(("OPENAI_API_KEY does not exist in the local or global environment."
               "Generate an OpenAI API key, then export it as an environment variable in terminal via:"
               "export OPENAI_API_KEY='your_api_key_here'"))
        sys.exit()

def get_source_paths():
    """
    Get filepaths for all the required CSV/XLSX sources for data pre-processing and ingestion.
    """
    source_dir = Path("./source")
    data_files = [file for file in source_dir.iterdir() if file.suffix == '.csv' or file.suffix == '.xlsx']
    required_data = ['supplier data', 'image URLs', 'metafields to extract']
    required_data_paths = []

    if len(data_files) < 3:
        print(("There needs to be 3 files in the ./source directory."
               "1. CSV/XLSX product data from a supplier."
               "2. CSV/XLSX with list of SKUs and hosted image URLs."
               "3. CSV/XLSX with list of metafields to extract."
               "Refer to the README.md for required formatting for each file."))
        sys.exit()
    else:
        for required_file in required_data:
            print_options(data_files)
            file_idx = int(input(f"Which CSV/XLSX file has the data for {required_file}?: "))
            required_data_paths.append(data_files[file_idx])

    return required_data_paths

def generate_batch_payload(supplier_data_path: Path, image_urls_path: Path, metafields_path: Path) -> List[Dict]:
    """
    Generate a list of prompt payloads for each product in the supplier CSV, matching with image files.
    """
    supplier_data_df = pd.read_csv(supplier_data_path) if supplier_data_path.suffix == '.csv' else pd.read_excel(supplier_data_path)
    image_urls_df = pd.read_csv(image_urls_path) if supplier_data_path.suffix == '.csv' else pd.read_excel(image_urls_path)
    metafields_df = pd.read_csv(metafields_path) if supplier_data_path.suffix == '.csv' else pd.read_excel(metafields_path)
    process_order = sorted(metafields_df['Process Order'].dropna().unique())
    payloads = []

    # Get column name representing column data for SKU from the supplier data CSV/XLSX
    headers = list(supplier_data_df)
    print_options(headers)
    sku_idx = int(input(f"Which name represents column data for SKU?: "))
    sku_col_name = headers[sku_idx]

    # Begin generating batch payloads for each SKU/row, processed in sequence based on shared context fragments
    for order_number in process_order:
        metafields_segment: pd.Series = metafields_df[metafields_df['Process Order'] == order_number]

        for _, row in supplier_data_df.iterrows():
            sku = str(row[sku_col_name]).strip()

            if sku in image_urls_df['sku']:     # Only process SKUs that have product images
                product_id: str = image_urls_df[image_urls_df['sku'] == sku]['__parentId'].iloc[0]
                product_type: str = image_urls_df[image_urls_df['id'] == product_id]['productType']

                # Get all the supplier product data for this SKU, dropping any blank values
                supplier_data = row.dropna().to_dict()

                # Get the featured image URL for the product variant (NaN if not available) and image URLs for the product
                variant_img_url = image_urls_df[image_urls_df['sku'] == sku]['image/url'].iloc[0]
                product_img_urls = image_urls_df[image_urls_df['__parentId'] == product_id]['image/url'].drop_duplicates()

                # Get metafields to extract for the SKU based on its product type
                metafields_to_extract = metafields_segment.dropna(subset=[product_type])

                prompt = build_prompt(sku, supplier_data, variant_img_url, product_img_urls, metafields_to_extract)
                payloads.append(prompt)

    return payloads

def build_prompt(sku: str, supplier_data: Dict[str, str], variant_img_url: str, product_img_urls: List[str], metafields_to_extract: pd.DataFrame) -> str:
    """
    Construct a structured API prompt for a single SKU including supplier data, images, and field estimation rules.
    """
    system_message = (
        "You are an expert product data analyst for a large home goods retailer like Wayfair. "
        "Your job is to extract Shopify metafield values from supplier spreadsheet data, product images, and crawled website data. "
        "The user will provide supplier spreadsheet data as a stringified JSON object, while the images will be provided as URLs. "
        "All key-value pairs in the JSON should be carefully read and considered when evaluating each metafield. "
        "You will also perform web search on the suppliers' website to find additional data and verify suspicions for each SKU. "
        "Specific instructions and rules will be provided for evaluating certain metafields - follow them closely. "
        "Each metafield will also need to conform to their specified data type. "
        "Never guess or create dimension values on your own based on visual appearance. "
        "However, it is acceptable to reuse a dimension value from supplier data if the label clearly corresponds to the intended metafield. "
        "For example, 'Clearance Height' value of a coffee table may often be used for the 'Leg Dimension' metafield. "
        "You will sometimes be provided with supplier data for related SKUs along with the main SKU data. "
        "The related SKUs are similar to the main SKU in dimension and design, but can differ in color or material. "
        "It can sometimes include usable data that are missing from the main SKU's data. "
        "Since the related SKUs should share the same dimensions as the main SKU, use the dimension that appear the most often if there's an anomaly. "
        "The supplier data can sometimes be completely wrong due to typos - check all data sources available to you to make your decisions. "
        "When you're not sure and you believe estimating can lead to customer complaints, it's okay to leave the metafield blank. "
        "The order of data checking should be supplier data, images, crawled website data, and then related SKU data. "
        "Return a structured JSON with keys 'metafields', 'confidence', 'reasonings', and 'warnings'. "
        "Each key should map to a structured JSON object where keys are metafield names and values represent the extracted value, confidence rating, reasoning, or warnings. "
        "The values for confidence should indicate how confidence you are in your metafield value and be a single enum option from among 'Low', 'Medium', or 'High'. "
        "For every metafield listed in 'metafields', include the same set of keys under 'confidence', 'reasonings', and 'warnings', even if the value is null. "
        "A null value must still be accompanied by a confidence level (e.g., the system is highly confident that no valid data was available). "
        "A null vale must also still have reasoning that explains why the value was left blank (e.g., conflicting data or insufficient evidence)."
    )

    user_payload = ""
    metafields = metafields_to_extract['Field']

    return {
        "messages": [
            {"role": "system", "content": system_message},
            {"role": "user", "content": json.dumps(user_payload)}
        ],
        "response_format": "json"
    }

def export_batch_to_json(payloads: List[Dict], output_path: str):
    with open(output_path, 'w', encoding='utf-8') as f:
        for task in payloads:
            json.dump(task, f)
            f.write('\n')

if __name__ == "__main__":
    main()