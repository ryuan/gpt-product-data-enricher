from openai import OpenAI
import os
import sys
import re
import pandas as pd
from dotenv import load_dotenv
from pathlib import Path
from typing import List, Tuple, Dict


def init() -> OpenAI:
    load_dotenv()
    client = OpenAI()

    if "OPENAI_API_KEY" in os.environ:
        client.api_key = os.getenv('OPENAI_API_KEY')
    else:
        print(("OPENAI_API_KEY does not exist in the local or global environment."
               "Generate an OpenAI API key, then export it as an environment variable in terminal via:"
               "export OPENAI_API_KEY='your_api_key_here'"))
        sys.exit()

    return client

### Pre-processing and setup functions

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
    Read XLSX or CSV inputs files and return dataframes
    """
    supplier_data_df = pd.read_csv(supplier_data_path) if supplier_data_path.suffix == '.csv' else pd.read_excel(supplier_data_path)
    image_urls_df = pd.read_csv(image_urls_path) if image_urls_path.suffix == '.csv' else pd.read_excel(image_urls_path)
    fields_df = pd.read_csv(fields_path) if fields_path.suffix == '.csv' else pd.read_excel(fields_path)

    return supplier_data_df, image_urls_df, fields_df

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

### General utility functions

def print_options(options: list[str]) -> None:
    """
    Prints a list of string options with index numbers for user prompt input
    """
    print("\n")
    for i, option in enumerate(options):
        print(f"[{i}] {option}")