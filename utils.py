from openai import OpenAI
import os
import sys
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

def get_input_dfs(supplier_data_path: Path, store_data_path: Path, fields_data_path: Path) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Read XLSX or CSV inputs files and return dataframes
    """

    # Note that only for the supplier data, we read every cells as a string (dtype=object) to prevent any changes
    supplier_data_df = clean_df(pd.read_csv(supplier_data_path, dtype=object) if supplier_data_path.suffix == '.csv' else pd.read_excel(supplier_data_path, dtype=object))
    store_data_df = clean_df(pd.read_csv(store_data_path) if store_data_path.suffix == '.csv' else pd.read_excel(store_data_path))
    fields_data_df = clean_df(pd.read_csv(fields_data_path) if fields_data_path.suffix == '.csv' else pd.read_excel(fields_data_path))

    validate_fields_data_df(fields_data_path, fields_data_df)

    return supplier_data_df, store_data_df, fields_data_df

def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Forces all blanks and blank-like values (empty strings and values with only spaces/breaks) in a dataframe to be NaN
    """

    df = df.map(lambda x: pd.NA if isinstance(x, str) and x.strip() == "" else x)
    return df

def validate_fields_data_df(fields_data_path: Path, fields_data_df: pd.DataFrame):
    """
    Checks the fields data file to see if values abide by required rules
    """

    print(f"Validating {fields_data_path} for value/format issues...")

    # Check if each field name is unique
    if fields_data_df['Field'].duplicated().any():
        duplicate_fields = fields_data_df.loc[fields_data_df['Field'].duplicated(), 'Field'].drop_duplicates().to_list()
        print(f"These fields are duplicated: {duplicate_fields}")
        print("Program requires all fields to be unique. Correct and rerun program.")
        sys.exit()

    # Check if required fields in dependencies are always processed before the dependent fields
    fields_data_df_merged = fields_data_df.merge(
        fields_data_df[['Field', 'Process Order Number']],
        how='left',
        left_on='Dependency',
        right_on='Field',
        suffixes=('', '_Dependency')
    )
    invalid_dependent_fields = fields_data_df_merged.loc[
        (fields_data_df_merged['Dependency'].notna()) &
        (fields_data_df_merged['Process Order Number'] <= fields_data_df_merged['Process Order Number_Dependency']),
        'Field'
    ].to_list()

    if invalid_dependent_fields:
        print(f"These fields with dependencies are processed before their required fields: {invalid_dependent_fields}")
        print("All fields with dependencies must be processed after their required fields. Correct and rerun program.")
        sys.exit()

    # Check if all product fields and variant fields are not mixed together in any process order
    resource_per_process = fields_data_df.groupby('Process Order Number')['Resource'].nunique()
    invalid_process_order_numbers = resource_per_process[resource_per_process > 1].drop_duplicates().to_list()
    
    if invalid_process_order_numbers:
        print(f"These process order numbers are batching fields that belong to products and variants: {invalid_process_order_numbers}")
        print("Each batch process must contain fields that belong to either Product or Variant. Correct and rerun program.")
        sys.exit()

    print("Validation complete. All checks passed successfully.")
    

def sequence_batches(supplier_data_df: pd.DataFrame, fields_data_df: pd.DataFrame) -> List[Dict]:
    """
    Sequence batches into groups based on process order numbers
    """

    process_order_numbers = sorted(fields_data_df['Process Order Number'].dropna().unique())

    # Get column name representing column data for SKU from the supplier data CSV/XLSX
    headers = list(supplier_data_df)
    print_options(headers)
    sku_idx = int(input(f"Which name represents column data for SKU?: "))
    sku_col_name = headers[sku_idx]

    return sku_col_name, process_order_numbers

def get_product_ids_skus(store_data_df: pd.DataFrame) -> Dict:
    """
    For each product ID on store data, find and group together all variant SKUs from the supplier data
    """
    
    store_skus_df = store_data_df[store_data_df['sku'].notna()]
    product_skus = store_skus_df.groupby('__parentId')['sku'].apply(list).to_dict()
    return product_skus

### General utility functions

def print_options(options: list[str]) -> None:
    """
    Prints a list of string options with index numbers for user prompt input
    """

    print("\n")
    for i, option in enumerate(options):
        print(f"[{i}] {option}")