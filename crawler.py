from openai import OpenAI
import os
import json
import sys
import time
import pandas as pd
from typing import List, Dict, Tuple
from utils import print_options


class WebSearchTool:
    def __init__(self, client: OpenAI, endpoint: str, model: str, store_data_df: pd.DataFrame, web_search_results_path: str):
        self.client = client
        self.endpoint = endpoint
        self.model = model
        self.store_data_df = store_data_df

        # Create any missing parent directories
        os.makedirs(os.path.dirname(web_search_results_path), exist_ok=True)
        self.web_search_results_path = web_search_results_path
        self.web_search_results_file = None
        self.web_search_results = {}

    def run(self):
        print_options(['Do not run web search', 'Reuse web search results', 'Run new web search session'])
        choice_idx = int(input("Do you want to run web search to fetch data first?"))

        if choice_idx == 0:
            return
        elif choice_idx == 1:
            with open(self.web_search_results_path, 'r', encoding='ascii') as f:               
                for line in f:
                    variant_data = json.loads(line)
                    self.web_search_results[variant_data['id']] = variant_data['output']
        elif choice_idx == 2:
            self.web_search_results_file = open(self.web_search_results_path, 'w', encoding='ascii')
            self.__execute_write_web_searches()
            self.web_search_results_file.close()
        else:
            print("Your input was not one of the valid options. Please rerun program and try again.")
            sys.exit()

    def __execute_write_web_searches(self):
        product_ids: List[str] = self.store_data_df.loc[self.store_data_df['id'].str.startswith('gid://shopify/Product/'), 'id'].to_list()

        for product_id in product_ids:
            product_vendor: str = self.store_data_df.loc[self.store_data_df['id'] == product_id, 'vendor'].iloc[0]
            variants_data: List[str] = self.store_data_df.loc[
                (self.store_data_df['id'].str.startswith('gid://shopify/ProductVariant/')) & 
                (self.store_data_df['__parentId'] == product_id), 
                ['id', 'sku']
            ].to_dict(orient='records')

            for variant_data in variants_data:
                response = self.__execute_single_web_search(product_vendor, variant_data['sku'])
                self.web_search_results[variant_data['id']] = response
                self.__write(variant_data['id'], response)
    def __execute_single_web_search(self, product_vendor: str, sku: str) -> Dict:
        system_instructions, user_prompt = self.__build_instructions_prompt(sku, product_vendor)
        output_schema = self.__build_output_schema()

        response = self.client.responses.parse(
            model=self.model,
            tools=[{'type': 'web_search_preview'}],
            instructions=system_instructions,
            input=user_prompt,
            text={
                'format': {
                    'type': 'json_schema',
                    'name': 'web_search_response',
                    'strict': True,
                    'schema': output_schema
                }
            }
        )

        return response

    def __build_instructions_prompt(self, sku: str, product_vendor: str) -> Tuple[str, str]:
        system_instructions = (
            "You are a product data analyst with access to web search. "
            "You will perform web search only on the supplier’s official website to verify or extract structured product data. "
            "The output schema expects you to extract the product description, bulleted lists, and tabular data as key-value pairs. "
            "If the web search result page does not have descirption or bulleted lists, just leave them blank."
        )

        user_prompt = (
            f"Search for SKU {sku} from supplier {product_vendor} on the web. "
            "Use only the supplier's official website as the source. "
            "Note that some suppliers have multiple websites so search them all."
            "Be concise, accurate, and return all available fields in structured JSON format. "
            "Extract and return available product data including dimensions, materials, features, number of drawers, etc. "
            "Extract the data exactly as they appear in the description, bullet list, and tables - do not alter them in any way. "
        )

        return system_instructions, user_prompt
    
    def __build_output_schema(self) -> Dict:
        schema = {
            'type': 'object',
            'properties': {
                'url': {'type': 'string'},
                'description': {'type': 'string'},
                'highlights': {
                    'type': 'array',
                    'items': {'type': 'string'}
                },
                'attributes': {
                    'type': 'object',
                    'properties': {
                        'name': {'type': 'string'},
                        'value': {'type': 'string'}
                    },
                    'required': ['name', 'value'],
                    'additionalProperties': False
                }
            }
        }

        return schema

    def __write(self, variant_id: str, response: Dict):
        variant_data = {
            'id': variant_id,
            'output': response
        }
        self.web_search_results_file.write((json.dumps(variant_data, ensure_ascii=True) + '\n'))