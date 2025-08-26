from openai import OpenAI
import os
import json
import time
import datetime
import sys
import pandas as pd
from typing import List, Dict
from pprint import pprint
from collections import defaultdict


class BatchFiles:
    def __init__(self, date_time: str, process_order_number: int):
        self.process_order_number = process_order_number

        self.batch_payloads_path = f'payloads/{date_time}/batch_payloads_{process_order_number}.jsonl'
        self.batch_results_path = f'output/{date_time}/batch_results_{process_order_number}.jsonl'
        self.batch_outputs_path = f'output/{date_time}/batch_outputs_{process_order_number}.jsonl'

        # Create any missing parent directories
        os.makedirs(os.path.dirname(self.batch_payloads_path), exist_ok=True)
        os.makedirs(os.path.dirname(self.batch_results_path), exist_ok=True)
        os.makedirs(os.path.dirname(self.batch_outputs_path), exist_ok=True)

        self.batch_payloads_file = open(self.batch_payloads_path, 'w', encoding='ascii')

class BatchManager:
    def __init__(self, client: OpenAI, endpoint: str, model: str, date_time: str):
        self.client: OpenAI = client
        self.endpoint: str = endpoint
        self.model: str = model
        self.date_time: str = date_time

        self.all_batch_files: List[BatchFiles] = []
        self.current_batch_files: BatchFiles = None

        self.upload_response = None
        self.batch = None
        self.results = None
        self.input_tokens = 0
        self.output_tokens = 0
        self.total_tokens = 0

    def create_batch_files(self, process_order_number: int) -> None:
        self.current_batch_files = BatchFiles(self.date_time, process_order_number)
        self.all_batch_files.append(self.current_batch_files)

    def write(self, payload: Dict):
        self.current_batch_files.batch_payloads_file.write((json.dumps(payload, ensure_ascii=True) + '\n'))

    def upload_batch_payloads(self):
        """
        Upload batch payloads JSONL file to OpenAI servers, returning the file upload confirmation object
        """
        self.current_batch_files.batch_payloads_file.close()
        self.upload_response = self.client.files.create(
            file=open(self.current_batch_files.batch_payloads_path, 'rb'),
            purpose='batch'
        )

        print("Batch JSONL file upload response: ")
        print(self.upload_response)

    def create_batch(self):
        """
        Creates and executes a batch from an uploaded file of requests, returning the batch status object
        """
        self.batch = self.client.batches.create(
            input_file_id=self.upload_response.id,
            endpoint=self.endpoint,
            completion_window='24h',
            metadata={'task': 'product_field_enrichment'}
        )

        print("Batch job submitted. Batch ID:", self.batch.id)
        print("Status:", self.batch.status)

    def poll_batch_until_complete(self, poll_interval: int = 30):
        """
        Poll the batch job until it reaches a terminal state.
        """
        print(f"Polling batch job {self.batch.id} every {poll_interval} seconds...")
        start = time.monotonic()

        while True:
            self.batch = self.client.batches.retrieve(self.batch.id)
            elapsed = datetime.timedelta(seconds=int(time.monotonic()-start))
            print(f"[{elapsed}] Status: {self.batch.status}, {self.batch.request_counts}", end="\r", flush=True)

            if self.batch.status in ['completed', 'failed', 'cancelled', 'expired']:
                print(f"\nBatch job execution finished. Checking results for issues...")

                if self.batch.errors:
                    raise ValueError(f"Batch job finished with errors: {self.batch.errors}")
                elif self.batch.error_file_id:
                    print(f"Batch job completed but there were errors with requests.")
                    self.results = self.client.files.content(self.batch.error_file_id)
                    for line in self.results.text.splitlines():
                        result = json.loads(line)
                        if result['response']['body']['error'] is not None:
                            id = result['custom_id']
                            error = result['response']['body']['error']['message']
                            raise ValueError(f"First detected error at ID {id}: {error}")
                elif self.batch.status != "completed":
                    raise ValueError(f"Batch job did not complete successfully. Status: {self.batch.status}")
                else:
                    print("No errors detected. Proceeding to download.")
                return
            
            time.sleep(poll_interval)

    def download_batch_results(self):
        """
        Download the results of the completed batch job.
        """
        print(f"Downloading results...")
        self.results = self.client.files.content(self.batch.output_file_id)

        with open(self.current_batch_files.batch_results_path, 'w', encoding='ascii') as f:
            f.write(self.results.text)
            print(f"Results saved to {self.current_batch_files.batch_results_path}")

        with open(self.current_batch_files.batch_results_path, 'r', encoding='ascii') as results_f:
            with open(self.current_batch_files.batch_outputs_path, 'w', encoding='ascii') as outputs_f:
                for line in results_f:
                    if line.strip():  # Skip empty lines
                        try:
                            result = json.loads(line)
                            object_id= result['custom_id']
                            outputs: List[Dict] = result['response']['body']['output']

                            for output in outputs:
                                if 'content' in output.keys():
                                    structured_output: Dict = json.loads(output['content'][0]['text'])
                                    structured_line_json = {
                                        'id': object_id,
                                        'output': structured_output
                                    }
                                    outputs_f.write(json.dumps(structured_line_json, ensure_ascii=True) + "\n")
                                    self.__update_token_usage(result)
                        except json.JSONDecodeError as e:
                            print(f"Error decoding JSON on line: {line.strip()}. Error: {e}")

    def print_token_usage(self):
        usage_summary = {
            'input_tokens': self.input_tokens,
            'output_tokens': self.output_tokens,
            'total_tokens': self.total_tokens
        }

        print("Total tokens used in batch: ")
        pprint(usage_summary)

    def __update_token_usage(self, result: Dict):
        usage = result['response']['body']['usage']

        self.input_tokens += usage['input_tokens']
        self.output_tokens += usage['output_tokens']
        self.total_tokens += usage['total_tokens']

    def combine_outputs(self, store_data_df: pd.DataFrame, fields_data_df: pd.DataFrame) -> None:
        """
        Combine all batch results' outputs into an Excel workbook, aligned to Shopify IDs.
        """
        # Get all the extracted output data for all the processed object IDs
        extracted_data_ref = self.__get_extracted_data()
         
        # Build combined dataframe from extracted data (rows = IDs, columns = field names from outputs)
        out_df = pd.DataFrame.from_dict(extracted_data_ref, orient='index')

        # Get all product and variant IDs from store_data_df
        all_object_ids: pd.Series = store_data_df.loc[store_data_df['id'].str.contains(r'gid://shopify/Product/|gid://shopify/ProductVariant/'), 'id']

        # Create reference for field name to GraphQL field from fields_data_df
        field_names: List[str] = fields_data_df.loc[fields_data_df['Field'].isin(out_df.columns), 'Field'].tolist()
        graphql_fields: List[str] = fields_data_df.loc[fields_data_df['Field'].isin(out_df.columns), 'GraphQL Field'].tolist()
        names_to_gql_ref: Dict[str, str] = dict(zip(field_names, graphql_fields))

        # Sort IDs (rows) and fields (columns) based on order from store_data_df and fields_data_df, then rename fields to Graphql fields
        out_df = out_df.reindex(index=all_object_ids[all_object_ids.isin(out_df.index)], columns=field_names)
        out_df = out_df.rename(columns=names_to_gql_ref)

        # Create the XLSX file from the combined df
        combined_outputs_path = f'output/{self.date_time}/batch_outputs_combined_{self.date_time}.xlsx'

        with pd.ExcelWriter(combined_outputs_path) as writer:
            out_df.to_excel(writer, index=True, index_label='id')

        # Print completion and follow-up info
        missing_ids = all_object_ids[~all_object_ids.isin(out_df.index)].to_list()
        print(f"Combined outputs of all batches saved to {combined_outputs_path}")
        print(f"Product/variant IDs with not outputs from batch processing: {missing_ids}")

    def __get_extracted_data(self) -> Dict:
        """
        Parse batch output files and create a reference dict, keyed by product/variant ID, each with a dict of extracted field-value pairs 
        """
        extracted_data_ref = defaultdict(dict)

        for batch_files in self.all_batch_files:
            with open(batch_files.batch_outputs_path, 'r', encoding='utf-8') as outputs_f:
                for line in outputs_f:
                    if line.strip():  # Skip empty lines
                        try:
                            output = json.loads(line)
                            object_id: str = output['id']
                            structured_output: Dict[str, Dict] = output['output']

                            for field_name, output in structured_output.items():
                                extracted_data_ref[object_id][field_name] = output['value']
                        except json.JSONDecodeError as e:
                            print(f"Error decoding JSON on line: {line.strip()}. Error: {e}")

        return extracted_data_ref