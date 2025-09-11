from openai import OpenAI
import os
import json
import time
import datetime
import pandas as pd
from typing import List, Dict
from collections import defaultdict


class BatchFiles:
    def __init__(self, date_time: str, process_order_number: int):
        self.process_order_number = process_order_number

        self.batch_payloads_path = f'payloads/{date_time}/batch_payloads_{process_order_number}.jsonl'
        self.batch_results_path = f'output/{date_time}/batch_results_{process_order_number}.jsonl'
        self.batch_outputs_path = f'output/{date_time}/batch_outputs_{process_order_number}.jsonl'
        self.batch_errors_path = f'output/{date_time}/batch_errors_{process_order_number}.jsonl'

        # Create any missing parent directories
        os.makedirs(os.path.dirname(self.batch_payloads_path), exist_ok=True)
        os.makedirs(os.path.dirname(self.batch_results_path), exist_ok=True)
        os.makedirs(os.path.dirname(self.batch_outputs_path), exist_ok=True)
        os.makedirs(os.path.dirname(self.batch_errors_path), exist_ok=True)

        self.batch_payloads_file = open(self.batch_payloads_path, 'w', encoding='ascii')

        # Post batch creation data 
        self.upload_response = None
        self.batch = None
        self.results = None
        self.error_results = None
        self.input_tokens = 0
        self.output_tokens = 0
        self.total_tokens = 0

class BatchManager:
    def __init__(self, client: OpenAI, endpoint: str, model: str, date_time: str):
        self.client: OpenAI = client
        self.endpoint: str = endpoint
        self.model: str = model
        self.date_time: str = date_time

        self.all_batch_files: List[BatchFiles] = []
        self.current_batch_files: BatchFiles = None

        self.error_ids: set = set()

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
        self.current_batch_files.upload_response = self.client.files.create(
            file=open(self.current_batch_files.batch_payloads_path, 'rb'),
            purpose='batch'
        )

        print("Batch JSONL file upload response: ")
        print(self.current_batch_files.upload_response)

    def create_batch(self):
        """
        Creates and executes a batch from an uploaded file of requests, returning the batch status object
        """
        self.current_batch_files.batch = self.client.batches.create(
            input_file_id=self.current_batch_files.upload_response.id,
            endpoint=self.endpoint,
            completion_window='24h',
            metadata={'task': 'product_field_enrichment'}
        )

        print("Batch job submitted. Batch ID:", self.current_batch_files.batch.id)
        print("Status:", self.current_batch_files.batch.status)

    def poll_batch_until_complete(self, poll_interval: int = 30):
        """
        Poll the batch job until it reaches a terminal state.
        """
        print(f"Polling batch job {self.current_batch_files.batch.id} every {poll_interval} seconds...")
        start = time.monotonic()

        while True:
            self.current_batch_files.batch = self.client.batches.retrieve(self.current_batch_files.batch.id)
            elapsed = datetime.timedelta(seconds=int(time.monotonic()-start))
            print(f"[{elapsed}] Status: {self.current_batch_files.batch.status}, {self.current_batch_files.batch.request_counts}", end=f"{' ' * 20}\r", flush=True)

            if self.current_batch_files.batch.status in ['completed', 'failed', 'cancelled', 'expired']:
                print(f"\nBatch job execution finished. Checking final batch object for issues...")

                if self.current_batch_files.batch.errors:
                    raise ValueError(f"Batch job had errors: {self.current_batch_files.batch.errors}")
                elif self.current_batch_files.batch.status != "completed":
                    raise ValueError(f"Batch job did not complete successfully. Status: {self.current_batch_files.batch.status}")
                elif self.current_batch_files.batch.error_file_id and not self.current_batch_files.batch.output_file_id:
                    print(f"Batch job completed with errors and no downloadable output file: {self.current_batch_files.batch.request_counts}")
                    print("Program will proceed to download and parse the error file, but will terminate thereafter.")
                elif self.current_batch_files.batch.error_file_id and self.current_batch_files.batch.output_file_id:
                    print(f"Batch job completed with partial errors: {self.current_batch_files.batch.request_counts}")
                    print("Program will proceed while omitting all error IDs from future payloads and outputs.")
                else:
                    print("No errors detected with the batch process. Proceeding to download results.")
                return
            
            time.sleep(poll_interval)

    def download_batch_results(self):
        """
        Download the failed and completed results of the batch job.
        """
        # Download the failed results if the batch has an error file
        if self.current_batch_files.batch.error_file_id:
            self.current_batch_files.error_results = self.client.files.content(self.current_batch_files.batch.error_file_id)

            with open(self.current_batch_files.batch_errors_path, 'w', encoding='ascii') as f:
                f.write(self.current_batch_files.error_results.text)
                print(f"Failed results saved to: {self.current_batch_files.batch_errors_path}")

        # Download the completed requests
        if self.current_batch_files.batch.output_file_id:
            print(f"Downloading completed results to {self.current_batch_files.batch_results_path}")
            self.current_batch_files.results = self.client.files.content(self.current_batch_files.batch.output_file_id)

            with open(self.current_batch_files.batch_results_path, 'w', encoding='ascii') as f:
                f.write(self.current_batch_files.results.text)
                print(f"Completed results saved to: {self.current_batch_files.batch_results_path}")
        else:
            raise ValueError("No downloadable output file in batch job. Terminating program.")

    def update_error_ids(self):
        if os.path.exists(self.current_batch_files.batch_errors_path):
            with open(self.current_batch_files.batch_errors_path, 'r', encoding='ascii') as f:
                for line in f:
                    result = json.loads(line)

                    if result['response']['body']['error'] is not None:
                        id = result['custom_id']
                        error = result['response']['body']['error']['message']
                        self.error_ids.add(id)
                        print(f"Detected error at {id}: {error}")

            print("Program will proceed while omitting all error IDs from future payloads and outputs:")
            print(self.error_ids)

    def save_outputs_from_batch_results(self):
        """
        Save just the structured outputs from the downloaded batch results file.
        """
        with open(self.current_batch_files.batch_results_path, 'r', encoding='ascii') as results_f:
            with open(self.current_batch_files.batch_outputs_path, 'w', encoding='ascii') as outputs_f:
                for line in results_f:
                    if line.strip():  # Skip empty lines
                        try:
                            result = json.loads(line)
                            object_id= result['custom_id']
                            body: Dict = result['response']['body']
                            outputs: List[Dict] = body.get('output', body.get('choices'))

                            for output in outputs:
                                content = output.get('content') or output.get('message').get('content')

                                if content:
                                    if isinstance(content, list):
                                        structured_output: Dict = json.loads(content[0]['text'])
                                    else:
                                        structured_output: Dict = json.loads(content)

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
            'input_tokens': self.current_batch_files.input_tokens,
            'output_tokens': self.current_batch_files.output_tokens,
            'total_tokens': self.current_batch_files.total_tokens
        }

        print(f"Total tokens used in batch {self.current_batch_files.process_order_number}: ")
        print(usage_summary)
        print("\n")

    def __update_token_usage(self, result: Dict):
        usage: Dict = result['response']['body']['usage']

        self.current_batch_files.input_tokens += usage.get('input_tokens', usage.get('prompt_tokens'))
        self.current_batch_files.output_tokens += usage.get('output_tokens', usage.get('completion_tokens'))
        self.current_batch_files.total_tokens += usage['total_tokens']

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
        missing_ids = set(all_object_ids[~all_object_ids.isin(out_df.index)].to_list())
        print(f"Combined outputs of all batches saved to {combined_outputs_path}")
        print(f"Object IDs omitted from combined outputs due to errors: {self.error_ids}")
        print(f"Object IDs missing for non-error reasons: {missing_ids - self.error_ids}")

    def __get_extracted_data(self) -> Dict:
        """
        Parse batch output files and create a reference dict, keyed by product/variant ID, each with a dict of extracted field-value pairs 
        """
        extracted_data_ref = defaultdict(dict)

        for batch_files in self.all_batch_files:
            with open(batch_files.batch_outputs_path, 'r', encoding='ascii') as outputs_f:
                for line in outputs_f:
                    if line.strip():  # Skip empty lines
                        try:
                            output = json.loads(line)
                            object_id: str = output['id']
                            structured_output: Dict[str, Dict] = output['output']

                            if object_id not in self.error_ids:
                                for field_name, output in structured_output.items():
                                    if isinstance(output['value'], str):
                                        value = output['value']
                                    else:
                                        value = json.dumps(output['value'])
                                    extracted_data_ref[object_id][field_name] = value
                        except json.JSONDecodeError as e:
                            print(f"Error decoding JSON on line: {line.strip()}. Error: {e}")

        return extracted_data_ref