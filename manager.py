from openai import OpenAI
import os
import json
import time
import sys
from typing import Dict
from pprint import pprint


class BatchManager:
    def __init__(self, client: OpenAI, endpoint: str, model: str, batch_payloads_path: str, batch_results_path: str, batch_outputs_path: str):
        self.client = client
        self.endpoint = endpoint
        self.model = model

        # Create any missing parent directories
        os.makedirs(os.path.dirname(batch_payloads_path), exist_ok=True)
        os.makedirs(os.path.dirname(batch_results_path), exist_ok=True)
        os.makedirs(os.path.dirname(batch_outputs_path), exist_ok=True)
        self.batch_payloads_path = batch_payloads_path
        self.batch_results_path = batch_results_path
        self.batch_outputs_path = batch_outputs_path
        self.batch_payloads_file = open(batch_payloads_path, 'w', encoding='ascii')

        self.upload_response = None
        self.batch = None
        self.results = None
        self.input_tokens = 0
        self.output_tokens = 0
        self.total_tokens = 0

    def write(self, payload: Dict):
        self.batch_payloads_file.write((json.dumps(payload, ensure_ascii=True) + '\n'))

    def upload_batch_payloads(self):
        """
        Upload batch payloads JSONL file to OpenAI servers, returning the file upload confirmation object
        """
        self.batch_payloads_file.close()
        self.upload_response = self.client.files.create(
            file=open(self.batch_payloads_path, 'rb'),
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
        while True:
            self.batch = self.client.batches.retrieve(self.batch.id)
            print(f"Status: {self.batch.status}")
            if self.batch.status in ['completed', 'failed', 'cancelled', 'expired']:
                print(f"Batch execution complete. Status: {self.batch.status}")
                if self.batch.status == 'failed':
                    print(self.batch.errors.data[0].code)
                    print(self.batch.errors.data[0].message)
                return
            time.sleep(poll_interval)

    def download_batch_results(self):
        """
        Download the results of the completed batch job.
        """
        if self.batch.status != "completed":
            print(f"Batch job did not complete successfully. Status: {self.batch.status}")
            sys.exit()
        elif self.batch.errors:
            print(f"Batch job completed but with errors: {self.batch.errors}")
            sys.exit()

        print(f"Downloading results...")
        self.results = self.client.files.content(self.batch.output_file_id)

        with open(self.batch_results_path, 'w', encoding='ascii') as f:
            f.write(self.results.text)
            print(f"Results saved to {self.batch_results_path}")

        with open(self.batch_results_path, 'r', encoding='ascii') as results_f:
            with open(self.batch_outputs_path, 'w', encoding='ascii') as outputs_f:
                for line in results_f:
                    if line.strip():  # Skip empty lines
                        try:
                            result = json.loads(line)
                            object_id= result['custom_id']
                            output: Dict = json.loads(result['response']['body']['output'][0]['content'][0]['text'])
                            structured_line_json = {object_id: output}
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
