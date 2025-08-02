from openai import OpenAI
import os
import json
import time
import sys
from typing import Dict


class BatchManager:
    def __init__(self, client: OpenAI, endpoint: str, model: str, batch_payloads_path: str, batch_results_path: str):
        self.client = client
        self.endpoint = endpoint
        self.model = model

        # Create any missing parent directories
        os.makedirs(os.path.dirname(batch_payloads_path), exist_ok=True)
        os.makedirs(os.path.dirname(batch_results_path), exist_ok=True)
        self.batch_payloads_path = batch_payloads_path
        self.batch_results_path = batch_results_path
        self.batch_payloads_file = open(batch_payloads_path, 'w', encoding='ascii')

        self.upload_response = None
        self.batch = None
        self.results = None

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
                return
            time.sleep(poll_interval)

    def download_batch_results(self):
        """
        Download the results of the completed batch job.
        """
        if self.batch.status != "completed":
            print(f"Batch job did not complete successfully. Status: {self.batch.status}")
            sys.exit()

        print(f"Downloading results...")
        self.results = self.client.files.content(self.batch.output_file_id)

        with open(self.batch_results_path, 'w', encoding='ascii') as f:
            f.write(self.results.text)
            print(f"Results saved to {self.batch_results_path}")

