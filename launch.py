import modules


def main():
    modules.init()

    # Pre-process source files, construct prompts, structure output JSON schemas, and generate batch payloads
    supplier_data_path, image_urls_path, fields_path = modules.get_source_paths()
    payloads = modules.generate_batch_payloads(supplier_data_path, image_urls_path, fields_path)

    # Create batch payloads JSONL file, upload it, then execute batch payloads asynchronously
    output_path = 'output/batch_payloads.json'
    modules.export_batch_to_jsonl(payloads, output_path)
    file = modules.upload_batch_payloads(output_path)
    batch = modules.create_batch(file, '/v1/chat/responses', 'gpt-4o')
    print("Batch job submitted. Batch ID:", batch.id)
    print("Status URL:", batch.status_url)

    # Poll status of batch execution, downloading the output JSONL results upon completion
    final_status = modules.poll_batch_until_complete(batch.id)
    modules.download_batch_result(final_status, "output/batch_results.jsonl")

if __name__ == "__main__":
    main()