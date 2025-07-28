import modules


def main():
    modules.init()

    # Pre-process source files and prepare batch sequences
    supplier_data_path, image_urls_path, fields_path = modules.get_source_paths()
    supplier_data_df, image_urls_df, fields_df = modules.get_input_dfs(supplier_data_path, image_urls_path, fields_path)
    sku_col_name, process_order_numbers = modules.sequence_batches(supplier_data_df, fields_df)

    # Construct prompts, structure output JSON schemas, and generate batch payloads for each process sequence
    batch_payloads_path = 'output/batch_payloads.jsonl'
    batch_results_path = 'output/batch_results.jsonl'
    endpoint = '/v1/chat/responses'
    model = 'gpt-4o'

    for process_order_number in process_order_numbers:
        batch_payloads = modules.generate_batch_payloads(sku_col_name, process_order_number, endpoint, model, 
                                                         batch_results_path, supplier_data_df, image_urls_df, fields_df)

        # Create batch payloads JSONL file, upload it, then execute batch payloads asynchronously
        modules.export_batch_to_jsonl(batch_payloads, batch_payloads_path)
        file = modules.upload_batch_payloads(batch_payloads_path)
        batch = modules.create_batch(file, endpoint, model)
        print("Batch job submitted. Batch ID:", batch.id)
        print("Status URL:", batch.status_url)

        # Poll status of batch execution, downloading the output JSONL results upon completion
        final_status = modules.poll_batch_until_complete(batch.id)
        modules.download_batch_result(final_status, batch_results_path)

if __name__ == "__main__":
    main()