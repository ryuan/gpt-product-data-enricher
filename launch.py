import modules


def main():
    modules.init()

    # Pre-process source files and prepare batch sequences
    supplier_data_path, image_urls_path, fields_path = modules.get_source_paths()
    supplier_data_df, image_urls_df, fields_df = modules.get_input_dfs(supplier_data_path, image_urls_path, fields_path)
    sku_col_name, process_order_numbers = modules.sequence_batches(supplier_data_df, fields_df)
    sku_to_model, model_to_skus = modules.get_related_skus(sku_col_name, supplier_data_df, image_urls_df)

    # Construct prompts, structure output JSON schemas, and generate batch payloads for each process sequence
    endpoint = '/v1/chat/responses'
    model = 'gpt-4o'
    dependency_results = {}

    for process_order_number in process_order_numbers:
        batch_payloads_path = f'output/batch_payloads_{process_order_number}.jsonl'
        batch_results_path = f'output/batch_results_{process_order_number}.jsonl'
        batch_payloads = modules.generate_batch_payloads(process_order_number, dependency_results, endpoint, model, sku_col_name, sku_to_model, model_to_skus,
                                                         supplier_data_df, image_urls_df, fields_df)

        # Create batch payloads JSONL file, upload it, then execute batch payloads asynchronously
        modules.export_batch_to_jsonl(batch_payloads, batch_payloads_path)
        file = modules.upload_batch_payloads(batch_payloads_path)
        batch = modules.create_batch(file, endpoint, model)
        print("Batch job submitted. Batch ID:", batch.id)
        print("Status URL:", batch.status_url)

        # Poll status of batch execution, downloading the output JSONL results upon completion
        final_status = modules.poll_batch_until_complete(batch.id)
        modules.download_batch_result(final_status, batch_results_path)

        # If this is the first process order sequence, read the results output to fetch dependency field booleans for each processed SKU
        if process_order_number == 1:
            dependency_results = modules.get_dependency_results(fields_df, batch_results_path)

if __name__ == "__main__":
    main()