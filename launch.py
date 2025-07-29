import modules
from generator import PayloadsGenerator
from manager import BatchManager


def main():
    client = modules.init()

    # Pre-process source files and prepare batch sequences
    supplier_data_path, image_urls_path, fields_path = modules.get_source_paths()
    supplier_data_df, image_urls_df, fields_df = modules.get_input_dfs(supplier_data_path, image_urls_path, fields_path)
    sku_col_name, process_order_numbers = modules.sequence_batches(supplier_data_df, fields_df)
    sku_to_model, model_to_skus = modules.get_related_skus(sku_col_name, supplier_data_df)

    # Construct prompts, structure output JSON schemas, and generate batch payloads for each process sequence
    payloads_generator = PayloadsGenerator(sku_col_name, supplier_data_df, image_urls_df, fields_df, sku_to_model, model_to_skus)
    endpoint = '/v1/chat/responses'
    model = 'gpt-4o'

    for process_order_number in process_order_numbers:
        batch_payloads_path = f'output/batch_payloads_{process_order_number}.jsonl'
        batch_results_path = f'output/batch_results_{process_order_number}.jsonl'
        batch_manager = BatchManager(client, endpoint, model, batch_payloads_path, batch_results_path)

        # Generate and write request payloads to the batch payloads JSONL file
        payloads_generator.generate_batch_payloads(process_order_number, batch_manager)

        # Create batch payloads JSONL file, upload it, then execute batch payloads asynchronously
        batch_manager.upload_batch_payloads()
        batch_manager.create_batch(endpoint, model)

        # Poll status of batch execution, downloading the output JSONL results upon completion
        batch_manager.poll_batch_until_complete()
        batch_manager.download_batch_results()

if __name__ == "__main__":
    main()