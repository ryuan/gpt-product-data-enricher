import utils
from generator import PayloadsGenerator
from manager import BatchManager


def main():
    client = utils.init()

    # Pre-process source files and sequence batches by process order
    supplier_data_path, store_data_path, fields_data_path = utils.get_source_paths()
    supplier_data_df, store_data_df, fields_data_df = utils.get_input_dfs(supplier_data_path, store_data_path, fields_data_path)
    sku_col_name, process_order_numbers = utils.sequence_batches(supplier_data_df, fields_data_df)
    product_ids_skus = utils.get_product_ids_skus(store_data_df)

    # Initiate PayloadsGenerator object and set Batch API endpoint & model
    payloads_generator = PayloadsGenerator(sku_col_name, supplier_data_df, store_data_df, fields_data_df, product_ids_skus)
    endpoint = '/v1/chat/responses'
    model = 'gpt-4o'

    # Generate payloads for each sequenced batch process, upload the payloads JSONL file, execute the batch, then download results
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