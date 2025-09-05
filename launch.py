import utils
from datetime import datetime
from generator import PayloadsGenerator
from encoder import Encoder
from manager import BatchManager
from crawler import WebSearchTool


def main():
    client = utils.init()
    endpoint = utils.set_endpoint()
    model = 'gpt-5'
    date_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    # Pre-process source files and sequence batches by process order
    supplier_data_path, store_data_path, fields_data_path = utils.get_source_paths()
    supplier_data_df, store_data_df, fields_data_df = utils.get_input_dfs(supplier_data_path, store_data_path, fields_data_path)
    process_order_numbers = utils.sequence_batches(fields_data_df)

    # Prompt user to run web search tool (or reuse existing web search results)
    web_search_results_path = 'web-search/web_search_results.jsonl'
    crawler = WebSearchTool(client, endpoint, model, store_data_df, web_search_results_path)
    crawler.run()

    # Initiate PayloadsGenerator and BatchManager object
    encoder = Encoder(model)
    batch_manager = BatchManager(client, endpoint, model, date_time)
    payloads_generator = PayloadsGenerator(crawler, encoder, batch_manager, supplier_data_df, store_data_df, fields_data_df)

    # Generate payloads for each sequenced batch process, upload the payloads JSONL file, execute the batch, then download results
    for process_order_number in process_order_numbers:
        # Generate and write request payloads to the batch payloads JSONL file
        payloads_generator.generate_batch_payloads(process_order_number)

        if utils.get_file_size(batch_manager.current_batch_files.batch_payloads_path) > 0:
            # Create batch payloads JSONL file, upload it, then execute batch payloads asynchronously
            batch_manager.upload_batch_payloads()
            batch_manager.create_batch()

            # Poll status of batch execution, downloading the output JSONL results upon completion
            batch_manager.poll_batch_until_complete()
            batch_manager.download_batch_results()
            batch_manager.update_error_ids()
            batch_manager.save_outputs_from_batch_results()
            batch_manager.print_token_usage()

            # If this is the first process order sequence, read the results output to fetch dependency field booleans for each processed SKU
            if process_order_number == 1:
                payloads_generator.set_dependency_results()

            # If this is the final process order sequence, combine all batch outputs for Shopify import
            if process_order_number == max(process_order_numbers):
                batch_manager.combine_outputs(store_data_df, fields_data_df)

if __name__ == "__main__":
    main()