# GPT Product Data Enricher

**Enrich, enhance, and normalize ecommerce product data using GPT-4o.**

The program ingests data from 3 sources - tabular CSV data from suppliers, product images, and crawled website data from suppliers. It then processes all the data, mapping them to standardized fields, while adding, omitting, and modifying (both rewriting and normalizing) data for accuracy, originality, and filterability.

The program leverages the Batch API for cost efficiency, as well as the new Responses API endpoint to take advantage of advanced capabilities like ascynchronous Web Search to fetch even more data for each payload processed within a batch.

## To-Do's

1. Pre-process tabular data
2. Pre-process image data
3. Data segmenting and fragment design
4. Core algorithm for request sequencing
5. Prompt design
6. Output response design
7. Input payload construction
8. Sequentially process batch payloads
9. Format and save responses as JSONL
10. Validate and parse result JSONL
11. Heuristic QA flagging
12. Prepare Shopify GraphQL mutation inputs
13. Audit log