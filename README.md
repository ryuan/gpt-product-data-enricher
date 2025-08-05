# GPT Product Data Enricher

**Enrich, enhance, and normalize ecommerce product data using GPT-4o.**

The program ingests data from 3 sources - tabular CSV data from suppliers, product images, and crawled website data from suppliers. It then processes all the data, mapping them to standardized fields, while adding, omitting, and modifying (both rewriting and normalizing) data for accuracy, originality, and filterability.

The program leverages the Batch API for cost efficiency, as well as the new Responses API endpoint to take advantage of capabilities like ascynchronous Web Search to fetch even more data for each payload processed within a batch. Currently supports Shopify only (via GraphQL query results).

Featuring context-based, sequential batch processing, the program aims to transform tabular data into structured results that maintain reasoning fidelity and deep logic while reducing hallucinations and cognitive load.

## To-Do's

- [x] Pre-process tabular data
- [x] Web search tool architecture design
- [x] Web search payload generation
- [x] Web search execution and handling
- [x] Data segmenting and fragment design
- [x] Core algorithm for request sequencing
- [x] System instructions & prompt design
- [x] Output response schema design
- [x] Input payload construction
- [x] Sequential batch payloads processing
- [x] Format and save responses as JSONL
- [ ] Validate and parse result JSONL
- [ ] Heuristic QA flagging
- [ ] Prepare Shopify GraphQL mutation inputs
- [ ] Audit log

## Notes

- OpenAI's Batch API currently does not support the web search tool. Until this feature is unlocked on OpenAI's side, the program can synchronously call the Response API on a per-SKU basis, write results, then relay the parsed output as part of the prompt for each payload in the batch.
- As of now, the web search tool in its preview form produces very low quality results and high hallucination rates (outright inventing URLs or not copying data exactly from supplier webpages). The option will remain in the prorgram, but is not recommended for use.