# GPT Product Data Enricher

**Enrich, enhance, and normalize ecommerce product data using GPT-5.**

The program ingests data from 3 sources - tabular CSV data from suppliers, product images, and crawled website data from suppliers. It then processes all the data, mapping them to standardized fields, while adding, omitting, and modifying (both rewriting and normalizing) data for accuracy, originality, and filterability.

The program leverages the Batch API for cost efficiency, as well as the new Responses API endpoint to take advantage of capabilities like ascynchronous Web Search to fetch even more data for each payload processed within a batch. Optionally, the Chat Completions API endpoint can also be used in case there are bugs with Responses API. Currently only supports Shopify GraphQL query results as input data.

Featuring context-based, sequential batch processing, the program aims to transform tabular data into structured results that maintain reasoning fidelity and deep logic while reducing hallucinations and cognitive load.

## Notes

- Because using this program requires your own OpenAI API key, you will be rate limited to your account tier. At tier 1, you will be significantly rate limited to just 90000 tokens per day (rolling 24 hours), so each batch sequence will be chunked into very few payloads and processed slowly. To make the best use of this program, it's imperative to achieve tier 4+ on your account to increase the TPD to 200 million+.
- Some features in OpenAI's Responses API is currently broken when paired with the gpt-5 model. One of the issues is that the `detail` param is ignoring the `low` argument, so it's always processing images in `high` setting, causing token consumption to be around 6 times higher. A few people and I have lobbied our complaints and the peeps at OpenAI claim they're working on it, but we'll see - they said they'll update us here in [this community post](https://community.openai.com/t/responses-api-gpt-5-ignores-the-detail-parameter-on-image-inputs/1344058).
- OpenAI's Batch API currently does not support the web search tool. Until this feature is unlocked on OpenAI's side, the program can synchronously call the Response API on a per-SKU basis, write results, then relay the parsed output as part of the prompt for each payload in the batch.
- As of now, the web search tool in its preview form produces very low quality results and high hallucination rates (outright inventing URLs or not copying data exactly from supplier webpages). The option will remain in the prorgram, but is not recommended for use.