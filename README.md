# GPT Product Data Enricher

**Enrich, enhance, and normalize ecommerce product data using GPT-5/GPT-5.1**

The purpose of this program is to enrich and normalize ecommerce data for products, variants, and images. Use it to empower more intuitive sidebar filter labels, unique product descriptions, granular specifications data, classify images for showing on specific sections, label image alt text for shot type and SKU, and more.

The program ingests data from 3 sources - tabular CSV data from a supplier, product images, and Shopify GraphQL `products` query. It then processes all the data, mapping them to standardized fields, while adding, omitting, and modifying (both rewriting and normalizing) data for accuracy, originality, and filterability.

The program leverages the Batch API for cost efficiency, with the option to choose either the Responses API or Chat Completions API endpoint. This keeps the program future-proof in case ascynchronous Web Search and other Responses API tools are supported for the Batch API. The Chat Completions API endpoint has a longer support history, so is more stable than Responses API.

Unless there is an ongoing bug with Responses API (which is sadly all too common), you should opt for running the program with the Responses API endpoint. It offers slightly better reasoning outputs, superior image analysis pipeline (since Responses API uses the Files API, there is no risk of URL timeouts or hitting file size limit from passing base64-encoded images), lower costs from improved cached token usage, and more.

Featuring context-based, sequential batch processing, the program aims to transform tabular data into structured results that maintain reasoning fidelity and deep logic while reducing hallucinations and cognitive load.

## Features

- Support for extracting all Shopify base product, variant, and image attributes, as well as custom metafields
- Support for extracting all data types, with ability to specify enum options and custom JSON objects
- Sequences batch calls into staged process-ordered calls, enforcing tighter context per call and better model output accuracy 
- Batch sequencing supports dependency checks for subsequent fields (e.g., only if first call labeled `Upholstered` as `true`, request `Upholstery Color` in a later call)
- Ability to provide instructive notes for each extraction field to enforce specific labeling guidelines
- Custom inclusion/exclusion tag in instructive notes based on product type or field presence (e.g., show a particular block of notes only if the product type is `Floor Lamps` or `Table Lamps`)
- Text prompt and image inputs are automatically grouped together with their parent objects to improve model accuracy
- Support for processing product-level resource objects as a group of products sharing the same first word in their title (i.e., this can let the model extract data that requires context across multiple different but similar products such as defining combined listings or standardizing titles across products from the same collection and type)
- Decouples GraphQL field names from descriptive field names during data extraction for superior model output
- Results from all batch processes are unified and exported into a single XLSX file with GraphQL fields as headers for easy importing via GraphQL mutations
- ...too lazy to write more (just ask if you're wondering about a feature's existance)

## Usage Guide

- WIP

## Notes

- Because using this program requires your own OpenAI API key, you will be rate limited to your account tier. At tier 1, you will be significantly rate limited to just 1.5 million tokens per day (rolling 24 hours). Therefore, it's imperative to achieve tier 4+ on your account to increase the TPD to 200 million+.
- The program currently supports both `gpt-5` and `gpt-5.1` models. Based on internal tests, `gpt-5.2` models produce significantly inferior results for data extraction tasks. Its main issue is that it commits too heavily to "official" sources even when specific instructions ask it to deviate. For example, for a `Materials` sidebar filter, you may want faux marble made of polyresin to be labeled `Marble`, yet the model will inconsistently drift between `Plastic / Acrylic` and `Marble`.
- Some features in OpenAI's Responses API frequently breaks with the `gpt-5`/`gpt-5.1` model via Batch API. One of the issues is that the `detail` param often ignores the `low` argument, so it's always processing images in `high` setting, causing token consumption to be around 6 times higher. A few people and I have lobbied our complaints and the peeps at OpenAI claim they're working on it, but we'll see - they said they'll update us here in [this community post](https://community.openai.com/t/responses-api-gpt-5-ignores-the-detail-parameter-on-image-inputs/1344058).
- OpenAI's Batch API currently does not support the web search tool, but this will be built in when it's available. For now, I've tested building a pipeline to synchronously call the Response API on a per-SKU basis, write results, then relay the parsed output as part of the prompt for each payload in the batch. However, the model outputs had overly high rates of hallucinations to warrant supplementing the normal program batch processes.

## Contact

- Happy to float around ideas, answer questions, or collaborate (on this or related projects dealing with leveraging generative AI models to improve tabular/Excel data analyses, modeling, and extraction). You can reach me at {first_name}.{last_name}@gmail.com - obfuscating a bit to prevent spam, but youshould get the idea.