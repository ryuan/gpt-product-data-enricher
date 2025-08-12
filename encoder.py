import tiktoken
from typing import List, Dict
from collections import defaultdict


class Encoder:
    def __init__(self, model: str):
        if model in tiktoken.model.MODEL_TO_ENCODING.keys():
            self.encoder: tiktoken.Encoding = tiktoken.encoding_for_model(model)
        else:
            self.encoder: tiktoken.Encoding = tiktoken.encoding_for_model(model + '-')
        self.batch_tokens_estimate: Dict = defaultdict(int)

    def estimate_input_tokens(self, process_order_number: int, system_instructions: str, user_prompt: str, product_img_urls: List[str], output_schema: Dict) -> int:
        tokens = 0

        tokens += len(self.encoder.encode(system_instructions))
        tokens += len(self.encoder.encode(user_prompt))
        tokens += 85 * len(product_img_urls)        # 85 tokens limit if image 'detail' is set to 'low'
        tokens += len(self.encoder.encode(str(output_schema)))

        self.batch_tokens_estimate[process_order_number] += tokens

        return tokens