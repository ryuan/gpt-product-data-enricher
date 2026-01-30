import tiktoken
from typing import List, Dict
from collections import defaultdict


class Encoder:
    def __init__(self, model: str):
        if model in tiktoken.model.MODEL_TO_ENCODING.keys():
            self.encoder: tiktoken.Encoding = tiktoken.encoding_for_model(model)
        elif (model + '-') in tiktoken.model.MODEL_TO_ENCODING.keys():
            self.encoder: tiktoken.Encoding = tiktoken.encoding_for_model(model + '-')
        else:
            print(f"Model {model} not in tiktoken's model-to-encoding directory. Defaulting to o200k_base for encoding.")
            self.encoder: tiktoken.Encoding = tiktoken.encoding_for_model('gpt-5')  # default to gpt-5's o200k_base encoding
        self.batch_tokens_estimate: Dict = defaultdict(int)

    def estimate_input_tokens(self, process_order_number: int, instructions: str, content: List[Dict], output_schema: Dict) -> int:
        prompts = "".join(obj['text'] for obj in content if 'text' in obj.keys())
        images = [obj for obj in content if 'text' not in obj.keys()]

        tokens = 0

        tokens += len(self.encoder.encode(instructions))
        tokens += len(self.encoder.encode(prompts))
        tokens += 70 * len(images)        # 70 tokens limit if image 'detail' is set to 'low' for gpt-5
        tokens += len(self.encoder.encode(str(output_schema)))

        self.batch_tokens_estimate[process_order_number] += tokens

        return tokens