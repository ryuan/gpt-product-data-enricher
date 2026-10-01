import json
import math
import tiktoken
from typing import List, Dict, Optional, Tuple
from collections import defaultdict


class Encoder:
    def __init__(self, model: str):
        self.model = model

        try:
            self.encoder: tiktoken.Encoding = tiktoken.encoding_for_model(model)
        except KeyError:
            print(f"Model {model} not in tiktoken's model-to-encoding directory. Defaulting to o200k_base for encoding.")
            self.encoder: tiktoken.Encoding = tiktoken.encoding_for_model('gpt-5')  # default to gpt-5's o200k_base encoding

        self.batch_tokens_estimate: Dict = defaultdict(int)
        self.batch_image_dimension_fallbacks: Dict = defaultdict(int)

    def estimate_input_tokens(self, process_order_number: int, instructions: str, content: List[Dict], output_schema: Dict, image_dimensions: Optional[List[Optional[Tuple[int, int]]]] = None) -> int:
        prompts = [obj['text'] for obj in content if 'text' in obj.keys()]
        images = [obj for obj in content if 'text' not in obj.keys()]
        image_dimensions = image_dimensions or []

        tokens = 0

        tokens += len(self.encoder.encode(instructions))
        tokens += sum(len(self.encoder.encode(prompt)) for prompt in prompts)
        tokens += self.__estimate_image_tokens(process_order_number, images, image_dimensions)
        tokens += len(self.encoder.encode(json.dumps(output_schema, ensure_ascii=True)))

        self.batch_tokens_estimate[process_order_number] += tokens

        return tokens

    def __estimate_image_tokens(self, process_order_number: int, images: List[Dict], image_dimensions: List[Optional[Tuple[int, int]]]) -> int:
        tokens = 0

        for idx, image in enumerate(images):
            detail = image.get('detail') if image.get('type') == 'input_image' else image.get('image_url', {}).get('detail')

            if detail not in ['low', 'auto']:
                raise ValueError(f"Image token estimator only supports detail='low' or detail='auto'. Received detail={detail!r}.")

            dimensions = image_dimensions[idx] if idx < len(image_dimensions) else None

            if self.model in ['gpt-5', 'gpt-5.1']:
                if detail == 'low':
                    tokens += 70
                elif dimensions:
                    tokens += self.__estimate_tile_image_tokens(*dimensions)
                else:
                    tokens += 1190
                    self.batch_image_dimension_fallbacks[process_order_number] += 1
            elif self.model in ['gpt-6-luna', 'gpt-6-sol']:
                if dimensions:
                    if detail == 'low':
                        tokens += self.__estimate_low_patch_image_tokens(*dimensions)
                    else:
                        tokens += self.__estimate_original_patch_image_tokens(*dimensions)
                else:
                    tokens += 308 if detail == 'low' else 36000
                    self.batch_image_dimension_fallbacks[process_order_number] += 1
            else:
                raise ValueError(f"Image token estimation is not configured for model {self.model}.")

        return tokens

    @staticmethod
    def __estimate_low_patch_image_tokens(width: int, height: int) -> int:
        if width <= 0 or height <= 0:
            raise ValueError(f"Image dimensions must be greater than 0. Received {width}x{height}.")

        if width <= 512 and height <= 512:
            width_patches = math.ceil(width / 32)
            height_patches = math.ceil(height / 32)
        elif width >= height:
            width_patches = 16
            height_patches = math.ceil(16 * height / width)
        else:
            width_patches = math.ceil(16 * width / height)
            height_patches = 16

        return math.ceil(width_patches * height_patches * 1.2)

    @staticmethod
    def __estimate_original_patch_image_tokens(width: int, height: int) -> int:
        if width <= 0 or height <= 0:
            raise ValueError(f"Image dimensions must be greater than 0. Received {width}x{height}.")

        if max(width, height) > 65535:
            scale = 65535 / max(width, height)
            width = math.floor(width * scale)
            height = math.floor(height * scale)

        patches = math.ceil(width / 32) * math.ceil(height / 32)

        if patches > 30000:
            raise ValueError(f"Image requires {patches} patches at detail='auto', exceeding the 30,000-patch limit.")

        return math.ceil(patches * 1.2)

    @staticmethod
    def __estimate_tile_image_tokens(width: int, height: int) -> int:
        if width <= 0 or height <= 0:
            raise ValueError(f"Image dimensions must be greater than 0. Received {width}x{height}.")

        scale = min(1, 2048 / width, 2048 / height)
        width = math.floor(width * scale)
        height = math.floor(height * scale)

        if min(width, height) > 768:
            scale = 768 / min(width, height)
            width = math.floor(width * scale)
            height = math.floor(height * scale)

        tiles = math.ceil(width / 512) * math.ceil(height / 512)
        return 70 + tiles * 140
