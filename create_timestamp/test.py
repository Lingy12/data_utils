from transformers import WhisperProcessor

processor = WhisperProcessor.from_pretrained("openai/whisper-tiny")

tokenizer = processor.tokenizer

tokenizer.predict_timestamps = True

input_ids = tokenizer.encode('<|0.00|>Good morning<|0.80|>', add_special_tokens=True)

print(input_ids)

decoded_string = tokenizer.decode(input_ids,skip_special_tokens=False, decode_with_timestamps=True)

print(decoded_string)