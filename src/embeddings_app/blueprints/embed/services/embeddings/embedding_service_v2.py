from transformers import AutoTokenizer, AutoModel
import torch
import torch.nn.functional as F
from torch import Tensor

import logging

class EmbeddingService:
    def __init__(self, model_name):
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModel.from_pretrained(model_name)
        self.device = "cuda:0" if torch.cuda.is_available() else "cpu"

    def _average_pool(self, last_hidden_states: Tensor, attention_mask: Tensor) -> Tensor:
        last_hidden = last_hidden_states.masked_fill(
            ~attention_mask[..., None].bool(), 0.0)
        sum_hidden = last_hidden.sum(dim=1)
        attention_sum = attention_mask.sum(dim=1)[..., None]
        attention_sum = attention_sum.where(attention_sum != 0, torch.ones_like(attention_sum))
        return sum_hidden / attention_sum

    def generate_embeddings(self, texts: list) -> list:
        inputs = self.tokenizer(texts, return_tensors='pt', padding=True, 
                                truncation=True).to(self.device)
        with torch.no_grad():
            outputs = self.model.to(self.device)(**inputs)

        attention_mask = inputs['attention_mask']
        embeddings = self._average_pool(outputs.last_hidden_state, attention_mask)
        embeddings = F.normalize(embeddings, p=2, dim=1)

        return embeddings.numpy().tolist()
