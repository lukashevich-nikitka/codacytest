import os
import torch
from llama_index.embeddings import HuggingFaceEmbedding
from transformers import AutoTokenizer, AutoModel
from sentence_transformers import SentenceTransformer

from config.logger import configure_logger
logging = configure_logger('Embedding Service')

class EmbeddingService:
    def __init__(self, model_name, device):
        self.model_name = model_name if model_name else 'BAAI/bge-small-en-v1.5'
        self.model_path = os.path.join(os.getcwd(), 'model_weights', model_name)
        self.device = "cuda" if torch.cuda.is_available() and device == 'cuda' else "cpu"
        self.ensure_model_downloaded()
        self.embedding_model = SentenceTransformer(
                self.model_name, 
                trust_remote_code=True, 
                cache_folder=self.model_path, 
                device=self.device
            ) if self.model_name == "nomic-ai/nomic-embed-text-v1" else HuggingFaceEmbedding(
                model_name=self.model_path,
                device=self.device,
                embed_batch_size=1024,
            )

    def generate_embeddings(self, lst_batch: list[str]):
        logging.info(f'Start generating embeddings of {len(lst_batch)} pieces of text')
        
        if self.model_name == 'nomic-ai/nomic-embed-text-v1':
            logging.info(f"Take {self.model_name} in use")
            embeddings = self.embedding_model.encode(lst_batch, show_progress_bar=True, batch_size=1024)
            return embeddings.tolist()
        else:
            embeddings = self.embedding_model._embed(lst_batch)
            assert len(lst_batch) == len(embeddings)
            return embeddings

    def ensure_model_downloaded(self):
        logging.info('Downloading model process if it not exist.')
        if not os.path.exists(self.model_path):
            os.makedirs(self.model_path)
            logging.info(f"Downloading model {self.model_name} to {self.model_path}")
            
            if self.model_name == 'nomic-ai/nomic-embed-text-v1':
                SentenceTransformer(
                    self.model_name, 
                    trust_remote_code=True, 
                    cache_folder=self.model_path, 
                    device=self.device
                )
            else:
                tokenizer = AutoTokenizer.from_pretrained(self.model_name)
                model = AutoModel.from_pretrained(self.model_name)
                model.save_pretrained(self.model_path)
                tokenizer.save_pretrained(self.model_path)
        else:
            logging.info(f"Model {self.model_name} already downloaded.")