from blueprints.embed.services.embeddings.embedding_service_v1 import EmbeddingService
from config.logger import configure_logger

common_logging = configure_logger("common_logger")



def prepare_embedding_model_to_use(str_model: str, str_device: str = 'cuda') -> EmbeddingService:
    """
    Prepare embedding model to use for embeddings. By default - 'BAAI/bge-small-en-v1.5'

    Parameters:
    str_model (str | None): model name on hugging face platform.
    
    Returns:
    - instance of EmbeddingService class.
    """
    common_logging.info('Prepareing embedding model to use...')
    try:
        embedding_service = EmbeddingService(str_model, str_device)
        return embedding_service
        
    except Exception as e:
        common_logging.error(f"Ann error occured during installing model. Error: {e}")
        str_default_model = 'BAAI/bge-small-en-v1.5'
        embedding_service = EmbeddingService(str_default_model, str_device)
        
        return embedding_service