import gc
import logging
from typing import Any, Optional
import requests
import json
import time
import torch

from blueprints.embed.services.helpers.statistic_collector.gpus_load_checker import GPUMonitor
from utils.status_sender import ip
from config.settings import MANAGEMENT_SCRIPT_TOKEN, MANAGING_SCRIPT_EMBED_API
from config.logger import configure_logger
from utils.notify_needed import notify_needed
from .helpers.execution_time_decorator import timeit
from .common.prepare_embed_model import prepare_embedding_model_to_use

stop_thread = False


logging = configure_logger('Embbedings thread')



@timeit
def prepare_transfer_data(lst_vectors: list[list[float]], int_taskid: int,
                          dct_statistic: dict[str, Any], int_dim: int, str_status: str = 'success') -> dict[str, Any]:
    """
    Prepare object for sending to receiver servise.

    Parameters:
    lst_vectors (list): list of dictionaries with embeddings and appropriate chunkids.
    int_taskid (int): ID of current task.
    dct_statistic (dict): full statistic for full batch.
    int_dim (int): model dimension (length of produced vector).
    str_status (str): status for full batch.
    
    Returns:
    dict[str, Any]: dictionary with all embedding process data.

    """
    
    transfer_data: dict[str, Any] = {
        "status": str_status,
        "taskid": int_taskid,
        "vectors": lst_vectors,
        "statistics": dct_statistic,
        "dim": int_dim,
        "ip_address": ip if ip else "ip error"
    }
    
    return transfer_data



@timeit
def send_data_to_management(lst_vectors: list[Optional[list[float]]], int_taskid: int,
                                  dct_statistic: dict[str, Any], int_dim: Optional[int], str_status = 'success') -> None:
    """
    Sending embeddings to receiver for further saving in db.

    Parameters:
    lst_vectors (list): list of dictionaries with embeddings and appropriate chunkids.
    int_taskid (int): ID of current task.
    dct_statistic (dict): full statistic for full batch.
    int_dim (int): model dimension (length of produced vector).
    str_status (str): status for full batch.
    
    Returns:
    None

    """
    
    transfer_data: dict[str, Any] = prepare_transfer_data(lst_vectors, int_taskid, dct_statistic, int_dim, str_status)

    logging.info('Start sending the embeddings to management service')
    
    data = json.dumps(transfer_data)
    
    headers: dict[str, Any] = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {MANAGEMENT_SCRIPT_TOKEN}",
    }
    
    requests.post(MANAGING_SCRIPT_EMBED_API, data=data, headers=headers)
    
    logging.info('Successfully sent to the management service')



@timeit
def paste_embeddings(lst_chunks: list[dict[str, Any]], lst_embeddings: list[Optional[list[float]]]) -> list[dict[str, Any]]:
    """
    Prepare necessary structure for sending embeddings to receiver service.

    Parameters:
    lst_chunks (list): list of dictionaries with chunks's texts to embed.
    lst_embeddings (list): list of ready embeddings.
    
    Returns:
    list[dict[str, Any]]: list of dictionaries with embeddings and appropriate chunkids.

    """
    
    lst_vectors: list[dict[str, Any]] = [
        {'chunkid': chunk['chunkid'], 'seq': chunk['seq'], 'publid': chunk['publid'], 'vector': vector} 
        for chunk, vector in zip(lst_chunks, lst_embeddings)
    ]
    
    return lst_vectors



@timeit
def produce_vectors(lst_chunks: list[dict[str, Any]], str_model: Optional[str], int_batch_size: int = 5) -> tuple[list[dict[str, Any]], str]:
    """
    Creates embeddings using the provided embedding model. If name of provided model is incorrect, the default embedding model will be paste.

    Parameters:
    lst_chunks (list): list of dictionaries with chunks's texts to embed.
    str_model (str | None): model name on hugging face platform.
    int_batch_size (int): batch size for batching all collection of chunks before makeing embeddings.
    
    Returns:
    tuple[list[dict[str, Any]]: tupple with 2 elements:
        - list of ready vectors.
        - using model_name for embeddings. In case of incorrect provided by operator model name.

    """
    
    logging.info('Producing embeddings...')
    
    int_work_batch_size: int = int_batch_size
    bln_togenearate: bool = True
    
    while bln_togenearate:
        try:
            device = 'cuda'
            embedding_service = prepare_embedding_model_to_use(str_model, device)
                
            lst_embeddings: list[list[float]] = []
            
            for i in range(0, len(lst_chunks), int_work_batch_size):
                lst_batch_chunks: list[dict[str, Any]] = lst_chunks[i:i + int_work_batch_size]
                lst_txts: list[str] = [dct_chunk["txt"] for dct_chunk in lst_batch_chunks]
                lst_embeddings += embedding_service.generate_embeddings(lst_txts)
            
            notify_needed.set()

            lst_vectors: list[dict[str, Any]] = paste_embeddings(lst_chunks, lst_embeddings)
            
            return lst_vectors, embedding_service.model_name
        
        except RuntimeError as runtime_error:
            if "CUDA out of memory" in str(runtime_error):
                gc.collect()  # Collect garbage to free memory
                torch.cuda.empty_cache()  # Empty the CUDA cache
                
                logging.warning('CUDA out of memory! Reduce batch size and repeat...')
                int_batch_reducer: int = 1
                int_work_batch_size -= int_batch_reducer
                
                if int_work_batch_size <= 0:
                    raise ValueError("Variable int_work_batch_size couldn't be less than 0")
            else:
                raise
        
        except Exception as e:
            logging.error(f'An error occured during execution of produce_vectors function, because of: {repr(e)}')
            raise



@timeit
def triger_embedding_task(lst_chunks: list[dict[str, Any]], str_model: Optional[str], int_taskid: int, int_batch_size: int = 5) -> None:
    """
    Creates embeddings using the provided embedding model and send the results to receiver service for further saving in db.

    Parameters:
    lst_chunks (list): list of dictionaries with chunks's texts to embed.
    str_model (str | None): model name on hugging face platform.
    int_taskid (int): ID of current task.
    int_batch_size (int): batch size for batching all collection of chunks before makeing embeddings.
    
    Returns:
    None

    """
    
    logging.info('Embedding task registration!')
    average_load = None
    gpu_monitor = None

    try:
        if torch.cuda.is_available():
            gpu_monitor = GPUMonitor(interval=1)

        start_time = time.time()
        
        lst_vectors, str_model = produce_vectors(lst_chunks, str_model, int_batch_size)
        
        end_time = time.time()

        if torch.cuda.is_available():
            average_load = gpu_monitor.stop_and_get_average_load()
        logging.info(f"Average GPU Load: {average_load}")


        statistic = {
            "Processing Time": end_time - start_time,
            "Number of nodes/embeddings": len(lst_vectors),
            "Embed Model": str_model,
            "Embedding model dimension": len(lst_vectors[0]["vector"]),
            "GPU type": torch.cuda.get_device_name(torch.cuda.current_device()) if torch.cuda.is_available() else 'No GPU',
            "GPU load": average_load
        }

        send_data_to_management(lst_vectors, int_taskid, statistic, len(lst_vectors[0]["vector"]))
        
    except Exception as e:
        if torch.cuda.is_available():
            average_load = gpu_monitor.stop_and_get_average_load()
        end_time = time.time()
        
        lst_error_embeddings: list[None] = [None]*len(lst_chunks)
        lst_error_vectors: list[dict[str, Any]] = paste_embeddings(lst_chunks, lst_error_embeddings)
        
        dct_error_statistic: dict[str, Any] = {
            "Processing Time": end_time - start_time,
            "Number of nodes/embeddings": len(lst_error_embeddings),
            "Embed Model": str_model,
            "Embedding model dimension": None,
            "GPU type": torch.cuda.get_device_name(torch.cuda.current_device()) if torch.cuda.is_available() else 'No GPU',
            "GPU load": average_load
        }
        
        str_fail_batch_status: str = 'fail'
        send_data_to_management(lst_error_vectors, int_taskid, dct_error_statistic, None, str_fail_batch_status)
        logging.info(e)