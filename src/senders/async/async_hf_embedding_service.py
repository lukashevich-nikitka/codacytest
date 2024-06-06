import argparse
import asyncio
import json
from typing import Any, Optional
import aiohttp
import asyncpg
from asyncpg import Pool, Record
import os
import logging
import time
import ssl
import certifi
from dotenv import load_dotenv

#Settings for streaming logger (are written to the file)
streaming_logger = logging.getLogger('StreamingLogger')
streaming_logger.setLevel(logging.INFO)
streaming_logger.propagate = False

streaming_handler = logging.FileHandler(os.path.join('logs', f'embeddings{time.time()}.log'))
streaming_handler.setLevel(logging.INFO)
streaming_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
streaming_logger.addHandler(streaming_handler)



#Console/status logs settings (output to the console)
console_logger = logging.getLogger('ConsoleLogger')
console_logger.setLevel(logging.DEBUG)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
console_logger.addHandler(console_handler)

load_dotenv("dev.env")

sslcontext = ssl.create_default_context(cafile=certifi.where())





def check_env_vars() -> None:
    lst_required_vars: list[str] = [
        'DB_NAME', 'DB_USER', 'DB_PASSWORD', 'DB_HOST', 'DB_PORT', 'GPU_SERVER_URL', 'RECEIVER_EMBED_API', 'RECEIVER_AUTH_TOKEN'
    ]
    for str_var in lst_required_vars:
        if os.getenv(str_var) is None:
            raise EnvironmentError(f"Environment variable {str_var} is not set.")

check_env_vars()





async def connect_to_db() -> Optional[Pool]:
    """
    Connect to postgress db using credentials from configuration file (dev.env).

    Parameters:
    str_database_name (str): Postgress db name.
    str_user (str): Postgress db user name.
    str_password (str): Postgress db user password.
    str_host (str): Postgress db host.
    str_port (str): Postgress db port.
    
    Returns:
    Pool: Pool of connections for connecting with postgress db or None(if no connection is established)
    
    """

    try:
        pool: Pool = await asyncpg.create_pool(
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            database=os.getenv('DB_NAME'),
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT'),
            min_size=1,
            max_size=10
        )
        streaming_logger.info("Successfully connected to the postgress database.")
        
        return pool
    
    except Exception as e:
        str_error_message: str = f"An error occurred while connecting to the database: {e}"
        
        console_logger.error(str_error_message)
        streaming_logger.error(str_error_message)
        return None





async def fetch_chunks(pool: Pool, int_needed: int, int_taskid: int) -> Optional[list[Record]]:
    """
    Get batch with updating statuses of processed rows to 'progress' by int_needed value.

    Parameters:
    pool (Pool): Instance of the Pool class for connecting with postgress.
    int_needed (int): Amount of chunks need to be extracted from the database.
    int_taskid (int): The task ID associated with the embedding task.
    
    Returns:
    list[Record]: List of asyncpg Records(rows extracted from the db) 
        or [](if there are no available embed_task_chunk instances)
        or None (if some errors occured during sql execution).
    
    """
    
    try:
        async with pool.acquire() as conn:
            lst_rec_chunkids: list[Record] = await conn.fetch("""
                    WITH updated AS (
                        UPDATE mgmt.embed_task_chunk
                        SET status = 'progress'
                        WHERE ctid IN (
                            SELECT ctid
                            FROM mgmt.embed_task_chunk
                            WHERE status = 'open'
                            AND taskid = $1
                            LIMIT $2
                        )
                        RETURNING *
                    )
                    SELECT chunkid FROM updated;
                    """, int_taskid, int_needed)
            
            if not lst_rec_chunkids:
                return []
            
            lst_chunkids: list[int] = [rec_chunk['chunkid'] for rec_chunk in lst_rec_chunkids]
            
            if lst_chunkids:
                return await conn.fetch("SELECT id, txt, publid, seq FROM mgmt.embed_chunk WHERE id = ANY($1) AND txt is not null", lst_chunkids)
            else:
                return []
        
    except Exception as e:
        str_error_message: str = f"An error occurred during fetching chunks. Error: {e}"
        
        console_logger.error(str_error_message)
        streaming_logger.error(str_error_message)
        return None





async def fetch_gpuserver_url(pool: Pool, session: aiohttp.ClientSession, lst_batch_embed_chunks: list[tuple[int, str, int, int]], 
                              dct_action_options: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Sends a batch of chunks to be embedded to the GPU server and handles the response.

    Parameters:
    pool (Pool): An instance of the Pool class for connecting to the PostgreSQL database.
    session (aiohttp.ClientSession): An instance of aiohttp.ClientSession for making HTTP requests.
    lst_batch_embed_chunks (list[tuple[int, str, int, int]]): A list of tuples representing the chunks to embed.
        Each tuple contains:
            - int: The chunk ID.
            - str: The text content of the chunk.
            - int: The publid associated with the chunk.
            - int: The sequence number of the chunk.
    dct_action_options (dict[str, Any]): A dictionary of action options that may include an "embed_prefix".

    Returns:
    list[dict[str, Any]]: A list of dictionaries with the embeddings. Each dictionary contains:
        - "chunkid": The ID of the chunk.
        - "seq": The sequence number of the chunk.
        - "publid": The publication ID associated with the chunk.
        - "vector": The embedding vector returned by the GPU server or None if an error occurred.
    
    """

    str_url: str | None = os.getenv('GPU_SERVER_URL')
    
    #prefix - symbols need to be in the start of chars sequence.
    str_prefix: Optional[str] = dct_action_options.get("embed_prefix", None)
    dct_data_to_gpuserver: dict[str, Any] = {
        "inputs": [
                str_prefix + ' ' + embed_chunk[1] for embed_chunk in lst_batch_embed_chunks] if str_prefix 
                else [embed_chunk[1] for embed_chunk in lst_batch_embed_chunks
            ],
        "normalize": True,
        "truncate": True
    }
        
    lst_embeddings_toreturn: list[dict[str, Any]] = [
        {
            "chunkid": embed_chunk[0],
            "seq": embed_chunk[3],
            "publid": embed_chunk[2],
            "vector": None
        } for embed_chunk in lst_batch_embed_chunks
    ]
    
    try:
        async with session.post(str_url, json=dct_data_to_gpuserver, ssl=sslcontext) as response:
            if response.status == 200:
                lst_response_embeddings: list[list[float]] = await response.json()
                
                for index, dct_item in enumerate(lst_embeddings_toreturn):
                    dct_item["vector"] = lst_response_embeddings[index]
                
                streaming_logger.info("Batch successfully processed on gpu server. Prepareing embeddings for sending to receiver...")
                
                return lst_embeddings_toreturn
            
            #if specified batch is more than 16000.
            if response.status == 413:
                streaming_logger.info("Payload too large, splitting batch into smaller chunks.")
                
                if len(lst_batch_embed_chunks) == 1:
                    streaming_logger.error("Single chunk too large to process. Marking as failed.")
                    
                    async with pool.acquire() as conn:
                        await conn.fetch("UPDATE mgmt.embed_task_chunk SET status = 'fail' WHERE chunkid = $1", lst_batch_embed_chunks[0][0])
                    return []
                
                #dividing a batch into smaller parts equal to one chunk
                lst_smaller_batches: list[list[tuple[int, str, int, int]]] = \
                    [lst_batch_embed_chunks[i:i + 1] for i in range(0, len(lst_batch_embed_chunks))]
                    
                lst_divided_results: list = []
                for lst_small_batch in lst_smaller_batches:
                    lst_res: list[dict[str, Any]] = await fetch_gpuserver_url(pool, session, lst_small_batch, dct_action_options)
                    if lst_res:
                        lst_divided_results.extend(lst_res)
                return lst_divided_results
            
            else:
                lst_fail_chunkids: list[int] = [tpl_embed_chunk[0] for tpl_embed_chunk in lst_batch_embed_chunks]
                
                str_log_error_message: str = \
                    f"Failed to fetch data. Error status code {response.status}. Error: {response.text()}. Change status to 'fail'"
                console_logger.error(str_log_error_message)
                streaming_logger.error(str_log_error_message)
        
                async with pool.acquire() as conn:
                    await conn.fetch("UPDATE mgmt.embed_task_chunk SET status = 'fail' WHERE chunkid = ANY($1)", lst_fail_chunkids)
                return []
            
    except Exception as e:
        str_error_message: str = f"An error occurred during sending batch to gpu server. Error: {e}"
        
        console_logger.error(str_error_message)
        streaming_logger.error(str_error_message)
        return []





async def fetch_receiver_url(session: aiohttp.ClientSession, lst_emebed_results: list[dict[str, Any]], 
                             int_taskid: int) -> None:
    """
    Sends a batch of embedded results to the receiver service and logs the outcome.

    Parameters:
    session (aiohttp.ClientSession): An instance of aiohttp.ClientSession for making HTTP requests.
    lst_emebed_results (list[dict[str, Any]]): A list of dictionaries containing embedded results.
        Each dictionary should include:
            - "chunkid": The ID of the chunk.
            - "seq": The sequence number of the chunk.
            - "publid": The publication ID associated with the chunk.
            - "vector": The embedding vector.
    int_taskid (int): The task ID associated with the embedding task.

    Returns:
    None
    
    """
    
    str_url: str | None = os.getenv("RECEIVER_EMBED_API")
    
    headers: dict[str, Any] = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {os.getenv('RECEIVER_AUTH_TOKEN')}",
    }
    
    dct_data_to_receiver: dict[str, Any] = {
        "status": 'success',
        "taskid": int_taskid,
        "vectors": lst_emebed_results,
        "statistics": {},
        "dim": len(lst_emebed_results[0]['vector']),
        "ip_address": "no needed"
    }
    
    try:
        async with session.post(str_url, json=dct_data_to_receiver, ssl=sslcontext, headers=headers) as response:
            if response.status == 200:
                console_logger.info("Batch was successfully sended to receiver service!")
                return
            else:
                error_message = await response.text()
                console_logger.error(f"Failed to fetch data. Error status code {response.status}. Error: {error_message}")
                return
            
    except Exception as e:
        str_error_message: str = f"An error occurred during sending batch to receiver. Error: {e}"
        
        console_logger.error(str_error_message)
        streaming_logger.error(str_error_message)
        return





async def get_task_options(pool: Pool, int_taskid: int, str_status: str) -> Optional[dict[str, Any]]:
    """
    Fetches the action options for a given task from the database and updates its status.

    Parameters:
    pool (Pool): An instance of the Pool class for connecting to the PostgreSQL database.
    int_taskid (int): The ID of the task whose options are to be fetched.
    str_status (str): The new status to set for the task (e.g., 'progress').

    Returns:
    Optional[dict[str, Any]]: A dictionary containing the action options for the task if found, 
                              or None if no task options are found or if an error occurs.
    """
    
    try:
        async with pool.acquire() as conn:
            lst_task_options: list[Record] = await conn.fetch("""
                WITH updated AS (
                    UPDATE mgmt.task
                    SET status = $1
                    WHERE (status = 'open' OR status = 'progress')
                    AND id = $2
                    RETURNING *
                )
                SELECT action_options FROM updated;
            """, str_status, int_taskid)
            
            if lst_task_options:
                # CTE made by task id - it means that only one task instance would be in lst_task_options list.
                return json.loads(lst_task_options[0]["action_options"])
            else:
                str_log_message: str = f"No task options found for task id {int_taskid}"
                console_logger.info(str_log_message)
                streaming_logger.info(str_log_message)
                
                return None
        
    except Exception as e:
        str_error_message: str = f"An error occurred while fetching task options: {repr(e)}"
        
        console_logger.error(str_error_message)
        streaming_logger.error(str_error_message)
        return None





async def process_embedding_task(int_taskid: int, int_task_batch_size: int, int_work_batch_size: int) -> None:
    """
    Processes an embedding task by managing database connections and asynchronous tasks.

    Parameters:
    str_database_name (str): The name of the database.
    str_user (str): The username for the database.
    str_password (str): The password for the database user.
    str_host (str): The host address of the database.
    str_port (str): The port number of the database.
    int_taskid (int): The ID of the task to be processed.
    int_task_batch_size (int): The desired batch size for tasks.
    int_work_batch_size (int): The batch size for individual work units.

    Returns:
    None: This function does not return any value. It manages tasks and logs the status of operations.
    
    """
    
    pool: Pool = await connect_to_db()
    if pool is None:
        console_logger.error("Failed to connect to the database")
        return
    
    tasks: list = []
    
    async with pool, aiohttp.ClientSession() as session:
        bln_todo: bool = True
        
        try:
            while bln_todo:
                dct_action_options: dict[str, Any] | None = await get_task_options(pool, int_taskid, str_status='progress')
                
                if tasks:
                    lst_done, lst_pending = await asyncio.wait(tasks, timeout=10.0, return_when=asyncio.FIRST_COMPLETED)
                    
                    tasks = list(lst_pending)
                    
                    lst_emebed_results: list = []
                    for task in lst_done:
                        result = task.result()
                        lst_emebed_results.extend(result)
                        
                    if lst_emebed_results:
                        asyncio.create_task(fetch_receiver_url(session, lst_emebed_results, int_taskid))
                
                
                if dct_action_options is None:
                    streaming_logger.info(
                        f"Couldn't get action options for taskid: {int_taskid}. The reason can be the task status or an error during sql query execution!"
                    )
                    await asyncio.sleep(10)
                    continue
                
                
                if len(tasks) < int_task_batch_size:
                    int_needed: int = int_task_batch_size - (len(tasks) * int_work_batch_size)
                    str_info_log_message: str = f"Need to extract new chunks: {int_needed}"
                    console_logger.info(str_info_log_message)
                    streaming_logger.info(str_info_log_message)
                    
                    if int_needed == 0:
                        str_info_log_message = "There are no chunks to embed."
                        console_logger.info(str_info_log_message)
                        streaming_logger.info(str_info_log_message)
                        await asyncio.sleep(10)
                        continue
                    
                    lst_embed_chunks: list[tuple[int, str, int, int]] | None = await fetch_chunks(pool, int_needed, int_taskid)
                    
                    if lst_embed_chunks is None:
                        continue

                    
                    lst_batched_embed_chunks = \
                        [lst_embed_chunks[i:i + int_work_batch_size] for i in range(0, len(lst_embed_chunks), int_work_batch_size)]
                    
                    new_tasks = \
                        [
                            asyncio.create_task(
                                    fetch_gpuserver_url(pool, session, lst_batch_embed_chunks, dct_action_options)
                                ) for lst_batch_embed_chunks in lst_batched_embed_chunks
                        ]
                    tasks.extend(new_tasks)
                
                await asyncio.sleep(2) #time between iterations to gather ready embeddings
        
        finally:
            await pool.close()





if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Sender for embedding tasks")
    parser.add_argument("-tid", "--taskid", required=True,
                        help="taskid of task you want to execute")
    
    parser.add_argument("-gpub", "--gpubatch", required=True,
                        help="gpu batch which we want to ALWAYS support on gpu machine")
    
    parser.add_argument("-reqb", "--reqbatch", required=True,
                        help="batch per request")
    args = parser.parse_args()
    
    #check whether the specified parameters lead to exceeding the limit of simultaneous requests.
    int_req_quantity: float = int(args.gpubatch) / int(args.reqbatch)
    if int_req_quantity > 500:
        raise ValueError(
            f"GPU Docker container supports a maximum of 512 concurrent requests. "
            f"Only up to 500 requests are allowed to leave 12 in reserve. "
            f"The provided parameters result in {int_req_quantity} requests, which exceeds the limit."
        )

    asyncio.run(process_embedding_task(
            int(args.taskid),
            int(args.gpubatch),
            int(args.reqbatch)
        )
    )
