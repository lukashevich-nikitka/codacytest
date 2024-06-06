import argparse
import os
import logging
from typing import Any, Optional, Union
import psycopg2
import time
from dotenv import load_dotenv
import time
import sys
import requests


sys.path.append(os.getcwd())


log_directory: str = 'logs/streaming_logs'
if not os.path.exists(log_directory):
    os.makedirs(log_directory)
log_file_path: str = os.path.join(log_directory, f'embeddings{time.time()}.log')


#Settings for streaming logger (are written to the file)
streaming_logger = logging.getLogger('StreamingLogger')
streaming_logger.setLevel(logging.INFO)
streaming_logger.propagate = False

streaming_handler = logging.FileHandler(log_file_path)
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

load_dotenv('dev.env')

from perflog_config import PerformanceLogger
performance_logger = PerformanceLogger("perf_logs")





def connect_to_db(str_database_name: str, str_user: str, str_password: str, str_host: str, str_port: str):
    """
    Connect to postgress db using credentials from configuration file (dev.env).

    Parameters:
    str_database_name (str): postgress db name.
    str_user (str): postgress db user name.
    str_password (str): postgress db user password.
    str_host (str): postgress db host.
    str_port (str): postgress db port.
    
    Returns:
    psycopg2.extensions.connection: psycopg connection class instance.
    
    """
    
    streaming_logger.info("Connecting to the postgress db...")
    
    try:
        timestamp_start = time.time()
        
        conn_db = psycopg2.connect(
            database=str_database_name,
            user=str_user,
            password=str_password,
            host=str_host,
            port=str_port
        )
        performance_logger.logperf(timestamp_start, 'db_connection')

        streaming_logger.info("Successfully connected to the postgress db.")
        console_logger.info("Successfully connected to the postgress db.")
        return conn_db
    
    except Exception as e:
        str_error_msg: str = f"Something went wrong during connection to the database, because of {repr(e)}"
        
        streaming_logger.error(str_error_msg)
        console_logger.error(str_error_msg)
        return None





def get_unfinished_tasks(conn_db: psycopg2.extensions.connection, cursor: psycopg2.extensions.cursor,
                         int_taskid: Union[int, None], action: str = 'embedding') -> list[tuple[int, dict[str, Any]]]:
    """
    Get task with status 'available' or 'progress' by int_taskid.

    Parameters:
    conn_db (connection): psycopg connection class instance for connection with postgress db.
    cursor (cursor): psycopg cursor class instance for executing sql comands in postgress db.
    int_taskid (int | None): certain taskid to process single task or None to process all 'available' tasks.
    str_action (str): type of task to process (default value for this Sender: 'embedding').
    
    Returns:
    list[tuple[int, dict[str, Any]]]: python list with tupples which store taskid and dict with settings for text generation service.
    
    """
    
    try:
        timestamp_start = time.time()

        str_query = """
            SELECT id, action_options
            FROM task
            WHERE (status = 'open' OR status = 'progress')
            AND action = %s
        """

        lst_params = [action]

        if int_taskid is not None:
            str_query += " AND id = %s"
            lst_params.append(int_taskid)

        str_query += " ORDER BY status ASC"

        cursor.execute(str_query, tuple(lst_params))
        tbl_lst_unfinshed_tasks: list[tuple[int, dict[str, Any]]] = cursor.fetchall()
        conn_db.commit()

        performance_logger.logperf(timestamp_start, 'get unfinishehd task')
        streaming_logger.info(f"{len(tbl_lst_unfinshed_tasks)} unfinished tasks have taken for processing")
        console_logger.info(f"{len(tbl_lst_unfinshed_tasks)} unfinished tasks have taken for processing")
        
        return tbl_lst_unfinshed_tasks
    
    except Exception as e:
        str_error_msg: str = f"An error occured during getting unfinished tasks. Error: {repr(e)}"
        
        streaming_logger.error(str_error_msg)
        console_logger.error(str_error_msg)
        raise





def get_available_worker(conn_db: psycopg2.extensions.connection, cursor: psycopg2.extensions.cursor,
                         int_taskid: Optional[int]) -> tuple[str]:
    """
    Get available vm info from worker table by taskid.

    Parameters:
    conn_db (connection): psycopg connection class instance for connection with postgress db.
    cursor (cursor): psycopg cursor class instance for executing sql comands in postgress db.
    int_taskid (int): taskid to process.
    
    Returns: 
    tuple[str]: Return tuple with worker info.
    
    """
    
    streaming_logger.info(f"Getting available worker for taskid: {int_taskid}...")
    
    bln_worker_to_search: bool = True
        
    while bln_worker_to_search:
        try:
            cursor.execute(f"""
                WITH updated AS (
                    UPDATE worker
                    SET status = 'busy'
                    WHERE ctid IN (
                        SELECT ctid
                        FROM worker
                        WHERE status = 'available'
                        AND action_type = 'embedding'
                        AND taskid_limit = {int_taskid}
                        LIMIT 1
                    )
                    RETURNING *
                )
                SELECT ip_address FROM updated;
                """)
            conn_db.commit()
            tbl_lst_workers: list[tuple[str]] = cursor.fetchall()
            
            if tbl_lst_workers:
                streaming_logger.info(f"Worker with ip: {tbl_lst_workers[0][0]} available and changed to busy")
                
                bln_worker_to_search = False
                return tbl_lst_workers[0]

        except Exception as e:
            str_error_msg: str = f"An error occured during getting available workers. Error: {repr(e)}"
            
            streaming_logger.error(str_error_msg)
            console_logger.error(str_error_msg)
            raise





def get_ip_port(str_vm_address: str) -> str:
    """
    Making full ip:port string.

    Parameters:
    str_vm_address (str): available vm address.
    
    Returns:
    str: full ip:port string.
    
    """
    
    try:
        lst_ip_port = str_vm_address.split(":")

        str_default_port: str = "5004"

        if len(lst_ip_port) < 2:
            lst_ip_port.append(str_default_port)

        str_full_ip: str = ":".join(lst_ip_port)
        
        return str_full_ip
    
    except Exception as e:
        str_error_msg: str = f"An error occured during getting ip:port string. Error: {repr(e)}"
        
        streaming_logger.error(str_error_msg)
        console_logger.error(str_error_msg)
        raise





def send_data_for_processing(dct_data_to_send: dict[str, Any], int_taskid: int,
                             conn_db: psycopg2.extensions.connection, cursor: psycopg2.extensions.cursor) -> None:
    """
    Send text chunks need to embed to embedding service.

    Parameters:
    dct_data_to_send (dict): data transfer object for text generation service.
    int_taskid (int): taskid to process.
    conn_db (connection): psycopg connection class instance for connection with postgress db.
    cursor (cursor): psycopg cursor class instance for executing sql comands in postgress db.
    
    Returns:
    None
    """
    
    str_endpoint: str = os.getenv('EMBEDDING_SERVICE_EDPOINT')
    str_ip: str = get_available_worker(conn_db, cursor, int_taskid)[0]
    
    streaming_logger.info(f"Sending data with chunks to embedding to vm: {str_ip}, endpoint: {str_endpoint} ...")
    
    headers: dict[str, str] = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {os.getenv('TEXT_GENERATION_SCRIPT_TOKEN')}",
            }
    
    bln_tosend: bool = True
    
    while bln_tosend:      
        try:
            timestamp_start = time.time()
            
            str_ip_address: str = get_ip_port(str_ip)
            response = requests.post(f"http://{str_ip_address}/{str_endpoint}", json=dct_data_to_send, headers=headers)

            performance_logger.logperf(timestamp_start, 'sending data to embedding service')

            if response.status_code == 503:
                streaming_logger.info(f"Service {str_ip} busy retrying...")
            elif response.status_code == 200:
                str_info_msg: str = f"Data with chunks to embedding successfully sended to vm: {str_ip}, endpoint: {str_endpoint}"
                streaming_logger.info(str_info_msg)
                console_logger.info(str_info_msg)
                
                bln_tosend = False
                return
            elif response.status_code == 500:
                streaming_logger.info(f"Service {str_ip} crashed! Continue...")
            else:
                streaming_logger.info(f"Service {str_ip} unavailable! Status code: {response.status_code}. {response.text}")
    
        except requests.exceptions.RequestException as e:
            str_req_error_msg: str = f"Request failed: {repr(e)}"
            
            streaming_logger.error(str_req_error_msg)
            console_logger.error(str_req_error_msg)
            
        except Exception as e:
            str_error_msg: str = f"An error occurred: {repr(e)}"
            
            streaming_logger.error(str_error_msg)
            console_logger.error(str_error_msg)





def check_and_decide_continue_task(conn_db: psycopg2.extensions.connection, cursor: psycopg2.extensions.cursor,
                                   int_taskid: int, tbl_lst_task_chunks: list[tuple[int]], int_batch_size: int) -> bool:
    """
    Check whether the task should be continued.

    Parameters:
    conn_db (connection): psycopg connection class instance for connection with postgress db.
    cursor (cursor): psycopg cursor class instance for executing sql comands in postgress db.
    int_taskid (int): taskid to process.
    tbl_lst_task_chunks (list): current extracted batch.
    int_batch_size (int): value from configuration file (dev.env) wich sets the size of the batches.
    
    Returns:
    bool:
        - True if task is not finished.
        - False task was stopped or finished.
    """
    
    bln_worktodo: bool = True
    
    try:
        timestamp_start = time.time()
        
        cursor.execute("SELECT status FROM task WHERE id = %s", (int_taskid,))
        tbl_lst_task_status: list[tuple[str]] = cursor.fetchall()
        conn_db.commit()
        
        performance_logger.logperf(timestamp_start, 'check task status at the end of iteration')
        
        if tbl_lst_task_status and tbl_lst_task_status[0][0] == 'waiting':
            streaming_logger.info(f"Stop execution of task with taskid: {int_taskid}. Reason: status: 'waiting'")
            bln_worktodo = False

        if not tbl_lst_task_chunks or len(tbl_lst_task_chunks) < int_batch_size:
            streaming_logger.info(f"Stop execution of task with taskid: {int_taskid}. Reason: no chunks for processing")
            bln_worktodo = False   # for each task
        
        return bln_worktodo
    
    except Exception as e:
        str_error_msg: str = f"An error occurred during the verification and decision to proceed or deny. Error: {repr(e)}"
        
        streaming_logger.error(str_error_msg)
        console_logger.error(str_error_msg)
        raise





def get_chunks_to_send(conn_db: psycopg2.extensions.connection, cursor: psycopg2.extensions.cursor,
                      tpl_task_chunks_ids: tuple[int, ...]) -> list[dict[str, Any]]:
    """
    Get chunks need to be embedded.

    Parameters:
    conn_db (connection): psycopg connection class instance for connection with postgress db.
    cursor (cursor): psycopg cursor class instance for executing sql comands in postgress db.
    tpl_task_chunks_ids (tuple): tuple with chunkids of chunks need to embed.
    
    Returns:
    list[dict[str, Any]]: list of units with chunk info need to embed.
    
    """
    
    try:
        lst_chunks_to_embed: list[dict[str, Any]] = []
        
        timestamp_start = time.time()
        
        cursor.execute("SELECT id, txt, publid, seq FROM embed_chunk WHERE id IN %s AND txt is not null", (tpl_task_chunks_ids,))
        tbl_lst_chunks: list[tuple[int, str, int, int]] = cursor.fetchall()
        conn_db.commit()
        
        performance_logger.logperf(timestamp_start, 'get chunks txts to send to embedding service')

        for tpl_chunk in tbl_lst_chunks:
            int_chunkid, str_chunk_txt, int_publid, int_seq = tpl_chunk
            data_unit_temlpate: dict[str, Any] = {
                "chunkid": int_chunkid,
                "publid": int_publid,
                "seq": int_seq,
                "txt": str_chunk_txt
            }
            lst_chunks_to_embed.append(data_unit_temlpate)

        return lst_chunks_to_embed
    
    except Exception as e:
        str_error_msg: str = f"An error occurred during execution of get_chunks_to_send function. Error: {repr(e)}"
        
        streaming_logger.error(str_error_msg)
        console_logger.error(str_error_msg)
        raise





def prepare_data_units_for_sending(conn_db: psycopg2.extensions.connection, cursor: psycopg2.extensions.cursor,
                        tbl_lst_task_chunks: list[tuple[int]], int_taskid: int, dct_action_options: dict[str, Any]) -> dict[str, Any] | None:
    """
    Prepare dict with chunks need to embed.

    Parameters:
    conn_db (connection): psycopg connection class instance for connection with postgress db.
    cursor (cursor): psycopg cursor class instance for executing sql comands in postgress db.
    tbl_lst_task_chunks (list): list with tuples where each of them include id of the chunk need to be paraphrased.
    int_taskid (cursor): taskid to process.
    dct_action_options (tuple): dict with settings for embedding service.
    
    Returns:
    dict[str, Any] | None: data transfer object for text generation service or None if there is no any chunks to embed.
    
    """
    
    streaming_logger.info("Prepareing data units for sending to paraphrase ...")
    
    try:
        timestamp_start = time.time()
        
        dct_data_to_send: dict[str, Any] = {
            "taskid": int_taskid,
            "embedding_model": dct_action_options.get("embedding_model", None),
            "task_chunks": list()
        }
        tpl_task_chunks_ids: tuple[int, ...] = tuple([tpl_chunk[0] for tpl_chunk in tbl_lst_task_chunks])

        dct_data_to_send["task_chunks"] = get_chunks_to_send(conn_db, cursor, tpl_task_chunks_ids)
                
        performance_logger.logperf(timestamp_start, 'prepareing batch for sending')
        
        #if dct_data_to_send["task_chunks"] list is empty - that mean that all the remaining chunks have null in txt field
        if len(dct_data_to_send["task_chunks"]):
            return dct_data_to_send
        else:
            return None
    
    except Exception as e:
        str_error_msg: str = f"An error occured during prepareing data units for sending. Error: {repr(e)}"
        
        streaming_logger.error(str_error_msg)
        console_logger.error(str_error_msg)
        raise
    
    
    
    
    
def get_batch(conn_db: psycopg2.extensions.connection, cursor: psycopg2.extensions.cursor,
              int_taskid: int, int_batch_size: str) -> list[tuple[int]]:
    """
    Get batch with updating statuses of processed rows to 'progress' by int_batch_size value from configuration file (dev.env).

    Parameters:
    conn_db (connection): psycopg connection class instance for connection with postgress db.
    cursor (cursor): psycopg cursor class instance for executing sql comands in postgress db.
    int_taskid (int): taskid to process.
    int_batch_size (int): batch size value for extracting rows to process. Defined in configuration file (dev.env)
    
    Returns:
    list[tuple[int]]: list with tuples where each of them include chunkid to paraphrase.
    
    """
    
    streaming_logger.info(f"Getting batch with batch_size: {int_batch_size}...")
    
    try:
        #Use of updated table can be problematic!
        timestamp_start = time.time()

        cursor.execute(f"""
            WITH updated AS (
                UPDATE embed_task_chunk
                SET status = 'progress'
                WHERE ctid IN (
                    SELECT ctid
                    FROM embed_task_chunk
                    WHERE status = 'open'
                    AND taskid = {int_taskid}
                    LIMIT {int_batch_size}
                )
                RETURNING *
            )
            SELECT chunkid FROM updated;
            """)
        conn_db.commit()
        
        tbl_lst_task_chunks: list[tuple[int]] = cursor.fetchall()
        
        performance_logger.logperf(timestamp_start, 'taking batch from db')
        
        return tbl_lst_task_chunks
    
    except Exception as e:
        str_error_msg: str = f"An error occured during getting new batch to proceess. Error: {repr(e)}"
        
        streaming_logger.error(str_error_msg)
        console_logger.error(str_error_msg)
        raise





def change_task_status(conn_db: psycopg2.extensions.connection, cursor: psycopg2.extensions.cursor,
                       int_taskid: int, str_status_tochange: str) -> None:
    """
    Change task status.

    Parameters:
    conn_db (connection): psycopg connection class instance for connection with postgress db.
    cursor (cursor): psycopg cursor class instance for executing sql comands in postgress db.
    int_taskid (int): taskid to process.
    str_status_tochange (int): status value need to paste.
    
    Returns:
    None
    
    """
    
    streaming_logger.info(f"Change task status to {str_status_tochange}")
    
    try:
        timestamp_start = time.time()
        
        cursor.execute("UPDATE task set status = %s WHERE taskid = %s", (str_status_tochange, int_taskid))
        conn_db.commit()
        
        performance_logger.logperf(timestamp_start, f'changing task status to {str_status_tochange}')
    
    except Exception as e:
        str_error_msg: str = f"An error occured during changing task status to {str_status_tochange}. Error: {repr(e)}"
        
        streaming_logger.error(str_error_msg)
        console_logger.error(str_error_msg)
        raise
    
    
    
    
    
def process_single_batch(conn_db: psycopg2.extensions.connection, cursor: psycopg2.extensions.cursor, int_taskid: int,
                         int_batch_size: int, dct_action_options: dict[str, Any]) -> list[tuple[int]]:
    """
    Process single batch by certain taskid.

    Parameters:
    conn_db (connection): psycopg connection class instance for connection with postgress db.
    cursor (cursor): psycopg cursor class instance for executing sql comands in postgress db.
    int_taskid (int): taskid to process.
    int_batch_size (int): batch size value for extracting rows to process. Defined in configuration file (dev.env)
    dct_action_options (dict): settings for embedding service for current batch processing.
    
    Returns:
    # I return this list to understand in future should if this task is completed or not.
    list[tuple[int]]: list with tupples where each of them store the id for chunk need to embed.
    
    """
    
    streaming_logger.info("Processing of the batch ...")
    
    dct_data_to_send: dict[str, Any] | None = None
    
    try:
        tbl_lst_task_chunks: list[tuple[int]] = get_batch(conn_db, cursor, int_taskid, int_batch_size)

        if tbl_lst_task_chunks:
            dct_data_to_send = prepare_data_units_for_sending(conn_db, cursor, tbl_lst_task_chunks, int_taskid, dct_action_options)
            
            if dct_data_to_send is None:
                change_task_status(conn_db, cursor, int_taskid, str_status_tochange="fail")
                return []
        else:
            return []
            
        send_data_for_processing(dct_data_to_send, int_taskid, conn_db, cursor)
        
        streaming_logger.info("Batch successfully processed!")
        
        return tbl_lst_task_chunks
    
    except Exception as e:
        str_error_msg: str = f"An error occured during processing of single batch. Error: {repr(e)}"
        
        streaming_logger.error(str_error_msg)
        console_logger.error(str_error_msg)
        raise
    
    
    
    
    
def update_task_status(conn_db: psycopg2.extensions.connection, cursor: psycopg2.extensions.cursor, int_taskid: int) -> None:
    """
    Change task status to progress.

    Parameters:
    conn_db (connection): psycopg connection class instance for connection with postgress db.
    cursor (cursor): psycopg cursor class instance for executing sql comands in postgress db.
    int_taskid (int): taskid to process.
    
    Returns:
    None
    
    """
    
    streaming_logger.info("Updating task status to progress if needed")
    
    try:
        timestamp_start = time.time()
        
        cursor.execute(f"UPDATE task SET status = 'progress' WHERE id = {int_taskid} AND NOT status='progress'")
        conn_db.commit()
        
        performance_logger.logperf(timestamp_start, 'task status updating')
    
    except Exception as e:
        str_error_msg: str = f"An error occured during updating task status. Error: {repr(e)}"
        
        streaming_logger.error(str_error_msg)
        console_logger.error(str_error_msg)
        raise
    
    
    
    
    
def process_single_task(tpl_task: tuple[int, dict[str, Any]], cursor: psycopg2.extensions.cursor, 
                        conn_db: psycopg2.extensions.connection, int_batch_size: int) -> None:
    """
    Process single batch from single task.

    Parameters:
    tpl_task (tuple): tuple with taskid to process and dict with settings for current task.
    cursor (cursor): psycopg cursor class instance for executing sql comands in postgress db.
    conn_db (connection): psycopg connection class instance for connection with postgress db.
    int_batch_size (int): batch size value for extracting rows to process. Defined in configuration file (dev.env)
    
    Returns:
    None
    
    """
    
    bln_worktodo: bool = True
    
    try:
        
        int_taskid, dct_action_options = tpl_task
        streaming_logger.info(f'Task with id: {int_taskid} have taken in process')

        update_task_status(conn_db, cursor, int_taskid)

        while bln_worktodo:
            tbl_lst_task_chunks: list[tuple[int]] = process_single_batch(conn_db, cursor, int_taskid, int_batch_size, dct_action_options)

            bln_worktodo = check_and_decide_continue_task(conn_db, cursor, int_taskid, tbl_lst_task_chunks, int_batch_size)
        
        streaming_logger.info(f'Task with id: {int_taskid} completed!')
    
    except Exception as e:
        str_error_msg: str = f"An error occurred during the verification and decision to proceed or deny. Error: {repr(e)}"
        
        streaming_logger.error(str_error_msg)
        console_logger.error(str_error_msg)
        raise
    
    
    
    
    
def process_tasks(str_database_name: str, str_user: str, str_password: str, 
                  str_host: str, str_port: str, int_taskid: int) -> None:
        
    conn_db: psycopg2.extensions.connection = connect_to_db(str_database_name, str_user, str_password, str_host, str_port)
    
    if conn_db is None:
        streaming_logger.error("Failed to connect to the database. Exiting...")
        return

    cursor: psycopg2.extensions.cursor = conn_db.cursor()
    cursor.execute('SET search_path TO mgmt')
    conn_db.commit()

    tbl_lst_unfinshed_tasks: list[tuple[int, dict[str, Any]]] = get_unfinished_tasks(conn_db, cursor, int_taskid)
    int_batch_size: int
    bln_todo: bool = True
    
    try:
        while bln_todo:
            int_batch_size = int(os.getenv('EMBEDDING_SERVICE_BATCH'))

            if not tbl_lst_unfinshed_tasks:
                streaming_logger.info('Unfinished task list empty')
                time.sleep(10)
            else:
                for task in tbl_lst_unfinshed_tasks:                    
                    process_single_task(task, cursor, conn_db, int_batch_size)
                                        
            tbl_lst_unfinshed_tasks: list[tuple[int, dict[str, Any]]] = get_unfinished_tasks(conn_db, cursor, int_taskid)    
                    
    except Exception as e:
        str_error_msg: str = f"An error occurred: {repr(e)}"
        
        streaming_logger.error(str_error_msg)
        console_logger.error(str_error_msg)
        
        conn_db.rollback()
        
    finally:
        cursor.close()
        conn_db.close()





if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Sender for embedding option")
    parser.add_argument("-tid", "--taskid", required=True,
                        help="taskid of task you want to execute")
    args = parser.parse_args()

    process_tasks(os.getenv('DB_NAME'), os.getenv('DB_USER'), os.getenv('DB_PASSWORD'), os.getenv('DB_HOST'), os.getenv('DB_PORT'), args.taskid)