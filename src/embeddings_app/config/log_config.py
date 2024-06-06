import os
import logging
import datetime
import time
from retry import retry
import logging
import time

class PerformanceLogger:
    def __init__(self, dir_path):
        logging.basicConfig(level=logging.ERROR)
        self.logger = logging.getLogger(__name__)
        self.dir_path = dir_path
        os.makedirs(self.dir_path, exist_ok=True)
        self.FILENAME_PERF = os.path.join('logs', self.dir_path, f'perflog{time.time()}.txt')

    @retry(tries=2)
    def logperf(self, start_time, name):
        file_path = self.FILENAME_PERF
        try:
            cur_time = time.time() 
            dt_object = datetime.datetime.fromtimestamp(cur_time)
            formatted_time = dt_object.strftime("%Y-%m-%d %H:%M:%S")
            delay =  cur_time - start_time
            with open(file_path, "a") as file:
                file.write(f"{formatted_time},{name},{delay}\n")
        except Exception as err:
            self.logger.error(f"Couldn't write logperf into {file_path} because of error: {repr(err)}")
            time.sleep(0.1)