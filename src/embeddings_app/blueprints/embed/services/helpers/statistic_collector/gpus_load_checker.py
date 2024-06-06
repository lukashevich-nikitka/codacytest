import time
import threading
import GPUtil

from config.logger import configure_logger
logging = configure_logger("GPU Monitoring")

class GPUMonitor:
    def __init__(self, interval=1):
        self.interval: int = interval
        self.gpu_load_array: list = []
        self.stop_thread = False
        self.thread = threading.Thread(target=self.monitor_gpu_load)
        self.thread.start()

    def monitor_gpu_load(self):
        while not self.stop_thread:
            gpu_load = self.get_gpus_load_percentage()
            if gpu_load is not None:
                self.gpu_load_array.append(gpu_load)
            time.sleep(self.interval)

    def get_gpus_load_percentage(self):
        lst_gpus: list = GPUtil.getGPUs()
        if len(lst_gpus) > 0:
            gpus_load = sum([gpu.load * 100 for gpu in lst_gpus]) / len(lst_gpus)
            return gpus_load
        else:
            self.logger.info("GPUs not found")
            return None

    def stop_and_get_average_load(self):
        self.stop_thread = True
        self.thread.join()
        if self.gpu_load_array:
            average_load = sum(self.gpu_load_array) / len(self.gpu_load_array)
            return average_load
        else:
            return 0