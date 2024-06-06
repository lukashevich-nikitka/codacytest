import time
from functools import wraps

from config.log_config import PerformanceLogger
embedding_service = PerformanceLogger('embedding_service')


def timeit(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        embedding_service.logperf(start_time, func.__name__)
        return result

    return wrapper