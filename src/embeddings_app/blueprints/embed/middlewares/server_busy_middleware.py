from flask import abort
from functools import wraps

from utils.notify_needed import notify_needed


def gpu_busy_middleware(f):
    @wraps(f)
    def check_gpu_availability(*args, **kwargs):
        if not notify_needed.is_set():
            abort(503, description="The embedding service is currently busy. Please try again later.")
        return f(*args, **kwargs)
    return check_gpu_availability
