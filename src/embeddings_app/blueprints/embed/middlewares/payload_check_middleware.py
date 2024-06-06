from flask import request, abort

from config.settings import MAX_JSON_WEIGHT

def check_content_length():
    max_size = 1024 * 1024 * int(MAX_JSON_WEIGHT)
    content_length = request.content_length
    if content_length is not None and content_length > max_size:
        abort(413, description="Payload too large.")
