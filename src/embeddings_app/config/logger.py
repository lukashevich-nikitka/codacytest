import logging
import sys


def configure_logger(app_name):
    logger = logging.getLogger(app_name)
    logger.setLevel(logging.INFO)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)

    formatter = logging.Formatter(
        '%(asctime)s %(name)s: %(process)d/%(processName)s-%(thread)d/%(threadName)s [%(levelname)s] %(message)s')
    console_handler.setFormatter(formatter)

    logger.addHandler(console_handler)

    return logger
