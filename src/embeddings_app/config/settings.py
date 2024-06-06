import os

from dotenv import load_dotenv

dir_path = os.getcwd()
load_dotenv(os.path.join(dir_path, 'local.env'))


SETTINGS_PATH = os.path.join(dir_path, 'embeddings/config.py')

SECRET_KEY = os.getenv("SECRET_KEY_FLASK")
JWT_SECRET = os.getenv('JWT_SECRET')
PASSWORD = "SECURE_PASSWORD"
USER = os.getenv("USER")

MANAGEMENT_SCRIPT_TOKEN = os.getenv('MANAGEMENT_SCRIPT_TOKEN')
MANAGING_SCRIPT_EMBED_API = os.getenv('MANAGING_SCRIPT_EMBED_API')
MANAGEMENT_SCRIPT_STATUS_API = os.getenv('MANAGEMENT_SCRIPT_STATUS_API')

MAX_JSON_WEIGHT = os.getenv('MAX_JSON_WEIGHT')

MODEL_NAME = os.getenv("MODEL_NAME")
ACTION_TYPE = os.getenv("ACTION_TYPE")