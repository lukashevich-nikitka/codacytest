from flask import Flask
import threading

from flask_cors import CORS
import os
import time

from docs.docs import add_swagger
from config.settings import SETTINGS_PATH

from dotenv import load_dotenv

from config.logger import configure_logger
from utils.status_sender import send_status

from utils.notify_needed import notify_needed

dir_path = os.getcwd()

load_dotenv(os.path.join(dir_path, 'local.env'))

log_flow = configure_logger('FlaskApp')


def send_status_until_receive() -> None:
    """Sends availability status to receiver script until the data isn't sent to the current service.
    This is checked by event handler notify_needed.
    """
    while True:
        log_flow.info("Start status sender script")           
        notify_needed.wait()
        log_flow.info("Sending status data...")
        send_status()
        time.sleep(10)
            
def create_app(settings_override=None) -> Flask:
    """Creates Flask application, makes basic configurations for the project and add endpoints,
    implemented ib the app
    """
    
    app = Flask(__name__, instance_relative_config=True)
    CORS(app, origins="*")

    add_swagger(app)

    app.config.from_pyfile(SETTINGS_PATH, silent=True)

    if settings_override:
        app.config.update(settings_override)

    from blueprints.embed.services.embeddings.embedding_service_v1 import EmbeddingService
    from blueprints.embed import views
    embed_instance = EmbeddingService('BAAI/bge-small-en-v1.5', device='cuda')
    embed_instance.ensure_model_downloaded()
    
    app.register_blueprint(views.embeddings)
    
    notify_needed.set()

    status_sender_thread = threading.Thread(target=send_status_until_receive)
    if not status_sender_thread.is_alive():
        status_sender_thread.start()

    return app

app = create_app()
