from flask import Blueprint, jsonify, request
from marshmallow import ValidationError
import logging
import threading

from auth.auth import auth
from .validators.embed_api_validator import dto_schema
from .validators.single_embed_val_schema import single_chunk_schema
from .middlewares.server_busy_middleware import gpu_busy_middleware
from .middlewares.payload_check_middleware import check_content_length
from .common.prepare_embed_model import prepare_embedding_model_to_use
from utils.notify_needed import notify_needed


embeddings = Blueprint("embeddings", __name__, url_prefix="/embeddings")



@embeddings.route("/batch", methods=["POST"])
@gpu_busy_middleware
@auth.login_required
def run_batch_embeddings():
    """
    Implement embeddings from chunks.

    This 'embeddings/batch' API provide embeddings from text chunks based on embedding model.

    ---
    tags:
      - name: Embeddings
    consumes:
      - application/json
    produces:
      - application/json
    security:
      - bearerAuth: []
    parameters:
      - in: body
        name: Input data
        description: Input data to process.
        schema:
          $ref: '#/definitions/DynamicKey'
    responses:
      200:
        description: Embeddings successfully processing.
        examples:
          application/json:
            batch_status: success
      400:
        description: Bad Request. Validation error, incorrect input data
      401:
        description: Unauthorized access.
      413:
        description: Payload too large. Maximum content length is set in env variable
      500:
        description: Internal Server Error. An unexpected issue occurred during embeddings process.
      503:
        description: Service unavailable. The embeddings batch API is currently busy. Please try again later.
    """
    try:
      notify_needed.clear()
        
      #dto_data = dto_schema.load(request.json)
      from .tasks import triger_embedding_task

      embeddings_thread = threading.Thread(
        target=triger_embedding_task, 
        args=(request.json["task_chunks"], request.json["embedding_model"], request.json["taskid"])
      )
      embeddings_thread.start()

      return jsonify({"batch_status": "accepted"}), 200

    except ValidationError as e:
        return jsonify({"error": "DTO validation failed", "messages": e.messages}), 400

    except Exception as e:
        logging.info(f"An unexpected error occurred: {repr(e)}")
        return jsonify({"error": "An unexpected server error occurred"}), 500



@embeddings.route("/single", methods=["POST"])
@auth.login_required 
def run_single_embedding():
    """
    Implement embeddings from chunks.

    This 'embeddings/single' API provide embedding from single text string based on embedding model from hugging face.

    ---
    tags:
      - name: Embeddings
    consumes:
      - application/json
    produces:
      - application/json
    security:
      - bearerAuth: []
    parameters:
      - in: body
        name: Input data
        description: Input data to process.
        schema:
          $ref: '#/definitions/SingleChunk'
    responses:
      200:
        description: Embeddings task added to queue successfully.
        examples:
          application/json:
            vector: list[float]
      400:
        description: Bad Request. Validation error, incorrect input data
      401:
        description: Unauthorized access.
      413:
        description: Payload too large. Maximum content length is set in env variable
      500:
        description: Internal Server Error. An unexpected issue occurred during embeddings process.
    """
    try:
      dto_data = single_chunk_schema.load(request.json)
      
      str_device: str = 'cpu'
      emnembedding_service = prepare_embedding_model_to_use(dto_data["embedding_model"], str_device)
      lst_embeddings: list[list[float]] = emnembedding_service.generate_embeddings([dto_data["chunk"]])
      
      return jsonify({"vector": lst_embeddings[0]}), 200

    except ValidationError as e:
        return jsonify({"error": "DTO validation failed", "messages": e.messages}), 400

    except Exception as e:
        logging.info(f"An unexpected error occurred: {repr(e)}")
        return jsonify({"error": "An unexpected server error occurred"}), 500