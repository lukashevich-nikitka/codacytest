from apispec.ext.marshmallow import MarshmallowPlugin
from apispec_webframeworks.flask import FlaskPlugin
from flasgger import APISpec, Swagger

from blueprints.embed.validators.embed_api_validator import DynamicKeySchema
from blueprints.embed.validators.single_embed_val_schema import SingleChunkSchema


def add_swagger(app):
    configs = {
        "headers": [
        ],
        "specs": [
            {
                "endpoint": 'apispec',
                "route": '/apispec.json',
                "rule_filter": lambda rule: True,
                "model_filter": lambda tag: True,
            }
        ],
        "static_url_path": "/flasgger_static",
        "swagger_ui": True,
        "specs_route": "/apidocs/",
        "title": "API",
    }

    spec = APISpec(
        title='Embeddings API',
        version='1.0.0',
        openapi_version='2.0',
        plugins=[
            FlaskPlugin(),
            MarshmallowPlugin(),
        ],
    )

    api_key_scheme = {"type": "apiKey", "in": "header", "name": "Authorization"}
    spec.components.security_scheme("bearerAuth", api_key_scheme)

    template = spec.to_flasgger(
        app,
        definitions=[DynamicKeySchema, SingleChunkSchema],
        paths=[app.view_functions[rule.endpoint] for rule in app.url_map.iter_rules()]
    )

    swag = Swagger(app, config=configs, template=template)
