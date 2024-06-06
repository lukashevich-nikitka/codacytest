from marshmallow import Schema, fields

class SingleChunkSchema(Schema):
    chunk = fields.String(required=True)
    embedding_model = fields.String(required=False)

single_chunk_schema = SingleChunkSchema()