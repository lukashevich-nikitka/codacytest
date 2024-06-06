from marshmallow import Schema, fields

class ChunkSchema(Schema):
    chunkid = fields.Int(required=True)
    txt = fields.String(required=True)
    publid = fields.Int(required=False)
    seq = fields.String(required=False)

class DynamicKeySchema(Schema):
    taskid = fields.Int(required = True)
    embedding_model = fields.String(required = False)
    task_chunks = fields.List(fields.Nested(ChunkSchema))

dto_schema = DynamicKeySchema()