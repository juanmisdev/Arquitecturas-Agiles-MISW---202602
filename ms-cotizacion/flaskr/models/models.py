import uuid
from flask_sqlalchemy import SQLAlchemy
from marshmallow_sqlalchemy import SQLAlchemyAutoSchema
from datetime import datetime, timezone
from marshmallow import Schema, fields
from marshmallow_sqlalchemy import SQLAlchemyAutoSchema

db = SQLAlchemy()

class Cotizacion(db.Model): 
    id = db.Column(db.String, primary_key=True, default=lambda: str(uuid.uuid4()))  # uuid
    cliente_id = db.Column(db.String(128), nullable=False)
    tipo_seguro = db.Column(db.String(128), nullable=False)
    valor_asegurado = db.Column(db.Float, nullable=False)
    prima_calculada = db.Column(db.Float)
    modo = db.Column(db.String(16)) # "sync" | "async"
    estado = db.Column(db.String(16), default="creada")  # "creada" | "enviada" | "confirmada" | "error"
    timestamp_creacion = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))

class CotizacionInputSchema(Schema):
    cliente_id = fields.Str(required=True)
    tipo_seguro = fields.Str(required=True)
    valor_asegurado = fields.Float(required=True)
    modo = fields.Str(required=True)

class EventoCotizacionCreadaSchema(Schema):
    evento_id = fields.Str(required=True)
    cotizacion_id = fields.Str(required=True)
    cliente_id = fields.Str(required=True)
    tipo_seguro = fields.Str(required=True)
    valor_asegurado = fields.Float(required=True)
    timestamp = fields.DateTime(required=True)

class EventoProcesado(db.Model):
    evento_id = db.Column(db.String, primary_key=True)
    procesado_en = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))

class CotizacionSchema(SQLAlchemyAutoSchema):
    class Meta:
        model = Cotizacion
        load_instance = True