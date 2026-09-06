"""Modelos de MS-Suscripcion: registro de eventos procesados (idempotencia)."""
from datetime import datetime, timezone

from .. import db


class EventoProcesado(db.Model):
    """Un evento cotizacion.creada ya procesado (evento_id = PK garantiza
    idempotencia: el INSERT de un duplicado falla y se cuenta aparte)."""
    evento_id = db.Column(db.String, primary_key=True)
    cotizacion_id = db.Column(db.String, nullable=True)
    procesado_en = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))