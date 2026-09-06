import logging
import uuid, requests

from flask_restful import Resource
from flask import request
from marshmallow import ValidationError
from datetime import datetime, timezone
from ..models import db, Cotizacion, CotizacionInputSchema, CotizacionSchema
from ..broker import publicar_evento_cotizacion_creada

logger = logging.getLogger("vistas")

SUSCRIPCION_URL = "http://ms-suscripcion:5002"
REQUEST_TIMEOUT = 3


class VistaCotizaciones(Resource):
    def post(self):
        try:
            datos = CotizacionInputSchema().load(request.get_json())
        except ValidationError as err:
            return {"errores": err.messages}, 400

        try:
            prima = CalculadoraPrima().calcular(datos["tipo_seguro"], datos["valor_asegurado"])
        except ValueError as err:
            return {"errores": str(err)}, 400

        cotizacion = Cotizacion(
            cliente_id=datos["cliente_id"],
            tipo_seguro=datos["tipo_seguro"],
            valor_asegurado=datos["valor_asegurado"],
            modo=datos["modo"],
            prima_calculada=prima,
        )
        db.session.add(cotizacion)
        db.session.commit()

        if cotizacion.modo == "async":
            self._enviar_async(cotizacion)
        else:
            self._enviar_sync(cotizacion)

        db.session.commit()
        return CotizacionSchema().dump(cotizacion), 201

    def _enviar_async(self, cotizacion):
        """Publica el evento cotizacion.creada en RabbitMQ."""
        try:
            publicar_evento_cotizacion_creada(cotizacion)
            cotizacion.estado = "enviada"
        except Exception as exc:
            cotizacion.estado = "error"
            logger.error("publicacion async falló para %s: %s", cotizacion.id, exc)

    def _enviar_sync(self, cotizacion):
        """Llama por REST a MS-Suscripcion con el payload de la cotizacion."""
        payload = {
            "cotizacion_id": cotizacion.id,
            "cliente_id": cotizacion.cliente_id,
            "tipo_seguro": cotizacion.tipo_seguro,
            "valor_asegurado": cotizacion.valor_asegurado,
            "prima_calculada": cotizacion.prima_calculada,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        try:
            respuesta = requests.post(
                f"{SUSCRIPCION_URL}/suscripciones",
                json=payload,
                timeout=REQUEST_TIMEOUT,
            )
            cotizacion.estado = "enviada" if respuesta.status_code in (200, 201) else "error"
        except Exception as exc:
            cotizacion.estado = "error"
            cotizacion.aviso = str(exc)


class CalculadoraPrima:

    TASAS_BASE = {
        "auto": 0.035,
        "hogar": 0.015,
        "vida": 0.008,
        "salud": 0.045,
    }

    RECARGO_MINIMO = 20.0

    def calcular(self, tipo_seguro: str, valor_asegurado: float) -> float:
        if valor_asegurado <= 0:
            raise ValueError("valor_asegurado debe ser mayor a 0")

        tasa = self.TASAS_BASE.get(tipo_seguro.lower())
        if tasa is None:
            raise ValueError(f"tipo_seguro '{tipo_seguro}' no reconocido")

        prima = valor_asegurado * tasa
        return round(max(prima, self.RECARGO_MINIMO), 2)


class VistaCotizacion(Resource):
     def get(self, id):
        cotizacion = Cotizacion.query.get(id)
        if not cotizacion:
            return {"error": "no encontrada"}, 404
        return CotizacionSchema().dump(cotizacion), 200


class VistaStats(Resource):
    """Contadores para el dashboard."""
    def get(self):
        por_estado = {}
        publicadas_async = 0
        for (estado, cantidad) in (
            db.session.query(Cotizacion.estado, db.func.count(Cotizacion.id))
            .group_by(Cotizacion.estado)
            .all()
        ):
            por_estado[estado or "creada"] = cantidad
        publicadas_async = por_estado.get("enviada", 0)
        return {"publicadas_async": publicadas_async, "por_estado": por_estado}, 200


class VistaHealth(Resource):
    def get(self):
        return {"status": "ok"}, 200