from flask import jsonify, request
from flask_restful import Resource

from flaskr import db
from flaskr.consumer import consumer_thread
from flaskr.models.models import EventoProcesado


class VistaProcesados(Resource):
    """Eventos procesados: total, duplicados y últimos 200 evento_ids."""
    def get(self):
        procesados = db.session.query(EventoProcesado).count()
        duplicados = consumer_thread.eventos_duplicados
        ids = [
            row[0]
            for row in db.session.query(EventoProcesado.evento_id)
            .order_by(EventoProcesado.procesado_en.desc())
            .limit(200)
            .all()
        ]
        return jsonify({
            "procesados": procesados,
            "duplicados": duplicados,
            "evento_ids": ids,
        })


class VistaSuscripciones(Resource):
    """Recepción sync de cotizaciones (llamada REST desde MS-Cotizacion).

    Idempotencia por evento_id si viene, o por cotizacion_id como fallback.
    """
    def post(self):
        datos = request.get_json(silent=True) or {}
        evento_id = datos.get("evento_id") or datos.get("cotizacion_id")
        if not evento_id:
            return {"error": "falta evento_id/cotizacion_id"}, 400
        try:
            db.session.add(EventoProcesado(evento_id=str(evento_id),
                                           cotizacion_id=datos.get("cotizacion_id")))
            db.session.commit()
        except Exception:
            db.session.rollback()
            consumer_thread.eventos_duplicados += 1
            return {"status": "duplicado"}, 200
        return {"status": "procesado"}, 201


class VistaHealth(Resource):
    def get(self):
        return {"status": "ok"}, 200