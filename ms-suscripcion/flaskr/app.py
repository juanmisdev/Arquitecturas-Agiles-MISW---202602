"""Punto de entrada de MS-Suscripcion: API + consumidor de RabbitMQ."""
from flask_restful import Api

from . import create_app
from .vistas import VistaProcesados, VistaSuscripciones, VistaReset, VistaHealth
from .consumer import ensure_app_context, start_consumer

app = ensure_app_context()

api = Api(app)
api.add_resource(VistaProcesados, '/procesados')
api.add_resource(VistaSuscripciones, '/suscripciones')
api.add_resource(VistaReset, '/reset')
api.add_resource(VistaHealth, '/health')

start_consumer()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5002)
