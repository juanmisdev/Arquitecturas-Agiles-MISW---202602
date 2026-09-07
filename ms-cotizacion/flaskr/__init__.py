"""MS-Cotizacion: API Flask-RESTful sobre el MS base de @santigore.

Endpoints:
- POST /cotizaciones    -> crea + calcula prima; modo async publica el evento
                           cotizacion.creada en RabbitMQ, modo sync llama a
                           MS-Suscripcion por REST
- GET  /cotizaciones/<id>
- GET  /stats           -> contadores para el dashboard
- GET  /health
"""
import logging
import os
from datetime import datetime, timezone

import requests
from flask import Flask
from flask_restful import Api

from .models import db
from .vistas import (
    VistaCotizaciones,
    VistaCotizacion,
    VistaHealth,
    VistaReset,
    VistaStats,
)

logging.basicConfig(level=logging.INFO)


def create_app(config_name='default'):
    app = Flask(__name__)
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///cotizaciones.db'
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

    db.init_app(app)
    with app.app_context():
        db.create_all()

    api = Api(app)
    api.add_resource(VistaCotizaciones, '/cotizaciones')
    api.add_resource(VistaCotizacion, '/cotizaciones/<id>')
    api.add_resource(VistaStats, '/stats')
    api.add_resource(VistaReset, '/reset')
    api.add_resource(VistaHealth, '/health')
    return app


app = create_app('default')


@app.get('/health')
def health():
    return {'status': 'ok'}, 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)