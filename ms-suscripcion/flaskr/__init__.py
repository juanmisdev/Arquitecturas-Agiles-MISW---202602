"""MS-Suscripcion: consume eventos cotizacion.creada y registra los procesados."""
import logging

from flask import Flask
from flask_sqlalchemy import SQLAlchemy

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("flaskr")

db = SQLAlchemy()

QUEUE = "cotizacion.creada"
DB_PATH = "/data/eventos_suscripcion.db"


def create_app(config_name='default'):
    app = Flask(__name__)
    app.config['SQLALCHEMY_DATABASE_URI'] = f'sqlite:///{DB_PATH}'
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

    from .models import models  # noqa: F401  (registra los modelos en db)

    db.init_app(app)
    with app.app_context():
        db.create_all()
    return app