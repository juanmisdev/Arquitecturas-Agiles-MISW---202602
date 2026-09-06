"""Consumidor de la cola cotizacion.creada.

Acks manuales, prefetch=1, reconexión con backoff (0.5s -> 8s).
Idempotencia: cada evento se registra en la tabla EventoProcesado
(evento_id PRIMARY KEY, insert-then-ack). Un duplicado falla el INSERT
y se cuenta en un contador aparte (eventos_duplicados).
"""
import json
import logging
import os
import threading
import time
from datetime import datetime, timezone

import pika

from . import QUEUE, db, create_app
from .models.models import EventoProcesado

logger = logging.getLogger("consumer")

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
# Pausa tras procesar cada evento (segundos): hace visible el drenaje en el
# dashboard. Env CONSUMER_DELAY, defecto 0.0 (procesa a máxima velocidad).
CONSUMER_DELAY = float(os.environ.get("CONSUMER_DELAY", "0"))

# Contexto de app para usar db.session fuera del request cycle.
# La app y el context se crean de forma diferida (lazy): el contenedor llama
# ensure_app_context() desde flaskr.app; los tests crean la suya.
_app = None


def ensure_app_context():
    """Crea (una vez) la app y empuja su contexto para el hilo consumidor."""
    global _app
    if _app is None:
        _app = create_app("default")
        _app.app_context().push()
    return _app


def _connect():
    """Conexión con backoff exponencial 0.5s -> 8s hasta éxito."""
    delay = 0.5
    attempt = 0
    while True:
        try:
            return pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=RABBITMQ_HOST,
                    heartbeat=60,
                    blocked_connection_timeout=60,
                )
            )
        except Exception as exc:
            attempt += 1
            logger.warning("RabbitMQ connect failed (%s). Reintento %d en %.1fs",
                           exc, attempt, delay)
            time.sleep(delay)
            delay = min(delay * 2, 8.0)


class ConsumerThread(threading.Thread):
    """Consumidor pika en hilo de fondo, sobrevive caídas del broker."""

    def __init__(self):
        super().__init__(daemon=True, name="consumer-thread")
        self._stop_requested = threading.Event()
        self.eventos_duplicados = 0

    def run(self):
        # flask-sqlalchemy liga db.session al app context DEL HILO: el
        # contexto del hilo principal no es visible aquí.
        app = ensure_app_context()
        with app.app_context():
            while not self._stop_requested.is_set():
                try:
                    self._consume_loop()
                except Exception as exc:
                    if self._stop_requested.is_set():
                        break
                    logger.warning("Consumer loop error: %s. Reconectando...", exc)
                    time.sleep(0.5)

    def _consume_loop(self):
        connection = _connect()
        channel = connection.channel()
        channel.queue_declare(queue=QUEUE, durable=True)
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=QUEUE, on_message_callback=self._on_message,
                              auto_ack=False)
        logger.info("Consumer conectado, esperando eventos...")
        channel.start_consuming()

    def _on_message(self, channel, method, properties, body):
        try:
            evento = json.loads(body.decode("utf-8"))
            evento_id = str(evento["evento_id"])
        except Exception:
            logger.error("Evento malformado body=%r; ack para descartar", body)
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return

        # Insert-then-ack: si el proceso muere entre INSERT y ack, la
        # redelivery produce un duplicado honesto contado aparte.
        duplicado = False
        try:
            db.session.add(EventoProcesado(
                evento_id=evento_id,
                cotizacion_id=evento.get("cotizacion_id"),
            ))
            db.session.commit()
        except Exception:
            db.session.rollback()
            duplicado = True
            self.eventos_duplicados += 1
            logger.info("Evento duplicado ignorado: %s", evento_id)

        channel.basic_ack(delivery_tag=method.delivery_tag)
        logger.info("Evento %s procesado (duplicados=%d)",
                    evento_id, self.eventos_duplicados)
        if CONSUMER_DELAY > 0:
            time.sleep(CONSUMER_DELAY)

    def stop(self):
        self._stop_requested.set()


consumer_thread = ConsumerThread()


def start_consumer():
    if not consumer_thread.is_alive():
        consumer_thread.start()