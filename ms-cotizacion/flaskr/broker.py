"""Publicación de eventos cotizacion.creada en RabbitMQ (pika).

Publica un JSON con delivery_mode=2 (persistente) en la cola durable
"cotizacion.creada". Se usa una conexión corta por petición (suficiente para
el experimento) con reintentos de conexión para el orden de arranque de
docker compose.
"""
import json
import logging
import os
import time
from datetime import datetime, timezone
from uuid import uuid4

import pika

logger = logging.getLogger("broker")

QUEUE = "cotizacion.creada"


def _host():
    return os.environ.get("RABBITMQ_HOST", "localhost")


def _connect(max_retries=8, base_delay=0.5):
    """Conexión con reintentos (0.5s -> 4s) para esperar a RabbitMQ en compose."""
    delay = base_delay
    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            return pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=_host(),
                    heartbeat=60,
                    blocked_connection_timeout=60,
                )
            )
        except Exception as exc:  # pika.exceptions.AMQPConnectionError y amigos
            last_exc = exc
            logger.warning(
                "RabbitMQ connect failed (%s). Reintento %d/%d en %.1fs",
                exc, attempt, max_retries, delay,
            )
            time.sleep(delay)
            delay = min(delay * 2, 4.0)
    raise ConnectionError(f"no se pudo conectar a RabbitMQ {_host()}: {last_exc}")


def publicar_evento_cotizacion_creada(cotizacion):
    """Publica el evento de cotización creada y lo devuelve como dict.

    Declaración durable de la cola + publicación persistente.
    """
    evento = {
        "evento_id": str(uuid4()),
        "cotizacion_id": cotizacion.id,
        "cliente_id": cotizacion.cliente_id,
        "tipo_seguro": cotizacion.tipo_seguro,
        "valor_asegurado": cotizacion.valor_asegurado,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    connection = _connect()
    try:
        channel = connection.channel()
        channel.queue_declare(queue=QUEUE, durable=True)
        channel.basic_publish(
            exchange="",
            routing_key=QUEUE,
            body=json.dumps(evento).encode("utf-8"),
            properties=pika.BasicProperties(
                delivery_mode=2,  # persistente
                content_type="application/json",
            ),
        )
    finally:
        try:
            connection.close()
        except Exception:
            pass
    return evento