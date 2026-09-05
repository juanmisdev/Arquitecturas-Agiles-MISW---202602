"""Publisher for cotizacion.creada events on RabbitMQ.

Publishes JSON events {seq, timestamp, payload} on a durable queue through
a topic exchange, with persistent delivery and reconnect backoff.
"""
import json
import logging
import os
import time
from datetime import datetime, timezone

import pika

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("publisher")

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
EXCHANGE = "cotizacion"
QUEUE = "cotizacion.creada"


def _connect(max_retries=None):
    """Connect to RabbitMQ with exponential backoff (0.5s -> 8s)."""
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
        except Exception as exc:  # pika.exceptions.AMQPConnectionError and friends
            attempt += 1
            if max_retries is not None and attempt > max_retries:
                raise
            logger.warning("RabbitMQ connect failed (%s). Retry %d in %.1fs",
                           exc, attempt, delay)
            time.sleep(delay)
            delay = min(delay * 2, 8.0)


class Publisher:
    """Publishes sequential cotizacion.creada events."""

    def __init__(self):
        self._seq = 0
        self._connection = None
        self._channel = None

    def _ensure_channel(self):
        if self._connection is not None and self._connection.is_open:
            return
        self._connection = _connect()
        self._channel = self._connection.channel()
        self._channel.exchange_declare(
            exchange=EXCHANGE, exchange_type="topic", durable=True
        )
        self._channel.queue_declare(queue=QUEUE, durable=True)
        self._channel.queue_bind(
            queue=QUEUE, exchange=EXCHANGE, routing_key="cotizacion.creada"
        )

    @property
    def seq(self):
        """Last published sequence number (0 if none published yet)."""
        return self._seq

    def publish(self, payload=None):
        """Publish one event with the next monotonic seq. Returns the event."""
        self._ensure_channel()
        self._seq += 1
        event = {
            "seq": self._seq,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "payload": payload if payload is not None else {"tipo": "cotizacion.creada"},
        }
        self._channel.basic_publish(
            exchange=EXCHANGE,
            routing_key="cotizacion.creada",
            body=json.dumps(event).encode("utf-8"),
            properties=pika.BasicProperties(
                delivery_mode=2,  # persistent
                content_type="application/json",
            ),
        )
        return event

    def close(self):
        if self._connection is not None and self._connection.is_open:
            try:
                self._connection.close()
            except Exception:
                pass
        self._connection = None
        self._channel = None