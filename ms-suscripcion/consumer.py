"""Consumer for cotizacion.creada events.

Manual acks, prefetch=1, exponential backoff reconnect (0.5s -> 8s).
Each event is inserted into SQLite BEFORE acking so a crash mid-processing
yields an honest redelivery duplicate instead of a lost message.
"""
import json
import logging
import os
import threading
import time
from datetime import datetime, timezone

import pika

from db import audit, init_db, insert_event

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("consumer")

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
EXCHANGE = "cotizacion"
QUEUE = "cotizacion.creada"
DB_PATH = os.environ.get("DB_PATH", "/data/events.db")


def _connect():
    """Connect with exponential backoff 0.5s -> 8s until success."""
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
            logger.warning("RabbitMQ connect failed (%s). Retry %d in %.1fs",
                           exc, attempt, delay)
            time.sleep(delay)
            delay = min(delay * 2, 8.0)


class ConsumerThread(threading.Thread):
    """Background pika consumer. Survives connection loss via backoff loop."""

    def __init__(self):
        super().__init__(daemon=True, name="consumer-thread")
        self._stop_requested = threading.Event()
        self.processed = 0
        self.duplicates = 0

    def run(self):
        init_db()
        while not self._stop_requested.is_set():
            try:
                self._consume_loop()
            except Exception as exc:
                if self._stop_requested.is_set():
                    break
                logger.warning("Consumer loop error: %s. Reconnecting...", exc)
                time.sleep(0.5)

    def _consume_loop(self):
        connection = _connect()
        channel = connection.channel()
        channel.exchange_declare(exchange=EXCHANGE, exchange_type="topic",
                                 durable=True)
        channel.queue_declare(queue=QUEUE, durable=True)
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=QUEUE, on_message_callback=self._on_message,
                              auto_ack=False)
        logger.info("Consumer connected, waiting for events...")
        channel.start_consuming()

    def _on_message(self, channel, method, properties, body):
        try:
            event = json.loads(body.decode("utf-8"))
            seq = int(event["seq"])
        except Exception:
            logger.error("Malformed event body=%r; acking to drop", body)
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return
        # Insert-then-ack: if we die between insert and ack, the redelivery
        # produces an honest duplicate recorded by the seq audit.
        insert_event(seq, datetime.now(timezone.utc).isoformat())
        before = audit()
        channel.basic_ack(delivery_tag=method.delivery_tag)
        if before["duplicates"] > self.duplicates:
            self.duplicates = before["duplicates"]
        self.processed = before["processed"]
        logger.info("Processed seq=%d (processed=%d, dup=%d)",
                    seq, self.processed, self.duplicates)

    def stop(self):
        self._stop_requested.set()


consumer_thread = ConsumerThread()


def start_consumer():
    if not consumer_thread.is_alive():
        consumer_thread.start()