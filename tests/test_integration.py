"""Integration tests against a real RabbitMQ from docker compose.

Run with:  pytest -m integration
Requires:  docker compose up rabbitmq  (port 5672 exposed)
Skipped automatically if RabbitMQ is unreachable.
"""
import os
import sys
import time

import pika
import pytest

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "localhost")
QUEUE = "cotizacion.creada.test"
EXCHANGE = "cotizacion.test"

integration_marker = pytest.mark.integration


def rabbitmq_available() -> bool:
    try:
        conn = pika.BlockingConnection(
            pika.ConnectionParameters(host=RABBITMQ_HOST, blocked_connection_timeout=3,
                                      socket_timeout=3)
        )
        conn.close()
        return True
    except Exception:
        return False


pytestmark = [
    integration_marker,
    pytest.mark.skipif(
        not rabbitmq_available(),
        reason="RabbitMQ not reachable; run 'docker compose up rabbitmq' first",
    ),
]


def make_channel():
    conn = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = conn.channel()
    channel.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
    channel.queue_declare(queue=QUEUE, durable=True)
    channel.queue_bind(queue=QUEUE, exchange=EXCHANGE, routing_key="cotizacion.creada")
    # drain leftovers from previous runs
    while True:
        method, _props, _body = channel.basic_get(queue=QUEUE, auto_ack=True)
        if method is None:
            break
    return conn, channel


def publish(channel, seq):
    import json
    channel.basic_publish(
        exchange=EXCHANGE,
        routing_key="cotizacion.creada",
        body=json.dumps({"seq": seq, "timestamp": "t", "payload": {}}).encode(),
        properties=pika.BasicProperties(delivery_mode=2),
    )


class TestRoundTrip:
    def test_publish_consume_in_order(self):
        conn, channel = make_channel()
        try:
            for seq in range(1, 11):
                publish(channel, seq)
            depth = channel.queue_declare(queue=QUEUE, durable=True,
                                          passive=True).method.message_count
            assert depth == 10

            received = []
            channel.basic_qos(prefetch_count=1)
            for _ in range(10):
                method, _props, body = channel.basic_get(queue=QUEUE, auto_ack=False)
                assert method is not None
                received.append(int(__import__("json").loads(body)["seq"]))
                channel.basic_ack(method.delivery_tag)
            assert received == list(range(1, 11))

            depth = channel.queue_declare(queue=QUEUE, durable=True,
                                          passive=True).method.message_count
            assert depth == 0
        finally:
            conn.close()

    def test_unacked_message_redelivers(self):
        """Simulates consumer crash mid-ack: no ack -> message stays queued."""
        conn, channel = make_channel()
        try:
            publish(channel, 1)
            method, _props, body = channel.basic_get(queue=QUEUE, auto_ack=False)
            assert method is not None
            # no ack — connection closes, message returns to queue
        finally:
            conn.close()

        conn2, channel2 = make_channel()
        try:
            method, _props, body = channel2.basic_get(queue=QUEUE, auto_ack=False)
            assert method is not None  # redelivered
            assert method.redelivered is True
            channel2.basic_ack(method.delivery_tag)
        finally:
            conn2.close()

    def test_passive_depth_matches_published(self):
        conn, channel = make_channel()
        try:
            for seq in range(1, 26):
                publish(channel, seq)
            time.sleep(0.2)
            depth = channel.queue_declare(queue=QUEUE, durable=True,
                                          passive=True).method.message_count
            assert depth == 25
        finally:
            conn.close()