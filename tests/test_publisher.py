"""Unit tests for the publisher seq logic (no broker needed)."""
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ms-cotizacion"))

import publisher as publisher_module  # noqa: E402
from publisher import Publisher  # noqa: E402


def _fake_channel():
    channel = MagicMock()
    channel.exchange_declare.return_value = None
    channel.queue_declare.return_value = None
    channel.queue_bind.return_value = None
    return channel


class TestPublisherSeq(unittest.TestCase):
    def _publisher_with_mock(self):
        p = Publisher()
        connection = MagicMock()
        connection.is_open = True
        channel = _fake_channel()
        connection.channel.return_value = channel
        with patch.object(publisher_module, "_connect", return_value=connection):
            # first publish triggers _ensure_channel: connect + declares
            p.publish()
        return p, channel

    def test_seq_increments(self):
        p, _ = self._publisher_with_mock()  # first publish -> seq 1
        e2 = p.publish()
        e3 = p.publish()
        e4 = p.publish()
        assert [e2["seq"], e3["seq"], e4["seq"]] == [2, 3, 4]
        assert p.seq == 4

    def test_event_shape(self):
        p, _ = self._publisher_with_mock()
        event = p.publish()
        assert set(event.keys()) == {"seq", "timestamp", "payload"}
        assert event["payload"] == {"tipo": "cotizacion.creada"}

    def test_delivery_mode_persistent(self):
        p, channel = self._publisher_with_mock()
        p.publish()
        props = channel.basic_publish.call_args.kwargs["properties"]
        assert props.delivery_mode == 2

    def test_declares_durable_queue_and_exchange(self):
        p, channel = self._publisher_with_mock()
        p.publish()
        kwargs = channel.queue_declare.call_args.kwargs
        assert kwargs["queue"] == "cotizacion.creada"
        assert kwargs["durable"] is True
        ex_kwargs = channel.exchange_declare.call_args.kwargs
        assert ex_kwargs["exchange"] == "cotizacion"
        assert ex_kwargs["durable"] is True


if __name__ == "__main__":
    unittest.main()