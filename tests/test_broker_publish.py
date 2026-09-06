"""Unit tests del publicador de eventos (broker.py, sin broker real)."""
import os
import sys
import types
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ms-cotizacion"))

def _purge_flaskr_modules():
    """Ambos servicios se llaman flaskr: purga el cache antes de importar."""
    import sys
    for name in [m for m in sys.modules if m == "flaskr" or m.startswith("flaskr.")]:
        del sys.modules[name]


_purge_flaskr_modules()
import flaskr.broker as broker  # noqa: E402


class _FakeCotizacion:
    id = "cot-123"
    cliente_id = "cli-1"
    tipo_seguro = "auto"
    valor_asegurado = 2500.0


def test_evento_shape_y_durable():
    connection = MagicMock()
    channel = connection.channel.return_value
    with patch.object(broker, "_connect", return_value=connection):
        evento = broker.publicar_evento_cotizacion_creada(_FakeCotizacion())
    assert set(evento.keys()) == {
        "evento_id", "cotizacion_id", "cliente_id",
        "tipo_seguro", "valor_asegurado", "timestamp",
    }
    assert evento["cotizacion_id"] == "cot-123"
    kwargs = channel.queue_declare.call_args.kwargs
    assert kwargs["queue"] == "cotizacion.creada"
    assert kwargs["durable"] is True
    props = channel.basic_publish.call_args.kwargs["properties"]
    assert props.delivery_mode == 2
    connection.close.assert_called_once()


def test_connect_retries_then_raises(monkeypatch):
    sleeps = []
    monkeypatch.setattr("time.sleep", lambda s: sleeps.append(s))
    with patch.object(
        broker.pika, "BlockingConnection",
        side_effect=Exception("boom"),
    ):
        try:
            broker._connect(max_retries=2, base_delay=0.1)
            raised = False
        except ConnectionError:
            raised = True
    assert raised is True
    assert len(sleeps) >= 2
