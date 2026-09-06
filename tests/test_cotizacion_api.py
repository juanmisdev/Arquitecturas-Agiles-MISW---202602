"""Unit tests para la API de ms-cotizacion (sin broker real)."""
import os
import sys
import tempfile
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ms-cotizacion"))

def _purge_flaskr_modules():
    """Ambos servicios se llaman flaskr: purga el cache antes de importar."""
    import sys
    for name in [m for m in sys.modules if m == "flaskr" or m.startswith("flaskr.")]:
        del sys.modules[name]


_purge_flaskr_modules()
import flaskr  # noqa: E402
import flaskr.app as cotizacion_entry  # noqa: E402


@pytest.fixture()
def client(monkeypatch, tmp_path):
    monkeypatch.setenv("RABBITMQ_HOST", "localhost")
    # instance path a tmp para sqlite aislada
    monkeypatch.setattr(cotizacion_entry.app, "instance_path", str(tmp_path), raising=False)
    # la app ya fue creada al importar; reset del db file no es posible —
    # usamos la sqlite por defecto del repo (tests no destructivos)
    return cotizacion_entry.app.test_client()


def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.get_json() == {"status": "ok"}


def test_crear_cotizacion_sync_devuelve_201_con_estado(client):
    r = client.post("/cotizaciones", json={
        "cliente_id": "test-cliente",
        "tipo_seguro": "auto",
        "valor_asegurado": 1000.0,
        "modo": "sync",
    })
    assert r.status_code == 201
    body = r.get_json()
    assert body["cliente_id"] == "test-cliente"
    assert body["prima_calculada"] == 35.0
    assert body["modo"] == "sync"
    assert body["estado"] in ("enviada", "error")  # sin ms-suscripcion: error


def test_crear_cotizacion_validacion(client):
    r = client.post("/cotizaciones", json={"cliente_id": "x"})
    assert r.status_code == 400


def test_tipo_seguro_desconocido_400(client):
    r = client.post("/cotizaciones", json={
        "cliente_id": "x", "tipo_seguro": "yate",
        "valor_asegurado": 100.0, "modo": "sync",
    })
    assert r.status_code == 400


def test_cotizacion_no_encontrada_404(client):
    r = client.get("/cotizaciones/no-existe")
    assert r.status_code == 404


def test_stats_shape(client):
    r = client.get("/stats")
    assert r.status_code == 200
    body = r.get_json()
    assert "publicadas_async" in body
    assert "por_estado" in body


def test_calculadora_prima_rates():
    """CalculadoraPrima del MS base: tasas y mínimo intactos."""
    import importlib
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ms-cotizacion"))
    for name in [m for m in sys.modules if m == "flaskr" or m.startswith("flaskr.")]:
        del sys.modules[name]
    from flaskr.vistas.vistas import CalculadoraPrima

    calc = CalculadoraPrima()
    assert calc.calcular("auto", 1000.0) == 35.0     # 0.035
    assert calc.calcular("hogar", 1000.0) == 20.0    # 15 -> min 20
    assert calc.calcular("vida", 5000.0) == 40.0     # 0.008
    assert calc.calcular("salud", 1000.0) == 45.0    # 0.045
    with pytest.raises(ValueError):
        calc.calcular("desconocido", 100.0)
    with pytest.raises(ValueError):
        calc.calcular("auto", 0)
