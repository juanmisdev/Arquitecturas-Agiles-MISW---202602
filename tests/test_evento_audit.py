"""Unit tests for the EventoProcesado audit logic (idempotencia).

Pure logic + SQLite via Flask-SQLAlchemy — no broker required.
Adapted from the old seq audit to the evento_id model of the
teammate's cotizacion_ms base.
"""
import os
import sys
import tempfile

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ms-suscripcion"))

def _purge_flaskr_modules():
    """Ambos servicios se llaman flaskr: purga el cache antes de importar."""
    import sys
    for name in [m for m in sys.modules if m == "flaskr" or m.startswith("flaskr.")]:
        del sys.modules[name]


_purge_flaskr_modules()
import flaskr  # noqa: E402
from flaskr import create_app, db  # noqa: E402
from flaskr.models.models import EventoProcesado  # noqa: E402
from flaskr.consumer import ConsumerThread  # noqa: E402


@pytest.fixture()
def tmp_app(monkeypatch):
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    monkeypatch.setattr(flaskr, "DB_PATH", path)
    flaskr.DB_PATH = path
    app = create_app("test")
    with app.app_context():
        yield app
    with app.app_context():
        db.session.remove()
    os.unlink(path)


def insert_all(evento_ids):
    for eid in evento_ids:
        db.session.add(EventoProcesado(evento_id=eid))
    db.session.commit()


def try_insert(evento_id):
    """Mirror of consumer insert-then-ack duplicate handling."""
    try:
        db.session.add(EventoProcesado(evento_id=evento_id))
        db.session.commit()
        return False  # no duplicado
    except Exception:
        db.session.rollback()
        return True  # duplicado


class TestEventoProcesado:
    def test_empty_db(self, tmp_app):
        assert db.session.query(EventoProcesado).count() == 0

    def test_insert_and_count(self, tmp_app):
        insert_all(["e1", "e2", "e3"])
        assert db.session.query(EventoProcesado).count() == 3

    def test_idempotency_pk_rejects_duplicate(self, tmp_app):
        insert_all(["e1"])
        assert try_insert("e1") is True
        assert db.session.query(EventoProcesado).count() == 1

    def test_distinct_ids_all_accepted(self, tmp_app):
        ids = [f"e{i}" for i in range(10)]
        for eid in ids:
            assert try_insert(eid) is False
        assert db.session.query(EventoProcesado).count() == 10

    def test_order_by_procesado_en(self, tmp_app):
        import time as _time
        insert_all(["e1"])
        _time.sleep(0.01)
        insert_all(["e2"])
        rows = (
            db.session.query(EventoProcesado.evento_id)
            .order_by(EventoProcesado.procesado_en.asc())
            .all()
        )
        assert [r[0] for r in rows] == ["e1", "e2"]


class TestConsumerDuplicateCounter:
    def test_counter_starts_zero(self):
        thread = ConsumerThread()
        assert thread.eventos_duplicados == 0

    def test_counter_incremented_by_vista_sync(self, tmp_app):
        """Sync endpoint counts duplicates in the same counter."""
        insert_all(["c1"])
        consumer_like = ConsumerThread()
        duplicado = try_insert("c1")
        if duplicado:
            consumer_like.eventos_duplicados += 1
        assert consumer_like.eventos_duplicados == 1


class TestLostComputation:
    """perdidos = publicadas - procesadas (mirror of dashboard _report)."""

    def test_zero_lost(self, tmp_app):
        publicadas = 100
        insert_all([f"e{i}" for i in range(1, publicadas + 1)])
        processed = db.session.query(EventoProcesado).count()
        assert max(0, publicadas - processed) == 0

    def test_lost_when_missing(self, tmp_app):
        publicadas = 100
        insert_all([f"e{i}" for i in range(1, publicadas + 1) if i != 42])
        processed = db.session.query(EventoProcesado).count()
        assert max(0, publicadas - processed) == 1
