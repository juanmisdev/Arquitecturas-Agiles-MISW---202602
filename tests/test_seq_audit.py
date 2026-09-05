"""Unit tests for the seq audit logic (lost/order/duplicates).

Pure logic + SQLite — no broker required.
"""
import os
import sys
import tempfile

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ms-suscripcion"))

import db  # noqa: E402


@pytest.fixture()
def tmp_db(monkeypatch):
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    monkeypatch.setattr(db, "DB_PATH", path)
    db.DB_PATH = path
    db.init_db()
    yield path
    os.unlink(path)


def insert_all(seqs):
    for s in seqs:
        db.insert_event(s, "2026-09-05T00:00:00+00:00")


class TestAudit:
    def test_empty_db(self, tmp_db):
        a = db.audit()
        assert a["processed"] == 0
        assert a["seqs"] == []
        assert a["duplicates"] == 0
        assert a["in_order"] is True  # vacuously in order

    def test_all_in_order(self, tmp_db):
        insert_all([1, 2, 3, 4, 5])
        a = db.audit()
        assert a["processed"] == 5
        assert a["duplicates"] == 0
        assert a["in_order"] is True
        assert a["min_seq"] == 1
        assert a["max_seq"] == 5

    def test_duplicates_detected(self, tmp_db):
        insert_all([1, 2, 2, 3])
        a = db.audit()
        assert a["processed"] == 4
        assert a["duplicates"] == 1
        assert a["duplicate_seqs"] == [2]

    def test_out_of_order(self, tmp_db):
        insert_all([1, 3, 2, 4])
        a = db.audit()
        assert a["in_order"] is False

    def test_duplicate_then_order_still_in_order(self, tmp_db):
        # redelivery: 1,2 then crash mid-ack on 2, redelivery 2,3 -> 1,2,2,3
        insert_all([1, 2, 2, 3])
        a = db.audit()
        assert a["in_order"] is True  # first occurrences are 1,2,3
        assert a["duplicates"] == 1


class TestLostComputation:
    """lost = published - unique_processed (mirror of dashboard logic)."""

    def test_zero_lost(self, tmp_db):
        published = 100
        insert_all(list(range(1, published + 1)))
        a = db.audit()
        lost = max(0, published - (a["processed"] - a["duplicates"]))
        assert lost == 0

    def test_lost_when_missing(self, tmp_db):
        published = 100
        insert_all([s for s in range(1, published + 1) if s != 42])
        a = db.audit()
        lost = max(0, published - (a["processed"] - a["duplicates"]))
        assert lost == 1

    def test_lost_with_duplicates(self, tmp_db):
        published = 3
        insert_all([1, 2, 2, 3])  # seq 2 redelivered
        a = db.audit()
        lost = max(0, published - (a["processed"] - a["duplicates"]))
        assert lost == 0
        assert a["duplicates"] == 1


class TestSqliteInsertThenAck:
    def test_insert_persists(self, tmp_db):
        db.insert_event(7, "2026-09-05T00:00:00+00:00")
        a = db.audit()
        assert a["seqs"] == [7]

    def test_wal_mode(self, tmp_db):
        conn = db._connect()
        try:
            mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        finally:
            conn.close()
        assert mode.lower() == "wal"