"""SQLite persistence for processed cotizacion.creada events (seq audit).

WAL mode, stored in the mounted /data volume so counts survive restarts.
"""
import os
import sqlite3
import threading

DB_PATH = os.environ.get("DB_PATH", "/data/events.db")

_init_lock = threading.Lock()


def _connect():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_db():
    with _init_lock:
        conn = _connect()
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    seq INTEGER NOT NULL,
                    received_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_events_seq ON events(seq)"
            )
            conn.commit()
        finally:
            conn.close()


def insert_event(seq, received_at):
    """Persist one processed event (insert happens BEFORE acking)."""
    conn = _connect()
    try:
        conn.execute(
            "INSERT INTO events (seq, received_at) VALUES (?, ?)",
            (seq, received_at),
        )
        conn.commit()
    finally:
        conn.close()


def audit():
    """Seq audit: processed count, seq list, duplicates, order check.

    Returns dict with:
      processed: number of rows
      seqs: seqs in processing (insert) order
      duplicates: count of repeated seqs
      duplicate_seqs: sorted seqs seen more than once
      in_order: True if seqs (first occurrence) are strictly increasing 1..N
    """
    conn = _connect()
    try:
        rows = conn.execute(
            "SELECT seq FROM events ORDER BY id ASC"
        ).fetchall()
    finally:
        conn.close()
    seqs = [r[0] for r in rows]
    seen = set()
    dup_seqs = set()
    for s in seqs:
        if s in seen:
            dup_seqs.add(s)
        seen.add(s)
    # first occurrence of each seq, in order of first appearance
    first_list = []
    seen2 = set()
    for s in seqs:
        if s not in seen2:
            first_list.append(s)
            seen2.add(s)
    in_order = all(
        first_list[i] < first_list[i + 1] for i in range(len(first_list) - 1)
    )
    return {
        "processed": len(seqs),
        "seqs": seqs,
        "duplicates": len(seqs) - len(seen),
        "duplicate_seqs": sorted(dup_seqs),
        "in_order": in_order,
        "min_seq": min(seqs) if seqs else None,
        "max_seq": max(seqs) if seqs else None,
        "last_seq": seqs[-1] if seqs else None,
    }


def count_unique_seqs():
    return len({r[0] for r in _connect().execute("SELECT seq FROM events")})