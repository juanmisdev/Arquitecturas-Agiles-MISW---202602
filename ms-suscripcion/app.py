"""MS-Suscripcion HTTP API: report processed events."""
import logging

from flask import Flask, jsonify

from consumer import consumer_thread, start_consumer
from db import audit, init_db

logging.basicConfig(level=logging.INFO)

app = Flask(__name__)


@app.get("/health")
def health():
    return {"ok": True, "service": "ms-suscripcion"}


@app.get("/procesados")
def procesados():
    """Processed events: count + seq list (audit)."""
    init_db()
    return jsonify(audit())


@app.get("/stats")
def stats():
    init_db()
    a = audit()
    return {
        "processed": a["processed"],
        "duplicates": a["duplicates"],
        "min_seq": a["min_seq"],
        "max_seq": a["max_seq"],
        "last_seq": a["last_seq"],
    }


if __name__ == "__main__":
    init_db()
    start_consumer()
    app.run(host="0.0.0.0", port=5002)