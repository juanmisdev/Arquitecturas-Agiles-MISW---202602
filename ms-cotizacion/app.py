"""MS-Cotizacion HTTP API: publish cotizacion.creada events."""
import logging

from flask import Flask, jsonify, request

from publisher import Publisher

logging.basicConfig(level=logging.INFO)

app = Flask(__name__)
publisher = Publisher()


@app.get("/health")
def health():
    return {"ok": True, "service": "ms-cotizacion"}


@app.post("/publicar")
def publicar():
    """Publish n cotizacion.creada events. Body: {"n": <int>} (default 1)."""
    body = request.get_json(silent=True) or {}
    n = body.get("n", 1)
    if not isinstance(n, int) or n < 1:
        return {"ok": False, "error": "n debe ser un entero >= 1"}, 400
    last_seq = None
    try:
        for _ in range(n):
            last_seq = publisher.publish()
    except Exception as exc:
        app.logger.error("publish failed: %s", exc)
        return {"ok": False, "error": str(exc)}, 502
    return {"ok": True, "publicados": n, "last_seq": last_seq["seq"] if last_seq else 0}


@app.get("/stats")
def stats():
    return {"published": publisher.seq}


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)