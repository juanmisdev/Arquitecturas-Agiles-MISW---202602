"""Dashboard: aggregates REAL state and orchestrates the experiment.

- GET /            -> UI
- GET /api/estado  -> real aggregation (published, queue depth via pika
                     passive declare, processed/lost/duplicates via SQLite
                     seq audit, container status, docker_mode)
- POST /api/suscripcion/stop|start -> control the real container
- POST /api/experimento/iniciar {n} -> stop -> publish -> poll -> start ->
                     drain -> report; GET /api/experimento/estado
"""
import logging
import os
import sqlite3
import threading
import time
from datetime import datetime, timezone

import pika
import requests
from flask import Flask, jsonify, render_template, request

from controller import (
    MANUAL_START,
    MANUAL_STOP,
    detect_docker_mode,
    start_suscripcion,
    stop_suscripcion,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dashboard")

app = Flask(__name__)

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
COTIZACION_URL = os.environ.get("COTIZACION_URL", "http://ms-cotizacion:5001")
SUSCRIPCION_URL = os.environ.get("SUSCRIPCION_URL", "http://ms-suscripcion:5002")
DB_PATH = os.environ.get("DB_PATH", "/data/events.db")
QUEUE = "cotizacion.creada"
EXCHANGE = "cotizacion"

DEFAULT_N = 100


# ---------------------------------------------------------------- pika queue
def queue_depth():
    """Exact depth via passive declare (not the Management HTTP API)."""
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST, heartbeat=60)
    )
    try:
        channel = connection.channel()
        result = channel.queue_declare(queue=QUEUE, durable=True, passive=True)
        return result.method.message_count
    finally:
        connection.close()


def try_queue_depth():
    try:
        return queue_depth()
    except Exception as exc:
        logger.warning("queue_depth failed: %s", exc)
        return None


# ------------------------------------------------------------------ http aux
def _get_json(url, timeout=3):
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        return response.json()
    except Exception as exc:
        logger.warning("GET %s failed: %s", url, exc)
        return None


# ------------------------------------------------------------- sqlite audit
def seq_audit():
    """Read events table written by ms-suscripcion (same mounted volume)."""
    try:
        conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=5)
        try:
            rows = conn.execute("SELECT seq FROM events ORDER BY id ASC").fetchall()
        finally:
            conn.close()
    except Exception as exc:
        logger.warning("seq_audit failed: %s", exc)
        return None
    seqs = [r[0] for r in rows]
    seen = set()
    dup_seqs = set()
    for s in seqs:
        if s in seen:
            dup_seqs.add(s)
        seen.add(s)
    first_list = []
    seen2 = set()
    for s in seqs:
        if s not in seen2:
            first_list.append(s)
            seen2.add(s)
    return {
        "processed": len(seqs),
        "seqs": seqs,
        "duplicates": len(seqs) - len(seen),
        "in_order": all(
            first_list[i] < first_list[i + 1]
            for i in range(len(first_list) - 1)
        ),
        "last_seq": seqs[-1] if seqs else None,
    }


# -------------------------------------------------------------- /api/estado
@app.get("/api/estado")
def estado():
    cot_stats = _get_json(f"{COTIZACION_URL}/stats")
    sus_stats = _get_json(f"{SUSCRIPCION_URL}/stats")
    audit_data = seq_audit()
    depth = try_queue_depth()

    published = cot_stats.get("published") if cot_stats else None
    processed = audit_data["processed"] if audit_data else (
        sus_stats.get("processed") if sus_stats else None
    )

    lost = None
    if published is not None and audit_data is not None:
        unique_processed = processed - audit_data["duplicates"] if processed is not None else 0
        lost = max(0, published - unique_processed)

    suscripcion_status = "unknown"
    if sus_stats is not None:
        suscripcion_status = "running"
    elif depth is not None or cot_stats is not None:
        suscripcion_status = "stopped"

    return jsonify({
        "published": published,
        "queue_depth": depth,
        "processed": processed,
        "lost": lost,
        "duplicates": audit_data["duplicates"] if audit_data else (
            sus_stats.get("duplicates") if sus_stats else None
        ),
        "in_order": audit_data["in_order"] if audit_data else None,
        "seqs_processed": audit_data["seqs"] if audit_data else [],
        "suscripcion_status": suscripcion_status,
        "docker_mode": detect_docker_mode(),
        "manual_commands": {"stop": MANUAL_STOP, "start": MANUAL_START},
        "ts": datetime.now(timezone.utc).isoformat(),
    })


# ------------------------------------------------------- container controls
@app.post("/api/suscripcion/stop")
def sus_stop():
    result = stop_suscripcion()
    code = 200 if result["ok"] else 503
    return jsonify(result), code


@app.post("/api/suscripcion/start")
def sus_start():
    result = start_suscripcion()
    code = 200 if result["ok"] else 503
    return jsonify(result), code


# ------------------------------------------------------------ orchestrator
class ExperimentRunner(threading.Thread):
    """Runs: stop -> publish n -> poll depth -> start -> drain -> report."""

    def __init__(self, n):
        super().__init__(daemon=True, name="experiment-runner")
        self.n = n
        self.state = {
            "status": "running",
            "phase": "iniciando",
            "started_at": datetime.now(timezone.utc).isoformat(),
            "finished_at": None,
            "error": None,
            "report": None,
        }

    def run(self):
        try:
            self._run()
        except Exception as exc:
            logger.exception("experiment failed")
            self.state["status"] = "error"
            self.state["error"] = str(exc)
            self.state["finished_at"] = datetime.now(timezone.utc).isoformat()

    def _run(self):
        n = self.n
        self.state["phase"] = "deteniendo_suscripcion"
        stop_result = stop_suscripcion()
        if not stop_result["ok"]:
            self.state["status"] = "requires_manual"
            self.state["error"] = "No se pudo detener Suscripción automáticamente"
            self.state["manual"] = stop_result.get("manual", MANUAL_STOP)
            self.state["finished_at"] = datetime.now(timezone.utc).isoformat()
            return

        self.state["phase"] = "publicando"
        response = requests.post(f"{COTIZACION_URL}/publicar",
                                 json={"n": n}, timeout=120)
        response.raise_for_status()
        published = response.json().get("last_seq", n)

        self.state["phase"] = "acumulando"
        deadline = time.time() + 20
        while time.time() < deadline:
            depth = try_queue_depth()
            if depth is not None and depth >= published:
                break
            time.sleep(0.5)

        self.state["phase"] = "reintegrando_suscripcion"
        start_result = start_suscripcion()
        if not start_result["ok"]:
            self.state["status"] = "requires_manual"
            self.state["error"] = "No se pudo reiniciar Suscripción automáticamente"
            self.state["manual"] = start_result.get("manual", MANUAL_START)
            self.state["finished_at"] = datetime.now(timezone.utc).isoformat()
            return

        self.state["phase"] = "drenando"
        deadline = time.time() + 300
        while time.time() < deadline:
            depth = try_queue_depth()
            if depth == 0:
                break
            time.sleep(1)

        self.state["phase"] = "generando_reporte"
        report = self._report(published)
        self.state["report"] = report
        self.state["status"] = "done"
        self.state["finished_at"] = datetime.now(timezone.utc).isoformat()

    def _report(self, published):
        audit_data = seq_audit()
        stats = _get_json(f"{SUSCRIPCION_URL}/stats")
        processed = audit_data["processed"] if audit_data else (
            stats.get("processed") if stats else None
        )
        duplicates = audit_data["duplicates"] if audit_data else (
            stats.get("duplicates") if stats else None
        )
        in_order = audit_data["in_order"] if audit_data else None
        lost = None
        if processed is not None:
            lost = max(0, published - (processed - (duplicates or 0)))
        return {
            "published": published,
            "processed": processed,
            "lost": lost,
            "duplicates": duplicates,
            "in_order": in_order,
            "queue_depth": try_queue_depth(),
            "verdict": {
                "cero_perdidos": lost == 0,
                "en_orden": bool(in_order),
            },
        }


_runner = None
_runner_lock = threading.Lock()


@app.post("/api/experimento/iniciar")
def experimento_iniciar():
    global _runner
    body = request.get_json(silent=True) or {}
    n = body.get("n", DEFAULT_N)
    if not isinstance(n, int) or n < 1 or n > 10000:
        return jsonify({"ok": False, "error": "n debe ser un entero 1..10000"}), 400
    with _runner_lock:
        if _runner is not None and _runner.is_alive():
            return jsonify({"ok": False, "error": "experimento ya en curso"}), 409
        _runner = ExperimentRunner(n)
        _runner.start()
    return jsonify({"ok": True, "n": n})


@app.get("/api/experimento/estado")
def experimento_estado():
    with _runner_lock:
        runner = _runner
    if runner is None:
        return jsonify({"status": "idle"})
    snapshot = dict(runner.state)
    return jsonify(snapshot)


# --------------------------------------------------------------------- UI
@app.get("/")
def index():
    return render_template("index.html")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)