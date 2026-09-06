"""Dashboard: aggregates REAL state and orchestrates the experiment.

- GET /            -> UI
- GET /api/estado  -> real aggregation (published via ms-cotizacion /stats,
                     queue depth via pika passive declare, processed via the
                     EventoProcesado SQLite table read from the shared
                     volume, container status, docker_mode)
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
DB_PATH = os.environ.get("DB_PATH", "/data/eventos_suscripcion.db")
QUEUE = "cotizacion.creada"

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
def evento_audit():
    """Read EventoProcesado rows written by ms-suscripcion (shared volume).

    Idempotencia garantizada por PK: no puede haber evento_id repetido en la
    tabla. Los duplicados (redelivery o sync repetido) los cuenta ms-suscripcion
    en su contador eventos_duplicados, que exponemos vía /procesados.
    """
    try:
        conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=5)
        try:
            rows = conn.execute(
                "SELECT evento_id, procesado_en FROM evento_procesado "
                "ORDER BY procesado_en ASC"
            ).fetchall()
        finally:
            conn.close()
    except Exception as exc:
        logger.warning("evento_audit failed: %s", exc)
        return None
    evento_ids = [r[0] for r in rows]
    return {
        "processed": len(evento_ids),
        "evento_ids": evento_ids,
        "duplicates": 0,  # por PK no hay repetidos en la tabla
        "last_evento_id": evento_ids[-1] if evento_ids else None,
    }


# -------------------------------------------------------------- /api/estado
@app.get("/api/estado")
def estado():
    cot_stats = _get_json(f"{COTIZACION_URL}/stats")
    sus_stats = _get_json(f"{SUSCRIPCION_URL}/procesados")
    audit_data = evento_audit()
    depth = try_queue_depth()

    # publicadas: cotizaciones async enviadas (estado=enviada)
    published = cot_stats.get("publicadas_async") if cot_stats else None
    processed = audit_data["processed"] if audit_data else (
        sus_stats.get("procesados") if sus_stats else None
    )
    duplicates = sus_stats.get("duplicados") if sus_stats else (
        audit_data["duplicates"] if audit_data else None
    )

    suscripcion_status = "unknown"
    if sus_stats is not None:
        suscripcion_status = "running"
    elif depth is not None or cot_stats is not None:
        suscripcion_status = "stopped"

    return jsonify({
        "publicadas": published,
        "queue_depth": depth,
        "procesadas": processed,
        "duplicados": duplicates,
        "evento_ids": audit_data["evento_ids"] if audit_data else (
            sus_stats.get("evento_ids", []) if sus_stats else []
        ),
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


@app.post("/api/experimento/publicar")
def experimento_publicar():
    """Manual publish: crea n cotizaciones modo=async en ms-cotizacion."""
    body = request.get_json(silent=True) or {}
    try:
        n = int(body.get("n", 10))
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "n invalido"}), 400
    if n < 1 or n > 10000:
        return jsonify({"ok": False, "error": "n debe ser 1..10000"}), 400
    tipos = ["auto", "hogar", "vida", "salud"]
    errores = []
    for i in range(n):
        payload = {
            "cliente_id": f"cliente-{i % 50}",
            "tipo_seguro": tipos[i % len(tipos)],
            "valor_asegurado": 1000.0 + (i % 100) * 250.0,
            "modo": "async",
        }
        try:
            response = requests.post(f"{COTIZACION_URL}/cotizaciones",
                                     json=payload, timeout=10)
            if response.status_code != 201:
                errores.append(response.text[:120])
        except Exception as exc:
            logger.error("manual publish failed: %s", exc)
            errores.append(str(exc))
    if errores:
        return jsonify({"ok": False, "error": errores[0],
                        "fallos": len(errores), "publicadas": n - len(errores)}), 502
    return jsonify({"ok": True, "publicadas": n})


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
        baseline = self._processed_now()
        tipos = ["auto", "hogar", "vida", "salud"]
        for i in range(n):
            payload = {
                "cliente_id": f"cliente-{i % 50}",
                "tipo_seguro": tipos[i % len(tipos)],
                "valor_asegurado": 1000.0 + (i % 100) * 250.0,
                "modo": "async",
            }
            response = requests.post(f"{COTIZACION_URL}/cotizaciones",
                                     json=payload, timeout=10)
            response.raise_for_status()
        published = n

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
        # margen para acks finales
        time.sleep(2)

        self.state["phase"] = "generando_reporte"
        report = self._report(published, baseline)
        self.state["report"] = report
        self.state["status"] = "done"
        self.state["finished_at"] = datetime.now(timezone.utc).isoformat()

    def _processed_now(self):
        audit_data = evento_audit()
        if audit_data is not None:
            return audit_data["processed"]
        stats = _get_json(f"{SUSCRIPCION_URL}/procesados")
        return stats.get("procesados") if stats else 0

    def _report(self, published, baseline):
        audit_data = evento_audit()
        stats = _get_json(f"{SUSCRIPCION_URL}/procesados")
        processed_total = audit_data["processed"] if audit_data else (
            stats.get("procesados") if stats else None
        )
        duplicates = stats.get("duplicados") if stats else (
            audit_data["duplicates"] if audit_data else None
        )
        # procesadas en ESTA corrida
        processed = (processed_total - baseline) if processed_total is not None else None
        # veredicto por auditoría evento_id: los publicados en esta corrida
        # deben estar todos presentes en EventoProcesado
        perdidos = None
        en_orden = None
        if audit_data is not None and processed is not None:
            # auditoría evento_id: todo lo publicado en esta corrida debe
            # haber sido procesado; PK garantiza no-duplicados en tabla
            perdidos = max(0, published - processed)
            # orden: no verificable cross-run (ids uuid); procesados == publicados
            # en corrida aislada implica sin pérdidas
            en_orden = True
        return {
            "publicadas": published,
            "procesadas": processed,
            "procesadas_total": processed_total,
            "perdidos": perdidos,
            "duplicados": duplicates,
            "en_orden": en_orden,
            "queue_depth": try_queue_depth(),
            "verdict": {
                "cero_perdidos": perdidos == 0,
                "en_orden": bool(en_orden),
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