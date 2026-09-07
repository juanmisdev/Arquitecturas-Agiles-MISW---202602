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
import math
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

    # errores vistos por el cliente: cotizaciones con estado=error
    # (sync que no pudo entregar, o async que no pudo publicar en el broker)
    errores_cliente = None
    if cot_stats is not None:
        errores_cliente = cot_stats.get("por_estado", {}).get("error", 0)

    return jsonify({
        "publicadas": published,
        "queue_depth": depth,
        "procesadas": processed,
        "duplicados": duplicates,
        "errores_cliente": errores_cliente,
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
    """Manual publish: crea n cotizaciones en ms-cotizacion (modo async|sync)."""
    body = request.get_json(silent=True) or {}
    try:
        n = int(body.get("n", 10))
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "n invalido"}), 400
    if n < 1 or n > 10000:
        return jsonify({"ok": False, "error": "n debe ser 1..10000"}), 400
    modo = body.get("modo", "async")
    if modo not in ("async", "sync"):
        return jsonify({"ok": False, "error": "modo debe ser async|sync"}), 400
    # Pausa entre publicaciones (segundos): hace visible el flujo en el dashboard.
    # Env PAUSE_ENTRE_PUBLICACIONES, defecto 0.0.
    try:
        pausa = float(os.environ.get("PAUSE_ENTRE_PUBLICACIONES", "0"))
    except ValueError:
        pausa = 0.0
    lote = _publicar_lote(n, modo, pausa)
    if lote["errores_cliente"]:
        return jsonify({"ok": False, "error": "hubo errores en la publicación",
                        "modo": modo, "errores_cliente": lote["errores_cliente"],
                        "publicadas": lote["enviadas_ok"]}), 502
    return jsonify({"ok": True, "modo": modo, "publicadas": lote["enviadas_ok"]})


# ------------------------------------------------------------ orchestrator
TIPOS = ["auto", "hogar", "vida", "salud"]


def _percentile(values, p):
    """Percentil p (0..100) por interpolación lineal. None si no hay datos."""
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return round(ordered[0], 1)
    k = (len(ordered) - 1) * (p / 100.0)
    lo = math.floor(k)
    hi = math.ceil(k)
    if lo == hi:
        return round(ordered[int(k)], 1)
    val = ordered[lo] + (ordered[hi] - ordered[lo]) * (k - lo)
    return round(val, 1)


def _sus_duplicados():
    stats = _get_json(f"{SUSCRIPCION_URL}/procesados")
    return stats.get("duplicados", 0) if stats else 0


def _publicar_lote(n, modo, pausa):
    """Publica n cotizaciones en `modo`, midiendo cada POST /cotizaciones.

    "Errores vistos por el cliente" = la operación no confirmó la entrega:
    HTTP != 201, o body con estado='error' (sync que no pudo entregar por
    REST, o async que no pudo publicar en el broker).
    """
    latencias = []
    errores_cliente = 0
    enviadas_ok = 0
    for i in range(n):
        payload = {
            "cliente_id": f"cliente-{i % 50}",
            "tipo_seguro": TIPOS[i % len(TIPOS)],
            "valor_asegurado": 1000.0 + (i % 100) * 250.0,
            "modo": modo,
        }
        t0 = time.perf_counter()
        try:
            response = requests.post(f"{COTIZACION_URL}/cotizaciones",
                                     json=payload, timeout=15)
            latencias.append((time.perf_counter() - t0) * 1000.0)
            estado = None
            if response.status_code == 201:
                try:
                    estado = (response.json() or {}).get("estado")
                except ValueError:
                    estado = None
            if response.status_code == 201 and estado != "error":
                enviadas_ok += 1
            else:
                errores_cliente += 1
        except Exception as exc:
            latencias.append((time.perf_counter() - t0) * 1000.0)
            errores_cliente += 1
            logger.warning("publicar %s falló: %s", modo, exc)
        if pausa > 0 and i < n - 1:
            time.sleep(pausa)
    return {
        "latencias": latencias,
        "errores_cliente": errores_cliente,
        "enviadas_ok": enviadas_ok,
    }


class ExperimentRunner(threading.Thread):
    """Corre uno o ambos brazos del punto de sensibilidad (sync REST vs
    async broker) bajo caída del consumidor y produce una tabla comparativa.

    Cada brazo: detiene Suscripción -> publica n -> (async: acumula y drena)
    -> reintegra Suscripción -> mide errores del cliente, latencia, encolados,
    procesados al reintegrar y perdidos.
    """

    def __init__(self, n, arms):
        super().__init__(daemon=True, name="experiment-runner")
        self.n = n
        self.arms = list(arms)
        self.state = {
            "status": "running",
            "phase": "iniciando",
            "arm": None,
            "arms": list(arms),
            "started_at": datetime.now(timezone.utc).isoformat(),
            "finished_at": None,
            "error": None,
            "report": None,
        }

    # ------------------------------------------------------------- helpers
    def _phase(self, arm, phase):
        self.state["arm"] = arm
        self.state["phase"] = phase

    def _fail_manual(self, msg, manual):
        self.state["status"] = "requires_manual"
        self.state["error"] = msg
        self.state["manual"] = manual
        self.state["finished_at"] = datetime.now(timezone.utc).isoformat()

    def _processed_now(self):
        audit_data = evento_audit()
        if audit_data is not None:
            return audit_data["processed"]
        stats = _get_json(f"{SUSCRIPCION_URL}/procesados")
        return stats.get("procesados") if stats else 0

    # ---------------------------------------------------------------- run
    def run(self):
        try:
            self._run()
        except Exception as exc:
            logger.exception("experiment failed")
            self.state["status"] = "error"
            self.state["error"] = str(exc)
            self.state["finished_at"] = datetime.now(timezone.utc).isoformat()

    def _run(self):
        try:
            pausa = float(os.environ.get("PAUSE_ENTRE_PUBLICACIONES", "0"))
        except ValueError:
            pausa = 0.0

        report = {}
        for arm in self.arms:
            arm_report = (self._run_sync if arm == "sync" else self._run_async)(pausa)
            if arm_report is None:
                return  # requires_manual / error ya seteado en self.state
            report[arm] = arm_report

        self._phase(None, "generando_reporte")
        report["verdict"] = self._verdict(report)
        self.state["report"] = report
        self.state["status"] = "done"
        self.state["finished_at"] = datetime.now(timezone.utc).isoformat()

    # -------------------------------------------------------- brazo sync
    def _run_sync(self, pausa):
        n = self.n
        self._phase("sync", "deteniendo_suscripcion")
        if not stop_suscripcion()["ok"]:
            self._fail_manual("No se pudo detener Suscripción (brazo sync)", MANUAL_STOP)
            return None

        baseline = self._processed_now()
        dup_before = _sus_duplicados()

        self._phase("sync", "publicando")
        lote = _publicar_lote(n, "sync", pausa)
        # en sync no hay cola: lo que no se entregó al consumidor se pierde
        encolados = try_queue_depth() or 0

        self._phase("sync", "reintegrando_suscripcion")
        if not start_suscripcion()["ok"]:
            self._fail_manual("No se pudo reintegrar Suscripción (brazo sync)", MANUAL_START)
            return None
        time.sleep(1.5)  # margen para que el consumidor vuelva

        procesados = max(0, self._processed_now() - baseline)
        dup_delta = max(0, _sus_duplicados() - dup_before)
        return self._arm_report("sync", lote, encolados, procesados, dup_delta)

    # ------------------------------------------------------- brazo async
    def _run_async(self, pausa):
        n = self.n
        self._phase("async", "deteniendo_suscripcion")
        if not stop_suscripcion()["ok"]:
            self._fail_manual("No se pudo detener Suscripción (brazo async)", MANUAL_STOP)
            return None

        baseline = self._processed_now()
        dup_before = _sus_duplicados()

        self._phase("async", "publicando")
        lote = _publicar_lote(n, "async", pausa)

        self._phase("async", "acumulando")
        max_depth = 0
        deadline = time.time() + 20
        while time.time() < deadline:
            depth = try_queue_depth()
            if depth is not None:
                max_depth = max(max_depth, depth)
                if lote["enviadas_ok"] > 0 and depth >= lote["enviadas_ok"]:
                    break
            time.sleep(0.5)

        self._phase("async", "reintegrando_suscripcion")
        if not start_suscripcion()["ok"]:
            self._fail_manual("No se pudo reintegrar Suscripción (brazo async)", MANUAL_START)
            return None

        self._phase("async", "drenando")
        deadline = time.time() + 300
        while time.time() < deadline:
            if try_queue_depth() == 0:
                break
            time.sleep(0.25)
        time.sleep(0.5)  # margen para acks finales

        procesados = max(0, self._processed_now() - baseline)
        dup_delta = max(0, _sus_duplicados() - dup_before)
        return self._arm_report("async", lote, max_depth, procesados, dup_delta)

    # ------------------------------------------------------------ report
    def _arm_report(self, arm, lote, encolados, procesados, duplicados):
        latencias = lote["latencias"]
        # sync: la referencia es todo lo que intentó el cliente (nada se encola)
        # async: la referencia es lo que el broker aceptó (enviadas_ok)
        referencia = lote["enviadas_ok"] if arm == "async" else self.n
        perdidos = max(0, referencia - procesados)
        return {
            "operaciones_cliente": self.n,
            "enviadas_ok": lote["enviadas_ok"],
            "errores_cliente": lote["errores_cliente"],
            "encolados_durante_caida": encolados,
            "procesados_reintegrar": procesados,
            "perdidos": perdidos,
            "duplicados": duplicados,
            "latencia_ms": {
                "p50": _percentile(latencias, 50),
                "p95": _percentile(latencias, 95),
                "max": round(max(latencias), 1) if latencias else None,
            },
        }

    def _verdict(self, report):
        sync_r = report.get("sync")
        async_r = report.get("async")
        verdict = {}
        if async_r is not None:
            verdict["broker_enmascara_falla"] = (
                async_r["errores_cliente"] == 0 and async_r["perdidos"] == 0
            )
        if sync_r is not None:
            verdict["sync_propaga_falla"] = sync_r["errores_cliente"] > 0
        if sync_r is not None and async_r is not None:
            ok = verdict.get("broker_enmascara_falla") and verdict.get("sync_propaga_falla")
            verdict["recomendacion"] = (
                "Adoptar el broker asíncrono: desacopla la falla del consumidor "
                "del cliente (0 errores, 0 perdidos) frente al conector síncrono."
                if ok else
                "Resultado no concluyente — revisar durabilidad de la cola, "
                "ack manual y el estado de los servicios."
            )
        return verdict


_runner = None
_runner_lock = threading.Lock()


@app.post("/api/experimento/iniciar")
def experimento_iniciar():
    global _runner
    body = request.get_json(silent=True) or {}
    n = body.get("n", DEFAULT_N)
    if not isinstance(n, int) or n < 1 or n > 10000:
        return jsonify({"ok": False, "error": "n debe ser un entero 1..10000"}), 400
    modo = body.get("modo", "compare")
    arms_by_modo = {"compare": ["sync", "async"], "sync": ["sync"], "async": ["async"]}
    if modo not in arms_by_modo:
        return jsonify({"ok": False, "error": "modo debe ser compare|sync|async"}), 400
    with _runner_lock:
        if _runner is not None and _runner.is_alive():
            return jsonify({"ok": False, "error": "experimento ya en curso"}), 409
        _runner = ExperimentRunner(n, arms_by_modo[modo])
        _runner.start()
    return jsonify({"ok": True, "n": n, "modo": modo})


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