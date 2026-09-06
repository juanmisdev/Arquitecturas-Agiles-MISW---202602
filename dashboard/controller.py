"""Container control for ms-suscripcion.

Primary path: Docker SDK over the mounted docker.sock.
Fallback path: subprocess with fixed, constant command strings (never user
input). If neither works, the caller receives manual instructions and the
dashboard reports docker_mode="manual".
"""
import logging
import subprocess

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("controller")

CONTAINER = "ms-suscripcion"  # fixed container name from docker-compose.yml

MANUAL_STOP = "docker stop ms-suscripcion"
MANUAL_START = "docker start ms-suscripcion"

_sdk_module = None


def _docker_sdk():
    global _sdk_module
    if _sdk_module is False:
        return None
    if _sdk_module is None:
        try:
            import docker  # docker SDK for Python
            _sdk_module = docker
        except Exception:
            _sdk_module = False
    return _sdk_module or None


def _sdk_client():
    sdk = _docker_sdk()
    if sdk is None:
        raise RuntimeError("docker SDK unavailable")
    import os
    socket = os.environ.get("DOCKER_SOCKET", "/var/run/docker.sock")
    return sdk.DockerClient(base_url="unix://" + socket)


def _stop_via_sdk():
    client = _sdk_client()
    container = client.containers.get(CONTAINER)
    container.stop(timeout=5)


def _start_via_sdk():
    client = _sdk_client()
    container = client.containers.get(CONTAINER)
    container.start()


def _stop_via_subprocess():
    result = subprocess.run(["docker", "stop", CONTAINER],
                            capture_output=True, text=True, timeout=60)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "docker stop failed")


def _start_via_subprocess():
    result = subprocess.run(["docker", "start", CONTAINER],
                            capture_output=True, text=True, timeout=60)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "docker start failed")


def _container_status():
    try:
        client = _sdk_client()
        container = client.containers.get(CONTAINER)
        state = container.attrs.get("State", {})
        running = bool(state.get("Running"))
        return "running" if running else "stopped"
    except Exception:
        return "unknown"


def detect_docker_mode():
    """Probe: can we at least inspect the container?"""
    try:
        _sdk_client()
        return "sdk"
    except Exception:
        try:
            result = subprocess.run(["docker", "info"], capture_output=True,
                                    text=True, timeout=10)
            if result.returncode == 0:
                return "fallback"
        except Exception:
            pass
        return "manual"


def stop_suscripcion():
    """Stop the real ms-suscripcion container.

    Returns dict {ok, mode, manual?}.
    """
    try:
        _stop_via_sdk()
        return {"ok": True, "mode": "sdk"}
    except Exception as sdk_exc:
        logger.warning("SDK stop failed: %s; trying subprocess fallback", sdk_exc)
        try:
            _stop_via_subprocess()
            return {"ok": True, "mode": "fallback"}
        except Exception as sub_exc:
            logger.error("Subprocess stop failed: %s", sub_exc)
            return {"ok": False, "mode": "manual", "manual": MANUAL_STOP}


def start_suscripcion():
    """Start the real ms-suscripcion container.

    Returns dict {ok, mode, manual?}.
    """
    try:
        _start_via_sdk()
        return {"ok": True, "mode": "sdk"}
    except Exception as sdk_exc:
        logger.warning("SDK start failed: %s; trying subprocess fallback", sdk_exc)
        try:
            _start_via_subprocess()
            return {"ok": True, "mode": "fallback"}
        except Exception as sub_exc:
            logger.error("Subprocess start failed: %s", sub_exc)
            return {"ok": False, "mode": "manual", "manual": MANUAL_START}