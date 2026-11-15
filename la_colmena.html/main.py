"""
========================================================================================
PUNTO DE ENTRADA PRINCIPAL: LaColmena WEB MVP
Cátedra: Ingeniería de Software 3 [IF0129] - Corporación Universitaria Remington
Docente: Ing. Jhon Alexander Rosero Lasso, MSc en Inteligencia Artificial
Patrones: Clean Architecture, SOLID, ACID Local, SoD, Saga Orchestrator
Tecnología: Python 3.10+ Standard Library + argon2-cffi + confluent-kafka (Apache Kafka)
========================================================================================
"""
import os
import sys
import threading
import time
import subprocess
import webbrowser

# Configuración del PYTHONPATH para ejecución portable en cualquier entorno (Linux, Windows, WSL)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from src.app import run_server
from src.logger import get_logger

logger = get_logger("Main")

def abrir_navegador(url: str, delay: float = 0.8) -> None:
    """Abre el navegador disponible o indica que el servidor está listo."""
    time.sleep(delay)

    if _abrir_navegador_wsl(url):
        return

    try:
        old_stderr = os.dup(2)
        try:
            with open(os.devnull, "w") as devnull:
                os.dup2(devnull.fileno(), 2)
                try:
                    abierto = webbrowser.open(url)
                finally:
                    os.dup2(old_stderr, 2)
        finally:
            os.close(old_stderr)
    except (OSError, webbrowser.Error) as error:
        logger.info(
            "ℹ️ No se pudo abrir el navegador (%s). Abre manualmente: %s",
            error,
            url,
        )
        return

    if abierto:
        logger.info("🌐 LaColmena abierto en navegador local: %s", url)
    else:
        logger.info("ℹ️ Servidor listo. Abre manualmente en tu navegador: %s", url)


def _abrir_navegador_wsl(url: str) -> bool:
    try:
        with open("/proc/version", encoding="utf-8") as version_file:
            es_wsl = "microsoft" in version_file.read().lower()
    except OSError:
        return False

    comando_windows = "/mnt/c/Windows/System32/cmd.exe"
    if not es_wsl or not os.path.exists(comando_windows):
        return False

    try:
        subprocess.Popen(
            [comando_windows, "/c", "start", url],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except OSError as error:
        logger.warning("No se pudo abrir el navegador de Windows desde WSL: %s", error)
        return False

    logger.info("🌐 [WSL2 Host] Abriendo LaColmena en el navegador de Windows: %s", url)
    return True


if __name__ == "__main__":
    PORT = 8086
    URL = f"http://localhost:{PORT}"

    print("=" * 85)
    print("🏥 LaColmena MVP — INGENIERÍA DE SOFTWARE 3 [IF0129]")
    print("Corporación Universitaria Remington — Facultad de Ingenierías")
    print("Docente: Ing. Jhon Alexander Rosero Lasso, MSc en IA")
    print("Patrones: SOLID | ACID (SQLite WAL) | Saga Orchestrator | Outbox Pattern | Fail-Fast")
    print(f"🔗 Servidor Web y API REST activo en: {URL}")
    test_dir = os.path.join(BASE_DIR, "tests")
    print(f'🧪 Suite de pruebas: python -m unittest discover -s "{test_dir}" -p "test_*.py"')
    print("=" * 85)

    # Iniciar subproceso de apertura de interfaz web resiliente
    threading.Thread(target=abrir_navegador, args=(URL,), daemon=True).start()
    
    # Lanzar servidor HTTP nativo
    run_server(port=PORT)
