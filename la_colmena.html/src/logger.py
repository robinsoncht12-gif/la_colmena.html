"""
========================================================================================
MÓDULO DE LOGGING ESTRUCTURADO Y OBSERVABILIDAD
Cátedra: Ingeniería de Software 3 [IF0129] - Corporación Universitaria Remington
Patrón: Structured Logging (JSON / Console), SoD (Separación de Responsabilidades)
Dependencias: NINGUNA (Python Standard Library `logging`, `json`, `sys`, `time`)
========================================================================================
"""
import logging
import json
import os
import sys
import time
from src.tracing import TraceContextFilter


def _safe_console_text(value: str) -> str:
    encoding = getattr(sys.stdout, "encoding", None)
    if not encoding:
        return value
    return value.encode(encoding, errors="backslashreplace").decode(encoding)


class StructuredJSONFormatter(logging.Formatter):
    """
    Formateador para emisión de logs en formato JSON estructurado,
    compatible con agregadores tipo ELK, Loki, Datadog o CloudWatch.
    """
    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(record.created)),
            "level": record.levelname,
            "logger": record.name,
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
            "message": record.getMessage(),
        }
        # Agregar atributos adicionales si existen (contexto de negocio)
        if hasattr(record, "correlation_id"):
            log_entry["correlation_id"] = record.correlation_id
        if hasattr(record, "idempotency_key"):
            log_entry["idempotency_key"] = record.idempotency_key
        if hasattr(record, "cliente_id"):
            log_entry["cliente_id"] = record.cliente_id
        if hasattr(record, "order_id"):
            log_entry["order_id"] = record.order_id
        if hasattr(record, "traceparent"):
            log_entry["traceparent"] = record.traceparent
            log_entry["trace_id"] = record.trace_id
            log_entry["span_id"] = record.span_id

        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        return _safe_console_text(json.dumps(log_entry, ensure_ascii=False))


class ColoredConsoleFormatter(logging.Formatter):
    """
    Formateador de consola con colores ANSI para legibilidad pedagógica
    en terminales durante laboratorios presenciales o locales.
    """
    COLOR_MAP = {
        logging.DEBUG: "\033[36m",     # Cyan
        logging.INFO: "\033[32m",      # Verde
        logging.WARNING: "\033[33m",   # Amarillo
        logging.ERROR: "\033[31m",     # Rojo
        logging.CRITICAL: "\033[41m",  # Fondo Rojo
    }
    RESET = "\033[0m"

    def format(self, record: logging.LogRecord) -> str:
        color = self.COLOR_MAP.get(record.levelno, self.RESET)
        tiempo = time.strftime("%H:%M:%S", time.localtime(record.created))
        prefix = f"[{tiempo}] [{record.levelname:<8}] [{record.name}]"
        msg = record.getMessage()
        if hasattr(record, "traceparent"):
            msg = f"{msg} traceparent={record.traceparent}"
        if record.exc_info:
            msg += "\n" + self.formatException(record.exc_info)
        return _safe_console_text(f"{color}{prefix}{self.RESET} {msg}")


def get_logger(name: str, level: int = logging.INFO, use_json: bool = False) -> logging.Logger:
    """
    Factory de Loggers desacoplado para uso a lo largo de toda la aplicación.
    Permite alternar entre salida legible por humanos y JSON para producción/monitoreo.
    """
    use_json = use_json or os.environ.get("LACOLMENA_LOG_FORMAT", "").lower() == "json"
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Evitar duplicar handlers si ya está configurado
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(level)
        handler.addFilter(TraceContextFilter())
        if use_json:
            handler.setFormatter(StructuredJSONFormatter())
        else:
            handler.setFormatter(ColoredConsoleFormatter())
        logger.addHandler(handler)
        logger.propagate = False

    return logger
