"""W3C Trace Context helpers for HTTP requests and structured logs."""
from contextvars import ContextVar
import re
import secrets


_TRACEPARENT_RE = re.compile(
    r"^00-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$"
)
_traceparent = ContextVar("traceparent", default=None)


def start_trace(incoming=None):
    """Start a request span and return its W3C traceparent value."""
    match = _TRACEPARENT_RE.fullmatch(incoming or "")
    trace_id = match.group(1) if match and set(match.group(1)) != {"0"} else secrets.token_hex(16)
    span_id = secrets.token_hex(8)
    value = f"00-{trace_id}-{span_id}-01"
    _traceparent.set(value)
    return value


def current_traceparent():
    return _traceparent.get()


class TraceContextFilter:
    """Attach active W3C identifiers to each logging record."""
    def filter(self, record):
        value = current_traceparent()
        if value:
            record.traceparent = value
            parts = value.split("-")
            record.trace_id = parts[1]
            record.span_id = parts[2]
        return True
