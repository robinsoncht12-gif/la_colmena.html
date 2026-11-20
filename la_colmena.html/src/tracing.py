"""W3C Trace Context helpers for HTTP requests and structured logs."""
from contextvars import ContextVar
import re
import secrets


_TRACEPARENT_RE = re.compile(
    r"^00-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$"
)
_traceparent = ContextVar("traceparent", default=None)
_business_events = ContextVar("business_events", default=())


def _new_nonzero_id(byte_count):
    value = secrets.token_hex(byte_count)
    while set(value) == {"0"}:
        value = secrets.token_hex(byte_count)
    return value


def start_trace(incoming=None):
    """Start a request span and return its W3C traceparent value."""
    match = _TRACEPARENT_RE.fullmatch(incoming or "")
    valid_parent = match and set(match.group(1)) != {"0"} and set(match.group(2)) != {"0"}
    trace_id = match.group(1) if valid_parent else _new_nonzero_id(16)
    span_id = _new_nonzero_id(8)
    value = f"00-{trace_id}-{span_id}-01"
    _traceparent.set(value)
    _business_events.set(())
    return value


def current_traceparent():
    return _traceparent.get()


def record_business_event(event_type):
    """Attach an outbox event type to the active request trace."""
    if not event_type:
        return
    events = _business_events.get()
    _business_events.set((*events, event_type))


def current_business_events():
    return _business_events.get()


class TraceContextFilter:
    """Attach active W3C identifiers to each logging record."""
    def filter(self, record):
        value = current_traceparent()
        if value:
            record.traceparent = value
            parts = value.split("-")
            record.trace_id = parts[1]
            record.span_id = parts[2]
            record.business_events = current_business_events()
        return True
