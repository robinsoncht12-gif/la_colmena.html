"""Circuit breaker de pagos con prueba controlada en estado HALF-OPEN."""
import os
import sys
import threading
import time

if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.logger import get_logger

logger = get_logger("CircuitBreaker")


class CircuitOpenError(RuntimeError):
    """El circuito no permite otro cobro hasta la siguiente prueba."""


class CircuitBreaker:
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF-OPEN"

    def __init__(
        self,
        umbral_fallos: int = 3,
        tiempo_recuperacion: float = 10.0,
        *,
        failure_threshold=None,
        recovery_seconds=None,
    ):
        failure_threshold = (
            umbral_fallos if failure_threshold is None else failure_threshold
        )
        recovery_seconds = (
            tiempo_recuperacion if recovery_seconds is None else recovery_seconds
        )
        if not isinstance(failure_threshold, int) or failure_threshold < 1:
            raise ValueError("failure_threshold debe ser un entero mayor que cero")
        if recovery_seconds < 0:
            raise ValueError("recovery_seconds no puede ser negativo")
        self.failure_threshold = failure_threshold
        self.recovery_seconds = float(recovery_seconds)
        self.state = self.CLOSED
        self.consecutive_failures = 0
        self.opened_at = None
        self._probe_in_flight = False
        self._lock = threading.Lock()

    @property
    def estado(self):
        return self.state

    @property
    def fallos(self):
        return self.consecutive_failures

    @property
    def umbral_fallos(self):
        return self.failure_threshold

    @property
    def tiempo_recuperacion(self):
        return self.recovery_seconds

    @property
    def tiempo_apertura(self):
        return self.opened_at or 0.0

    def acquire(self):
        """Permite cobros en CLOSED y una única prueba tras la recuperación."""
        with self._lock:
            if self.state == self.OPEN:
                elapsed = time.monotonic() - self.opened_at
                if elapsed < self.recovery_seconds:
                    logger.warning("Cobro rechazado localmente; circuito OPEN.")
                    raise CircuitOpenError(
                        "Circuito de pagos abierto; reintente tras el periodo de recuperación."
                    )
                self.state = self.HALF_OPEN

            if self.state == self.HALF_OPEN:
                if self._probe_in_flight:
                    logger.warning("Cobro rechazado; prueba HALF-OPEN en curso.")
                    raise CircuitOpenError(
                        "Circuito de pagos HALF-OPEN; ya hay una prueba en curso."
                    )
                self._probe_in_flight = True

    def record_success(self):
        with self._lock:
            recovered = self.state != self.CLOSED
            self.state = self.CLOSED
            self.consecutive_failures = 0
            self.opened_at = None
            self._probe_in_flight = False
        if recovered:
            logger.info("Prueba de pago exitosa; circuito CLOSED.")

    def record_failure(self):
        with self._lock:
            self.consecutive_failures += 1
            self._probe_in_flight = False
            if (
                self.state == self.HALF_OPEN
                or self.consecutive_failures >= self.failure_threshold
            ):
                self.state = self.OPEN
                self.opened_at = time.monotonic()
            state = self.state
            failures = self.consecutive_failures
        logger.warning(
            "Fallo de pago %s/%s; estado=%s.",
            failures,
            self.failure_threshold,
            state,
        )

    # Nombres equivalentes al ejemplo en español.
    def registrar_fallo(self):
        self.record_failure()

    def registrar_exito(self):
        self.record_success()

    def snapshot(self):
        with self._lock:
            if (
                self.state == self.OPEN
                and time.monotonic() - self.opened_at >= self.recovery_seconds
            ):
                self.state = self.HALF_OPEN
            remaining = (
                max(
                    0.0,
                    self.recovery_seconds - (time.monotonic() - self.opened_at),
                )
                if self.state == self.OPEN and self.opened_at is not None
                else 0.0
            )
            return {
                "state": self.state,
                "consecutive_failures": self.consecutive_failures,
                "failure_threshold": self.failure_threshold,
                "recovery_seconds": self.recovery_seconds,
                "remaining_seconds": round(remaining, 1),
                "probe_in_flight": self._probe_in_flight,
            }


def _demo():
    breaker = CircuitBreaker(recovery_seconds=0.1)
    print(f"Inicio: {breaker.snapshot()['state']}")
    for _ in range(breaker.failure_threshold):
        breaker.record_failure()
    print(f"Tras {breaker.failure_threshold} fallos: {breaker.snapshot()['state']}")
    time.sleep(breaker.recovery_seconds + 0.02)
    breaker.acquire()
    print(f"Después de recuperarse: {breaker.snapshot()['state']} (prueba autorizada)")
    breaker.record_success()
    print(f"Prueba exitosa: {breaker.snapshot()['state']}")


if __name__ == "__main__":
    _demo()
