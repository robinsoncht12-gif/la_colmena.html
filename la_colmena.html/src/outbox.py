import json
import os
import sqlite3
import threading
from typing import Any, Dict, Optional, Protocol

from confluent_kafka import KafkaException, Producer

from src.interfaces import IOutboxRepository
from src.logger import get_logger

logger = get_logger("OutboxWorker")


class EventPublisher(Protocol):
    def publish(self, event: Dict[str, Any]) -> None:
        ...


class KafkaPublisher:
    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        topic: str = "bina.events",
    ):
        self.bootstrap_servers = bootstrap_servers or os.environ.get(
            "LACOLMENA_KAFKA_BOOTSTRAP_SERVERS",
            "127.0.0.1:9092",
        )
        self.topic = topic
        self._producer = Producer(
            {
                "bootstrap.servers": self.bootstrap_servers,
                "client.id": "lacolmena-outbox",
                "acks": "all",
                "enable.idempotence": True,
                "message.timeout.ms": 4000,
            }
        )

    def publish(self, event: Dict[str, Any]) -> None:
        delivery_errors = []

        def delivery_report(error, _):
            if error is not None:
                delivery_errors.append(error)

        self._producer.produce(
            self.topic,
            key=event["id"],
            value=json.dumps(
                event,
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
            ),
            headers=[("content-type", "application/cloudevents+json")],
            callback=delivery_report,
        )
        pending = self._producer.flush(timeout=4.0)
        if pending:
            raise TimeoutError(
                f"Kafka no confirmó la entrega; quedan {pending} mensajes."
            )
        if delivery_errors:
            raise KafkaException(delivery_errors[0])

    def close(self) -> None:
        pending = self._producer.flush(timeout=4.0)
        if pending:
            logger.warning(
                "Kafka conserva %s mensajes sin confirmar al cerrar el publicador.",
                pending,
            )


class OutboxWorker:
    def __init__(
        self,
        repository: IOutboxRepository,
        publisher: EventPublisher,
        poll_interval: float = 1.0,
    ):
        if poll_interval <= 0:
            raise ValueError("El intervalo del worker debe ser mayor que cero.")
        self.repository = repository
        self.publisher = publisher
        self.poll_interval = poll_interval
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._retry_delay = poll_interval
        self._last_attempt_failed = False

    def process_pending(self) -> int:
        processed = 0
        self._last_attempt_failed = False
        for record in self.repository.obtener_pendientes():
            try:
                self.publisher.publish(record["payload"])
            except (
                KafkaException,
                OSError,
                TimeoutError,
                ValueError,
                BufferError,
            ) as error:
                self._last_attempt_failed = True
                self._retry_delay = min(
                    self._retry_delay * 2,
                    max(60.0, self.poll_interval),
                )
                error_detail = repr(error)
                self.repository.registrar_error(record["id"], error_detail)
                logger.error(
                    "No se pudo publicar el evento %s; permanece PENDING: %s",
                    record["payload"].get("id", record["id"]),
                    error_detail,
                )
                break

            self._retry_delay = self.poll_interval
            if not self.repository.marcar_publicado(record["id"]):
                raise RuntimeError(
                    f"No se pudo marcar PROCESSED el evento de outbox {record['id']}."
                )
            processed += 1
        return processed

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="lacolmena-outbox",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=max(5.0, self.poll_interval + 3.0))

    def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                processed = self.process_pending()
            except sqlite3.Error:
                logger.exception("Error de base de datos en el worker de outbox")
                processed = 0
            if processed == 0 or self._last_attempt_failed:
                delay = (
                    self._retry_delay
                    if self._last_attempt_failed
                    else self.poll_interval
                )
                self._stop_event.wait(delay)
