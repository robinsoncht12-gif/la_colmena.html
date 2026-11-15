import json
import os
import sqlite3
import threading
from typing import Any, Dict, Optional, Protocol

import pika

from src.interfaces import IOutboxRepository
from src.logger import get_logger

logger = get_logger("OutboxWorker")


class EventPublisher(Protocol):
    def publish(self, event: Dict[str, Any]) -> None:
        ...


class RabbitMQPublisher:
    def __init__(
        self,
        url: Optional[str] = None,
        exchange: str = "bina.events",
        queue: str = "bina.events",
    ):
        self.url = url or os.environ.get(
            "LACOLMENA_RABBITMQ_URL",
            "amqp://guest:guest@localhost:5672/%2F",
        )
        self.exchange = exchange
        self.queue = queue

    def publish(self, event: Dict[str, Any]) -> None:
        parameters = pika.URLParameters(self.url)
        parameters.connection_attempts = 1
        parameters.socket_timeout = 3
        parameters.blocked_connection_timeout = 3
        connection = pika.BlockingConnection(parameters)
        try:
            channel = connection.channel()
            channel.exchange_declare(
                exchange=self.exchange,
                exchange_type="topic",
                durable=True,
            )
            channel.queue_declare(queue=self.queue, durable=True)
            channel.queue_bind(
                queue=self.queue,
                exchange=self.exchange,
                routing_key="bina.#",
            )
            channel.confirm_delivery()
            channel.basic_publish(
                exchange=self.exchange,
                routing_key=event["type"],
                body=json.dumps(
                    event,
                    ensure_ascii=False,
                    allow_nan=False,
                    separators=(",", ":"),
                ),
                properties=pika.BasicProperties(
                    content_type="application/cloudevents+json",
                    delivery_mode=2,
                    message_id=event["id"],
                ),
                mandatory=True,
            )
        finally:
            if connection.is_open:
                connection.close()


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

    def process_pending(self) -> int:
        processed = 0
        for record in self.repository.obtener_pendientes():
            try:
                self.publisher.publish(record["payload"])
            except (pika.exceptions.AMQPError, OSError, ValueError) as error:
                error_detail = repr(error)
                self.repository.registrar_error(record["id"], error_detail)
                logger.error(
                    "No se pudo publicar el evento %s; permanece PENDING: %s",
                    record["payload"].get("id", record["id"]),
                    error_detail,
                )
                break

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
            if processed == 0:
                self._stop_event.wait(self.poll_interval)
