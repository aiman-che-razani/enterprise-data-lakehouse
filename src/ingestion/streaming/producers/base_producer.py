"""
Base Kafka producer for all RetailNexus streaming domains.

Design:
  - Uses confluent_kafka (librdkafka binding) for high-throughput production
  - JSON serialisation with configurable key extraction
  - Delivery callback tracking with per-message error handling
  - Configurable batch sizing and linger time
  - Graceful flush on context manager exit
  - Integrated structured logging and retry via tenacity
"""

from __future__ import annotations

import json
import time
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from confluent_kafka import KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from src.common.config.settings import get_settings
from src.common.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Iterator

logger = get_logger(__name__)


class BaseKafkaProducer(ABC):
    """
    Abstract base for domain Kafka producers.

    Subclass contract:
        topic_name: str          — Kafka topic to produce to
        message_key(record)      — Returns the partition key (str or None)
        transform(record)        — Optional: transform record before producing
    """

    topic_name: str

    def __init__(
        self,
        batch_size: int = 1_000,
        linger_ms: int = 5,
        compression: str = "lz4",
    ) -> None:
        self._settings = get_settings()
        self._log = get_logger(self.__class__.__name__)
        self._batch_size = batch_size
        self._delivered = 0
        self._failed = 0
        self._start_time = time.perf_counter()

        conf = self._build_conf(linger_ms, compression)
        self._producer = Producer(conf)
        self._log.info(
            "producer_initialized",
            topic=self.topic_name,
            brokers=conf["bootstrap.servers"],
        )

    def _build_conf(self, linger_ms: int, compression: str) -> dict[str, Any]:
        settings = self._settings
        conf: dict[str, Any] = {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "linger.ms": linger_ms,
            "batch.size": 65536,
            "compression.type": compression,
            "acks": "all",
            "retries": 5,
            "retry.backoff.ms": 200,
            "enable.idempotence": True,
            "queue.buffering.max.messages": 100_000,
            "queue.buffering.max.kbytes": 131072,
        }
        if settings.kafka_security_protocol:
            conf["security.protocol"] = settings.kafka_security_protocol
        if settings.kafka_sasl_mechanism:
            conf["sasl.mechanism"] = settings.kafka_sasl_mechanism
            conf["sasl.username"] = settings.kafka_sasl_username or ""
            conf["sasl.password"] = (
                settings.kafka_sasl_password.get_secret_value()
                if settings.kafka_sasl_password
                else ""
            )
        return conf

    # ─── Abstract interface ───────────────────────────────────────────────────

    @abstractmethod
    def message_key(self, record: dict[str, Any]) -> str | None:
        """Return the Kafka message key for partitioning."""

    def transform(self, record: dict[str, Any]) -> dict[str, Any]:
        """Override to mutate/enrich a record before serialisation."""
        return record

    # ─── Public API ───────────────────────────────────────────────────────────

    def produce_batch(self, records: list[dict[str, Any]]) -> None:
        """Produce a list of records, polling every batch_size messages."""
        for i, record in enumerate(records):
            self._produce_one(self.transform(record))
            if i > 0 and i % self._batch_size == 0:
                self._producer.poll(0)
                self._log.debug(
                    "producer_progress",
                    topic=self.topic_name,
                    sent=i,
                    total=len(records),
                )
        self.flush()

    def produce_stream(self, records: Iterator[dict[str, Any]]) -> None:
        """Produce from an iterator (memory-efficient for large datasets)."""
        count = 0
        for record in records:
            self._produce_one(self.transform(record))
            count += 1
            if count % self._batch_size == 0:
                self._producer.poll(0)
        self.flush()
        self._log.info("stream_produce_complete", topic=self.topic_name, total=count)

    def flush(self, timeout: float = 30.0) -> None:
        """Flush all pending messages and wait for delivery."""
        remaining = self._producer.flush(timeout)
        if remaining > 0:
            self._log.warning(
                "producer_flush_incomplete",
                topic=self.topic_name,
                remaining=remaining,
            )

    def stats(self) -> dict[str, Any]:
        elapsed = time.perf_counter() - self._start_time
        total = self._delivered + self._failed
        return {
            "topic": self.topic_name,
            "delivered": self._delivered,
            "failed": self._failed,
            "total": total,
            "elapsed_s": round(elapsed, 2),
            "throughput_msg_s": round(total / max(elapsed, 0.001), 0),
        }

    # ─── Private ──────────────────────────────────────────────────────────────

    def _produce_one(self, record: dict[str, Any]) -> None:
        key = self.message_key(record)
        value = json.dumps(record, default=str).encode("utf-8")
        key_bytes = key.encode("utf-8") if key else None

        try:
            self._producer.produce(
                topic=self.topic_name,
                key=key_bytes,
                value=value,
                on_delivery=self._delivery_callback,
            )
        except KafkaException as exc:
            self._failed += 1
            self._log.error(
                "produce_error",
                topic=self.topic_name,
                error=str(exc),
                record_key=key,
            )

    def _delivery_callback(self, err: KafkaError | None, msg: Any) -> None:
        if err:
            self._failed += 1
            self._log.error(
                "delivery_failed",
                topic=self.topic_name,
                error=err.str(),
                key=msg.key().decode("utf-8") if msg.key() else None,
            )
        else:
            self._delivered += 1

    # ─── Context manager ──────────────────────────────────────────────────────

    def __enter__(self) -> BaseKafkaProducer:
        return self

    def __exit__(self, *_: Any) -> None:
        self.flush()
        s = self.stats()
        self._log.info("producer_closed", **s)

    # ─── Topic management ─────────────────────────────────────────────────────

    @classmethod
    def ensure_topic(
        cls,
        topic: str,
        num_partitions: int = 6,
        replication_factor: int = 1,
    ) -> None:
        """Create topic if it doesn't exist (idempotent)."""
        settings = get_settings()
        admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
        metadata = admin.list_topics(timeout=10)
        if topic not in metadata.topics:
            new_topic = NewTopic(topic, num_partitions, replication_factor)
            futures = admin.create_topics([new_topic])
            for t, future in futures.items():
                try:
                    future.result()
                    logger.info("topic_created", topic=t)
                except Exception as exc:
                    logger.warning("topic_create_skipped", topic=t, reason=str(exc))
