"""
Kafka producer for the `retailnexus.payments` topic.

Key: order_id (co-partitioned with orders for stream-stream joins).
"""

from __future__ import annotations

import glob
import json
import sys
from pathlib import Path
from typing import Any

from src.common.logging import get_logger, setup_logging
from src.ingestion.streaming.producers.base_producer import BaseKafkaProducer

TOPIC = "retailnexus.payments"
NUM_PARTITIONS = 12


class PaymentsProducer(BaseKafkaProducer):
    topic_name = TOPIC

    def message_key(self, record: dict[str, Any]) -> str | None:
        # Co-partition with orders so stream-stream joins are partition-local
        return record.get("order_id")

    def transform(self, record: dict[str, Any]) -> dict[str, Any]:
        # Mask card details at producer level before Kafka — belt-and-suspenders
        if record.get("card_last4"):
            record = {**record, "card_last4": "****"}
        record.setdefault("_schema_version", "payments/v1")
        return record


def produce_from_files(data_dir: Path, batch_size: int = 5_000) -> None:
    log = get_logger(__name__)
    pattern = str(data_dir / "json" / "payments" / "*.json")
    files = sorted(glob.glob(pattern))
    if not files:
        log.warning("no_payment_files_found", pattern=pattern)
        return

    PaymentsProducer.ensure_topic(TOPIC, num_partitions=NUM_PARTITIONS)

    with PaymentsProducer(batch_size=batch_size) as producer:
        for path in files:
            with open(path, encoding="utf-8") as f:
                records = [json.loads(line) for line in f if line.strip()]
            producer.produce_batch(records)
            log.info("file_produced", file=path, count=len(records))

    log.info("payments_producer_done")


if __name__ == "__main__":
    setup_logging(service_name="payments-producer")
    data_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("datasets/raw")
    produce_from_files(data_dir)
