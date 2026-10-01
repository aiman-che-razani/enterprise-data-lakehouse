"""
Kafka producer for the `retailnexus.inventory` topic.

Emits inventory snapshot events (SKU × store × date).
Key: {store_id}#{sku} (keeps all snapshots for a location together).
"""

from __future__ import annotations

import glob
import json
import sys
from pathlib import Path
from typing import Any

from src.common.logging import get_logger, setup_logging
from src.ingestion.streaming.producers.base_producer import BaseKafkaProducer

TOPIC = "retailnexus.inventory"
NUM_PARTITIONS = 6


class InventoryProducer(BaseKafkaProducer):
    topic_name = TOPIC

    def message_key(self, record: dict[str, Any]) -> str | None:
        store = record.get("store_id", "")
        sku = record.get("sku", "")
        return f"{store}#{sku}"

    def transform(self, record: dict[str, Any]) -> dict[str, Any]:
        record.setdefault("_schema_version", "inventory/v1")
        return record


def produce_from_files(data_dir: Path, batch_size: int = 5_000) -> None:
    log = get_logger(__name__)
    pattern = str(data_dir / "json" / "inventory" / "*.json")
    files = sorted(glob.glob(pattern))
    if not files:
        log.warning("no_inventory_files_found", pattern=pattern)
        return

    InventoryProducer.ensure_topic(TOPIC, num_partitions=NUM_PARTITIONS)

    with InventoryProducer(batch_size=batch_size) as producer:
        for path in files:
            with open(path, encoding="utf-8") as f:
                records = [json.loads(line) for line in f if line.strip()]
            producer.produce_batch(records)
            log.info("file_produced", file=path, count=len(records))

    log.info("inventory_producer_done")


if __name__ == "__main__":
    setup_logging(service_name="inventory-producer")
    data_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("datasets/raw")
    produce_from_files(data_dir)
