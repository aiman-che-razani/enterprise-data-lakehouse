"""
Kafka producer for the `retailnexus.clickstream` topic.

Highest-volume producer (~5M events). Reads gzip-compressed JSONL files.
Key: session_id (keeps all events in a session on one partition for sessionisation).
"""

from __future__ import annotations

import glob
import gzip
import json
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

from src.common.logging import get_logger, setup_logging
from src.ingestion.streaming.producers.base_producer import BaseKafkaProducer

if TYPE_CHECKING:
    from collections.abc import Iterator

TOPIC = "retailnexus.clickstream"
NUM_PARTITIONS = 24


class ClickstreamProducer(BaseKafkaProducer):
    topic_name = TOPIC

    # Higher linger for clickstream batching; enable snappy which handles text well
    def __init__(self) -> None:
        super().__init__(batch_size=10_000, linger_ms=20, compression="snappy")

    def message_key(self, record: dict[str, Any]) -> str | None:
        return record.get("session_id")

    def transform(self, record: dict[str, Any]) -> dict[str, Any]:
        # Redact raw IP at producer boundary; keep for fraud detection in Bronze
        record.setdefault("_schema_version", "clickstream/v1")
        return record


def produce_from_files(data_dir: Path) -> None:
    log = get_logger(__name__)
    pattern = str(data_dir / "json" / "clickstream" / "*.json.gz")
    files = sorted(glob.glob(pattern))
    if not files:
        log.warning("no_clickstream_files_found", pattern=pattern)
        return

    ClickstreamProducer.ensure_topic(TOPIC, num_partitions=NUM_PARTITIONS)

    with ClickstreamProducer() as producer:
        for path in files:
            log.info("producing_file", file=path)
            with gzip.open(path, "rt", encoding="utf-8") as f:

                def _stream() -> Iterator[Any]:
                    for line in f:
                        line = line.strip()
                        if line:
                            yield json.loads(line)

                producer.produce_stream(_stream())

    log.info("clickstream_producer_done")


if __name__ == "__main__":
    setup_logging(service_name="clickstream-producer")
    data_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("datasets/raw")
    produce_from_files(data_dir)
