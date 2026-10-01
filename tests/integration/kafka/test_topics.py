"""
Kafka broker checks — the topics the streaming consumers subscribe to must exist.

    KAFKA_BOOTSTRAP_SERVERS=localhost:9092 pytest tests/integration/kafka -m integration

Skips unless KAFKA_BOOTSTRAP_SERVERS is set and confluent-kafka is installed.
"""

from __future__ import annotations

import os
import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

_ROOT = Path(__file__).resolve().parents[3]


@pytest.fixture(scope="module")
def admin():
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    if not bootstrap:
        pytest.skip("KAFKA_BOOTSTRAP_SERVERS not set")
    confluent_kafka = pytest.importorskip("confluent_kafka.admin")
    return confluent_kafka.AdminClient({"bootstrap.servers": bootstrap})


@pytest.fixture(scope="module")
def broker_topics(admin):
    return set(admin.list_topics(timeout=10).topics)


def _declared_topics() -> set[str]:
    text = (_ROOT / "scripts" / "setup" / "create_kafka_topics.sh").read_text(encoding="utf-8")
    return set(re.findall(r'create_topic\s+"([^"]+)"', text))


def test_every_topic_from_the_setup_script_exists(broker_topics):
    missing = _declared_topics() - broker_topics
    assert not missing, f"run scripts/setup/create_kafka_topics.sh — missing: {sorted(missing)}"


def test_streaming_consumers_can_subscribe(broker_topics):
    from src.processing.bronze.domain_meta import DOMAIN_KAFKA_TOPICS

    assert set(DOMAIN_KAFKA_TOPICS.values()) <= broker_topics


def test_high_volume_topics_are_partitioned_for_parallelism(admin):
    meta = admin.list_topics(timeout=10).topics
    for topic in ("retailnexus.clickstream", "retailnexus.iot-sensors"):
        assert (
            len(meta[topic].partitions) >= 12
        ), f"{topic} needs partitions for parallel consumption"
