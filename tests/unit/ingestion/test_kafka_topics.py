"""Kafka topic names must agree across producers metadata, Spark consumers, and topic creation."""

from __future__ import annotations

import re
from pathlib import Path

from src.processing.bronze.domain_meta import DOMAIN_KAFKA_TOPICS

_ROOT = Path(__file__).resolve().parents[3]


def _script_topics() -> set[str]:
    text = (_ROOT / "scripts" / "setup" / "create_kafka_topics.sh").read_text(encoding="utf-8")
    return set(re.findall(r'create_topic\s+"([^"]+)"', text))


def _consumer_topics() -> dict[str, str]:
    """stream module name → topic literal declared on the consumer class."""
    out: dict[str, str] = {}
    for path in (_ROOT / "spark" / "streaming").glob("*_stream.py"):
        m = re.search(r'^\s*topic\s*=\s*"([^"]+)"', path.read_text(encoding="utf-8"), re.M)
        if m:
            out[path.stem] = m.group(1)
    return out


def test_every_streaming_domain_topic_is_created_by_the_setup_script():
    missing = set(DOMAIN_KAFKA_TOPICS.values()) - _script_topics()
    assert not missing, f"topics used by Bronze streaming but never created: {sorted(missing)}"


def test_every_spark_consumer_reads_a_declared_topic():
    consumers = _consumer_topics()
    assert len(consumers) == 5
    assert set(consumers.values()) == set(DOMAIN_KAFKA_TOPICS.values())


def test_consumer_topic_matches_its_domain():
    consumers = _consumer_topics()
    for domain, topic in DOMAIN_KAFKA_TOPICS.items():
        stem = {"iot_sensors": "sensors_stream"}.get(domain, f"{domain}_stream")
        assert consumers[stem] == topic, f"{stem} consumes {consumers[stem]}, metadata says {topic}"


def test_topic_names_are_namespaced_and_unique():
    topics = list(DOMAIN_KAFKA_TOPICS.values())
    assert len(topics) == len(set(topics))
    assert all(t.startswith("retailnexus.") for t in topics)


def test_dead_letter_topics_exist_for_transactional_streams():
    script = _script_topics()
    assert {"retailnexus.dlq.orders", "retailnexus.dlq.payments"} <= script
