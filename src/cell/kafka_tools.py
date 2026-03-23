"""Kafka inspection tools for agent cell nuclei.

All operations run in thread executors to avoid blocking the event loop.
"""

import asyncio
import json

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient

from src.config import KAFKA_BOOTSTRAP_SERVERS


async def sample_topic(topic: str, count: int = 5) -> list[dict]:
    """Read recent messages from a Kafka topic."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _sample_topic_sync, topic, count)


def _sample_topic_sync(topic: str, count: int) -> list[dict]:
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    metadata = admin.list_topics(topic, timeout=5)
    if topic not in metadata.topics:
        return []

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "sample-reader-ephemeral",
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
    })

    # Get partition info and seek to near the end
    partitions = []
    for pid in metadata.topics[topic].partitions.keys():
        tp = TopicPartition(topic, pid)
        partitions.append(tp)

    # Assign partitions at the end, then seek back `count` messages
    end_offsets = []
    for tp in partitions:
        _, high = consumer.get_watermark_offsets(tp, timeout=5)
        seek_to = max(0, high - count)
        end_offsets.append(TopicPartition(tp.topic, tp.partition, seek_to))

    consumer.assign(end_offsets)

    messages = []
    empty_polls = 0
    while empty_polls < 2 and len(messages) < count:
        msg = consumer.poll(0.5)
        if msg is None:
            empty_polls += 1
            continue
        if msg.error():
            continue
        empty_polls = 0
        try:
            value = json.loads(msg.value())
        except Exception:
            value = msg.value().decode(errors="replace")
        messages.append({
            "partition": msg.partition(),
            "offset": msg.offset(),
            "timestamp": msg.timestamp()[1] if msg.timestamp()[0] else None,
            "key": msg.key().decode() if msg.key() else None,
            "value": value,
        })

    consumer.close()
    return messages[-count:]


async def topic_stats(topics: list[str] | None = None) -> list[dict]:
    """Get message counts and partition info for Kafka topics."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _topic_stats_sync, topics)


def _topic_stats_sync(topics: list[str] | None) -> list[dict]:
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    metadata = admin.list_topics(timeout=5)

    all_topics = sorted(t for t in metadata.topics.keys() if not t.startswith("_"))
    if topics:
        all_topics = [t for t in all_topics if t in topics]

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "stats-reader-ephemeral",
        "enable.auto.commit": False,
    })

    results = []
    for topic_name in all_topics:
        topic_meta = metadata.topics[topic_name]
        partition_count = len(topic_meta.partitions)

        total_messages = 0
        for pid in topic_meta.partitions.keys():
            tp = TopicPartition(topic_name, pid)
            try:
                low, high = consumer.get_watermark_offsets(tp, timeout=3)
                total_messages += high - low
            except Exception:
                pass

        # Classify topic type
        if topic_name.startswith("network."):
            ttype = "source"
        elif topic_name.startswith("agent.decisions"):
            ttype = "decisions"
        elif topic_name.startswith("dlq."):
            ttype = "dlq"
        elif topic_name in ("threats.detected", "traffic.anomalies", "device.health.scores"):
            ttype = "derived"
        else:
            ttype = "other"

        results.append({
            "topic": topic_name,
            "type": ttype,
            "partitions": partition_count,
            "messages": total_messages,
        })

    consumer.close()
    return results
