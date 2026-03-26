"""Consumer spawning and management for agent cells.

Supports two runtimes:
- **python**: The nucleus authors Python code that runs in-process via exec().
  Has full access to the knowledge API (custom tables, embeddings, semantic search).
- **flink_sql**: The nucleus authors Flink SQL that runs as a Flink job via the
  Flink SQL Gateway. Handles windowed aggregations, joins, CEP. No knowledge API.

The ConsumerManager dispatches to the appropriate runtime based on ConsumerSpec.runtime.
"""

import asyncio
import json
import time
import traceback
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone

import orjson
from confluent_kafka import Consumer, Producer, KafkaError

from src.config import KAFKA_BOOTSTRAP_SERVERS
from src.cell.knowledge import KnowledgeStore


@dataclass
class ConsumerSpec:
    """Specification for a dynamically authored consumer."""
    consumer_id: str
    source_topics: list[str]
    output_topic: str
    consumer_code: str  # Python source or Flink SQL authored by the nucleus
    description: str = ""  # Human-readable description of what this consumer does
    detection_patterns: list[str] = field(default_factory=list)  # What patterns it detects
    knowledge_tables: list[str] = field(default_factory=list)  # What tables it creates/uses
    runtime: str = "python"  # "python" | "flink_sql"


@dataclass
class ManagedConsumer:
    """A running consumer with its stats."""
    spec: ConsumerSpec
    task: asyncio.Task | None = None
    events_processed: int = 0
    alerts_emitted: int = 0
    last_event_time: float = 0
    errors: int = 0
    dlq_topic: str = ""
    running: bool = False
    flink_job_id: str | None = None

    def to_dict(self, include_code: bool = False) -> dict:
        d = {
            "consumer_id": self.spec.consumer_id,
            "source_topics": self.spec.source_topics,
            "output_topic": self.spec.output_topic,
            "description": self.spec.description,
            "detection_patterns": self.spec.detection_patterns,
            "knowledge_tables": self.spec.knowledge_tables,
            "runtime": self.spec.runtime,
            "events_processed": self.events_processed,
            "alerts_emitted": self.alerts_emitted,
            "errors": self.errors,
            "dlq_topic": self.dlq_topic,
            "running": self.running,
        }
        if self.flink_job_id:
            d["flink_job_id"] = self.flink_job_id
        if include_code:
            d["consumer_code"] = self.spec.consumer_code
        return d


class ConsumerKnowledge:
    """Knowledge interface exposed to nucleus-authored consumer code.

    This is the API that authored code calls to build and query its own
    persistent knowledge structures — tables, embeddings, key-value state.
    """

    def __init__(self, store: KnowledgeStore):
        self._store = store

    def create_table(self, sql: str):
        """Create a custom table in the cell's schema.

        Example:
            knowledge.create_table('''
                CREATE TABLE IF NOT EXISTS ip_reputation (
                    ip VARCHAR(45) PRIMARY KEY,
                    score FLOAT DEFAULT 0,
                    first_seen TIMESTAMPTZ DEFAULT NOW(),
                    last_seen TIMESTAMPTZ DEFAULT NOW(),
                    event_count INT DEFAULT 0
                )
            ''')
        """
        self._store.execute(sql)

    def query(self, sql: str, params: tuple = ()) -> list[tuple]:
        """Run a SELECT query against the cell's schema.

        Tables created via create_table can be referenced by name directly.
        """
        return self._store.execute(sql, params)

    def execute(self, sql: str, params: tuple = ()):
        """Run an INSERT/UPDATE/DELETE against the cell's schema."""
        self._store.execute(sql, params)

    def store_embedding(self, content: str, category: str, metadata: dict | None = None):
        """Store text with vector embedding for semantic search. Chunks long content automatically."""
        self._store.store(content, category, metadata)

    def search(self, query: str, limit: int = 5) -> list[dict]:
        """Semantic search over stored embeddings."""
        return self._store.semantic_search(query, limit)

    def hybrid_search(self, query: str, limit: int = 5, keyword: str | None = None) -> list[dict]:
        """Hybrid search: vector similarity + full-text search merged via RRF."""
        return self._store.hybrid_search(query, limit, keyword)

    def fts_search(self, query: str, limit: int = 5) -> list[dict]:
        """Full-text keyword search over stored knowledge."""
        return self._store.fts_search(query, limit)

    def get(self, key: str) -> dict | None:
        """Get a persistent key-value entry."""
        return self._store.get_state(key)

    def set(self, key: str, value: dict):
        """Set a persistent key-value entry (survives restarts)."""
        self._store.set_state(key, value)


def _compile_consumer_code(code: str) -> tuple[callable, callable]:
    """Compile nucleus-authored code and extract functions.

    The authored code must define:
        process_event(event: dict, state: dict, knowledge: ConsumerKnowledge) -> list[dict]

    It may also define:
        init(state: dict, knowledge: ConsumerKnowledge) -> None
            Called once at startup to create tables, load persisted state, etc.

    Available in scope: time, datetime, timezone, defaultdict, math, re, json.
    """
    import decimal
    import json as json_mod
    import math
    import re

    sandbox_globals = {
        "__builtins__": __builtins__,
        "time": time,
        "datetime": datetime,
        "timezone": timezone,
        "defaultdict": defaultdict,
        "math": math,
        "re": re,
        "json": json_mod,
        "Decimal": decimal.Decimal,
        "float": float,  # ensure float() is available for Decimal conversion
    }

    exec(code, sandbox_globals)

    process_event = sandbox_globals.get("process_event")
    if process_event is None:
        raise ValueError("Authored code must define process_event(event, state, knowledge) -> list[dict]")

    init_fn = sandbox_globals.get("init", lambda state, knowledge: None)
    return process_event, init_fn


class ConsumerManager:
    """Manages dynamically authored consumers for a cell.

    Dispatches to the appropriate runtime (Python or Flink SQL) based on
    ConsumerSpec.runtime.
    """

    def __init__(self, cell_id: str, cell_name: str, knowledge_store: KnowledgeStore):
        self.cell_id = cell_id
        self.cell_name = cell_name
        self.knowledge_store = knowledge_store
        self.consumers: dict[str, ManagedConsumer] = {}
        self._generation: int = 0  # incremented on each spawn to avoid consumer group conflicts
        self._flink_runtime = None  # lazy init

    def _get_flink_runtime(self):
        """Lazy-init the Flink runtime (avoids import/connection when not needed)."""
        if self._flink_runtime is None:
            from src.cell.flink_runtime import FlinkRuntime
            self._flink_runtime = FlinkRuntime()
        return self._flink_runtime

    def spawn(self, spec: ConsumerSpec) -> ManagedConsumer:
        """Dispatch to the appropriate runtime based on spec.runtime."""
        if spec.runtime == "flink_sql":
            return self._spawn_flink(spec)
        return self._spawn_python(spec)

    def _spawn_python(self, spec: ConsumerSpec) -> ManagedConsumer:
        """Compile the authored Python code and spawn as an asyncio task."""
        process_event, init_fn = _compile_consumer_code(spec.consumer_code)

        knowledge = ConsumerKnowledge(self.knowledge_store)
        state = {}
        init_fn(state, knowledge)

        # Ensure output and DLQ topics exist before consumer starts
        self._ensure_topics(spec.output_topic, f"dlq.{self.cell_id}.{spec.consumer_id}")

        self._generation += 1
        managed = ManagedConsumer(spec=spec, running=True)
        managed.dlq_topic = f"dlq.{self.cell_id}.{spec.consumer_id}"
        managed.task = asyncio.create_task(
            self._run_consumer(managed, process_event, state, knowledge)
        )
        self.consumers[spec.consumer_id] = managed
        return managed

    def _spawn_flink(self, spec: ConsumerSpec) -> ManagedConsumer:
        """Submit Flink SQL to the Flink cluster and track the job."""
        self._ensure_topics(spec.output_topic)
        flink = self._get_flink_runtime()
        job_id = flink.submit(spec)
        managed = ManagedConsumer(spec=spec, running=True, flink_job_id=job_id)
        self.consumers[spec.consumer_id] = managed
        print(f"  [{self.cell_id}] Flink job '{spec.consumer_id}' submitted → job_id={job_id}")
        return managed

    async def _run_consumer(
        self,
        managed: ManagedConsumer,
        process_event: callable,
        state: dict,
        knowledge: ConsumerKnowledge,
    ):
        """Kafka read loop → authored process_event → emit alerts to output topic.

        Runs the blocking Kafka poll + processing in a thread executor so it
        doesn't starve the server's asyncio event loop.
        """
        loop = asyncio.get_event_loop()

        def _blocking_consumer_loop():
            consumer = Consumer({
                "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
                "group.id": f"cell-{self.cell_id}-{managed.spec.consumer_id}-gen{self._generation}",
                "auto.offset.reset": "latest",
                "enable.auto.commit": True,
            })
            consumer.subscribe(managed.spec.source_topics)

            producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

            print(f"  [{self.cell_id}] Consumer '{managed.spec.consumer_id}' started on {managed.spec.source_topics} → {managed.spec.output_topic}")

            try:
                while managed.running:
                    msg = consumer.poll(0.5)
                    if msg is None:
                        continue
                    if msg.error():
                        if msg.error().code() != KafkaError._PARTITION_EOF:
                            print(f"  [{self.cell_id}] Consumer error: {msg.error()}")
                        continue

                    try:
                        event = orjson.loads(msg.value())
                    except Exception:
                        continue

                    managed.events_processed += 1
                    managed.last_event_time = time.time()

                    try:
                        alerts = process_event(event, state, knowledge)
                        if alerts:
                            for alert in alerts:
                                alert.setdefault("timestamp", datetime.now(timezone.utc).isoformat())
                                alert.setdefault("cell_id", self.cell_id)
                                alert.setdefault("cell_name", self.cell_name)
                                producer.produce(
                                    managed.spec.output_topic,
                                    key=self.cell_id.encode(),
                                    value=json.dumps(alert, default=str).encode(),
                                )
                                managed.alerts_emitted += 1
                            producer.poll(0)
                    except Exception as e:
                        managed.errors += 1
                        # Write to DLQ
                        dlq_entry = {
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                            "cell_id": self.cell_id,
                            "consumer_id": managed.spec.consumer_id,
                            "error": str(e),
                            "error_type": type(e).__name__,
                            "traceback": traceback.format_exc(),
                            "event": event,
                        }
                        try:
                            producer.produce(
                                managed.dlq_topic,
                                key=self.cell_id.encode(),
                                value=json.dumps(dlq_entry, default=str).encode(),
                            )
                            producer.poll(0)
                        except Exception:
                            pass
                        if managed.errors <= 5:
                            print(f"  [{self.cell_id}] Consumer '{managed.spec.consumer_id}' error → DLQ: {e}")
            finally:
                consumer.close()
                producer.flush(5)
                managed.running = False
                print(f"  [{self.cell_id}] Consumer '{managed.spec.consumer_id}' stopped ({managed.events_processed} events, {managed.alerts_emitted} alerts)")

        try:
            await loop.run_in_executor(None, _blocking_consumer_loop)
        except asyncio.CancelledError:
            managed.running = False

    @staticmethod
    def _ensure_topics(*topics: str):
        """Create Kafka topics if they don't exist."""
        from confluent_kafka.admin import AdminClient, NewTopic
        admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
        existing = set(admin.list_topics(timeout=5).topics.keys())
        to_create = [NewTopic(t, num_partitions=1, replication_factor=1) for t in topics if t and t not in existing]
        if to_create:
            futures = admin.create_topics(to_create)
            for topic, future in futures.items():
                try:
                    future.result()
                    print(f"  Created topic: {topic}")
                except Exception as e:
                    if "TOPIC_ALREADY_EXISTS" not in str(e):
                        print(f"  Warning: could not create topic {topic}: {e}")

    def stop(self, consumer_id: str):
        if consumer_id in self.consumers:
            managed = self.consumers[consumer_id]
            managed.running = False
            if managed.flink_job_id:
                try:
                    flink = self._get_flink_runtime()
                    flink.cancel(managed.flink_job_id)
                except Exception as e:
                    print(f"  [{self.cell_id}] Warning: could not cancel Flink job {managed.flink_job_id}: {e}")
            elif managed.task:
                managed.task.cancel()

    def stop_all(self):
        for cid in list(self.consumers.keys()):
            self.stop(cid)

    def list_consumers(self, include_code: bool = False) -> list[dict]:
        return [m.to_dict(include_code=include_code) for m in self.consumers.values()]

    def get_consumer_code(self, consumer_id: str) -> str | None:
        managed = self.consumers.get(consumer_id)
        return managed.spec.consumer_code if managed else None

    def total_events(self) -> int:
        return sum(m.events_processed for m in self.consumers.values())

    @staticmethod
    async def read_dlq(dlq_topic: str, limit: int = 20) -> list[dict]:
        """Read recent entries from a consumer's DLQ topic. Runs in thread to avoid blocking."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, ConsumerManager._read_dlq_sync, dlq_topic, limit)

    @staticmethod
    def _read_dlq_sync(dlq_topic: str, limit: int) -> list[dict]:
        from confluent_kafka import Consumer as KConsumer, TopicPartition

        consumer = KConsumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "dlq-reader-ephemeral",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })

        metadata = consumer.list_topics(dlq_topic, timeout=5)
        if dlq_topic not in metadata.topics:
            consumer.close()
            return []

        partitions = [
            TopicPartition(dlq_topic, p, 0)
            for p in metadata.topics[dlq_topic].partitions.keys()
        ]
        consumer.assign(partitions)

        messages = []
        empty_polls = 0
        max_read = limit * 5  # read at most 5x the limit to avoid infinite loop
        reads = 0
        while empty_polls < 2 and reads < max_read:
            msg = consumer.poll(0.5)
            if msg is None:
                empty_polls += 1
                continue
            if msg.error():
                continue
            empty_polls = 0
            reads += 1
            try:
                messages.append(json.loads(msg.value()))
            except Exception:
                pass

        consumer.close()
        return messages[-limit:]
