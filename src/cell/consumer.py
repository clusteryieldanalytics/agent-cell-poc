"""Consumer spawning and management for agent cells.

The nucleus authors Python consumer code dynamically. This module provides
the Kafka read/write scaffold and executes the authored code. The nucleus
writes a `process_event(event, state, knowledge)` function that:
- Processes each event
- Maintains in-memory state via `state` dict
- Builds persistent knowledge via the `knowledge` interface
- Returns alert dicts to emit to the output topic
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
    consumer_code: str  # Python source authored by the nucleus
    description: str = ""  # Human-readable description of what this consumer does
    detection_patterns: list[str] = field(default_factory=list)  # What patterns it detects
    knowledge_tables: list[str] = field(default_factory=list)  # What tables it creates/uses


@dataclass
class ManagedConsumer:
    """A running consumer with its stats."""
    spec: ConsumerSpec
    task: asyncio.Task | None = None
    events_processed: int = 0
    alerts_emitted: int = 0
    last_event_time: float = 0
    errors: int = 0
    running: bool = False

    def to_dict(self, include_code: bool = False) -> dict:
        d = {
            "consumer_id": self.spec.consumer_id,
            "source_topics": self.spec.source_topics,
            "output_topic": self.spec.output_topic,
            "description": self.spec.description,
            "detection_patterns": self.spec.detection_patterns,
            "knowledge_tables": self.spec.knowledge_tables,
            "events_processed": self.events_processed,
            "alerts_emitted": self.alerts_emitted,
            "errors": self.errors,
            "running": self.running,
        }
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
    """Manages dynamically authored consumers for a cell."""

    def __init__(self, cell_id: str, cell_name: str, knowledge_store: KnowledgeStore):
        self.cell_id = cell_id
        self.cell_name = cell_name
        self.knowledge_store = knowledge_store
        self.consumers: dict[str, ManagedConsumer] = {}

    def spawn(self, spec: ConsumerSpec) -> ManagedConsumer:
        """Compile the authored code and spawn the consumer as an asyncio task."""
        process_event, init_fn = _compile_consumer_code(spec.consumer_code)

        knowledge = ConsumerKnowledge(self.knowledge_store)
        state = {}
        init_fn(state, knowledge)

        managed = ManagedConsumer(spec=spec, running=True)
        managed.task = asyncio.create_task(
            self._run_consumer(managed, process_event, state, knowledge)
        )
        self.consumers[spec.consumer_id] = managed
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
                "group.id": f"cell-{self.cell_id}-{managed.spec.consumer_id}",
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
                    except Exception:
                        managed.errors += 1
                        if managed.errors <= 5:
                            traceback.print_exc()
            finally:
                consumer.close()
                producer.flush(5)
                managed.running = False
                print(f"  [{self.cell_id}] Consumer '{managed.spec.consumer_id}' stopped ({managed.events_processed} events, {managed.alerts_emitted} alerts)")

        try:
            await loop.run_in_executor(None, _blocking_consumer_loop)
        except asyncio.CancelledError:
            managed.running = False

    def stop(self, consumer_id: str):
        if consumer_id in self.consumers:
            managed = self.consumers[consumer_id]
            managed.running = False
            if managed.task:
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
