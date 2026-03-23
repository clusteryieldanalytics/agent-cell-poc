"""AgentCell — the core unit of the agent cell architecture."""

import asyncio
import json
from datetime import datetime, timezone
from enum import Enum

import orjson
import psycopg
from confluent_kafka import Producer

from src.config import KAFKA_BOOTSTRAP_SERVERS, POSTGRES_URL
from src.cell.consumer import ConsumerManager, ConsumerSpec
from src.cell.knowledge import KnowledgeStore
from src.cell.nucleus import Nucleus


class CellStatus(Enum):
    INITIALIZING = "initializing"
    ACTIVE = "active"
    PAUSED = "paused"
    TERMINATED = "terminated"


class AgentCell:
    """A fully autonomous agent cell with nucleus, consumers, and knowledge base.

    The nucleus reasons at spawn time about what infrastructure to provision.
    Consumers run nucleus-authored code that handles event processing and alert
    emission autonomously. Consumer specs are persisted to Postgres so cells
    can be reloaded after server restart.
    """

    def __init__(self, cell_id: str, name: str, directive: str):
        self.cell_id = cell_id
        self.name = name
        self.directive = directive
        self.status = CellStatus.INITIALIZING

        # Core components
        self.knowledge = KnowledgeStore(cell_id)
        self.nucleus = Nucleus(cell_id, directive, self.knowledge)
        self.consumer_manager = ConsumerManager(cell_id, name, self.knowledge)

        # Decision log producer
        self.decision_topic = f"agent.decisions.{cell_id}"
        self._decision_producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    async def propose(self) -> list[dict]:
        """Phase 1: Initialize the cell and ask the nucleus to propose consumers.
        Returns the proposed decisions for operator review without spawning anything."""
        print(f"[{self.name}] Starting cell...")
        self.knowledge.initialize()
        self.status = CellStatus.INITIALIZING
        self._register()

        print(f"[{self.name}] Nucleus reasoning about directive...")
        decisions = await self.nucleus.reason(
            "You have just been activated. Review the available Kafka topics and their schemas. "
            "Your job is to author and spawn Kafka consumers that fulfill your directive. "
            "For each consumer, write the complete Python processing logic — the init() and "
            "process_event() functions — that will run autonomously on every event. "
            "Use the spawn_consumer tool now to create your consumers. You MUST call spawn_consumer "
            "at least once."
        )

        print(f"[{self.name}] Nucleus proposed {len(decisions)} decisions")
        return decisions

    async def approve(self, decisions: list[dict]):
        """Phase 2: Execute approved decisions and activate the cell."""
        self.status = CellStatus.ACTIVE

        for decision in decisions:
            print(f"  [{self.name}] Executing: {decision.get('decision_type')}")
            await self._execute_decision(decision)

        self._decision_producer.flush(5)
        self._persist_consumers()
        self._update_status("active")
        print(f"[{self.name}] Cell active with {len(self.consumer_manager.consumers)} consumers")

    async def start(self):
        """Initialize, propose, and immediately approve all consumers (no review)."""
        decisions = await self.propose()
        await self.approve(decisions)

    async def reload(self):
        """Reload a cell from persisted state — no nucleus call needed."""
        print(f"[{self.name}] Reloading cell from persisted state...")
        self.knowledge.initialize()
        self.status = CellStatus.ACTIVE

        persisted = self._load_persisted_consumers()
        if not persisted:
            print(f"  [{self.name}] No persisted consumers found")
            return

        for entry in persisted:
            spec = entry["spec"]
            try:
                managed = self.consumer_manager.spawn(spec)
                # Restore counters from before shutdown
                managed.events_processed = entry.get("events_processed", 0)
                managed.alerts_emitted = entry.get("alerts_emitted", 0)
                print(f"  [{self.name}] Reloaded consumer '{spec.consumer_id}' ({managed.events_processed} prior events)")
            except Exception as e:
                print(f"  [{self.name}] Failed to reload consumer '{spec.consumer_id}': {e}")

        print(f"[{self.name}] Reloaded with {len(self.consumer_manager.consumers)} consumers")

    async def stop(self):
        """Gracefully stop the cell. Persisted state is retained for reload."""
        self.status = CellStatus.TERMINATED
        self._persist_consumers()  # save latest counters before stopping
        self.consumer_manager.stop_all()
        self._decision_producer.flush(5)
        self._update_status("stopped")
        print(f"[{self.name}] Cell stopped")

    async def destroy(self):
        """Stop and clean up all resources including persisted state."""
        await self.stop()
        self.knowledge.destroy()
        self._unregister()
        print(f"[{self.name}] Cell destroyed")

    async def purge(self) -> list[str]:
        """Stop and destroy ALL resources — Postgres schema, Kafka topics, registry entry.
        Returns a log of what was purged."""
        purged = []

        # Stop consumers
        if self.consumer_manager.consumers:
            self.consumer_manager.stop_all()
            purged.append(f"Stopped {len(self.consumer_manager.consumers)} consumer(s)")

        self._decision_producer.flush(5)

        # Drop Postgres schema (knowledge tables + any custom tables)
        try:
            stats = self.knowledge.stats()
            table_names = list(stats.get("tables", {}).keys())
            purged.append(f"Dropping schema {self.knowledge.schema} ({len(table_names)} tables: {', '.join(table_names)})")
        except Exception:
            purged.append(f"Dropping schema {self.knowledge.schema}")
        self.knowledge.destroy()

        # Delete Kafka topics (decision log + derived output topics)
        topics_to_delete = [self.decision_topic]
        for managed in self.consumer_manager.consumers.values():
            if managed.spec.output_topic:
                topics_to_delete.append(managed.spec.output_topic)
        # Also check the registry for output topics (in case consumers were already stopped)
        try:
            with psycopg.connect(POSTGRES_URL) as conn:
                row = conn.execute(
                    "SELECT topics_produced FROM public.cells WHERE cell_id = %s", (self.cell_id,)
                ).fetchone()
                if row and row[0]:
                    produced = row[0] if isinstance(row[0], list) else json.loads(row[0])
                    for t in produced:
                        if t not in topics_to_delete:
                            topics_to_delete.append(t)
        except Exception:
            pass

        topics_to_delete = list(set(topics_to_delete))
        try:
            from confluent_kafka.admin import AdminClient
            admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
            futures = admin.delete_topics(topics_to_delete, operation_timeout=10)
            for topic, future in futures.items():
                try:
                    future.result()
                    purged.append(f"Deleted Kafka topic: {topic}")
                except Exception as e:
                    purged.append(f"Could not delete topic {topic}: {e}")
        except Exception as e:
            purged.append(f"Could not connect to Kafka admin: {e}")

        # Remove from cells registry
        self._unregister()
        purged.append(f"Removed from cells registry")

        self.status = CellStatus.TERMINATED
        print(f"[{self.name}] Cell purged ({len(purged)} actions)")
        return purged

    def pause(self):
        self.status = CellStatus.PAUSED
        self.consumer_manager.stop_all()
        self._update_status("paused")

    def resume(self):
        self.status = CellStatus.ACTIVE
        self._update_status("active")

    # Tools that modify consumers — require operator approval
    DEPLOYMENT_TOOLS = {"replace_consumer", "spawn_consumer", "remove_consumer"}

    async def chat(self, message: str) -> tuple[str, list[dict]]:
        """Chat with the cell's nucleus.

        Returns (reply, pending_actions) where pending_actions are code changes
        that need operator approval before deployment.
        """
        pending_actions = []

        async def _intercept_tool(tool_name: str, tool_input: dict) -> str:
            if tool_name in self.DEPLOYMENT_TOOLS:
                # Don't execute — stash for operator approval
                pending_actions.append({"type": tool_name, **tool_input})
                return f"Acknowledged — {tool_name} will be presented to the operator for approval before deployment."
            else:
                # Read-only tools execute immediately
                return await self._handle_chat_tool_action(tool_name, tool_input)

        context = self._build_context()
        reply = await self.nucleus.chat(message, context, on_tool_action=_intercept_tool)
        return reply, pending_actions

    async def deploy_action(self, action: dict) -> str:
        """Execute an operator-approved deployment action."""
        tool_name = action.get("type")
        if tool_name == "replace_consumer":
            return await self.replace_consumer(action)
        elif tool_name == "spawn_consumer":
            return await self.add_consumer(self._spec_from_action(action))
        elif tool_name == "remove_consumer":
            return await self.remove_consumer(action["consumer_id"])
        return f"Unknown action: {tool_name}"

    def inspect(self) -> dict:
        """Return full cell state for inspection."""
        kb_stats = {}
        try:
            kb_stats = self.knowledge.stats()
        except Exception:
            kb_stats = {"error": "Could not query knowledge base"}

        return {
            "cell_id": self.cell_id,
            "name": self.name,
            "directive": self.directive,
            "status": self.status.value,
            "consumers": self.consumer_manager.list_consumers(),
            "events_processed": self.consumer_manager.total_events(),
            "knowledge_base": kb_stats,
            "decision_topic": self.decision_topic,
        }

    # --- Consumer management (used by chat refinement) ---

    @staticmethod
    def _spec_from_action(action: dict) -> ConsumerSpec:
        """Build a ConsumerSpec from a tool action dict."""
        return ConsumerSpec(
            consumer_id=action.get("consumer_id", ""),
            source_topics=action.get("source_topics", []),
            output_topic=action.get("output_topic", ""),
            consumer_code=action.get("consumer_code", ""),
            description=action.get("description", ""),
            detection_patterns=action.get("detection_patterns", []),
            knowledge_tables=action.get("knowledge_tables", []),
        )

    async def replace_consumer(self, action: dict) -> str:
        """Hot-swap a consumer's code. Stops old, spawns new with updated code + metadata."""
        consumer_id = action["consumer_id"]
        old = self.consumer_manager.consumers.get(consumer_id)
        if old is None:
            return f"Consumer '{consumer_id}' not found"

        new_spec = ConsumerSpec(
            consumer_id=consumer_id,
            source_topics=old.spec.source_topics,
            output_topic=old.spec.output_topic,
            consumer_code=action.get("consumer_code", old.spec.consumer_code),
            description=action.get("description", old.spec.description),
            detection_patterns=action.get("detection_patterns", old.spec.detection_patterns),
            knowledge_tables=action.get("knowledge_tables", old.spec.knowledge_tables),
        )

        # Stop old consumer
        self.consumer_manager.stop(consumer_id)
        # Wait for task to actually finish
        if old.task:
            try:
                await asyncio.wait_for(old.task, timeout=5)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
        del self.consumer_manager.consumers[consumer_id]

        # Spawn new
        try:
            self.consumer_manager.spawn(new_spec)
            self._persist_consumers()
            self._log_decision({
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "cell_id": self.cell_id,
                "decision_type": "replace_consumer",
                "reasoning": "Consumer code updated via interactive refinement",
                "action": {"type": "replace_consumer", **action},
            })
            return f"Consumer '{consumer_id}' replaced and running"
        except Exception as e:
            return f"Failed to spawn updated consumer: {e}"

    async def add_consumer(self, spec: ConsumerSpec) -> str:
        """Add a new consumer from a spec."""
        try:
            self.consumer_manager.spawn(spec)
            self._persist_consumers()
            self._update_registry()
            return f"Consumer '{spec.consumer_id}' spawned"
        except Exception as e:
            return f"Failed to spawn consumer: {e}"

    async def remove_consumer(self, consumer_id: str) -> str:
        """Remove a consumer."""
        if consumer_id not in self.consumer_manager.consumers:
            return f"Consumer '{consumer_id}' not found"
        self.consumer_manager.stop(consumer_id)
        managed = self.consumer_manager.consumers.pop(consumer_id)
        if managed.task:
            try:
                await asyncio.wait_for(managed.task, timeout=5)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
        self._persist_consumers()
        return f"Consumer '{consumer_id}' removed"

    # --- Chat tool actions ---

    async def _handle_chat_tool_action(self, tool_name: str, tool_input: dict) -> str | None:
        """Handle read-only side-effect tools during chat. Deployment tools are intercepted separately."""
        if tool_name == "get_consumer_code":
            code = self.consumer_manager.get_consumer_code(tool_input["consumer_id"])
            return code or f"Consumer '{tool_input['consumer_id']}' not found"
        return None

    # --- Decision & persistence ---

    async def _execute_decision(self, decision: dict):
        """Execute a decision from the nucleus."""
        self._log_decision(decision)
        action = decision.get("action", {})
        action_type = action.get("type")

        if action_type == "spawn_consumer":
            spec = self._spec_from_action(action)
            if not spec.consumer_id:
                spec.consumer_id = f"consumer-{len(self.consumer_manager.consumers)}"
            try:
                self.consumer_manager.spawn(spec)
                self._update_registry()
            except Exception as e:
                print(f"  [{self.name}] Failed to spawn consumer: {e}")

        elif action_type == "store_knowledge":
            self.knowledge.store(
                content=action.get("content", ""),
                category=action.get("category", "observation"),
                metadata=action.get("metadata"),
            )

    def _log_decision(self, decision: dict):
        """Write decision to the Kafka decision log topic."""
        self._decision_producer.produce(
            self.decision_topic,
            key=self.cell_id.encode(),
            value=orjson.dumps(decision),
        )
        self._decision_producer.flush(2)

    def _build_context(self) -> str:
        consumers = self.consumer_manager.list_consumers(include_code=False)
        total = self.consumer_manager.total_events()
        return f"""Active consumers: {len(consumers)}
Total events processed: {total}
Consumers: {json.dumps(consumers, indent=2)}
Cell status: {self.status.value}

To answer questions about what your consumers do, use the descriptions and detection_patterns above.
To view actual code, use get_consumer_code. To inspect your knowledge base, use describe_schema and query_knowledge."""

    # --- Postgres persistence ---

    def _persist_consumers(self):
        """Save all consumer specs + stats to Postgres for reload."""
        specs = []
        for managed in self.consumer_manager.consumers.values():
            specs.append({
                "consumer_id": managed.spec.consumer_id,
                "source_topics": managed.spec.source_topics,
                "output_topic": managed.spec.output_topic,
                "consumer_code": managed.spec.consumer_code,
                "description": managed.spec.description,
                "detection_patterns": managed.spec.detection_patterns,
                "knowledge_tables": managed.spec.knowledge_tables,
                "events_processed": managed.events_processed,
                "alerts_emitted": managed.alerts_emitted,
            })
        with psycopg.connect(POSTGRES_URL) as conn:
            conn.execute(
                "UPDATE public.cells SET consumers = %s WHERE cell_id = %s",
                (json.dumps(specs), self.cell_id),
            )
            conn.commit()

    def _load_persisted_consumers(self) -> list[dict]:
        """Load persisted consumer specs + stats from Postgres."""
        with psycopg.connect(POSTGRES_URL) as conn:
            row = conn.execute(
                "SELECT consumers FROM public.cells WHERE cell_id = %s", (self.cell_id,)
            ).fetchone()
        if not row or not row[0]:
            return []
        results = []
        for s in row[0] if isinstance(row[0], list) else json.loads(row[0]):
            if s.get("consumer_code"):
                results.append({
                    "spec": ConsumerSpec(
                        consumer_id=s["consumer_id"],
                        source_topics=s["source_topics"],
                        output_topic=s["output_topic"],
                        consumer_code=s["consumer_code"],
                        description=s.get("description", ""),
                        detection_patterns=s.get("detection_patterns", []),
                        knowledge_tables=s.get("knowledge_tables", []),
                    ),
                    "events_processed": s.get("events_processed", 0),
                    "alerts_emitted": s.get("alerts_emitted", 0),
                })
        return results

    def _register(self):
        with psycopg.connect(POSTGRES_URL) as conn:
            conn.execute("DELETE FROM public.cells WHERE name = %s", (self.name,))
            conn.execute(
                """INSERT INTO public.cells (cell_id, name, directive, status)
                   VALUES (%s, %s, %s, %s)""",
                (self.cell_id, self.name, self.directive, self.status.value),
            )
            conn.commit()

    def _unregister(self):
        with psycopg.connect(POSTGRES_URL) as conn:
            conn.execute("DELETE FROM public.cells WHERE cell_id = %s", (self.cell_id,))
            conn.commit()

    def _update_status(self, status: str):
        with psycopg.connect(POSTGRES_URL) as conn:
            conn.execute(
                "UPDATE public.cells SET status = %s WHERE cell_id = %s",
                (status, self.cell_id),
            )
            conn.commit()

    def _update_registry(self):
        topics_sub = list({t for c in self.consumer_manager.consumers.values() for t in c.spec.source_topics})
        topics_prod = list({c.spec.output_topic for c in self.consumer_manager.consumers.values() if c.spec.output_topic})
        with psycopg.connect(POSTGRES_URL) as conn:
            conn.execute(
                "UPDATE public.cells SET topics_subscribed = %s, topics_produced = %s WHERE cell_id = %s",
                (json.dumps(topics_sub), json.dumps(topics_prod), self.cell_id),
            )
            conn.commit()
