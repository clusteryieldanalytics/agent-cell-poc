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

        # Set by orchestrator after creation
        self.dashboard_registry = None
        self.orchestrator = None  # reference to CellOrchestrator for cross-cell queries

        # Track failed spawns so propose_next doesn't re-propose them
        self._failed_spawns: list[dict] = []  # [{consumer_id, runtime, error}, ...]

        # Wire nucleus decision logging to Kafka
        self.nucleus.on_decision = self._log_decision

    async def initialize(self):
        """Initialize the cell's knowledge base and register it."""
        print(f"[{self.name}] Starting cell...")
        self.knowledge.initialize()
        self.status = CellStatus.INITIALIZING
        self._register()

    async def plan_architecture(self) -> str:
        """Phase 1: Sample live data, then ask the nucleus to produce an architecture plan.

        Samples events from all source topics so the nucleus can see actual data shapes,
        field values, event rates, and volume before designing consumers and choosing thresholds.

        Returns the plan text. The plan is stored on the cell and used as context
        for subsequent propose_next() calls.
        """
        from src.cell.kafka_tools import sample_topic, topic_stats
        from src.config import TOPIC_FLOWS, TOPIC_DEVICE_STATUS, TOPIC_SYSLOG

        # Reconnaissance: sample live data from source topics
        print(f"[{self.name}] Sampling source topics for reconnaissance...")
        source_topics = [TOPIC_FLOWS, TOPIC_DEVICE_STATUS, TOPIC_SYSLOG]
        recon_sections = []

        try:
            stats = await topic_stats(source_topics)
            recon_sections.append(f"Topic stats:\n{json.dumps(stats, indent=2, default=str)}")
        except Exception as e:
            recon_sections.append(f"Could not get topic stats: {e}")

        for topic in source_topics:
            try:
                samples = await sample_topic(topic, count=3)
                if samples:
                    recon_sections.append(
                        f"Sample events from {topic} ({len(samples)} samples):\n"
                        + json.dumps(samples, indent=2, default=str)
                    )
                else:
                    recon_sections.append(f"No events found in {topic}")
            except Exception as e:
                recon_sections.append(f"Could not sample {topic}: {e}")

        recon_data = "\n\n".join(recon_sections)
        print(f"[{self.name}] Reconnaissance complete — {len(recon_sections)} sections")

        prompt = (
            "You have just been activated. Before writing any code, I've sampled live data from "
            "the source topics so you can see the actual event shapes, field names, value ranges, "
            "and volume.\n\n"
            f"LIVE DATA SAMPLES:\n{recon_data}\n\n"
            "Study these samples carefully. Note:\n"
            "- The actual field names and types in each event (use these exactly in your code)\n"
            "- Value ranges (IP addresses, port numbers, byte counts, device IDs)\n"
            "- Event rates and volume (from topic stats)\n"
            "- What anomalies or patterns might look like in this data\n\n"
            "Now design your consumer architecture. Think through the tradeoffs:\n"
            "- What processing does my directive require? Which data sources?\n"
            "- What's the simplest design that covers everything? Could one consumer do it?\n"
            "- Where does splitting into multiple consumers earn its keep?\n"
            "- Does the workload genuinely benefit from Flink SQL?\n\n"
            "For each planned consumer, specify:\n"
            "- Consumer ID, runtime (flink_sql or python), source topics → output topic\n"
            "- What it does and WHY it's a separate consumer (not merged into another)\n"
            "- Key thresholds and detection parameters, justified by the data samples above\n"
            "  (e.g., 'baseline bytes_sent is ~15K-900K, so I'll flag transfers >5MB as unusual')\n"
            "- Deployment order (Flink first if Python depends on its output)\n\n"
            "DO NOT call any tools. Respond with text only — your architecture plan. "
            "I will ask you to implement each consumer one at a time after this."
        )

        print(f"[{self.name}] Nucleus planning architecture...")
        decisions = await self.nucleus.reason(prompt)
        # Extract the plan text from reasoning decisions
        plan_text = ""
        for d in decisions:
            if d.get("reasoning"):
                plan_text += d["reasoning"] + "\n"

        self._architecture_plan = plan_text.strip()

        # Store the plan in knowledge base for future reference
        if self._architecture_plan:
            try:
                self.knowledge.store(
                    f"Architecture plan:\n{self._architecture_plan}",
                    "design_rationale",
                    {"type": "architecture_plan"},
                )
            except Exception:
                pass

        print(f"[{self.name}] Architecture plan ready ({len(self._architecture_plan)} chars)")
        return self._architecture_plan

    async def propose_next(self) -> list[dict]:
        """Phase 2: Propose the next consumer, one at a time, following the architecture plan.

        Call repeatedly until it returns no spawn_consumer decisions.
        """
        existing = self.consumer_manager.list_consumers()

        # Enrich Flink consumers with live status
        flink_consumers = [m for m in self.consumer_manager.consumers.values() if m.flink_job_id]
        if flink_consumers:
            try:
                flink = self.consumer_manager._get_flink_runtime()
                for m in flink_consumers:
                    for c in existing:
                        if c["consumer_id"] == m.spec.consumer_id:
                            try:
                                status = flink.status(m.flink_job_id)
                                c["flink_status"] = status.get("state", "UNKNOWN")
                                metrics = flink.job_metrics(m.flink_job_id)
                                c["flink_records_in"] = metrics.get("total_read_records", 0)
                                c["flink_records_out"] = metrics.get("total_write_records", 0)
                            except Exception:
                                c["flink_status"] = "UNREACHABLE"
                            break
            except Exception:
                pass

        plan_context = ""
        if hasattr(self, '_architecture_plan') and self._architecture_plan:
            plan_context = f"\n\nYOUR ARCHITECTURE PLAN (designed before any consumers were deployed):\n{self._architecture_plan}\n"

        # Build a clear status summary of deployed consumers
        deployed_context = ""
        if existing:
            lines = []
            for c in existing:
                cid = c["consumer_id"]
                runtime = c.get("runtime", "python")
                status_parts = []
                if runtime == "flink_sql":
                    flink_status = c.get("flink_status", "")
                    job_id = c.get("flink_job_id", "")
                    status_parts.append(f"flink_sql, job={job_id}, state={flink_status or '?'}")
                    records_in = c.get("flink_records_in", 0)
                    records_out = c.get("flink_records_out", 0)
                    if records_in or records_out:
                        status_parts.append(f"{records_in} records in, {records_out} out")
                else:
                    status_parts.append(f"python, {'running' if c.get('running') else 'stopped'}")
                    status_parts.append(f"{c.get('events_processed', 0)} events, {c.get('errors', 0)} errors")
                topics = ", ".join(c.get("source_topics", []))
                output = c.get("output_topic", "?")
                lines.append(
                    f"  ✓ {cid} ({', '.join(status_parts)})\n"
                    f"    {topics} → {output}\n"
                    f"    {c.get('description', '')[:100]}"
                )
            deployed_context = f"\n\nDEPLOYED AND RUNNING ({len(existing)} consumer(s)):\n" + "\n".join(lines) + "\n"

        # Include info about failed spawns
        failure_context = ""
        if self._failed_spawns:
            failure_lines = json.dumps(self._failed_spawns, indent=2)
            failure_context = (
                f"\n\nFAILED SPAWNS (attempted but failed — do NOT re-propose with the same code):\n"
                f"{failure_lines}\n"
                f"Fix the underlying issue or skip and move on.\n"
            )

        if not existing:
            prompt = (
                f"{plan_context}{failure_context}\n"
                "Now implement the FIRST consumer from your plan. Follow the deployment order "
                "you specified. Call spawn_consumer EXACTLY ONCE with the complete code."
            )
        else:
            prompt = (
                f"{deployed_context}{plan_context}{failure_context}\n"
                "Implement the NEXT consumer from your plan that hasn't been deployed yet. "
                "The consumers listed above under DEPLOYED AND RUNNING are already live — do not re-propose them. "
                "Deploy the next one in sequence. Call spawn_consumer EXACTLY ONCE.\n\n"
                "If ALL consumers from your plan are deployed, respond with text only explaining "
                "that your pipeline is complete."
            )

        print(f"[{self.name}] Nucleus proposing next consumer...")
        decisions = await self.nucleus.reason(prompt, single_spawn=True)
        print(f"[{self.name}] Nucleus returned {len(decisions)} decisions")
        return decisions

    async def approve(self, decisions: list[dict]):
        """Execute approved decisions and activate the cell."""
        self.status = CellStatus.ACTIVE

        for decision in decisions:
            dtype = decision.get('decision_type', decision.get('action', {}).get('type', '?'))
            action = decision.get('action', {})
            cid = action.get('consumer_id', '?')
            print(f"  [{self.name}] Executing: {dtype}")
            try:
                await self._execute_decision(decision)
            except Exception as e:
                print(f"  [{self.name}] FAILED to execute {dtype} '{cid}': {e}")
                if dtype == "spawn_consumer":
                    self._failed_spawns.append({
                        "consumer_id": cid,
                        "runtime": action.get("runtime", "python"),
                        "error": str(e),
                    })

        self._decision_producer.flush(5)
        self._persist_consumers()
        self._update_status("active")
        print(f"[{self.name}] Cell active with {len(self.consumer_manager.consumers)} consumers")

    async def verify(self, max_iterations: int = 5, settle_seconds: int = 15) -> dict:
        """Phase 3: Iteratively verify consumers and self-fix until stable.

        Waits for events to flow, checks DLQs for errors, samples output topics,
        and asks the nucleus to fix any issues. Returns a summary of the verification.

        Args:
            max_iterations: max fix attempts before giving up
            settle_seconds: how long to wait for events between checks
        """
        from src.cell.consumer import ConsumerManager as CM

        summary = {"iterations": 0, "fixes": [], "stable": False, "errors_found": 0}

        for iteration in range(max_iterations):
            summary["iterations"] = iteration + 1
            await self.nucleus._log(f"Verification pass {iteration + 1}/{max_iterations} — waiting {settle_seconds}s for events...")

            # Sleep in small increments so event streaming stays alive
            for elapsed in range(settle_seconds):
                await asyncio.sleep(1)
                # Emit a progress tick every 5 seconds
                if (elapsed + 1) % 5 == 0:
                    py_events = 0
                    py_errors = 0
                    flink_records = 0
                    for m in self.consumer_manager.consumers.values():
                        if m.spec.runtime == "flink_sql":
                            if m.flink_job_id:
                                try:
                                    flink = self.consumer_manager._get_flink_runtime()
                                    metrics = flink.job_metrics(m.flink_job_id)
                                    flink_records += metrics.get("total_read_records", 0)
                                except Exception:
                                    pass
                        else:
                            py_events += m.events_processed
                            py_errors += m.errors

                    parts = [f"{elapsed + 1}s elapsed"]
                    parts.append(f"{py_events} python events, {py_errors} errors")
                    if any(m.spec.runtime == "flink_sql" for m in self.consumer_manager.consumers.values()):
                        parts.append(f"{flink_records} flink records")
                    await self.nucleus._log(f"  ... {' — '.join(parts)}")

            # Check each consumer for errors
            all_healthy = True
            for managed in list(self.consumer_manager.consumers.values()):
                cid = managed.spec.consumer_id

                # Flink consumers: identified by runtime, not just flink_job_id
                if managed.spec.runtime == "flink_sql":
                    if not managed.flink_job_id:
                        all_healthy = False
                        summary["errors_found"] += 1
                        await self.nucleus._log(
                            f"Flink consumer '{cid}' has no job ID — "
                            f"submit likely failed. Asking nucleus to fix..."
                        )
                        current_sql = managed.spec.consumer_code
                        fix_decisions = await self.nucleus.reason(
                            f"My Flink SQL consumer '{cid}' failed to submit — there is no Flink job running.\n\n"
                            f"Source topics: {managed.spec.source_topics}\n"
                            f"Current SQL:\n```sql\n{current_sql}\n```\n\n"
                            f"The SQL was rejected by the Flink SQL Gateway. Common causes:\n"
                            f"- Syntax errors in the SQL\n"
                            f"- Wrong bootstrap server (must use 'kafka:29092' for Flink)\n"
                            f"- Missing semicolons between statements\n"
                            f"- Invalid column types or watermark definitions\n\n"
                            f"Fix the SQL and call spawn_consumer with runtime='flink_sql'. "
                            f"Keep consumer_id '{cid}'."
                        )
                        for decision in fix_decisions:
                            action = decision.get("action", {})
                            if action.get("type") == "spawn_consumer" and action.get("consumer_code"):
                                action["consumer_id"] = cid
                                action["runtime"] = "flink_sql"
                                result = await self.replace_consumer(action)
                                await self.nucleus._log(f"Fix result: {result}")
                                summary["fixes"].append({
                                    "consumer_id": cid,
                                    "iteration": iteration + 1,
                                    "error": "Flink job not submitted",
                                    "result": result,
                                })
                                break
                        continue

                    try:
                        flink = self.consumer_manager._get_flink_runtime()
                        status = flink.status(managed.flink_job_id)
                        state = status.get("state", "UNKNOWN")
                    except Exception as e:
                        all_healthy = False
                        await self.nucleus._log(f"Flink consumer '{cid}' — could not reach Flink: {e}")
                        continue

                    if state in ("FAILED", "CANCELED"):
                        all_healthy = False
                        summary["errors_found"] += 1
                        error_detail = ""
                        try:
                            exc = flink.job_exceptions(managed.flink_job_id)
                            error_detail = exc.get("root_exception", "")[:300]
                        except Exception:
                            pass
                        await self.nucleus._log(
                            f"Flink consumer '{cid}' — job {state}"
                            + (f": {error_detail}" if error_detail else "")
                        )
                        # Ask nucleus to fix
                        current_sql = managed.spec.consumer_code
                        fix_decisions = await self.nucleus.reason(
                            f"My Flink SQL consumer '{cid}' (job {managed.flink_job_id}) has {state}.\n"
                            f"{'Root exception: ' + error_detail if error_detail else ''}\n\n"
                            f"Current SQL:\n```sql\n{current_sql}\n```\n\n"
                            f"Fix the SQL and call spawn_consumer with runtime='flink_sql'. "
                            f"Keep consumer_id '{cid}'."
                        )
                        for decision in fix_decisions:
                            action = decision.get("action", {})
                            if action.get("type") == "spawn_consumer" and action.get("consumer_code"):
                                action["consumer_id"] = cid
                                action["runtime"] = "flink_sql"
                                result = await self.replace_consumer(action)
                                await self.nucleus._log(f"Fix result: {result}")
                                summary["fixes"].append({
                                    "consumer_id": cid,
                                    "iteration": iteration + 1,
                                    "error": f"Flink job {state}",
                                    "result": result,
                                })
                                break
                        continue

                    if state != "RUNNING":
                        await self.nucleus._log(f"Flink consumer '{cid}' — job state: {state}")
                        continue

                    # Job is RUNNING — check if it's actually processing records
                    records_in = 0
                    try:
                        metrics = flink.job_metrics(managed.flink_job_id)
                        records_in = metrics.get("total_read_records", 0)
                        records_out = metrics.get("total_write_records", 0)
                    except Exception:
                        # Can't get metrics — treat as 0 to trigger investigation
                        records_out = 0

                    if records_in == 0:
                        all_healthy = False
                        summary["errors_found"] += 1
                        await self.nucleus._log(
                            f"Flink consumer '{cid}' RUNNING but 0 records read after {settle_seconds}s — investigating..."
                        )
                        current_sql = managed.spec.consumer_code
                        fix_decisions = await self.nucleus.reason(
                            f"My Flink SQL consumer '{cid}' (job {managed.flink_job_id}) is RUNNING "
                            f"but has read 0 records after {settle_seconds}s.\n\n"
                            f"Source topics: {managed.spec.source_topics}\n"
                            f"Current SQL:\n```sql\n{current_sql}\n```\n\n"
                            f"Possible causes:\n"
                            f"- Wrong topic name in CREATE TABLE\n"
                            f"- Wrong bootstrap server (must use 'kafka:29092' for Flink inside Docker)\n"
                            f"- Schema mismatch (field names/types don't match the JSON events)\n"
                            f"- Watermark is too strict (events arrive out of order)\n"
                            f"- Source topic genuinely has no data yet\n\n"
                            f"Use flink_inspect or flink_cluster to gather more info if needed. "
                            f"If you can fix the SQL, call spawn_consumer with runtime='flink_sql' "
                            f"and the corrected code. Keep consumer_id '{cid}'."
                        )
                        for decision in fix_decisions:
                            action = decision.get("action", {})
                            if action.get("type") == "spawn_consumer" and action.get("consumer_code"):
                                action["consumer_id"] = cid
                                action["runtime"] = "flink_sql"
                                result = await self.replace_consumer(action)
                                await self.nucleus._log(f"Fix result: {result}")
                                summary["fixes"].append({
                                    "consumer_id": cid,
                                    "iteration": iteration + 1,
                                    "error": "Flink job 0 records",
                                    "result": result,
                                })
                                break
                    else:
                        await self.nucleus._log(
                            f"Flink consumer '{cid}' healthy — RUNNING "
                            f"({records_in} records in, {records_out} out)"
                        )
                    continue

                # Python consumers: check DLQ for errors
                dlq_entries = await CM.read_dlq(managed.dlq_topic, limit=5)
                recent_errors = [e for e in dlq_entries if e.get("error")]

                if recent_errors and managed.errors > 0:
                    all_healthy = False
                    summary["errors_found"] += len(recent_errors)
                    error_sample = recent_errors[-1]
                    await self.nucleus._log(
                        f"Consumer '{cid}' has {managed.errors} errors. "
                        f"Latest: {error_sample.get('error_type', '?')}: {error_sample.get('error', '?')[:100]}"
                    )

                    # Ask nucleus to diagnose and fix
                    await self.nucleus._log(f"Asking nucleus to fix '{cid}'...")

                    error_details = json.dumps(recent_errors[-3:], indent=2, default=str)
                    current_code = managed.spec.consumer_code

                    fix_decisions = await self.nucleus.reason(
                        f"My consumer '{cid}' is failing with errors. Here are the most recent DLQ entries:\n\n"
                        f"{error_details}\n\n"
                        f"Here is the current consumer code:\n```python\n{current_code}\n```\n\n"
                        f"Fix the code and call spawn_consumer with the corrected version. "
                        f"Keep the same consumer_id '{cid}', source_topics, and output_topic. "
                        f"IMPORTANT: Fix the root cause shown in the error, don't just add try/except."
                    )

                    for decision in fix_decisions:
                        action = decision.get("action", {})
                        if action.get("type") == "spawn_consumer" and action.get("consumer_code"):
                            action["consumer_id"] = cid
                            result = await self.replace_consumer(action)
                            await self.nucleus._log(f"Fix result: {result}")
                            summary["fixes"].append({
                                "consumer_id": cid,
                                "iteration": iteration + 1,
                                "error": error_sample.get("error", ""),
                                "result": result,
                            })
                            break

                elif managed.events_processed == 0:
                    all_healthy = False
                    summary["errors_found"] += 1
                    await self.nucleus._log(
                        f"Consumer '{cid}' has processed 0 events after {settle_seconds}s — investigating..."
                    )

                    # Ask nucleus to diagnose: wrong topic? bad subscription? code error?
                    current_code = managed.spec.consumer_code
                    source_topics = managed.spec.source_topics

                    # Sample source topics to check if data is flowing
                    from src.cell.kafka_tools import topic_stats
                    try:
                        stats = await topic_stats(source_topics)
                        topic_info = json.dumps(stats, indent=2, default=str)
                    except Exception:
                        topic_info = "(could not read topic stats)"

                    fix_decisions = await self.nucleus.reason(
                        f"My consumer '{cid}' has processed 0 events after {settle_seconds}s.\n\n"
                        f"Source topics: {source_topics}\n"
                        f"Topic stats:\n{topic_info}\n\n"
                        f"Current code:\n```python\n{current_code}\n```\n\n"
                        f"Diagnose the issue. Possible causes:\n"
                        f"- Wrong topic name (check available topics above)\n"
                        f"- Consumer code crashes before processing (check init())\n"
                        f"- Topic exists but has no data yet (if stats show 0 messages)\n\n"
                        f"If you can fix the issue, call spawn_consumer with corrected code. "
                        f"Keep the same consumer_id '{cid}', source_topics, and output_topic. "
                        f"If the topic simply has no data yet, say so and I'll retry."
                    )

                    for decision in fix_decisions:
                        action = decision.get("action", {})
                        if action.get("type") == "spawn_consumer" and action.get("consumer_code"):
                            action["consumer_id"] = cid
                            result = await self.replace_consumer(action)
                            await self.nucleus._log(f"Fix result: {result}")
                            summary["fixes"].append({
                                "consumer_id": cid,
                                "iteration": iteration + 1,
                                "error": "0 events processed",
                                "result": result,
                            })
                            break

                else:
                    await self.nucleus._log(
                        f"Consumer '{cid}' healthy — {managed.events_processed} events, "
                        f"{managed.alerts_emitted} alerts, {managed.errors} errors"
                    )

            if all_healthy:
                await self.nucleus._log("All consumers healthy and processing events. Verification complete.")
                summary["stable"] = True
                break

        if not summary["stable"]:
            await self.nucleus._log(
                f"Verification ended after {summary['iterations']} iterations. "
                f"{summary['errors_found']} errors found, {len(summary['fixes'])} fixes applied."
            )

        self._persist_consumers()
        return summary

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
            persisted_job_id = entry.get("flink_job_id")
            try:
                # For Flink consumers, check if the persisted job is still running
                if spec.runtime == "flink_sql" and persisted_job_id:
                    try:
                        flink = self.consumer_manager._get_flink_runtime()
                        status = flink.status(persisted_job_id)
                        if status.get("state") == "RUNNING":
                            # Job is still alive — reattach instead of resubmitting
                            from src.cell.consumer import ManagedConsumer
                            managed = ManagedConsumer(
                                spec=spec, running=True, flink_job_id=persisted_job_id,
                            )
                            self.consumer_manager.consumers[spec.consumer_id] = managed
                            managed.events_processed = entry.get("events_processed", 0)
                            managed.alerts_emitted = entry.get("alerts_emitted", 0)
                            print(f"  [{self.name}] Reattached Flink consumer '{spec.consumer_id}' → job {persisted_job_id} (RUNNING)")
                            continue
                    except Exception:
                        print(f"  [{self.name}] Flink job {persisted_job_id} unreachable — resubmitting")

                managed = self.consumer_manager.spawn(spec)
                managed.events_processed = entry.get("events_processed", 0)
                managed.alerts_emitted = entry.get("alerts_emitted", 0)
                if managed.flink_job_id:
                    print(f"  [{self.name}] Reloaded Flink consumer '{spec.consumer_id}' → new job {managed.flink_job_id}")
                else:
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
        """Stop and destroy ALL resources — Postgres schema, Kafka topics, Flink jobs, registry entry.
        Returns a log of what was purged."""
        purged = []

        # Stop consumers — list each one individually
        for managed in list(self.consumer_manager.consumers.values()):
            cid = managed.spec.consumer_id

            # Cancel Flink jobs
            if managed.flink_job_id:
                try:
                    flink = self.consumer_manager._get_flink_runtime()
                    flink.cancel(managed.flink_job_id)
                    purged.append(f"Cancelled Flink job: {cid} (job_id={managed.flink_job_id})")
                except Exception as e:
                    purged.append(f"Could not cancel Flink job {cid} ({managed.flink_job_id}): {e}")
            else:
                purged.append(f"Stopped Python consumer: {cid}")

        if self.consumer_manager.consumers:
            self.consumer_manager.stop_all()

        self._decision_producer.flush(5)

        # Drop Postgres schema (knowledge tables + any custom tables) — list each table
        try:
            stats = self.knowledge.stats()
            table_names = list(stats.get("tables", {}).keys())
            for tname in table_names:
                rows = stats["tables"][tname].get("rows", 0)
                purged.append(f"Dropping table: {self.knowledge.schema}.{tname} ({rows} rows)")
        except Exception:
            purged.append(f"Dropping schema: {self.knowledge.schema}")
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
        queued_consumers = set()  # track consumer IDs already queued in this chat turn

        async def _intercept_tool(tool_name: str, tool_input: dict) -> str:
            if tool_name in self.DEPLOYMENT_TOOLS:
                consumer_id = tool_input.get("consumer_id", "")
                action_key = f"{tool_name}:{consumer_id}"

                # Reject duplicate actions on the same consumer in one chat turn
                if action_key in queued_consumers:
                    return (
                        f"Already queued — {tool_name} for '{consumer_id}' is already pending operator approval. "
                        f"Do NOT call {tool_name} again for the same consumer. Move on to your next task or "
                        f"summarize what you've done."
                    )
                queued_consumers.add(action_key)

                # Don't execute — stash for operator approval
                pending_actions.append({"type": tool_name, **tool_input})
                return (
                    f"Done — {tool_name} has been queued. The operator will be prompted to approve it "
                    f"and it will be deployed automatically if approved. Do NOT ask the operator to "
                    f"confirm or deploy — that is handled by the system. Do NOT call {tool_name} again "
                    f"for '{consumer_id}'. Simply summarize what you changed and why."
                )
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

    async def self_audit(self) -> dict:
        """Periodic self-audit: ask the nucleus to review consumer health and DLQ errors.

        The nucleus can use its chat tools (inspect_dlq, query_knowledge, get_consumer_code,
        replace_consumer, etc.) to diagnose and fix issues autonomously.

        Returns a summary dict with audit findings and any actions taken.
        """
        if self.status != CellStatus.ACTIVE:
            return {"skipped": True, "reason": f"cell status is {self.status.value}"}

        if not self.consumer_manager.consumers:
            return {"skipped": True, "reason": "no consumers"}

        # Build a health snapshot for the nucleus
        consumer_health = []
        for managed in self.consumer_manager.consumers.values():
            cid = managed.spec.consumer_id
            health = {
                "consumer_id": cid,
                "running": managed.running,
                "events_processed": managed.events_processed,
                "alerts_emitted": managed.alerts_emitted,
                "errors": managed.errors,
            }

            # Read recent DLQ entries
            try:
                dlq_entries = await ConsumerManager.read_dlq(managed.dlq_topic, limit=5)
                if dlq_entries:
                    health["recent_dlq_errors"] = [
                        {
                            "error_type": e.get("error_type", "?"),
                            "error": e.get("error", "?")[:200],
                            "timestamp": e.get("timestamp", "?"),
                        }
                        for e in dlq_entries
                    ]
            except Exception:
                pass

            consumer_health.append(health)

        # Pre-compute topology analysis for the audit prompt
        topo = self.topology_analysis()
        topo_context = ""
        if topo.get("warnings"):
            topo_context = (
                f"\n\nTOPOLOGY WARNINGS ({len(topo['warnings'])} issues found):\n"
                + "\n".join(f"- {w}" for w in topo["warnings"])
                + "\n\nFix these wiring issues using replace_consumer to update source_topics.\n"
            )
        else:
            topo_context = "\n\nTopology check: CLEAN — all topics wired correctly.\n"

        audit_prompt = (
            "SELF-AUDIT: Review your consumers' health, pipeline topology, and data quality.\n\n"
            f"Consumer health snapshot:\n{json.dumps(consumer_health, indent=2)}\n"
            f"{topo_context}\n"
            "Audit steps:\n"
            "1. If there are TOPOLOGY WARNINGS above, fix the wiring issues first (use replace_consumer to update source_topics)\n"
            "2. If any consumer has DLQ errors, inspect the DLQ and fix the root cause\n"
            "3. If a consumer is not running, investigate why and respawn if needed\n"
            "4. If a consumer has low alert rates relative to events processed, consider whether thresholds need tuning\n"
            "5. Call check_topology to verify your fixes resolved the wiring issues\n"
            "6. If everything looks healthy, say so briefly\n\n"
            "Use inspect_dlq, get_consumer_code, check_topology, and replace_consumer as needed. "
            "Be conservative — only change code if there's a clear problem.\n\n"
            "IMPORTANT: As you examine your data, use store_knowledge to record anything interesting:\n"
            "- Patterns you notice (e.g., 'IP 10.0.1.45 appears in both exfil and lateral movement alerts')\n"
            "- Threshold observations (e.g., 'baseline bytes_sent is averaging 50K, current threshold of 5MB may be too high')\n"
            "- Correlations between consumers (e.g., 'brute force signals from Flink correlate with auth_failure spikes in syslog')\n"
            "- Anomalies in topic throughput or consumer behavior\n"
            "- Any hypothesis about attacker behavior or network state\n"
            "Use query_knowledge and sample_topic to dig into your data — don't just check health, learn from it."
        )

        print(f"[{self.name}] Starting self-audit...")
        actions_applied = []

        async def _handle_audit_tool(tool_name: str, tool_input: dict) -> str:
            """During self-audit, deployment tools execute directly (no operator approval)."""
            if tool_name in self.DEPLOYMENT_TOOLS:
                try:
                    result = await self.deploy_action({"type": tool_name, **tool_input})
                    actions_applied.append({"type": tool_name, **tool_input, "result": result})
                    return result
                except Exception as e:
                    return f"Error: {e}"
            else:
                return await self._handle_chat_tool_action(tool_name, tool_input)

        context = self._build_context()
        reply = await self.nucleus.chat(audit_prompt, context, on_tool_action=_handle_audit_tool)

        # Store the audit result in knowledge base
        try:
            self.knowledge.store(
                f"Self-audit result: {reply}",
                "audit_finding",
                {"audit": True, "actions_applied": len(actions_applied)},
            )
        except Exception:
            pass

        self._log_decision({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "cell_id": self.cell_id,
            "decision_type": "self_audit",
            "reasoning": reply,
            "action": {"type": "self_audit", "actions_applied": actions_applied},
        })

        summary = {
            "cell_name": self.name,
            "reply": reply,
            "actions_applied": actions_applied,
            "consumers_audited": len(consumer_health),
        }
        print(f"[{self.name}] Self-audit complete — {len(actions_applied)} action(s) applied")
        return summary

    def inspect(self) -> dict:
        """Return full cell state for inspection."""
        kb_stats = {}
        try:
            kb_stats = self.knowledge.stats()
        except Exception:
            kb_stats = {"error": "Could not query knowledge base"}

        consumers = self.consumer_manager.list_consumers()

        # Enrich Flink consumers with live job status
        flink_consumers = [m for m in self.consumer_manager.consumers.values() if m.flink_job_id]
        if flink_consumers:
            try:
                flink = self.consumer_manager._get_flink_runtime()
                for m in flink_consumers:
                    for c in consumers:
                        if c["consumer_id"] == m.spec.consumer_id:
                            try:
                                status = flink.status(m.flink_job_id)
                                c["flink_status"] = status.get("state", "UNKNOWN")
                                c["flink_duration_ms"] = status.get("duration")
                                metrics = flink.job_metrics(m.flink_job_id)
                                c["flink_records_in"] = metrics.get("total_read_records", 0)
                                c["flink_records_out"] = metrics.get("total_write_records", 0)
                            except Exception:
                                c["flink_status"] = "UNREACHABLE"
                            break
            except Exception:
                pass

        return {
            "cell_id": self.cell_id,
            "name": self.name,
            "directive": self.directive,
            "status": self.status.value,
            "consumers": consumers,
            "events_processed": self.consumer_manager.total_events(),
            "knowledge_base": kb_stats,
            "decision_topic": self.decision_topic,
        }

    def topology_analysis(self) -> dict:
        """Analyze the consumer pipeline topology for wiring issues.

        Returns a dict with:
        - consumers: list of consumer summaries
        - external_sources: source topics not produced by any consumer
        - intermediate_topics: output of one consumer consumed by another
        - unwired_outputs: Flink outputs that no consumer subscribes to
        - orphan_inputs: topics a consumer subscribes to that don't exist as
          source topics or outputs of another consumer
        - warnings: list of human-readable warning strings
        """
        consumers = self.consumer_manager.list_consumers()

        output_to_consumer = {}
        all_outputs = set()
        all_inputs = set()

        for c in consumers:
            out = c.get("output_topic", "")
            if out:
                output_to_consumer[out] = c["consumer_id"]
                all_outputs.add(out)
            all_inputs.update(c.get("source_topics", []))

        # Known external source topics (Kafka source topics from producers)
        from src.config import TOPIC_FLOWS, TOPIC_DEVICE_STATUS, TOPIC_SYSLOG
        known_sources = {TOPIC_FLOWS, TOPIC_DEVICE_STATUS, TOPIC_SYSLOG}

        external_sources = all_inputs - all_outputs  # not produced by any consumer
        intermediate = all_outputs & all_inputs  # produced and consumed
        unwired_outputs = []
        orphan_inputs = []
        warnings = []

        # Flink outputs not consumed by any consumer
        for c in consumers:
            if c.get("runtime") == "flink_sql":
                out = c.get("output_topic", "")
                if out and out not in all_inputs:
                    unwired_outputs.append({
                        "topic": out,
                        "producer": c["consumer_id"],
                        "issue": f"Flink consumer '{c['consumer_id']}' writes to '{out}' but no consumer subscribes to it",
                    })
                    warnings.append(
                        f"UNWIRED: '{out}' produced by Flink consumer '{c['consumer_id']}' "
                        f"but no consumer reads from it. Data is being written to Kafka but never processed."
                    )

        # Consumer inputs that aren't external sources or intermediate topics
        for c in consumers:
            for t in c.get("source_topics", []):
                if t not in known_sources and t not in all_outputs:
                    orphan_inputs.append({
                        "topic": t,
                        "consumer": c["consumer_id"],
                        "issue": f"Consumer '{c['consumer_id']}' reads from '{t}' which is not a known source topic and not produced by any consumer",
                    })
                    warnings.append(
                        f"ORPHAN INPUT: '{c['consumer_id']}' subscribes to '{t}' but nothing produces to it. "
                        f"This consumer will never receive events on this topic."
                    )

        # Consumers with identical output topics (potential conflict)
        output_writers = {}
        for c in consumers:
            out = c.get("output_topic", "")
            if out:
                output_writers.setdefault(out, []).append(c["consumer_id"])
        for topic, writers in output_writers.items():
            if len(writers) > 1:
                warnings.append(
                    f"SHARED OUTPUT: {len(writers)} consumers write to '{topic}': {', '.join(writers)}. "
                    f"This is valid but may cause interleaved events."
                )

        return {
            "consumer_count": len(consumers),
            "external_sources": sorted(external_sources),
            "intermediate_topics": sorted(intermediate),
            "unwired_outputs": unwired_outputs,
            "orphan_inputs": orphan_inputs,
            "warnings": warnings,
            "healthy": len(warnings) == 0,
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
            runtime=action.get("runtime", "python"),
        )

    async def replace_consumer(self, action: dict) -> str:
        """Hot-swap a consumer's code. Validates new code BEFORE stopping old consumer."""
        consumer_id = action["consumer_id"]
        old = self.consumer_manager.consumers.get(consumer_id)
        if old is None:
            return f"Consumer '{consumer_id}' not found"

        runtime = action.get("runtime", old.spec.runtime)
        new_spec = ConsumerSpec(
            consumer_id=consumer_id,
            source_topics=action.get("source_topics", old.spec.source_topics),
            output_topic=action.get("output_topic", old.spec.output_topic),
            consumer_code=action.get("consumer_code", old.spec.consumer_code),
            description=action.get("description", old.spec.description),
            detection_patterns=action.get("detection_patterns", old.spec.detection_patterns),
            knowledge_tables=action.get("knowledge_tables", old.spec.knowledge_tables),
            runtime=runtime,
        )

        # Validate before touching the old consumer
        if runtime == "flink_sql":
            # Basic SQL validation — check it has an INSERT INTO
            if "INSERT INTO" not in new_spec.consumer_code.upper():
                return "Flink SQL must include an INSERT INTO statement — old consumer kept running."
        else:
            from src.cell.consumer import _compile_consumer_code
            try:
                _compile_consumer_code(new_spec.consumer_code)
            except Exception as e:
                return f"New code failed to compile — old consumer kept running. Error: {e}"

        # Stop old consumer
        old.running = False
        self.consumer_manager.stop(consumer_id)
        if old.task:
            for _ in range(20):  # up to 10 seconds
                if old.task.done():
                    break
                await asyncio.sleep(0.5)
            if not old.task.done():
                print(f"  [{self.name}] Warning: old consumer thread still running after 10s")

        # Carry forward event counters
        prior_events = old.events_processed
        prior_alerts = old.alerts_emitted
        del self.consumer_manager.consumers[consumer_id]

        # Small delay to ensure Kafka consumer group is fully released
        await asyncio.sleep(1)

        # Spawn new
        try:
            managed = self.consumer_manager.spawn(new_spec)
            managed.events_processed = prior_events
            managed.alerts_emitted = prior_alerts
            self._persist_consumers()

            # Stability check
            if managed.flink_job_id:
                # Flink: poll job status until RUNNING or terminal, up to 30 seconds
                flink = self.consumer_manager._get_flink_runtime()
                stable = False
                last_state = "UNKNOWN"
                for attempt in range(15):
                    await asyncio.sleep(2)
                    try:
                        status = flink.status(managed.flink_job_id)
                        last_state = status.get("state", "UNKNOWN")
                    except Exception as e:
                        if attempt < 3:
                            continue  # Flink may not have registered the job yet
                        return f"Could not verify Flink job status after {attempt * 2}s: {e}"

                    if last_state == "RUNNING":
                        # Job is running — wait a bit more then check it's still stable
                        await asyncio.sleep(3)
                        try:
                            recheck = flink.status(managed.flink_job_id)
                            if recheck.get("state") == "RUNNING":
                                stable = True
                                break
                            last_state = recheck.get("state", "UNKNOWN")
                        except Exception:
                            stable = True  # optimistic — it was RUNNING
                            break

                    if last_state in ("FAILED", "CANCELED", "FINISHED"):
                        # Fetch exception details for the error message
                        error_detail = ""
                        try:
                            exc = flink.job_exceptions(managed.flink_job_id)
                            root = exc.get("root_exception", "")
                            if root:
                                error_detail = f" Root cause: {root[:200]}"
                        except Exception:
                            pass
                        managed.running = False
                        return (
                            f"Flink job '{consumer_id}' reached {last_state} after replace.{error_detail} "
                            f"Use flink_inspect to see full details."
                        )

                if not stable:
                    return (
                        f"Flink job '{consumer_id}' still in state {last_state} after 30s. "
                        f"Use flink_job_status to monitor progress."
                    )
            else:
                # Python: wait up to 5 seconds, verify consumer is still alive
                for _ in range(10):
                    await asyncio.sleep(0.5)
                    if not managed.running:
                        return (
                            f"Consumer '{consumer_id}' crashed shortly after starting. "
                            f"Errors: {managed.errors}. Check the DLQ topic '{managed.dlq_topic}' for details."
                        )
                    if managed.events_processed > prior_events:
                        break

            self._log_decision({
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "cell_id": self.cell_id,
                "decision_type": "replace_consumer",
                "reasoning": "Consumer code updated via interactive refinement",
                "action": {"type": "replace_consumer", **action},
            })

            if managed.flink_job_id:
                try:
                    metrics = flink.job_metrics(managed.flink_job_id)
                    records = metrics.get("total_read_records", 0)
                    return (
                        f"Consumer '{consumer_id}' replaced — Flink job {managed.flink_job_id} stable and RUNNING"
                        f" ({records} records read so far)"
                    )
                except Exception:
                    return f"Consumer '{consumer_id}' replaced — Flink job {managed.flink_job_id} stable and RUNNING"
            if managed.events_processed > prior_events:
                return f"Consumer '{consumer_id}' replaced and stable ({managed.events_processed - prior_events} events processed)"
            return f"Consumer '{consumer_id}' replaced and running (awaiting first events)"
        except Exception as e:
            # Spawn failed after stopping old — try to restore the old consumer
            try:
                restored = self.consumer_manager.spawn(old.spec)
                restored.events_processed = prior_events
                restored.alerts_emitted = prior_alerts
                return f"Failed to spawn new consumer ({e}) — restored old consumer"
            except Exception as restore_err:
                return f"Failed to spawn new consumer ({e}) AND failed to restore old ({restore_err}). Consumer '{consumer_id}' is gone."

    async def add_consumer(self, spec: ConsumerSpec) -> str:
        """Add a new consumer from a spec."""
        try:
            managed = self.consumer_manager.spawn(spec)
            self._persist_consumers()
            self._update_registry()

            # Stabilization for Flink jobs
            if managed.flink_job_id:
                flink = self.consumer_manager._get_flink_runtime()
                for attempt in range(15):
                    await asyncio.sleep(2)
                    try:
                        status = flink.status(managed.flink_job_id)
                        state = status.get("state", "UNKNOWN")
                    except Exception:
                        if attempt < 3:
                            continue
                        return f"Consumer '{spec.consumer_id}' submitted but could not verify Flink job status"

                    if state == "RUNNING":
                        await asyncio.sleep(3)
                        try:
                            recheck = flink.status(managed.flink_job_id)
                            if recheck.get("state") == "RUNNING":
                                return f"Consumer '{spec.consumer_id}' — Flink job {managed.flink_job_id} stable and RUNNING"
                        except Exception:
                            pass
                        return f"Consumer '{spec.consumer_id}' — Flink job {managed.flink_job_id} RUNNING"

                    if state in ("FAILED", "CANCELED", "FINISHED"):
                        error_detail = ""
                        try:
                            exc = flink.job_exceptions(managed.flink_job_id)
                            root = exc.get("root_exception", "")
                            if root:
                                error_detail = f" Root cause: {root[:200]}"
                        except Exception:
                            pass
                        managed.running = False
                        return f"Flink job '{spec.consumer_id}' {state}.{error_detail}"

                return f"Consumer '{spec.consumer_id}' submitted — Flink job still initializing after 30s"

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
        elif tool_name == "sample_topic":
            from src.cell.kafka_tools import sample_topic
            messages = await sample_topic(tool_input["topic"], tool_input.get("count", 5))
            if not messages:
                return f"No messages found in topic '{tool_input['topic']}' (topic may not exist or be empty)"
            return json.dumps(messages, indent=2, default=str)
        elif tool_name == "topic_stats":
            from src.cell.kafka_tools import topic_stats
            stats = await topic_stats(tool_input.get("topics"))
            return json.dumps(stats, indent=2, default=str)
        elif tool_name == "flink_job_status":
            consumer_id = tool_input.get("consumer_id")
            results = []
            targets = []
            if consumer_id:
                managed = self.consumer_manager.consumers.get(consumer_id)
                if not managed:
                    return f"Consumer '{consumer_id}' not found"
                if not managed.flink_job_id:
                    return f"Consumer '{consumer_id}' is a Python consumer, not Flink SQL"
                targets = [managed]
            else:
                targets = [m for m in self.consumer_manager.consumers.values() if m.flink_job_id]
            if not targets:
                return "No Flink SQL consumers found"
            flink = self.consumer_manager._get_flink_runtime()
            for m in targets:
                try:
                    status = flink.status(m.flink_job_id)
                    results.append({
                        "consumer_id": m.spec.consumer_id,
                        "flink_job_id": m.flink_job_id,
                        **status,
                    })
                except Exception as e:
                    results.append({
                        "consumer_id": m.spec.consumer_id,
                        "flink_job_id": m.flink_job_id,
                        "error": str(e),
                    })
            return json.dumps(results, indent=2, default=str)
        elif tool_name == "flink_inspect":
            consumer_id = tool_input["consumer_id"]
            managed = self.consumer_manager.consumers.get(consumer_id)
            if not managed:
                return f"Consumer '{consumer_id}' not found"
            if not managed.flink_job_id:
                return f"Consumer '{consumer_id}' is a Python consumer, not Flink SQL"
            flink = self.consumer_manager._get_flink_runtime()
            include = tool_input.get("include", "all")
            result = {}
            try:
                if include in ("details", "all"):
                    result["details"] = flink.job_details(managed.flink_job_id)
                if include in ("exceptions", "all"):
                    result["exceptions"] = flink.job_exceptions(managed.flink_job_id)
                if include in ("metrics", "all"):
                    result["metrics"] = flink.job_metrics(managed.flink_job_id)
                if include not in ("details", "exceptions", "metrics", "all"):
                    result["details"] = flink.job_details(managed.flink_job_id)
            except Exception as e:
                return f"Error inspecting Flink job {managed.flink_job_id}: {e}"
            return json.dumps(result, indent=2, default=str)
        elif tool_name == "flink_cluster":
            try:
                flink = self.consumer_manager._get_flink_runtime()
                overview = flink.cluster_overview()
                return json.dumps(overview, indent=2, default=str)
            except Exception as e:
                return f"Error reaching Flink cluster: {e}"
        elif tool_name == "flink_cleanup":
            try:
                flink = self.consumer_manager._get_flink_runtime()
                states = tool_input.get("states", ["FAILED", "FINISHED"])
                cancelled = flink.cancel_jobs_by_state(states)
                if not cancelled:
                    return f"No jobs found in states {states}"
                overview = flink.cluster_overview()
                return (
                    f"Cancelled {len(cancelled)} job(s): "
                    + ", ".join(f"{c['job_id'][:8]}... ({c['state']})" for c in cancelled)
                    + f"\nCluster now: {overview.get('slots_available', '?')} slots available "
                    + f"of {overview.get('slots_total', '?')} total"
                )
            except Exception as e:
                return f"Error during cleanup: {e}"
        elif tool_name == "flink_scale":
            try:
                flink = self.consumer_manager._get_flink_runtime()
                count = tool_input.get("taskmanager_count", 1)
                result = flink.scale_taskmanagers(count)
                return result
            except Exception as e:
                return f"Error scaling Flink: {e}"
        elif tool_name == "check_topology":
            analysis = self.topology_analysis()
            return json.dumps(analysis, indent=2, default=str)
        elif tool_name == "list_cells":
            if not self.orchestrator:
                return "No orchestrator available — cannot list cells"
            cells = []
            for name, cell in self.orchestrator.cells.items():
                cells.append({
                    "name": name,
                    "cell_id": cell.cell_id,
                    "directive": cell.directive[:150],
                    "status": cell.status.value,
                    "consumers": len(cell.consumer_manager.consumers),
                    "is_self": name == self.name,
                })
            return json.dumps(cells, indent=2, default=str)
        elif tool_name == "search_cell_knowledge":
            if not self.orchestrator:
                return "No orchestrator available"
            target_name = tool_input["cell_name"]
            target = self.orchestrator.cells.get(target_name)
            if not target:
                return f"Cell '{target_name}' not found. Use list_cells to see available cells."
            if target_name == self.name:
                return "Use search_knowledge to search your own knowledge base."
            try:
                results = target.knowledge.hybrid_search(
                    query=tool_input["query"],
                    limit=tool_input.get("limit", 5),
                )
                if not results:
                    return f"No matching knowledge found in '{target_name}'."
                return json.dumps(results, indent=2, default=str)
            except Exception as e:
                return f"Error searching '{target_name}': {e}"
        elif tool_name == "query_cell_knowledge":
            if not self.orchestrator:
                return "No orchestrator available"
            target_name = tool_input["cell_name"]
            target = self.orchestrator.cells.get(target_name)
            if not target:
                return f"Cell '{target_name}' not found. Use list_cells to see available cells."
            if target_name == self.name:
                return "Use query_knowledge to query your own knowledge base."
            sql = tool_input.get("sql", "")
            if not sql.strip().upper().startswith("SELECT"):
                return "Only SELECT queries are allowed on other cells' knowledge bases."
            try:
                rows = target.knowledge.execute(sql)
                if not rows:
                    return "No results."
                return json.dumps([list(r) for r in rows], indent=2, default=str)
            except Exception as e:
                return f"Error querying '{target_name}': {e}"
        elif tool_name == "read_decisions":
            last = tool_input.get("last", 20)
            entry_type_filter = tool_input.get("entry_type")
            from src.cell.consumer import ConsumerManager as CM
            entries = await CM.read_dlq(self.decision_topic, limit=last * 3)  # over-read then filter
            if entry_type_filter:
                entries = [e for e in entries if e.get("entry_type") == entry_type_filter or e.get("decision_type") == entry_type_filter]
            entries = entries[-last:]
            if not entries:
                return "No decision log entries found."
            # Slim down entries for readability — drop large fields
            slim = []
            for e in entries:
                s = {
                    "timestamp": e.get("timestamp", "")[:19],
                    "type": e.get("entry_type") or e.get("decision_type", "?"),
                }
                if e.get("reasoning"):
                    s["reasoning"] = e["reasoning"][:200]
                if e.get("action", {}).get("type"):
                    s["action_type"] = e["action"]["type"]
                    cid = e["action"].get("consumer_id")
                    if cid:
                        s["consumer_id"] = cid
                if e.get("message"):
                    s["message"] = e["message"][:100]
                if e.get("reply"):
                    s["reply"] = e["reply"][:100]
                if e.get("tool"):
                    s["tool"] = e["tool"]
                if e.get("result"):
                    s["result"] = str(e["result"])[:100]
                slim.append(s)
            return json.dumps(slim, indent=2, default=str)
        elif tool_name == "inspect_dlq":
            consumer_id = tool_input.get("consumer_id")
            limit = tool_input.get("limit", 10)
            if consumer_id:
                managed = self.consumer_manager.consumers.get(consumer_id)
                if not managed:
                    return f"Consumer '{consumer_id}' not found"
                from src.cell.consumer import ConsumerManager as CM
                entries = await CM.read_dlq(managed.dlq_topic, limit)
                if not entries:
                    return f"No errors in DLQ for '{consumer_id}' (errors count: {managed.errors})"
                return json.dumps(entries, indent=2, default=str)
            else:
                summary = []
                for m in self.consumer_manager.consumers.values():
                    summary.append({
                        "consumer_id": m.spec.consumer_id,
                        "errors": m.errors,
                        "dlq_topic": m.dlq_topic,
                    })
                return json.dumps(summary, indent=2)
        elif tool_name == "create_dashboard":
            if not self.dashboard_registry:
                return "Dashboard server not available"
            dashboard = self.dashboard_registry.create(
                cell_id=self.cell_id,
                cell_name=self.name,
                title=tool_input.get("title", "Dashboard"),
                description=tool_input.get("description", ""),
                panels=tool_input.get("panels", []),
            )
            url = f"http://localhost:3000/dashboard/{dashboard.dashboard_id}"
            return f"Dashboard created: {url} ({len(dashboard.panels)} panels)"
        elif tool_name == "inspect_dashboard":
            if not self.dashboard_registry:
                return "Dashboard server not available"
            detail = self.dashboard_registry.get_detail(tool_input["dashboard_id"])
            if not detail:
                return f"Dashboard '{tool_input['dashboard_id']}' not found"
            return json.dumps(detail, indent=2, default=str)
        elif tool_name == "update_dashboard_panel":
            if not self.dashboard_registry:
                return "Dashboard server not available"
            result = self.dashboard_registry.update_panel(
                tool_input["dashboard_id"],
                tool_input["panel_id"],
                tool_input["updates"],
            )
            return result
        elif tool_name == "add_dashboard_panel":
            if not self.dashboard_registry:
                return "Dashboard server not available"
            result = self.dashboard_registry.add_panel(
                tool_input["dashboard_id"],
                tool_input["panel"],
            )
            return result
        return None

    # --- Decision & persistence ---

    async def _execute_decision(self, decision: dict):
        """Execute a decision from the nucleus.

        Raises on spawn failure so the caller can handle it (e.g., report to CLI).
        """
        self._log_decision(decision)
        action = decision.get("action", {})
        action_type = action.get("type")

        if action_type == "spawn_consumer":
            spec = self._spec_from_action(action)
            if not spec.consumer_id:
                spec.consumer_id = f"consumer-{len(self.consumer_manager.consumers)}"
            managed = self.consumer_manager.spawn(spec)
            self._persist_consumers()
            self._update_registry()
            if managed.flink_job_id:
                print(f"  [{self.name}] Spawned Flink consumer '{spec.consumer_id}' → job {managed.flink_job_id}")
            else:
                print(f"  [{self.name}] Spawned Python consumer '{spec.consumer_id}'")

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

        # Enrich Flink consumers with live job status
        flink_consumers = [m for m in self.consumer_manager.consumers.values() if m.flink_job_id]
        if flink_consumers:
            try:
                flink = self.consumer_manager._get_flink_runtime()
                for m in flink_consumers:
                    try:
                        status = flink.status(m.flink_job_id)
                        # Find the matching consumer dict and add status
                        for c in consumers:
                            if c["consumer_id"] == m.spec.consumer_id:
                                c["flink_status"] = status.get("state", "UNKNOWN")
                                c["flink_duration_ms"] = status.get("duration")
                                break
                    except Exception:
                        for c in consumers:
                            if c["consumer_id"] == m.spec.consumer_id:
                                c["flink_status"] = "UNREACHABLE"
                                break
            except Exception:
                pass

        return f"""Active consumers: {len(consumers)}
Total events processed: {total}
Consumers: {json.dumps(consumers, indent=2)}
Cell status: {self.status.value}

To answer questions about what your consumers do, use the descriptions and detection_patterns above.
To view actual code, use get_consumer_code. To inspect your knowledge base, use describe_schema and query_knowledge.
For Flink SQL consumers, use flink_job_status to check job health. Use replace_consumer to update Flink SQL."""

    # --- Postgres persistence ---

    def _persist_consumers(self):
        """Save all consumer specs + stats to Postgres for reload."""
        specs = []
        for managed in self.consumer_manager.consumers.values():
            entry = {
                "consumer_id": managed.spec.consumer_id,
                "source_topics": managed.spec.source_topics,
                "output_topic": managed.spec.output_topic,
                "consumer_code": managed.spec.consumer_code,
                "description": managed.spec.description,
                "detection_patterns": managed.spec.detection_patterns,
                "knowledge_tables": managed.spec.knowledge_tables,
                "runtime": managed.spec.runtime,
                "events_processed": managed.events_processed,
                "alerts_emitted": managed.alerts_emitted,
            }
            if managed.flink_job_id:
                entry["flink_job_id"] = managed.flink_job_id
            specs.append(entry)
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
                        runtime=s.get("runtime", "python"),
                    ),
                    "events_processed": s.get("events_processed", 0),
                    "alerts_emitted": s.get("alerts_emitted", 0),
                    "flink_job_id": s.get("flink_job_id"),
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
