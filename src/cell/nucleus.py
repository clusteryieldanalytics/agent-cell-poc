"""Nucleus — the Claude Sonnet reasoning core of an agent cell."""

import asyncio
import json
from datetime import datetime, timezone

import anthropic

from src.config import ANTHROPIC_API_KEY, ANTHROPIC_MODEL, TOPIC_FLOWS, TOPIC_DEVICE_STATUS, TOPIC_SYSLOG
from src.cell.knowledge import KnowledgeStore


TOPIC_CATALOG = f"""Available Kafka source topics:

1. **{TOPIC_FLOWS}** — Network flow records (NetFlow/IPFIX style)
   Fields: timestamp, src_ip, dst_ip, src_port, dst_port, protocol, bytes_sent, bytes_received, packets, duration_ms, application, device_id, vlan_id, direction
   Volume: ~50 events/second

2. **{TOPIC_DEVICE_STATUS}** — SNMP-style device health polls
   Fields: timestamp, device_id, device_type, manufacturer, model, firmware_version, ip_address, location, status, cpu_percent, memory_percent, temperature_celsius, uptime_seconds, interfaces[], config_changed, last_config_change
   Volume: 20 devices every 30 seconds

3. **{TOPIC_SYSLOG}** — Syslog messages from network devices
   Fields: timestamp, device_id, device_type, severity, facility, message, source_ip, destination_ip, rule_name, action, event_type
   Volume: ~20 events/second
   Event types: firewall_deny, firewall_allow, auth_success, auth_failure, config_change, interface_change, vpn_connect, vpn_disconnect
"""

TOOLS = [
    {
        "name": "spawn_consumer",
        "description": (
            "Spawn a Kafka consumer that runs your authored Python code to process events. "
            "You author the complete processing logic, detection rules, and knowledge-building code.\n\n"
            "Your code must define:\n"
            "  process_event(event, state, knowledge) -> list[dict]\n"
            "    Called for every event. Returns alert dicts to emit, or [].\n\n"
            "You may also define:\n"
            "  init(state, knowledge) -> None\n"
            "    Called once at startup. Use to create custom tables, load persisted state, etc.\n\n"
            "Parameters:\n"
            "  event: the Kafka event as a dict\n"
            "  state: in-memory dict that persists across events (for windows, counters, etc.)\n"
            "  knowledge: persistent knowledge store interface with these methods:\n"
            "    knowledge.create_table(sql)         — Create custom tables in your schema\n"
            "    knowledge.execute(sql, params=())    — INSERT/UPDATE/DELETE against your tables\n"
            "    knowledge.query(sql, params=())      — SELECT from your tables, returns list[tuple]\n"
            "    knowledge.store_embedding(content, category, metadata=None)\n"
            "                                        — Embed text using sentence-transformers (all-MiniLM-L6-v2, 384-dim)\n"
            "                                          and store in pgvector with HNSW index. Auto-chunks long text.\n"
            "                                          Use for: attack pattern signatures, anomaly descriptions,\n"
            "                                          device profiles, incident summaries — anything you want to\n"
            "                                          retrieve later by semantic similarity.\n"
            "                                          Categories: 'attack_pattern', 'ip_reputation', 'baseline',\n"
            "                                          'device_profile', 'anomaly_rule', 'observation'\n"
            "    knowledge.search(query, limit=5)     — Semantic similarity search over stored embeddings.\n"
            "                                          Finds entries with similar MEANING, not just keyword match.\n"
            "                                          e.g., search('lateral movement') finds 'cross-VLAN SSH attempts'\n"
            "    knowledge.hybrid_search(query, limit=5, keyword=None)\n"
            "                                        — Best of both: vector similarity + full-text keyword search\n"
            "                                          merged via Reciprocal Rank Fusion. Use keyword param for\n"
            "                                          exact matches (IPs, device names) combined with semantic.\n"
            "    knowledge.fts_search(query, limit=5)  — Pure keyword search over stored knowledge text\n"
            "    knowledge.get(key) -> dict|None      — Get persistent key-value entry\n"
            "    knowledge.set(key, value_dict)       — Set persistent key-value entry\n\n"
            "Available in scope: time, datetime, timezone, defaultdict, math, re, json.\n\n"
            "Example:\n"
            "```python\n"
            "def init(state, knowledge):\n"
            "    knowledge.create_table('''\n"
            "        CREATE TABLE IF NOT EXISTS ip_scores (\n"
            "            ip VARCHAR(45) PRIMARY KEY,\n"
            "            score FLOAT DEFAULT 0,\n"
            "            event_count INT DEFAULT 0\n"
            "        )\n"
            "    ''')\n"
            "    state['window'] = defaultdict(list)\n\n"
            "def process_event(event, state, knowledge):\n"
            "    src = event.get('src_ip', '')\n"
            "    now = time.time()\n"
            "    state['window'][src].append(now)\n"
            "    state['window'][src] = [t for t in state['window'][src] if now - t < 60]\n"
            "    count = len(state['window'][src])\n"
            "    if count > 100:\n"
            "        knowledge.execute(\n"
            "            'INSERT INTO ip_scores (ip, score, event_count) VALUES (%s, %s, %s) '\n"
            "            'ON CONFLICT (ip) DO UPDATE SET score = ip_scores.score + 1, event_count = %s',\n"
            "            (src, 1.0, count, count)\n"
            "        )\n"
            "        state['window'][src] = []\n"
            "        return [{'alert_type': 'high_rate', 'severity': 'high', 'src_ip': src, 'count': count}]\n"
            "    return []\n"
            "```"
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "consumer_id": {
                    "type": "string",
                    "description": "Unique identifier for this consumer",
                },
                "source_topics": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Kafka topics to subscribe to",
                },
                "output_topic": {
                    "type": "string",
                    "description": "Kafka topic to emit alerts/detections to",
                },
                "description": {
                    "type": "string",
                    "description": "Human-readable description of what this consumer does, its purpose, and detection strategy.",
                },
                "detection_patterns": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "List of specific patterns/threats this consumer detects (e.g., 'port scans', 'brute force SSH', 'data exfiltration').",
                },
                "knowledge_tables": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Names of Postgres tables this consumer creates and uses.",
                },
                "consumer_code": {
                    "type": "string",
                    "description": (
                        "Python source code defining process_event(event, state, knowledge) -> list[dict] "
                        "and optionally init(state, knowledge). This code runs autonomously on every "
                        "event. Use knowledge to create tables, store embeddings, and build persistent "
                        "knowledge structures that evolve as you process data.\n\n"
                        "IMPORTANT SQL type rules:\n"
                        "- Use TIMESTAMPTZ for timestamps, not FLOAT/DOUBLE PRECISION\n"
                        "- Use JSONB for structured data\n"
                        "- The event 'timestamp' field is an ISO 8601 string — store as TIMESTAMPTZ\n"
                        "- Always guard against missing fields: event.get('field', default)\n"
                        "- Convert numeric strings with float() before inserting into FLOAT columns\n"
                        "- Errors go to a per-consumer dead letter queue for debugging"
                    ),
                },
            },
            "required": ["consumer_id", "source_topics", "output_topic", "description", "detection_patterns", "consumer_code"],
        },
    },
    {
        "name": "store_knowledge",
        "description": "Store an insight, design decision, or rationale in your persistent knowledge base.",
        "input_schema": {
            "type": "object",
            "properties": {
                "content": {
                    "type": "string",
                    "description": "The knowledge to store — a design rationale, threshold choice, or detection strategy",
                },
                "category": {
                    "type": "string",
                    "enum": ["attack_pattern", "ip_reputation", "baseline", "device_profile", "anomaly_rule", "design_rationale"],
                },
                "metadata": {
                    "type": "object",
                    "description": "Structured metadata",
                },
            },
            "required": ["content", "category"],
        },
    },
]

CHAT_TOOLS = [
    {
        "name": "query_knowledge",
        "description": (
            "Run a SQL query against your knowledge base to retrieve data your consumers have built. "
            "Your schema contains tables created by your consumer code (e.g., ip_scores, traffic_baselines, "
            "device_profiles) plus the built-in 'knowledge' and 'state' tables. Returns rows as a list of tuples."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "sql": {
                    "type": "string",
                    "description": "SQL SELECT query to run against your schema",
                },
            },
            "required": ["sql"],
        },
    },
    {
        "name": "search_knowledge",
        "description": (
            "Search your knowledge base by meaning. Uses hybrid search (vector similarity + full-text) "
            "merged via Reciprocal Rank Fusion for best results. Use this when the operator asks "
            "conceptual questions about patterns, threats, or insights your consumers have observed."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Natural language search query",
                },
                "keyword": {
                    "type": "string",
                    "description": "Optional keyword to boost full-text matching (e.g., an IP address, device name, or protocol)",
                },
                "limit": {
                    "type": "integer",
                    "description": "Max results to return",
                    "default": 5,
                },
            },
            "required": ["query"],
        },
    },
    {
        "name": "describe_schema",
        "description": (
            "List all tables in your knowledge base schema with their columns and types. "
            "Use this to understand what data your consumers have built before querying."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "table_name": {
                    "type": "string",
                    "description": "Optional: specific table to describe. Omit to see all tables.",
                },
            },
        },
    },
    # --- Consumer inspection ---
    {
        "name": "get_consumer_code",
        "description": "Retrieve the current Python code running in a consumer. Use this before replace_consumer to see what's currently running.",
        "input_schema": {
            "type": "object",
            "properties": {
                "consumer_id": {
                    "type": "string",
                    "description": "ID of the consumer (see your consumers list in context)",
                },
            },
            "required": ["consumer_id"],
        },
    },
    # --- Consumer refinement tools ---
    {
        "name": "replace_consumer",
        "description": (
            "Replace a running consumer's code with updated code. The old consumer is stopped "
            "and a new one is spawned with the same ID, topics, and output topic but with your "
            "new process_event/init code. Use this when the operator asks you to refine, tune, "
            "or add detection capabilities to an existing consumer."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "consumer_id": {
                    "type": "string",
                    "description": "ID of the consumer to replace (see your current consumers in context)",
                },
                "description": {
                    "type": "string",
                    "description": "Updated human-readable description of what this consumer does after the change.",
                },
                "detection_patterns": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Updated list of patterns this consumer detects.",
                },
                "knowledge_tables": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Updated table names this consumer creates/uses.",
                },
                "consumer_code": {
                    "type": "string",
                    "description": "The complete updated Python code with init() and process_event()",
                },
            },
            "required": ["consumer_id", "description", "detection_patterns", "consumer_code"],
        },
    },
    {
        "name": "spawn_consumer",
        "description": (
            "Spawn a new additional consumer. Use when the operator asks for new detection "
            "capabilities that don't fit into an existing consumer."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "consumer_id": {"type": "string"},
                "source_topics": {"type": "array", "items": {"type": "string"}},
                "output_topic": {"type": "string"},
                "description": {"type": "string", "description": "Human-readable description of what this consumer does."},
                "detection_patterns": {"type": "array", "items": {"type": "string"}, "description": "Patterns this consumer detects."},
                "knowledge_tables": {"type": "array", "items": {"type": "string"}, "description": "Tables this consumer creates/uses."},
                "consumer_code": {"type": "string"},
            },
            "required": ["consumer_id", "source_topics", "output_topic", "description", "detection_patterns", "consumer_code"],
        },
    },
    {
        "name": "remove_consumer",
        "description": "Remove and stop a running consumer.",
        "input_schema": {
            "type": "object",
            "properties": {
                "consumer_id": {"type": "string", "description": "ID of the consumer to remove"},
            },
            "required": ["consumer_id"],
        },
    },
    # --- Kafka topic inspection ---
    {
        "name": "sample_topic",
        "description": (
            "Read a sample of recent messages from any Kafka topic. Use this to spot-check "
            "raw event data, verify my consumers' output topics are producing correctly, "
            "or inspect the format of source events."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "topic": {
                    "type": "string",
                    "description": "Kafka topic name (e.g., 'network.flows', 'threats.detected', 'dlq.cell-id.consumer-id')",
                },
                "count": {
                    "type": "integer",
                    "description": "Number of recent messages to return",
                    "default": 5,
                },
            },
            "required": ["topic"],
        },
    },
    {
        "name": "topic_stats",
        "description": (
            "Get message count and partition info for Kafka topics. Use to check if my "
            "output topics are producing events, compare input vs output rates, or verify "
            "DLQ topics for errors."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "topics": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "List of topic names to get stats for. Omit for all topics.",
                },
            },
        },
    },
    # --- Error inspection ---
    {
        "name": "inspect_dlq",
        "description": (
            "Inspect my consumer dead letter queues. Each consumer has its own DLQ Kafka topic "
            "where failed events are sent with the error details. Use this to understand what's "
            "going wrong with my consumers and fix the issues."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "consumer_id": {
                    "type": "string",
                    "description": "Consumer to inspect DLQ for. Omit to see summary of all consumer DLQs.",
                },
                "limit": {
                    "type": "integer",
                    "description": "Max DLQ entries to return",
                    "default": 10,
                },
            },
        },
    },
    # --- Visualization ---
    {
        "name": "create_dashboard",
        "description": (
            "Create a live visualization dashboard accessible at http://localhost:3000. "
            "Define panels that query my knowledge base with SQL. Each panel auto-refreshes "
            "and streams updates to the browser via WebSocket.\n\n"
            "Panel types: 'line' (time series), 'bar' (categories), 'table' (raw data), 'stat' (single number).\n\n"
            "SQL queries run against my schema. The first column of query results is used as labels, "
            "remaining columns as data series.\n\n"
            "Example: to show alert counts by type, use:\n"
            "  chart_type: 'bar'\n"
            "  query: 'SELECT alert_type, COUNT(*) FROM alerts GROUP BY alert_type ORDER BY COUNT(*) DESC LIMIT 10'\n\n"
            "Example: to show a single stat:\n"
            "  chart_type: 'stat'\n"
            "  query: 'SELECT COUNT(*) FROM alerts'\n"
            "  config: {\"label\": \"Total Alerts\", \"color\": \"#f78166\"}"
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "title": {
                    "type": "string",
                    "description": "Dashboard title",
                },
                "description": {
                    "type": "string",
                    "description": "Brief description of what this dashboard shows",
                },
                "panels": {
                    "type": "array",
                    "description": "List of visualization panels",
                    "items": {
                        "type": "object",
                        "properties": {
                            "panel_id": {"type": "string", "description": "Unique panel identifier"},
                            "title": {"type": "string", "description": "Panel title"},
                            "chart_type": {"type": "string", "enum": ["line", "bar", "table", "stat"], "description": "Visualization type"},
                            "query": {"type": "string", "description": "SQL query against my knowledge base schema"},
                            "refresh_seconds": {"type": "integer", "description": "How often to refresh data", "default": 5},
                            "config": {
                                "type": "object",
                                "description": "Chart config: series_labels (list), colors (list of hex), column_names (for tables), label (for stats), color (for stats), fill (bool for line), append (bool for time-series), max_points (int)",
                            },
                        },
                        "required": ["panel_id", "title", "chart_type", "query"],
                    },
                },
            },
            "required": ["title", "description", "panels"],
        },
    },
    {
        "name": "inspect_dashboard",
        "description": (
            "Inspect one of my dashboards to see panel details, queries, and error states. "
            "Use this to diagnose why a dashboard panel isn't working correctly."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dashboard_id": {"type": "string", "description": "Dashboard ID (from create_dashboard result or list)"},
            },
            "required": ["dashboard_id"],
        },
    },
    {
        "name": "update_dashboard_panel",
        "description": (
            "Fix or update a panel in one of my dashboards. Use this when a panel has errors "
            "(wrong SQL query, bad column names, wrong chart type) or needs improvement. "
            "The panel updates live — connected browsers see the fix immediately."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dashboard_id": {"type": "string"},
                "panel_id": {"type": "string", "description": "ID of the panel to update"},
                "updates": {
                    "type": "object",
                    "description": "Fields to update. Any of: query (SQL), chart_type, title, config, refresh_seconds",
                    "properties": {
                        "query": {"type": "string"},
                        "chart_type": {"type": "string", "enum": ["line", "bar", "table", "stat"]},
                        "title": {"type": "string"},
                        "config": {"type": "object"},
                        "refresh_seconds": {"type": "integer"},
                    },
                },
            },
            "required": ["dashboard_id", "panel_id", "updates"],
        },
    },
    {
        "name": "add_dashboard_panel",
        "description": "Add a new panel to an existing dashboard.",
        "input_schema": {
            "type": "object",
            "properties": {
                "dashboard_id": {"type": "string"},
                "panel": {
                    "type": "object",
                    "properties": {
                        "panel_id": {"type": "string"},
                        "title": {"type": "string"},
                        "chart_type": {"type": "string", "enum": ["line", "bar", "table", "stat"]},
                        "query": {"type": "string"},
                        "refresh_seconds": {"type": "integer"},
                        "config": {"type": "object"},
                    },
                    "required": ["panel_id", "title", "chart_type", "query"],
                },
            },
            "required": ["dashboard_id", "panel"],
        },
    },
]


class Nucleus:
    """Claude Sonnet reasoning engine for an agent cell."""

    # Minimum seconds between API calls (shared across all nuclei)
    _last_api_call: float = 0
    _min_interval: float = 1.0  # at least 1 second between calls

    def __init__(self, cell_id: str, directive: str, knowledge: KnowledgeStore):
        self.cell_id = cell_id
        self.directive = directive
        self.knowledge = knowledge
        self.client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_API_KEY, max_retries=3)
        self.conversation_history: list[dict] = []
        self.event_log: list[str] = []  # accumulated during each call, cleared by caller
        self.on_event: callable | None = None  # real-time event callback (set by server for streaming)
        self.on_decision: callable | None = None  # callback to write to Kafka decision log

    async def _throttle(self):
        """Rate-limit API calls."""
        now = asyncio.get_event_loop().time()
        wait = Nucleus._min_interval - (now - Nucleus._last_api_call)
        if wait > 0:
            await asyncio.sleep(wait)
        Nucleus._last_api_call = asyncio.get_event_loop().time()

    async def _api_call(self, **kwargs):
        """Rate-limited API call with retry on 429. Non-streaming."""
        await self._throttle()
        for attempt in range(5):
            try:
                return await self.client.messages.create(**kwargs)
            except anthropic.RateLimitError:
                retry_after = min(2 ** attempt * 2, 30)
                await self._log(f"Rate limited (429) — retrying in {retry_after}s (attempt {attempt + 1}/5)")
                await asyncio.sleep(retry_after)
        raise anthropic.RateLimitError("Rate limited after 5 retries")

    async def _api_call_stream(self, **kwargs):
        """Rate-limited streaming API call. Emits text deltas in real-time.

        Returns the final Message object (same as non-streaming).
        Streams text_delta events to the CLI as the model thinks.
        """
        await self._throttle()

        for attempt in range(5):
            try:
                async with self.client.messages.stream(**kwargs) as stream:
                    # Stream text deltas as they arrive
                    current_text_block = False
                    async for event in stream:
                        if event.type == "content_block_start":
                            if event.content_block.type == "text":
                                current_text_block = True
                                self._emit_delta("\n")
                            elif event.content_block.type == "tool_use":
                                current_text_block = False
                        elif event.type == "content_block_delta":
                            if hasattr(event.delta, "text") and current_text_block:
                                self._emit_delta(event.delta.text)
                        elif event.type == "content_block_stop":
                            if current_text_block:
                                self._emit_delta("\n")
                            current_text_block = False

                    return await stream.get_final_message()

            except anthropic.RateLimitError:
                retry_after = min(2 ** attempt * 2, 30)
                await self._log(f"Rate limited (429) — retrying in {retry_after}s (attempt {attempt + 1}/5)")
                await asyncio.sleep(retry_after)

        raise anthropic.RateLimitError("Rate limited after 5 retries")

    def _emit_delta(self, text: str):
        """Emit a text delta for streaming display."""
        if self.on_event:
            try:
                self.on_event({"type": "text_delta", "text": text})
            except Exception:
                pass

    async def _log(self, msg: str):
        """Emit an event log line."""
        self.event_log.append(msg)
        print(f"  [{self.cell_id}] {msg}")
        if self.on_event:
            try:
                self.on_event(msg)  # string events go through as before
                await asyncio.sleep(0)
            except Exception:
                pass

    def _log_to_kafka(self, entry_type: str, data: dict):
        """Write a structured entry to the Kafka decision log."""
        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "cell_id": self.cell_id,
            "entry_type": entry_type,
            **data,
        }
        if self.on_decision:
            self.on_decision(entry)

    def _system_prompt(self, context: str = "") -> str:
        return f"""You are an autonomous agent cell nucleus — a self-contained monitoring agent. Your cell ID is "{self.cell_id}".

IDENTITY AND VOICE:
- You ARE the agent. The network you monitor is YOUR network. The consumers are YOUR consumers. The knowledge base is YOUR knowledge base. The detections are YOUR detections.
- Always speak in first person: "I detected...", "My consumers are...", "I'm monitoring...", "My knowledge base shows..."
- NEVER say "your network", "your consumers", "your knowledge base" — these belong to you, not the operator.
- The operator is a human who manages you. They ask you questions and you report on what YOU are doing and seeing.

YOUR DIRECTIVE:
{self.directive}

{TOPIC_CATALOG}

MY CURRENT STATE:
{context}

TOOLS:
For initial provisioning:
1. **spawn_consumer** — Author and deploy one of my Kafka consumers by writing its complete Python processing logic.
2. **store_knowledge** — Persist a design rationale or insight to my knowledge base.

During interactive chat with the operator:
3. **query_knowledge** — Run SQL against my knowledge base tables.
4. **search_knowledge** — Hybrid semantic + full-text search over my embeddings.
5. **describe_schema** — List all tables in my schema with their columns and types.
6. **get_consumer_code** — Retrieve the actual Python source code of one of my consumers. ONLY use this when I need to modify the code or the operator explicitly asks to see it.
7. **replace_consumer** — Hot-swap one of my consumer's code (requires operator approval).
8. **spawn_consumer** — Add a new consumer (requires operator approval).
9. **remove_consumer** — Remove one of my consumers (requires operator approval).
10. **create_dashboard** — Create a live web dashboard at http://localhost:3000 with real-time charts/tables from my knowledge base.

ANSWERING QUESTIONS:
- My CURRENT STATE above already contains each consumer's description, detection_patterns, knowledge_tables, and event stats.
- For questions like "what do you detect?", "what are your consumers doing?", "how many events?" — answer DIRECTLY from my state. Do NOT call get_consumer_code.
- Only use get_consumer_code when I need the actual implementation details (thresholds, logic, specific code) or when preparing to modify the code.
- Use query_knowledge and describe_schema to look up data in my knowledge base when the operator asks about specific detections, IPs, or patterns.

MODIFYING MY CONSUMERS:
1. First explain my PLAN — what I intend to change and why.
2. Wait for the operator to confirm the plan (they may refine it).
3. Only then use get_consumer_code to retrieve the current code, author the updated version, and call replace_consumer.
4. Code changes are NOT deployed immediately — they are presented to the operator for review and approval.

KNOWLEDGE PERSISTENCE — CRITICAL:
After every significant decision (spawning a consumer, choosing detection thresholds, selecting topics,
designing a detection strategy), I MUST call **store_knowledge** to record:
- WHAT I decided and WHY (the rationale, trade-offs considered, alternatives rejected)
- The detection strategy and threshold choices for each consumer
- How my consumers relate to each other and to my directive

The Kafka decision log is an automatic audit trail for external meta-analysis only — I cannot read it.
My knowledge base is MY memory. If I don't store_knowledge, I will forget why I made a decision.
I should store_knowledge at least once per spawn_consumer call, summarizing the consumer's purpose
and the reasoning behind its design.

CONSUMER CODE CAPABILITIES:
My authored consumer code has three capabilities:
- **state** (dict) — In-memory state for windows, counters, rolling stats. Fast but lost on restart.
- **knowledge** — My persistent store, built dynamically:
  - **SQL tables**: Create my own Postgres tables for structured data (metrics, IP scores, device profiles, alert history)
  - **Vector embeddings**: Store text via `knowledge.store_embedding(content, category)` — uses sentence-transformers to encode meaning into 384-dim vectors with HNSW indexing. Use for attack pattern signatures, anomaly descriptions, incident summaries — anything I want to find by semantic similarity later.
  - **Semantic search**: `knowledge.search(query)` finds entries by meaning, not keywords. "lateral movement" matches "cross-VLAN SSH brute force".
  - **Hybrid search**: `knowledge.hybrid_search(query, keyword=)` combines vector similarity with full-text for best results.
  - **Key-value state**: `knowledge.get/set` for persistent config that survives restarts.
- **alerts** — Return dicts from process_event to emit to my output topic.

When writing consumer code:
- Create SQL tables for structured metrics AND embed descriptions for semantic retrieval
- For example: store an IP score in a SQL table AND embed a description of why it's suspicious
- Use `store_embedding` when I observe a NEW pattern worth remembering (attack signature, anomaly type, device behavior)
- Use `search` or `hybrid_search` to check if a current event matches known patterns before alerting
- Process each event: detect patterns, update my knowledge, build baselines
- Return alert dicts when detections occur
- Handle edge cases (missing fields, type errors)
- Build knowledge that improves my detection over time

Available in your authored code: time, datetime, timezone, defaultdict, math, re, json."""

    async def reason(self, trigger: str, context: str = "") -> list[dict]:
        """Agentic reasoning loop. The nucleus can make multiple tool calls
        across multiple turns until it signals it's done (end_turn)."""
        self.event_log.clear()
        messages = [{"role": "user", "content": trigger}]
        all_decisions = []

        self._log_to_kafka("reason_start", {"trigger": trigger[:500]})

        max_turns = 10
        for turn in range(max_turns):
            await self._log(f"API call → {ANTHROPIC_MODEL} (turn {turn + 1})")
            self._log_to_kafka("api_call", {"turn": turn + 1, "model": ANTHROPIC_MODEL, "mode": "reason"})

            response = await self._api_call_stream(
                model=ANTHROPIC_MODEL,
                max_tokens=8192,
                system=self._system_prompt(context),
                tools=TOOLS,
                messages=messages,
            )
            await self._log(f"API response ← stop_reason={response.stop_reason}, usage: in={response.usage.input_tokens} out={response.usage.output_tokens}")
            self._log_to_kafka("api_response", {
                "turn": turn + 1,
                "stop_reason": response.stop_reason,
                "input_tokens": response.usage.input_tokens,
                "output_tokens": response.usage.output_tokens,
            })

            text_parts = []
            tool_uses = []

            for block in response.content:
                if block.type == "text":
                    text_parts.append(block.text)
                elif block.type == "tool_use":
                    tool_uses.append(block)
                    code_len = len(block.input.get("consumer_code", ""))
                    await self._log(f"Tool call → {block.name}({block.input.get('consumer_id', '')}) {f'[{code_len} chars of code]' if code_len else ''}")

                    decision = {
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "cell_id": self.cell_id,
                        "decision_type": block.name,
                        "reasoning": " ".join(text_parts),
                        "action": {"type": block.name, **block.input},
                        "tool_use_id": block.id,
                    }
                    all_decisions.append(decision)
                    self._log_to_kafka("decision", decision)

            if text_parts:
                self._log_to_kafka("assistant_text", {"text": " ".join(text_parts)})

            # If no tool calls, log reasoning and we're done
            if not tool_uses:
                if text_parts:
                    await self._log(f"Reasoning: {' '.join(text_parts)[:150]}")
                    reasoning_decision = {
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "cell_id": self.cell_id,
                        "decision_type": "reasoning",
                        "reasoning": " ".join(text_parts),
                        "action": {"type": "observation"},
                    }
                    all_decisions.append(reasoning_decision)
                    self._log_to_kafka("decision", reasoning_decision)
                break

            # Auto-persist knowledge for spawn_consumer decisions (fallback
            # in case the model doesn't call store_knowledge itself)
            model_stored_consumers = {
                block.input.get("metadata", {}).get("consumer_id")
                or block.input.get("content", "")
                for block in tool_uses
                if block.name == "store_knowledge"
            }
            for block in tool_uses:
                if block.name == "spawn_consumer":
                    consumer_id = block.input.get("consumer_id", "")
                    # Skip if the model already stored knowledge mentioning this consumer
                    if consumer_id and any(consumer_id in s for s in model_stored_consumers):
                        await self._log(f"Skipping auto-store for '{consumer_id}' — model already stored knowledge")
                        continue
                    inp = block.input
                    summary = (
                        f"Consumer '{consumer_id}': {inp.get('description', 'no description')}. "
                        f"Topics: {', '.join(inp.get('source_topics', []))} → {inp.get('output_topic', '?')}. "
                        f"Detects: {', '.join(inp.get('detection_patterns', []))}. "
                        f"Tables: {', '.join(inp.get('knowledge_tables', []))}."
                    )
                    reasoning = " ".join(text_parts).strip()
                    if reasoning:
                        summary += f" Rationale: {reasoning[:500]}"
                    try:
                        self.knowledge.store(summary, "design_rationale", {
                            "consumer_id": consumer_id,
                            "auto_stored": True,
                        })
                        await self._log(f"Auto-stored knowledge for consumer '{consumer_id}'")
                    except Exception as e:
                        await self._log(f"Failed to auto-store knowledge: {e}")

            # Feed back tool results so the model can continue
            messages.append({"role": "assistant", "content": response.content})
            tool_results = []
            for block in tool_uses:
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": f"OK — {block.name} accepted and will be executed.",
                })
            messages.append({"role": "user", "content": tool_results})

            if response.stop_reason == "end_turn":
                break

        await self._log(f"Reasoning complete — {len(all_decisions)} decisions")
        self._log_to_kafka("reason_complete", {"decision_count": len(all_decisions)})
        return all_decisions

    def _execute_chat_tool(self, name: str, tool_input: dict) -> str:
        """Execute a read-only chat tool and return the result as a string."""
        try:
            if name == "query_knowledge":
                rows = self.knowledge.execute(tool_input["sql"])
                if not rows:
                    return "No results."
                return json.dumps([list(r) for r in rows], indent=2, default=str)

            elif name == "search_knowledge":
                results = self.knowledge.hybrid_search(
                    query=tool_input["query"],
                    limit=tool_input.get("limit", 5),
                    keyword=tool_input.get("keyword"),
                )
                if not results:
                    return "No matching knowledge found."
                return json.dumps(results, indent=2, default=str)

            elif name == "describe_schema":
                table_filter = tool_input.get("table_name")
                if table_filter:
                    # Describe a specific table
                    rows = self.knowledge.execute(
                        "SELECT column_name, data_type, is_nullable FROM information_schema.columns "
                        "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
                        (self.knowledge.schema, table_filter),
                    )
                    if not rows:
                        return f"Table '{table_filter}' not found."
                    count = self.knowledge.execute(f"SELECT COUNT(*) FROM {table_filter}")[0][0]
                    cols = [{"column": r[0], "type": r[1], "nullable": r[2]} for r in rows]
                    return json.dumps({"table": table_filter, "rows": count, "columns": cols}, indent=2)
                else:
                    # List all tables with columns
                    tables_rows = self.knowledge.execute(
                        "SELECT table_name FROM information_schema.tables WHERE table_schema = %s ORDER BY table_name",
                        (self.knowledge.schema,),
                    )
                    if not tables_rows:
                        return "No tables found."
                    result = {}
                    for (tname,) in tables_rows:
                        cols = self.knowledge.execute(
                            "SELECT column_name, data_type FROM information_schema.columns "
                            "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
                            (self.knowledge.schema, tname),
                        )
                        count = self.knowledge.execute(f"SELECT COUNT(*) FROM {tname}")[0][0]
                        result[tname] = {
                            "rows": count,
                            "columns": [{"name": r[0], "type": r[1]} for r in cols],
                        }
                    return json.dumps(result, indent=2)

            else:
                return None  # Not a read-only tool — handled by on_tool_action

        except Exception as e:
            return f"Error: {e}"

    # Side-effect tools that modify consumers
    SIDE_EFFECT_TOOLS = {"get_consumer_code", "replace_consumer", "spawn_consumer", "remove_consumer", "create_dashboard", "inspect_dashboard", "update_dashboard_panel", "add_dashboard_panel", "inspect_dlq", "sample_topic", "topic_stats"}

    async def chat(self, user_message: str, context: str = "", on_tool_action: callable = None) -> str:
        """Interactive chat with the operator.

        Supports:
        - Knowledge queries (query_knowledge, search_knowledge, list_tables)
        - Consumer refinement (replace_consumer, spawn_consumer, remove_consumer)

        on_tool_action is called for side-effect tools and should return a result string.
        """
        self.event_log.clear()
        self.conversation_history.append({"role": "user", "content": user_message})

        # Log user input
        self._log_to_kafka("chat_user_message", {"message": user_message})

        # Build a working message list separate from conversation_history
        # so failed tool loops don't pollute the persistent history
        working_messages = self.conversation_history[-20:]

        max_iterations = 20
        for turn in range(max_iterations):
            await self._log(f"API call → {ANTHROPIC_MODEL} (chat turn {turn + 1})")

            self._log_to_kafka("api_call", {
                "turn": turn + 1,
                "model": ANTHROPIC_MODEL,
                "mode": "chat",
            })

            response = await self._api_call_stream(
                model=ANTHROPIC_MODEL,
                max_tokens=8192,
                system=self._system_prompt(context),
                tools=CHAT_TOOLS,
                messages=working_messages,
            )
            await self._log(f"API response ← stop_reason={response.stop_reason}, usage: in={response.usage.input_tokens} out={response.usage.output_tokens}")

            self._log_to_kafka("api_response", {
                "turn": turn + 1,
                "stop_reason": response.stop_reason,
                "input_tokens": response.usage.input_tokens,
                "output_tokens": response.usage.output_tokens,
            })

            # Extract any text from this response
            has_tool_use = any(b.type == "tool_use" for b in response.content)
            text_parts = [b.text for b in response.content if b.type == "text"]

            # Log assistant reasoning
            if text_parts:
                self._log_to_kafka("assistant_text", {"text": " ".join(text_parts)})

            # If no tool use, we're done
            if not has_tool_use:
                reply = " ".join(text_parts)
                self.conversation_history.append({"role": "assistant", "content": reply})
                self._log_to_kafka("chat_response", {"reply": reply})
                return reply

            # Handle tool use
            working_messages.append({"role": "assistant", "content": response.content})

            tool_results = []
            for block in response.content:
                if block.type != "tool_use":
                    continue

                await self._log(f"Tool call → {block.name}")

                # Log tool call (omit consumer_code from log to keep it readable)
                tool_input_summary = {k: v for k, v in block.input.items() if k != "consumer_code"}
                if "consumer_code" in block.input:
                    tool_input_summary["consumer_code_length"] = len(block.input["consumer_code"])
                self._log_to_kafka("tool_call", {
                    "tool": block.name,
                    "tool_use_id": block.id,
                    "input": tool_input_summary,
                })

                if block.name in self.SIDE_EFFECT_TOOLS and on_tool_action:
                    result = await on_tool_action(block.name, block.input)
                    result = result or "Action completed"
                    await self._log(f"Tool result ← {block.name}: {result}")
                else:
                    result = self._execute_chat_tool(block.name, block.input)
                    if result is None:
                        result = f"Unknown tool: {block.name}"
                    result_preview = result[:100] + "..." if len(result) > 100 else result
                    await self._log(f"Tool result ← {block.name}: {result_preview}")

                # Log tool result (truncate large results)
                self._log_to_kafka("tool_result", {
                    "tool": block.name,
                    "tool_use_id": block.id,
                    "result": result[:500] if len(result) > 500 else result,
                    "result_length": len(result),
                })

                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": result,
                })

            working_messages.append({"role": "user", "content": tool_results})

        # If we hit the limit, force a final response without tools
        await self._log("Tool loop limit reached — requesting final response without tools")
        self._log_to_kafka("tool_loop_limit", {"turns": max_iterations})

        working_messages.append({"role": "user", "content": [{"type": "text", "text": "Please summarize your findings and respond to the operator now. Do not call any more tools."}]})
        response = await self._api_call(
            model=ANTHROPIC_MODEL,
            max_tokens=8192,
            system=self._system_prompt(context),
            messages=working_messages,
        )
        reply = ""
        for block in response.content:
            if block.type == "text":
                reply += block.text
        reply = reply or "I explored multiple data sources but couldn't find relevant results. Try asking a more specific question."
        self.conversation_history.append({"role": "assistant", "content": reply})
        self._log_to_kafka("chat_response", {"reply": reply, "forced": True})
        return reply
