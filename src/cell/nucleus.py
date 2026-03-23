"""Nucleus — the Claude Sonnet reasoning core of an agent cell."""

import asyncio
import json
from datetime import datetime, timezone

import anthropic

from src.config import ANTHROPIC_API_KEY, TOPIC_FLOWS, TOPIC_DEVICE_STATUS, TOPIC_SYSLOG
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
            "                                        — Store text with vector embedding (384-dim, auto-chunked) for semantic search\n"
            "    knowledge.search(query, limit=5)     — Pure semantic (vector) search over stored embeddings\n"
            "    knowledge.hybrid_search(query, limit=5, keyword=None)\n"
            "                                        — Hybrid search: vector + full-text merged via Reciprocal Rank Fusion\n"
            "    knowledge.fts_search(query, limit=5)  — Full-text keyword search over stored knowledge\n"
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
                        "knowledge structures that evolve as you process data."
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

    async def _api_call(self, **kwargs):
        """Rate-limited API call with retry on 429."""
        # Throttle
        now = asyncio.get_event_loop().time()
        wait = Nucleus._min_interval - (now - Nucleus._last_api_call)
        if wait > 0:
            await asyncio.sleep(wait)
        Nucleus._last_api_call = asyncio.get_event_loop().time()

        # Retry with backoff on rate limit
        for attempt in range(5):
            try:
                return await self.client.messages.create(**kwargs)
            except anthropic.RateLimitError as e:
                retry_after = min(2 ** attempt * 2, 30)  # 2, 4, 8, 16, 30
                await self._log(f"Rate limited (429) — retrying in {retry_after}s (attempt {attempt + 1}/5)")
                await asyncio.sleep(retry_after)
        raise anthropic.RateLimitError("Rate limited after 5 retries")

    async def _log(self, msg: str):
        """Emit an event log line."""
        self.event_log.append(msg)
        print(f"  [{self.cell_id}] {msg}")
        # Fire real-time callback if set (for streaming to CLI)
        if self.on_event:
            try:
                self.on_event(msg)
                # Yield so the event can be flushed to the socket
                await asyncio.sleep(0)
            except Exception:
                pass  # Client disconnected

    def _system_prompt(self, context: str = "") -> str:
        return f"""You are an autonomous agent cell nucleus. Your cell ID is "{self.cell_id}".

YOUR DIRECTIVE:
{self.directive}

{TOPIC_CATALOG}

CURRENT CONTEXT:
{context}

You have tools to:
1. **spawn_consumer** — Author and deploy a Kafka consumer by writing its complete Python processing logic.
2. **store_knowledge** — Persist design rationale and insights to your vector knowledge base.

During interactive chat with the operator, you also have:
3. **query_knowledge** — Run SQL against your knowledge base tables.
4. **search_knowledge** — Hybrid semantic + full-text search over your embeddings.
5. **describe_schema** — List all tables with their columns and types, or describe a specific table.
6. **get_consumer_code** — Retrieve the actual Python source code of a consumer. ONLY use this when you need to modify the code or the operator explicitly asks to see it.
7. **replace_consumer** — Hot-swap a consumer's code (requires operator approval).
8. **spawn_consumer** — Add a new consumer (requires operator approval).
9. **remove_consumer** — Remove a consumer (requires operator approval).

IMPORTANT — Answering questions about your consumers:
- Your CURRENT CONTEXT above already contains each consumer's description, detection_patterns, knowledge_tables, and event stats.
- For questions like "what do you detect?", "what are your consumers doing?", "how many events?" — answer DIRECTLY from the context. Do NOT call get_consumer_code.
- Only use get_consumer_code when you need the actual implementation details (thresholds, logic, specific code) or when preparing to modify the code.

When the operator asks you to MODIFY consumers:
1. First explain your PLAN — what you intend to change and why.
2. Wait for the operator to confirm the plan (they may refine it).
3. Only then use get_consumer_code to retrieve the current code, author the updated version, and call replace_consumer.
4. Code changes are NOT deployed immediately — they are presented to the operator for review and approval.

Your authored consumer code has three capabilities:
- **state** (dict) — In-memory state for windows, counters, rolling stats. Fast but lost on restart.
- **knowledge** — Persistent store you build dynamically:
  - Create your own Postgres tables (CREATE TABLE in your schema)
  - Store and query structured data (INSERT, SELECT, UPDATE)
  - Store vector embeddings for semantic search
  - Persist key-value state that survives restarts
- **alerts** — Return dicts from process_event to emit to the output topic.

Write real, working Python that:
- Creates whatever persistent data structures you need (tables, embeddings, k/v state) in your `init()` function
- Processes each event: detects patterns, updates knowledge, builds baselines
- Returns alert dicts when detections occur
- Handles edge cases (missing fields, type errors)
- Builds knowledge that improves detection over time (IP reputation, pattern libraries, baseline profiles)

Available in your authored code: time, datetime, timezone, defaultdict, math, re, json."""

    async def reason(self, trigger: str, context: str = "") -> list[dict]:
        """Agentic reasoning loop. The nucleus can make multiple tool calls
        across multiple turns until it signals it's done (end_turn)."""
        self.event_log.clear()
        messages = [{"role": "user", "content": trigger}]
        all_decisions = []

        max_turns = 10
        for turn in range(max_turns):
            await self._log(f"API call → claude-sonnet-4 (turn {turn + 1})")
            response = await self._api_call(
                model="claude-sonnet-4-20250514",
                max_tokens=8192,
                system=self._system_prompt(context),
                tools=TOOLS,
                messages=messages,
            )
            await self._log(f"API response ← stop_reason={response.stop_reason}, usage: in={response.usage.input_tokens} out={response.usage.output_tokens}")

            text_parts = []
            tool_uses = []

            for block in response.content:
                if block.type == "text":
                    text_parts.append(block.text)
                elif block.type == "tool_use":
                    tool_uses.append(block)
                    code_len = len(block.input.get("consumer_code", ""))
                    await self._log(f"Tool call → {block.name}({block.input.get('consumer_id', '')}) {f'[{code_len} chars of code]' if code_len else ''}")
                    all_decisions.append({
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "cell_id": self.cell_id,
                        "decision_type": block.name,
                        "reasoning": " ".join(text_parts),
                        "action": {"type": block.name, **block.input},
                        "tool_use_id": block.id,
                    })

            # If no tool calls, log reasoning and we're done
            if not tool_uses:
                if text_parts:
                    await self._log(f"Reasoning: {' '.join(text_parts)[:150]}")
                    all_decisions.append({
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "cell_id": self.cell_id,
                        "decision_type": "reasoning",
                        "reasoning": " ".join(text_parts),
                        "action": {"type": "observation"},
                    })
                break

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
    SIDE_EFFECT_TOOLS = {"get_consumer_code", "replace_consumer", "spawn_consumer", "remove_consumer"}

    async def chat(self, user_message: str, context: str = "", on_tool_action: callable = None) -> str:
        """Interactive chat with the operator.

        Supports:
        - Knowledge queries (query_knowledge, search_knowledge, list_tables)
        - Consumer refinement (replace_consumer, spawn_consumer, remove_consumer)

        on_tool_action is called for side-effect tools and should return a result string.
        """
        self.event_log.clear()
        self.conversation_history.append({"role": "user", "content": user_message})

        # Build a working message list separate from conversation_history
        # so failed tool loops don't pollute the persistent history
        working_messages = self.conversation_history[-20:]

        max_iterations = 20
        for turn in range(max_iterations):
            await self._log(f"API call → claude-sonnet-4 (chat turn {turn + 1})")
            response = await self._api_call(
                model="claude-sonnet-4-20250514",
                max_tokens=8192,
                system=self._system_prompt(context),
                tools=CHAT_TOOLS,
                messages=working_messages,
            )
            await self._log(f"API response ← stop_reason={response.stop_reason}, usage: in={response.usage.input_tokens} out={response.usage.output_tokens}")

            # Extract any text from this response
            has_tool_use = any(b.type == "tool_use" for b in response.content)

            # If no tool use, we're done
            if not has_tool_use:
                reply = ""
                for block in response.content:
                    if block.type == "text":
                        reply += block.text
                self.conversation_history.append({"role": "assistant", "content": reply})
                return reply

            # Handle tool use
            working_messages.append({"role": "assistant", "content": response.content})

            tool_results = []
            for block in response.content:
                if block.type != "tool_use":
                    continue

                await self._log(f"Tool call → {block.name}")

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

                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": result,
                })

            working_messages.append({"role": "user", "content": tool_results})

        # If we hit the limit, force a final response without tools
        await self._log("Tool loop limit reached — requesting final response without tools")
        working_messages.append({"role": "user", "content": [{"type": "text", "text": "Please summarize your findings and respond to the operator now. Do not call any more tools."}]})
        response = await self._api_call(
            model="claude-sonnet-4-20250514",
            max_tokens=8192,
            system=self._system_prompt(context),
            messages=working_messages,
        )
        reply = ""
        for block in response.content:
            if block.type == "text":
                reply += block.text
        self.conversation_history.append({"role": "assistant", "content": reply or "I explored multiple data sources but couldn't find relevant results. Try asking a more specific question."})
        return reply or "I explored multiple data sources but couldn't find relevant results. Try asking a more specific question."
