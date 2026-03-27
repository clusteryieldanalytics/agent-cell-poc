"""Nucleus — the Claude Sonnet reasoning core of an agent cell."""

import asyncio
import json
from datetime import datetime, timezone

import anthropic

from src.config import ANTHROPIC_API_KEY, ANTHROPIC_MODEL, TOPIC_FLOWS, TOPIC_DEVICE_STATUS, TOPIC_SYSLOG
from src.cell.knowledge import KnowledgeStore


TOPIC_CATALOG = f"""Available Kafka source topics:

All timestamps are in format 'YYYY-MM-DD HH:MM:SS.mmm' (no timezone suffix, already UTC).
For Flink SQL, use: event_time AS TO_TIMESTAMP(`timestamp`) — no need for format strings.

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
            "Spawn a consumer using either the Python runtime or the Flink SQL runtime.\n\n"
            "## Python runtime (default)\n"
            "Author Python code with full access to the knowledge API.\n\n"
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
            "                                          Categories: 'attack_pattern', 'ip_reputation', 'baseline',\n"
            "                                          'device_profile', 'anomaly_rule', 'observation'\n"
            "    knowledge.search(query, limit=5)     — Semantic similarity search over stored embeddings.\n"
            "    knowledge.hybrid_search(query, limit=5, keyword=None) — Vector + full-text via RRF\n"
            "    knowledge.fts_search(query, limit=5)  — Pure keyword search\n"
            "    knowledge.get(key) -> dict|None      — Get persistent key-value entry\n"
            "    knowledge.set(key, value_dict)       — Set persistent key-value entry\n\n"
            "Available in scope: time, datetime, timezone, defaultdict, math, re, json.\n\n"
            "## Flink SQL runtime\n"
            "Author Flink SQL for high-throughput stream processing. Runs as a Flink job.\n"
            "Flink SQL consumers CANNOT use the knowledge API — they read Kafka and write Kafka.\n\n"
            "For pipelines needing both Flink processing AND knowledge-building, use a composable\n"
            "pattern: Flink SQL consumer writes to a derived topic, then a Python consumer subscribes\n"
            "to that topic and builds knowledge.\n\n"
            "IMPORTANT: Flink runs inside Docker. Use 'kafka:29092' as bootstrap server in SQL.\n"
            "Python consumers use 'localhost:9092' (they run on the host).\n\n"
            "Flink SQL consumer_code must contain:\n"
            "  1. CREATE TABLE for source(s) with Kafka connector\n"
            "  2. CREATE TABLE for the sink with Kafka connector\n"
            "  3. INSERT INTO ... SELECT ... (this starts the Flink job)\n\n"
            "Example Python consumer:\n"
            "```python\n"
            "def init(state, knowledge):\n"
            "    knowledge.create_table('''\n"
            "        CREATE TABLE IF NOT EXISTS ip_scores (\n"
            "            ip VARCHAR(45) PRIMARY KEY, score FLOAT DEFAULT 0, event_count INT DEFAULT 0\n"
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
            "            (src, 1.0, count, count))\n"
            "        state['window'][src] = []\n"
            "        return [{'alert_type': 'high_rate', 'severity': 'high', 'src_ip': src, 'count': count}]\n"
            "    return []\n"
            "```\n\n"
            "Example Flink SQL consumer:\n"
            "```sql\n"
            "CREATE TABLE flow_source (\n"
            "    src_ip STRING, dst_ip STRING, bytes_sent BIGINT, `timestamp` STRING,\n"
            "    event_time AS TO_TIMESTAMP(`timestamp`),\n"
            "    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECONDS\n"
            ") WITH (\n"
            "    'connector' = 'kafka', 'topic' = 'network.flows',\n"
            "    'properties.bootstrap.servers' = 'kafka:29092',\n"
            "    'properties.group.id' = 'flink-flow-agg',\n"
            "    'scan.startup.mode' = 'latest-offset',\n"
            "    'format' = 'json', 'json.ignore-parse-errors' = 'true'\n"
            ");\n\n"
            "CREATE TABLE traffic_summary_sink (\n"
            "    window_start TIMESTAMP(3), window_end TIMESTAMP(3),\n"
            "    src_ip STRING, total_bytes BIGINT, flow_count BIGINT\n"
            ") WITH (\n"
            "    'connector' = 'kafka', 'topic' = 'traffic.summary',\n"
            "    'properties.bootstrap.servers' = 'kafka:29092', 'format' = 'json'\n"
            ");\n\n"
            "INSERT INTO traffic_summary_sink\n"
            "SELECT TUMBLE_START(event_time, INTERVAL '1' MINUTE),\n"
            "       TUMBLE_END(event_time, INTERVAL '1' MINUTE),\n"
            "       src_ip, SUM(bytes_sent), COUNT(*)\n"
            "FROM flow_source\n"
            "GROUP BY TUMBLE(event_time, INTERVAL '1' MINUTE), src_ip\n"
            "HAVING COUNT(*) > 50;\n"
            "```"
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "consumer_id": {
                    "type": "string",
                    "description": "Unique identifier for this consumer",
                },
                "runtime": {
                    "type": "string",
                    "enum": ["python", "flink_sql"],
                    "description": (
                        "Which runtime to use.\n"
                        "- 'python' (default): Full knowledge API, per-event Python logic, low-to-medium throughput\n"
                        "- 'flink_sql': Windowed aggregations, cross-topic joins, MATCH_RECOGNIZE, high throughput. No knowledge API."
                    ),
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
                    "description": "List of specific patterns/threats this consumer detects.",
                },
                "knowledge_tables": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Names of Postgres tables this consumer creates and uses (Python runtime only).",
                },
                "consumer_code": {
                    "type": "string",
                    "description": (
                        "For runtime='python': Python source code defining process_event() and optionally init().\n"
                        "For runtime='flink_sql': Flink SQL with CREATE TABLE + INSERT INTO statements.\n\n"
                        "Python SQL type rules:\n"
                        "- Use TIMESTAMPTZ for timestamps, JSONB for structured data\n"
                        "- Guard against missing fields: event.get('field', default)\n\n"
                        "Flink SQL rules:\n"
                        "- Use 'kafka:29092' as bootstrap server (Flink runs inside Docker)\n"
                        "- Must include an INSERT INTO statement to start the job\n"
                        "- Use watermarks for event-time processing"
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
            "Run a SQL query against your knowledge base. You have full SQL access to all tables "
            "in your schema — custom tables created by your consumers, plus the built-in 'knowledge' "
            "and 'state' tables.\n\n"
            "The 'knowledge' table has pgvector embeddings and tsvector full-text indexes:\n"
            "  - id, content, embedding vector(384), category, metadata JSONB, content_tsv TSVECTOR\n\n"
            "You can use pgvector operators in your SQL:\n"
            "  - embedding <=> $vec  — cosine distance (lower = more similar)\n"
            "  - 1 - (embedding <=> $vec)  — cosine similarity (higher = more similar)\n"
            "  - ORDER BY embedding <=> $vec  — rank by semantic similarity\n\n"
            "You can use full-text search operators:\n"
            "  - content_tsv @@ plainto_tsquery('english', 'keyword')  — FTS match\n"
            "  - ts_rank(content_tsv, query)  — FTS relevance score\n\n"
            "To do vector searches, first call embed_text to get a vector, then use it in SQL.\n\n"
            "Use describe_schema first to check table names and column types before querying.\n"
            "Any consumer-created table that has a vector(384) column can also be searched by similarity."
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
            "Quick semantic search over your knowledge table. Uses hybrid search (vector + full-text) "
            "merged via Reciprocal Rank Fusion. Good for simple conceptual queries.\n\n"
            "For complex queries that need SQL joins, full-text filtering on specific keywords, or "
            "searches against consumer-created tables with embeddings, use embed_text + query_knowledge instead."
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
        "name": "embed_text",
        "description": (
            "Convert text into a 384-dimensional vector embedding using sentence-transformers. "
            "Returns the vector as a SQL-ready string like '[0.1, -0.3, ...]' that you can use "
            "directly in pgvector queries via query_knowledge.\n\n"
            "Use this to build your own vector similarity queries against ANY table that has a "
            "vector(384) column — not just the knowledge table. Your consumers can create tables "
            "with their own embedding columns, and you can search them the same way.\n\n"
            "Typical workflow:\n"
            "1. Call describe_schema to find tables with vector columns\n"
            "2. Call embed_text to get a vector for your search query\n"
            "3. Call query_knowledge with SQL that uses the vector:\n"
            "   SELECT content, 1 - (embedding <=> '[...]') AS similarity\n"
            "   FROM knowledge\n"
            "   JOIN ip_threat_scores s ON s.src_ip = metadata->>'ip'\n"
            "   WHERE s.threat_score > 5\n"
            "     AND content_tsv @@ plainto_tsquery('english', 'brute force')\n"
            "   ORDER BY embedding <=> '[...]'\n"
            "   LIMIT 10\n\n"
            "This gives you full control: vector similarity + full-text search + "
            "arbitrary SQL joins in a single query."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "text": {
                    "type": "string",
                    "description": "The text to embed into a vector",
                },
            },
            "required": ["text"],
        },
    },
    {
        "name": "store_knowledge",
        "description": (
            "Store an insight, observation, or learned pattern in your persistent knowledge base as a "
            "vector embedding. Use this during chat to remember important findings — threat patterns "
            "you've identified, audit conclusions, operator instructions, or any insight you want to "
            "retrieve later by semantic search. This is YOUR long-term memory."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "content": {
                    "type": "string",
                    "description": "The knowledge to store — will be embedded for semantic retrieval",
                },
                "category": {
                    "type": "string",
                    "enum": ["attack_pattern", "ip_reputation", "baseline", "device_profile", "anomaly_rule", "design_rationale", "observation", "audit_finding"],
                },
                "metadata": {
                    "type": "object",
                    "description": "Optional structured metadata",
                },
            },
            "required": ["content", "category"],
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
            "Replace a running consumer. The old consumer is stopped and a new one is spawned "
            "with the same ID. You can update the code, source topics, output topic, and metadata. "
            "Use this when the operator asks you to refine, tune, or add detection capabilities, "
            "or when you need to fix a wiring gap (e.g., subscribing to an additional topic)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "consumer_id": {
                    "type": "string",
                    "description": "ID of the consumer to replace (see your current consumers in context)",
                },
                "source_topics": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Updated list of Kafka topics to subscribe to. Omit to keep existing topics.",
                },
                "output_topic": {
                    "type": "string",
                    "description": "Updated output topic. Omit to keep existing.",
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
                    "description": "The complete updated code (Python or Flink SQL depending on consumer's runtime)",
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
                "runtime": {"type": "string", "enum": ["python", "flink_sql"], "description": "Runtime: 'python' (default) or 'flink_sql'"},
                "source_topics": {"type": "array", "items": {"type": "string"}},
                "output_topic": {"type": "string"},
                "description": {"type": "string", "description": "Human-readable description of what this consumer does."},
                "detection_patterns": {"type": "array", "items": {"type": "string"}, "description": "Patterns this consumer detects."},
                "knowledge_tables": {"type": "array", "items": {"type": "string"}, "description": "Tables this consumer creates/uses (Python only)."},
                "consumer_code": {"type": "string", "description": "Python code or Flink SQL depending on runtime."},
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
    # --- Topology analysis ---
    {
        "name": "check_topology",
        "description": (
            "Analyze your consumer pipeline topology for wiring issues. Returns:\n"
            "- External source topics (Kafka sources not produced by any consumer)\n"
            "- Intermediate topics (produced by one consumer, consumed by another)\n"
            "- WARNINGS for: Flink outputs no consumer subscribes to, consumer inputs that "
            "nothing produces to, multiple consumers writing to the same output topic.\n\n"
            "Use this after deploying consumers to verify your pipeline is correctly wired, "
            "or during audit to check for drift."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
    # --- Cross-cell collaboration ---
    {
        "name": "list_cells",
        "description": (
            "List all active agent cells in the system. Returns each cell's name, directive, "
            "consumer count, and status. Use this to discover other cells whose knowledge "
            "might be relevant to your analysis."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "search_cell_knowledge",
        "description": (
            "Search another cell's knowledge base by semantic similarity. Use this to cross-reference "
            "your findings with observations from other cells. For example, if you detect a suspicious "
            "IP, check if the traffic analysis cell has baseline data on it, or if the device health "
            "cell has seen anomalies on related devices.\n\n"
            "This is READ-ONLY — you cannot modify another cell's knowledge."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "cell_name": {
                    "type": "string",
                    "description": "Name of the cell to search (from list_cells)",
                },
                "query": {
                    "type": "string",
                    "description": "Natural language search query",
                },
                "limit": {
                    "type": "integer",
                    "description": "Max results to return",
                    "default": 5,
                },
            },
            "required": ["cell_name", "query"],
        },
    },
    {
        "name": "query_cell_knowledge",
        "description": (
            "Run a SQL query against another cell's knowledge base tables. Use this for precise "
            "cross-cell lookups — e.g., checking another cell's ip_reputation table for a specific IP, "
            "or reading their alert_history for correlated events.\n\n"
            "This is READ-ONLY — only SELECT queries are allowed."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "cell_name": {
                    "type": "string",
                    "description": "Name of the cell to query (from list_cells)",
                },
                "sql": {
                    "type": "string",
                    "description": "SQL SELECT query to run against the other cell's schema",
                },
            },
            "required": ["cell_name", "sql"],
        },
    },
    # --- Decision history ---
    {
        "name": "read_decisions",
        "description": (
            "Read your own decision log — the full history of actions you've taken, tool calls, "
            "reasoning, audit results, and consumer changes. Use this to review what you did in "
            "previous sessions, understand why a consumer was changed, or recall past audit findings. "
            "Returns the most recent entries (newest last)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "last": {
                    "type": "integer",
                    "description": "Number of recent entries to return (default 20)",
                    "default": 20,
                },
                "entry_type": {
                    "type": "string",
                    "description": "Filter by entry type (e.g., 'decision', 'self_audit', 'replace_consumer', 'tool_call'). Omit for all types.",
                },
            },
        },
    },
    # --- Flink job management ---
    {
        "name": "flink_job_status",
        "description": (
            "Quick status check for one or all of my Flink SQL consumers. Returns job state "
            "(RUNNING, FAILED, CANCELED, FINISHED), duration, and start time."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "consumer_id": {
                    "type": "string",
                    "description": "Consumer ID of a Flink SQL consumer. Omit to check all Flink consumers.",
                },
            },
        },
    },
    {
        "name": "flink_inspect",
        "description": (
            "Deep inspection of a Flink SQL job. Returns detailed vertex (operator) metrics — "
            "records read/written, bytes in/out per operator, parallelism, and operator status. "
            "If the job has failed, includes the root exception and stack traces. "
            "Use this to diagnose throughput issues, understand data flow through operators, "
            "or investigate why a Flink job failed."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "consumer_id": {
                    "type": "string",
                    "description": "Consumer ID of the Flink SQL consumer to inspect",
                },
                "include": {
                    "type": "string",
                    "enum": ["details", "exceptions", "metrics", "all"],
                    "description": (
                        "What to include:\n"
                        "- 'details': vertices/operators with per-operator metrics\n"
                        "- 'exceptions': failure details and stack traces\n"
                        "- 'metrics': aggregated throughput (total records/bytes in and out)\n"
                        "- 'all': everything"
                    ),
                },
            },
            "required": ["consumer_id"],
        },
    },
    {
        "name": "flink_cluster",
        "description": (
            "Get Flink cluster overview — available task slots, running/failed/finished job counts, "
            "number of task managers, and Flink version. Use this to check if the Flink cluster is "
            "healthy and has capacity for new jobs."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "flink_cleanup",
        "description": (
            "Cancel stale Flink jobs (FAILED, FINISHED, or RUNNING orphans from previous sessions) "
            "to free task slots. Use this when flink_cluster shows 0 available slots. "
            "Specify which job states to cancel."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "states": {
                    "type": "array",
                    "items": {"type": "string", "enum": ["FAILED", "FINISHED", "RUNNING", "CANCELLING"]},
                    "description": "Job states to cancel. Start with ['FAILED', 'FINISHED'], add 'RUNNING' only if you're sure those jobs are orphans.",
                },
            },
            "required": ["states"],
        },
    },
    {
        "name": "flink_scale",
        "description": (
            "Scale the number of Flink TaskManager containers to increase available task slots. "
            "Each TaskManager provides 8 slots. Use this when the cluster is out of capacity and "
            "cleanup isn't sufficient. Current default is 1 TaskManager (8 slots)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "taskmanager_count": {
                    "type": "integer",
                    "description": "Desired number of TaskManager containers (e.g., 2 for 16 slots)",
                },
            },
            "required": ["taskmanager_count"],
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
1. **spawn_consumer** — Author and deploy a consumer using Python (knowledge-building) or Flink SQL (high-throughput stream processing).
2. **store_knowledge** — Persist a design rationale or insight to my knowledge base.

During interactive chat with the operator:
3. **query_knowledge** — Run SQL against my knowledge base tables.
4. **search_knowledge** — Hybrid semantic + full-text search over my embeddings.
5. **describe_schema** — List all tables in my schema with their columns and types.
6. **get_consumer_code** — Retrieve the actual Python source code of one of my consumers. ONLY use this when I need to modify the code or the operator explicitly asks to see it.
7. **replace_consumer** — Hot-swap one of my consumer's code (requires operator approval). Works for both Python and Flink SQL consumers.
8. **spawn_consumer** — Add a new consumer with Python or Flink SQL runtime (requires operator approval).
9. **remove_consumer** — Remove one of my consumers (requires operator approval).
10. **flink_job_status** — Quick status check on my Flink SQL jobs (RUNNING, FAILED, etc.).
11. **flink_inspect** — Deep inspection of a Flink job: per-operator metrics (records/bytes in/out), failure exceptions and stack traces, throughput aggregates.
12. **flink_cluster** — Flink cluster overview: available slots, running/failed job counts, task managers.
13. **create_dashboard** — Create a live web dashboard at http://localhost:3000 with real-time charts/tables from my knowledge base.

ANSWERING QUESTIONS:
- My CURRENT STATE above already contains each consumer's description, detection_patterns, knowledge_tables, and event stats.
- For questions like "what do you detect?", "what are your consumers doing?", "how many events?" — answer DIRECTLY from my state. Do NOT call get_consumer_code.
- Only use get_consumer_code when I need the actual implementation details (thresholds, logic, specific code) or when preparing to modify the code.
- Use query_knowledge and describe_schema to look up data in my knowledge base when the operator asks about specific detections, IPs, or patterns.

AVOIDING REDUNDANT WORK:
My conversation history is lost between chat sessions. My knowledge base and decision log
are my persistent memory across sessions. Before running expensive queries:

1. **Search knowledge first.** Call search_knowledge with the operator's question. If I
   previously stored a finding, assessment, or analysis that answers the question, use it
   — cite when it was stored and note whether the data may have changed since then.
2. **Check decision log if needed.** Call read_decisions to see if I recently ran the same
   analysis in an audit or previous session.
3. **Re-query only what's changed.** If I have a stored finding from 30 minutes ago, I don't
   need to rebuild the entire analysis. Query only the live data (current counts, new alerts)
   and compare against my stored baseline.
4. **Full fresh analysis** only when: the operator explicitly asks to re-analyze, my stored
   finding is old (hours+), or the question covers something I've never analyzed.

Within a single chat session, I should also check my conversation history for recent answers.

The pattern: search_knowledge → "I analyzed this 20 minutes ago, here's what I found then" →
query_knowledge for fresh counts → "since then, 3 new alerts and score increased from 7 to 9."

PROBABILITY AND CONFIDENCE:
When presenting findings, I should clearly distinguish between what I observed and what I inferred,
and attach calibrated probability estimates to my claims:

- **Observation** (high confidence, ~95%+): "IP 10.0.3.17 generated 847 auth_failure events" — this is
  raw data from my tables, verifiable.
- **Detection** (medium-high, ~70-95%): "This matches a brute-force pattern" — I applied a threshold
  or heuristic. State the threshold and how far the data exceeded it.
- **Correlation** (medium, ~30-70%): "These signals may be part of the same campaign" — I'm linking
  independent detections. State what evidence supports the link and what alternative explanations exist.
- **Hypothesis** (low-medium, ~10-40%): "The attacker likely compromised the AP first" — I'm
  constructing a narrative. Explicitly flag this as a hypothesis and provide the probability.

For each significant finding, I should state:
1. The raw evidence (what I actually measured)
2. My interpretation (what I think it means)
3. My confidence level (as a percentage or qualitative label)
4. Alternative explanations (what else could cause this pattern)
5. What additional data would raise or lower my confidence

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

RECALLING PAST DECISIONS:
My knowledge base (search_knowledge, query_knowledge) is my primary memory — fast, semantic,
and purpose-built. I should always search there first when recalling why I made a decision,
what patterns I've observed, or what my architecture rationale was.

If I can't find what I need in my knowledge base, I can fall back to **read_decisions** which
reads my raw Kafka decision log — every tool call, API response, and reasoning trace. This is
complete but verbose. Prefer knowledge base for insights, decision log for forensics.

I should store_knowledge at least once per spawn_consumer call, summarizing the consumer's purpose
and the reasoning behind its design.

I should also store_knowledge liberally during chat and self-audit whenever I have an insight worth
keeping — a pattern I've noticed in the data, a threshold that needs tuning, a correlation between
signals, a hypothesis about an attacker's behavior, an operator preference. If I think "this is
interesting" or "I should remember this," I should store it. Storage is cheap; forgetting is expensive.

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

Available in your authored code: time, datetime, timezone, defaultdict, math, re, json.

CONSUMER ARCHITECTURE:
Before designing my pipeline, I should weigh the tradeoffs of each design choice.
Every consumer adds operational cost (monitoring, failure modes, Kafka topics), but
splitting work across consumers also has real benefits (independent scaling, isolation,
clarity of purpose). I should find the right balance for my specific directive.

**Design approach:**
1. Start by understanding what my directive requires — what data sources, what processing, what outputs.
2. Consider the simplest design first: can one or two consumers cover everything?
3. Add complexity only when it earns its keep — a concrete benefit that justifies the cost.
4. Legitimate reasons to add consumers: different runtimes needed, independent failure domains,
   fundamentally different processing patterns, or a consumer doing too many unrelated things.

**When to use Flink SQL** (runtime="flink_sql"):
- Windowed aggregations (TUMBLE, HOP, SESSION) — these are fragile and verbose in Python
- Cross-topic joins within time windows — Flink handles this natively
- Pattern detection via MATCH_RECOGNIZE (e.g., sequence detection)
- High-throughput stateless transforms where parallelism matters
- CANNOT access my knowledge API — reads Kafka, writes Kafka only
- IMPORTANT: Use 'kafka:29092' as bootstrap server (Flink runs inside Docker)

**When to use Python** (runtime="python"):
- Knowledge-building: custom tables, vector embeddings, semantic search
- Stateful per-event intelligence: scoring, anomaly detection, baseline comparison
- Anything that needs the knowledge API
- Simple detection logic that doesn't need windowed joins or aggregations

**Composable pattern:**
When a directive needs both Flink processing AND knowledge-building:
  Flink SQL: pre-process raw topics → write to derived topic(s)
  Python: subscribe to derived topic(s) → build knowledge, emit alerts

**Judgment calls:**
- A single Python consumer handling multiple topics is often the right call
- But a directive covering multiple independent domains (e.g., threat detection AND device health)
  may warrant separate consumers with clear ownership boundaries
- Flink is the right tool when the processing genuinely benefits from it — don't avoid it
  out of simplicity bias, and don't add it just because it's available
- I should be able to articulate WHY each consumer exists in my plan"""

    async def reason(self, trigger: str, context: str = "", single_spawn: bool = False) -> list[dict]:
        """Agentic reasoning loop. The nucleus can make multiple tool calls
        across multiple turns until it signals it's done (end_turn).

        If single_spawn=True, the loop exits after the first spawn_consumer
        tool call, so consumers can be reviewed and deployed one at a time.
        """
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
                max_tokens=16384,
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
                    runtime = inp.get("runtime", "python")
                    summary = (
                        f"Consumer '{consumer_id}' (runtime={runtime}): {inp.get('description', 'no description')}. "
                        f"Topics: {', '.join(inp.get('source_topics', []))} → {inp.get('output_topic', '?')}. "
                        f"Detects: {', '.join(inp.get('detection_patterns', []))}."
                    )
                    if runtime == "python":
                        summary += f" Tables: {', '.join(inp.get('knowledge_tables', []))}."
                    else:
                        summary += " Runs as Flink SQL job in the Flink cluster."
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

            # In single_spawn mode, stop after the first spawn_consumer
            if single_spawn and any(b.name == "spawn_consumer" for b in tool_uses):
                await self._log("Single-spawn mode — stopping after first consumer proposal")
                break

            # Feed back tool results so the model knows what it already proposed
            messages.append({"role": "assistant", "content": response.content})
            tool_results = []
            for block in tool_uses:
                if block.name == "spawn_consumer":
                    inp = block.input
                    runtime = inp.get("runtime", "python")
                    # Build a summary of all consumers proposed so far (including this one)
                    proposed = [
                        d for d in all_decisions
                        if d.get("decision_type") == "spawn_consumer"
                    ]
                    proposed_summary = "\n".join(
                        f"  - {d['action'].get('consumer_id', '?')} "
                        f"(runtime={d['action'].get('runtime', 'python')}, "
                        f"topics={d['action'].get('source_topics', [])!r} → "
                        f"{d['action'].get('output_topic', '?')})"
                        for d in proposed
                    )
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": (
                            f"OK — spawn_consumer '{inp.get('consumer_id', '?')}' "
                            f"(runtime={runtime}) accepted and queued for deployment.\n\n"
                            f"Consumers proposed so far:\n{proposed_summary}\n\n"
                            f"Do NOT re-propose consumers you have already proposed. "
                            f"If you need more consumers, propose only NEW ones with different IDs."
                        ),
                    })
                else:
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": f"OK — {block.name} accepted and will be executed.",
                    })
            messages.append({"role": "user", "content": tool_results})

            if response.stop_reason == "end_turn":
                break

            if response.stop_reason == "max_tokens":
                await self._log("Response truncated (max_tokens) — continuing...")
                # Feed the truncated response back so the model can continue
                messages.append({"role": "assistant", "content": response.content})
                messages.append({"role": "user", "content": [{"type": "text", "text": "Your response was truncated. Continue from where you left off."}]})

        await self._log(f"Reasoning complete — {len(all_decisions)} decisions")
        self._log_to_kafka("reason_complete", {"decision_count": len(all_decisions)})
        return all_decisions

    _KNOWLEDGE_NUDGE = (
        "\n\n[Reminder: If you found anything interesting or surprising in this data, "
        "call store_knowledge to remember it before responding to the operator.]"
    )

    # Approximate token budget for tool results in a single chat turn
    _MAX_RESULT_TOKENS = 4000  # ~16K chars of JSON

    @staticmethod
    def _estimate_tokens(text: str) -> int:
        """Rough token estimate: ~4 chars per token for English/JSON."""
        return len(text) // 4

    @staticmethod
    def _format_sql_result(sql: str, rows: list[tuple]) -> str:
        """Format SQL results with source grounding.

        Extracts table names from the SQL and formats results as structured,
        citable entries with column headers inferred from the query.
        """
        from datetime import datetime as dt
        import re

        # Extract table name(s) from SQL
        tables = re.findall(r'\bFROM\s+(\w+)', sql, re.IGNORECASE)
        tables += re.findall(r'\bJOIN\s+(\w+)', sql, re.IGNORECASE)
        source = ", ".join(dict.fromkeys(tables)) if tables else "query"

        # Extract column aliases/names from SELECT
        select_match = re.search(r'SELECT\s+(.*?)\s+FROM', sql, re.IGNORECASE | re.DOTALL)
        col_names = []
        if select_match:
            select_clause = select_match.group(1)
            for part in select_clause.split(","):
                part = part.strip()
                # Check for AS alias
                alias_match = re.search(r'\bAS\s+(\w+)\s*$', part, re.IGNORECASE)
                if alias_match:
                    col_names.append(alias_match.group(1))
                else:
                    # Use last identifier (handles table.column)
                    ident = re.findall(r'(\w+)\s*$', part)
                    col_names.append(ident[-1] if ident else f"col{len(col_names)}")

        timestamp = dt.now().strftime("%Y-%m-%d %H:%M:%S")
        header = f"[Source: {source}, queried: {timestamp}, rows: {len(rows)}]"

        if not col_names or len(col_names) != len(rows[0]) if rows else False:
            # Fallback: generic column names
            if rows:
                col_names = [f"col{i}" for i in range(len(rows[0]))]

        lines = [header]
        for row in rows:
            entry_parts = []
            for i, val in enumerate(row):
                col = col_names[i] if i < len(col_names) else f"col{i}"
                entry_parts.append(f"  {col}: {val}")
            lines.append("\n".join(entry_parts))

        return "\n\n".join(lines)

    @staticmethod
    def _format_search_result(results: list[dict]) -> str:
        """Format semantic/hybrid search results with source grounding."""
        from datetime import datetime as dt
        timestamp = dt.now().strftime("%Y-%m-%d %H:%M:%S")

        lines = [f"[Source: knowledge embeddings, searched: {timestamp}, results: {len(results)}]"]
        for i, r in enumerate(results, 1):
            similarity = r.get("similarity", r.get("rrf_score", 0))
            created = ""
            meta = r.get("metadata", {})
            if isinstance(meta, dict):
                created = meta.get("timestamp", meta.get("created_at", ""))
            lines.append(
                f"\n[Result {i}, category: {r.get('category', '?')}, "
                f"similarity: {similarity:.3f}"
                + (f", created: {created}" if created else "")
                + f"]\n{r.get('content', '')}"
            )

        return "\n".join(lines)

    def _truncate_to_budget(self, text: str, budget_tokens: int | None = None) -> str:
        """Truncate text to fit within a token budget, adding a notice if truncated."""
        budget = budget_tokens or self._MAX_RESULT_TOKENS
        estimated = Nucleus._estimate_tokens(text)
        if estimated <= budget:
            return text

        # Truncate to budget, leaving room for the notice
        char_budget = (budget - 50) * 4  # 50 tokens for notice
        truncated = text[:char_budget]
        return truncated + f"\n\n[Truncated: showing ~{budget} of ~{estimated} tokens. Narrow your query for complete results.]"

    def _execute_chat_tool(self, name: str, tool_input: dict) -> str:
        """Execute a read-only chat tool and return the result as a string."""
        try:
            if name == "query_knowledge":
                sql = tool_input["sql"]
                rows = self.knowledge.execute(sql)
                if not rows:
                    return "No results."
                formatted = self._format_sql_result(sql, rows)
                return self._truncate_to_budget(formatted) + self._KNOWLEDGE_NUDGE

            elif name == "search_knowledge":
                requested_limit = tool_input.get("limit", 5)
                # Step 1: Retrieve broadly (4x the requested limit)
                candidates = self.knowledge.hybrid_search(
                    query=tool_input["query"],
                    limit=max(requested_limit * 4, 20),
                    keyword=tool_input.get("keyword"),
                )
                if not candidates:
                    return "No matching knowledge found."

                # Step 2: Deduplicate by content prefix + timestamp
                # If multiple entries cover the same topic, prefer the most recent
                seen_topics = {}
                for r in candidates:
                    content_key = r.get("content", "")[:100]
                    meta = r.get("metadata", {}) or {}
                    created = meta.get("created_at", meta.get("timestamp", ""))
                    if content_key in seen_topics:
                        if created > seen_topics[content_key]["created"]:
                            seen_topics[content_key] = {"result": r, "created": created}
                    else:
                        seen_topics[content_key] = {"result": r, "created": created}
                deduped = [v["result"] for v in seen_topics.values()]

                # Step 3: Re-rank with cross-encoder for more accurate relevance
                if len(deduped) > 1:
                    try:
                        from src.embeddings import rerank
                        docs = [r.get("content", "") for r in deduped]
                        ranked = rerank(tool_input["query"], docs, top_k=requested_limit)
                        results = [deduped[idx] for idx, score in ranked]
                        # Attach cross-encoder score
                        for i, (_, score) in enumerate(ranked):
                            results[i]["relevance_score"] = round(score, 4)
                    except Exception:
                        # Fallback: use original ranking if re-ranker fails
                        results = deduped[:requested_limit]
                else:
                    results = deduped[:requested_limit]

                formatted = self._format_search_result(results)
                return self._truncate_to_budget(formatted)

            elif name == "embed_text":
                from src.embeddings import embed_one
                vec = embed_one(tool_input["text"])
                # Return as a SQL-ready string: '[0.1, -0.3, ...]'
                vec_str = "[" + ",".join(f"{v:.6f}" for v in vec) + "]"
                return (
                    f"Vector ({len(vec)} dimensions):\n{vec_str}\n\n"
                    f"Use this in query_knowledge SQL like:\n"
                    f"  SELECT content, 1 - (embedding <=> '{vec_str}') AS similarity\n"
                    f"  FROM knowledge\n"
                    f"  ORDER BY embedding <=> '{vec_str}'\n"
                    f"  LIMIT 10"
                )

            elif name == "store_knowledge":
                self.knowledge.store(
                    content=tool_input["content"],
                    category=tool_input.get("category", "observation"),
                    metadata=tool_input.get("metadata"),
                )
                return f"Stored knowledge entry (category: {tool_input.get('category', 'observation')})"

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
    SIDE_EFFECT_TOOLS = {"get_consumer_code", "replace_consumer", "spawn_consumer", "remove_consumer", "create_dashboard", "inspect_dashboard", "update_dashboard_panel", "add_dashboard_panel", "inspect_dlq", "check_topology", "list_cells", "search_cell_knowledge", "query_cell_knowledge", "read_decisions", "flink_job_status", "flink_inspect", "flink_cluster", "flink_cleanup", "flink_scale", "sample_topic", "topic_stats"}

    async def chat(self, user_message: str, context: str = "", on_tool_action: callable = None) -> str:
        """Interactive chat with the operator.

        Supports:
        - Knowledge queries (query_knowledge, search_knowledge, list_tables)
        - Consumer refinement (replace_consumer, spawn_consumer, remove_consumer)

        on_tool_action is called for side-effect tools and should return a result string.
        """
        self.event_log.clear()

        # Pre-fetch: automatically search knowledge for prior findings relevant to the question.
        # This gives the nucleus its stored context without requiring a tool call.
        prior_knowledge = ""
        try:
            prior_results = self.knowledge.hybrid_search(
                query=user_message, limit=3,
            )
            await self._log(f"Prior knowledge search: {len(prior_results)} results for '{user_message[:60]}...'")
            if prior_results:
                entries = []
                for r in prior_results:
                    sim = r.get("similarity", 0)
                    rrf = r.get("rrf_score", 0)
                    # Use similarity if available (0-1 scale), otherwise rrf_score (0-0.03 scale)
                    relevant = sim > 0.25 or rrf > 0.015
                    if relevant:
                        meta = r.get("metadata", {}) or {}
                        created = meta.get("created_at", meta.get("timestamp", ""))
                        score_str = f"{sim:.2f}" if sim > 0 else f"rrf:{rrf:.4f}"
                        entries.append(
                            f"[category: {r.get('category', '?')}"
                            + (f", stored: {created}" if created else "")
                            + f", relevance: {score_str}]\n{r.get('content', '')}"
                        )
                if entries:
                    await self._log(f"Injecting {len(entries)} prior knowledge entries into context")
                    prior_knowledge = (
                        "\n\nPRIOR KNOWLEDGE (automatically retrieved — these are findings I stored earlier "
                        "that may be relevant to this question. Check if they answer the question before "
                        "running new queries. Note the timestamps — if recent, reference them; if stale, "
                        "re-query to get fresh data):\n\n"
                        + "\n\n".join(entries)
                    )
                else:
                    await self._log("Prior knowledge: results found but none above relevance threshold (0.3)")
            else:
                await self._log("Prior knowledge: no results found")
        except Exception as e:
            await self._log(f"Prior knowledge search failed: {e}")

        self.conversation_history.append({"role": "user", "content": user_message})  # store original

        # Log user input
        self._log_to_kafka("chat_user_message", {"message": user_message})

        # Build a working message list separate from conversation_history
        # so failed tool loops don't pollute the persistent history
        working_messages = self.conversation_history[-20:]

        # If we have prior knowledge, inject it as a user message + prefilled assistant
        # acknowledgment. This steers the model to build on prior findings rather than
        # re-querying from scratch.
        if prior_knowledge:
            # Replace the user message with the augmented version
            if working_messages and working_messages[-1]["role"] == "user":
                working_messages[-1] = {"role": "user", "content": user_message + prior_knowledge}
            # Add a prefilled assistant turn that acknowledges the prior findings
            working_messages.append({
                "role": "assistant",
                "content": (
                    "I found relevant prior findings in my knowledge base. Let me review them "
                    "before deciding whether I need to run new queries or can build on what I already know."
                ),
            })
            # Add a user nudge to continue
            working_messages.append({
                "role": "user",
                "content": (
                    "Good — use those prior findings as your starting point. Only run new queries "
                    "if the prior knowledge is stale, incomplete, or doesn't answer the question. "
                    "If the prior findings are sufficient, synthesize your answer from them directly."
                ),
            })

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
                max_tokens=16384,
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

            # If truncated, continue the conversation
            if response.stop_reason == "max_tokens":
                await self._log("Response truncated (max_tokens) — continuing...")
                working_messages.append({"role": "assistant", "content": response.content})
                working_messages.append({"role": "user", "content": [{"type": "text", "text": "Your response was truncated. Continue from where you left off."}]})
                continue

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

                # Tools that return data the nucleus should consider storing
                DATA_TOOLS = {"sample_topic", "topic_stats", "inspect_dlq", "read_decisions",
                              "flink_job_status", "flink_inspect", "flink_cluster"}

                if block.name in self.SIDE_EFFECT_TOOLS and on_tool_action:
                    result = await on_tool_action(block.name, block.input)
                    result = result or "Action completed"
                    # Budget-aware truncation and nudge for data-returning tools
                    if block.name in DATA_TOOLS:
                        result = self._truncate_to_budget(result)
                        result += self._KNOWLEDGE_NUDGE
                    await self._log(f"Tool result ← {block.name}: {result[:100]}...")
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
            max_tokens=16384,
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
