# Agent Cell PoC: Network Security & Observability

## Implementation Spec for Claude Code
## Domain: Network monitoring, threat detection, traffic analysis
## Relevance: Demonstrates agentic data infrastructure for Auvik-style network observability

## External Specs

Feature specs and platform briefs live in `../specs/agent-cell-platform/`:

- **`flink-integration-spec.md`** — Adds Apache Flink SQL as a second consumer runtime. The nucleus chooses python vs flink_sql per consumer. Flink handles windowed aggregations, joins, and CEP; Python handles knowledge-building. Composable pipeline pattern: Flink → derived topic → Python.
- **`agent-cell-platform-brief.md`** — Platform-level brief for the agent cell architecture.

When implementing a feature from these specs, read the full spec file first.

---

## Overview

A local proof-of-concept that demonstrates the agent cell architecture from the blog post. Multiple agent cells operate over a shared Kafka event substrate, each with its own directive, spawned consumers, and pgvector knowledge base. An operator can add, remove, inspect, and chat with individual agent cells through a CLI/TUI interface.

The domain is network security and observability: synthetic producers generate realistic network telemetry, and agent cells autonomously build their own streaming consumers and knowledge bases to fulfill their directives.

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                   Operator CLI/TUI                   │
│  (add cell, remove cell, chat, inspect decisions)    │
└──────────────────────┬──────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────┐
│                 Cell Orchestrator                     │
│  (manages cell lifecycle, routes commands)            │
└──────┬───────────────┬───────────────┬──────────────┘
       │               │               │
┌──────▼──────┐ ┌──────▼──────┐ ┌──────▼──────┐
│ Threat Cell │ │ Traffic Cell│ │ Device Cell │
│             │ │             │ │             │
│ ┌─────────┐ │ │ ┌─────────┐ │ │ ┌─────────┐ │
│ │ Sonnet  │ │ │ │ Sonnet  │ │ │ │ Sonnet  │ │
│ │ Nucleus │ │ │ │ Nucleus │ │ │ │ Nucleus │ │
│ └────┬────┘ │ │ └────┬────┘ │ │ └────┬────┘ │
│      │      │ │      │      │ │      │      │
│ ┌────▼────┐ │ │ ┌────▼────┐ │ │ ┌────▼────┐ │
│ │Consumer │ │ │ │Consumer │ │ │ │Consumer │ │
│ │ Spawns  │ │ │ │ Spawns  │ │ │ │ Spawns  │ │
│ └─────────┘ │ │ └─────────┘ │ │ └─────────┘ │
│ ┌─────────┐ │ │ ┌─────────┐ │ │ ┌─────────┐ │
│ │pgvector │ │ │ │pgvector │ │ │ │pgvector │ │
│ │knowledge│ │ │ │knowledge│ │ │ │knowledge│ │
│ └─────────┘ │ │ └─────────┘ │ │ └─────────┘ │
└─────────────┘ └─────────────┘ └─────────────┘
       │               │               │
┌──────▼───────────────▼───────────────▼──────────────┐
│                    Kafka Cluster                      │
│                                                       │
│  Source Topics:              Derived Topics:           │
│  ├── network.flows           ├── threats.detected     │
│  ├── network.device.status   ├── traffic.anomalies    │
│  └── network.syslog          └── device.health.scores │
│                                                       │
│  Orchestration Topics:                                │
│  └── agent.decisions.{cell_id}                        │
└──────────────────────────────────────────────────────┘
       ▲               ▲               ▲
┌──────┴──────┐ ┌──────┴──────┐ ┌──────┴──────┐
│  NetFlow    │ │   Device    │ │   Syslog    │
│  Producer   │ │  Producer   │ │  Producer   │
└─────────────┘ └─────────────┘ └─────────────┘
```

---

## Infrastructure (Docker Compose)

All infrastructure runs locally via Docker Compose:

```yaml
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.7.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181

  kafka:
    image: confluentinc/cp-kafka:7.7.0
    depends_on: [zookeeper]
    ports: ["9092:9092"]
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"

  postgres:
    image: pgvector/pgvector:pg16
    ports: ["5432:5432"]
    environment:
      POSTGRES_USER: agentcell
      POSTGRES_PASSWORD: agentcell
      POSTGRES_DB: agentcell
    volumes:
      - pgdata:/var/lib/postgresql/data

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    ports: ["8080:8080"]
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9092

volumes:
  pgdata:
```

**Note on Flink:** For the PoC, agent cells spawn Python consumers (using `confluent-kafka` or `faust`) rather than Flink jobs. This keeps the local setup simple while demonstrating the pattern. The spec calls out where Flink would replace Python consumers in a production deployment.

---

## Synthetic Data Producers

Three producers generate realistic network telemetry. Each runs as a Python process writing to Kafka.

### Producer 1: NetFlow Generator (`network.flows`)

Simulates network flow records similar to what Auvik's TrafficInsights would ingest.

```python
# Event schema
{
    "timestamp": "2026-03-23T14:22:01.332Z",
    "src_ip": "10.0.1.45",
    "dst_ip": "203.0.113.22",
    "src_port": 49832,
    "dst_port": 443,
    "protocol": "TCP",
    "bytes_sent": 15234,
    "bytes_received": 892341,
    "packets": 142,
    "duration_ms": 3200,
    "application": "HTTPS",
    "device_id": "switch-core-01",
    "vlan_id": 100,
    "direction": "outbound"
}
```

**Generation behavior:**
- Normal traffic: randomized flows between internal IPs (10.0.0.0/8) and external IPs
- Baseline: ~50 events/second
- Inject anomaly patterns every 2-5 minutes:
  - Port scan: rapid sequential connections from one source to many ports on one target
  - Data exfiltration: sustained large outbound transfer to a single external IP
  - Lateral movement: internal-to-internal connections across unusual VLANs
  - DDoS: many sources targeting single internal IP with high packet counts
- Devices: 20 simulated network devices (switches, routers, firewalls, APs)
- Internal network: 3 VLANs (100=corporate, 200=servers, 300=IoT/guest)

### Producer 2: Device Status Generator (`network.device.status`)

Simulates SNMP-style device health polling.

```python
# Event schema
{
    "timestamp": "2026-03-23T14:22:05.000Z",
    "device_id": "switch-core-01",
    "device_type": "switch",
    "manufacturer": "Cisco",
    "model": "Catalyst 9300",
    "firmware_version": "17.9.4",
    "ip_address": "10.0.0.1",
    "location": "Building A, Floor 2, MDF",
    "status": "online",
    "cpu_percent": 34.2,
    "memory_percent": 61.8,
    "temperature_celsius": 42.1,
    "uptime_seconds": 8640000,
    "interfaces": [
        {
            "name": "GigabitEthernet1/0/1",
            "status": "up",
            "speed_mbps": 1000,
            "utilization_percent": 45.2,
            "errors_in": 0,
            "errors_out": 0
        }
    ],
    "config_changed": false,
    "last_config_change": "2026-03-15T09:30:00Z"
}
```

**Generation behavior:**
- Poll interval: one status event per device every 30 seconds
- 20 devices with persistent state (CPU, memory, uptime increment realistically)
- Inject anomalies:
  - Device goes offline (status: "offline", no further events until recovery)
  - CPU/memory spike (sudden jump to >90%)
  - Interface errors accumulating
  - Config change detected (config_changed: true)
  - Firmware mismatch (one device running old firmware)

### Producer 3: Syslog Generator (`network.syslog`)

Simulates syslog messages from network devices.

```python
# Event schema
{
    "timestamp": "2026-03-23T14:22:03.112Z",
    "device_id": "fw-edge-01",
    "device_type": "firewall",
    "severity": "warning",
    "facility": "security",
    "message": "Denied TCP connection from 203.0.113.55:44321 to 10.0.2.10:22 (rule: block-external-ssh)",
    "source_ip": "203.0.113.55",
    "destination_ip": "10.0.2.10",
    "rule_name": "block-external-ssh",
    "action": "deny",
    "event_type": "firewall_deny"
}
```

**Generation behavior:**
- Mixed severity: 70% info, 20% warning, 8% error, 2% critical
- Event types: firewall_deny, firewall_allow, auth_success, auth_failure, config_change, interface_change, vpn_connect, vpn_disconnect
- Inject attack patterns that correlate with NetFlow anomalies:
  - When port scan is active in NetFlow, syslog shows corresponding firewall denies
  - When lateral movement occurs, syslog shows unusual auth attempts
  - Brute force: repeated auth_failure from same source IP
- ~20 events/second baseline

---

## Agent Cell Architecture

### Cell Structure

Each agent cell is a Python class that encapsulates:

```python
class AgentCell:
    cell_id: str                    # Unique identifier
    directive: str                  # Business directive (natural language)
    nucleus: ClaudeClient           # Sonnet API client
    consumers: list[Consumer]       # Spawned Kafka consumers
    producers: list[Producer]       # Derived topic producers
    knowledge_db: PgVectorStore     # Cell-specific pgvector schema
    decision_log: KafkaProducer     # Writes to agent.decisions.{cell_id}
    status: CellStatus              # active, paused, terminated
    
    async def reason(self, trigger: str) -> Decision
    async def spawn_consumer(self, topic: str, processor: str) -> Consumer
    async def update_knowledge(self, embeddings: list) -> None
    async def inspect(self) -> CellState
```

### Cell Lifecycle

1. **Birth:** Operator provides a directive. The orchestrator creates the cell, provisions a pgvector schema (`cell_{id}`), and creates the decision log topic.

2. **Reconnaissance:** The cell's nucleus (Sonnet) receives its directive along with a catalog of available Kafka topics and their schemas. It reasons about what data it needs and what consumers to spawn.

3. **Infrastructure provisioning:** The nucleus generates consumer specifications (which topics to read, what aggregations/joins to perform, what to emit). The cell spawns Python consumer processes accordingly.

4. **Knowledge building:** As consumers process events, the cell builds its knowledge base in pgvector: embeddings of observed patterns, structured summaries, statistical baselines.

5. **Steady state:** The cell monitors its consumers, periodically reasons about whether its knowledge base needs updating, and responds to operator queries.

6. **Death:** Operator removes the cell. Consumers are stopped, derived topics are optionally deleted, pgvector schema is dropped.

### Decision Logging

Every action the nucleus takes is logged to `agent.decisions.{cell_id}`:

```python
{
    "timestamp": "2026-03-23T14:25:00Z",
    "cell_id": "threat-detection-01",
    "decision_type": "consumer_spawn",
    "reasoning": "I need to correlate NetFlow data with syslog firewall denies to detect coordinated attacks. Spawning a consumer that joins network.flows with network.syslog on src_ip within a 60-second window.",
    "action": {
        "type": "spawn_consumer",
        "source_topics": ["network.flows", "network.syslog"],
        "join_key": "src_ip",
        "window_seconds": 60,
        "output_topic": "threats.detected"
    },
    "confidence": 0.85
}
```

This is the audit trail from the blog post — immutable, ordered, replayable.

---

## Suggested Agent Cell Directives

### Cell 1: Threat Detection Cell

**Directive:**
"You are a network threat detection agent. Your job is to identify potential security threats by correlating network flow data with firewall logs and syslog events. You should detect: port scans, data exfiltration attempts, lateral movement between VLANs, brute force authentication attempts, and DDoS patterns. When you detect a threat, emit an event to your output topic with a severity score, affected devices, and recommended response. Build a knowledge base of known attack patterns and IP reputation scores to improve your detection over time."

**Expected behavior:**
- Spawns a consumer joining `network.flows` + `network.syslog` on source IP
- Builds windowed aggregations (connections per source IP per minute, bytes out per destination per hour)
- Embeds observed attack signatures in pgvector for similarity matching
- Emits `threats.detected` events with severity, evidence, and recommendations
- Learns: after seeing repeated patterns from the same CIDR block, escalates confidence on future events from that range

### Cell 2: Traffic Anomaly Cell

**Directive:**
"You are a network traffic analysis agent. Your job is to establish baseline traffic patterns for each device and VLAN, and detect when traffic deviates significantly from normal. Monitor bandwidth utilization, application mix, and flow patterns. You should flag: unusual spikes in traffic volume, new application protocols appearing on the network, traffic flowing between VLANs that don't normally communicate, and sudden changes in the ratio of inbound to outbound traffic. Build a statistical model of normal behavior for each device and alert when deviations exceed thresholds you determine are appropriate."

**Expected behavior:**
- Spawns a consumer on `network.flows`, aggregates by device_id and vlan_id
- Computes rolling statistics (mean, stddev) for bytes/packets per device per 5-minute window
- Stores baseline profiles in pgvector as embeddings of traffic pattern vectors
- Emits `traffic.anomalies` when current window deviates >2 stddev from baseline
- Learns: adjusts baselines over time as it observes more data, reduces false positives

### Cell 3: Device Health Cell

**Directive:**
"You are a network device health monitoring agent. Your job is to track the health and status of all network devices and predict potential failures before they cause outages. Monitor device CPU, memory, temperature, interface errors, and uptime patterns. You should flag: devices approaching resource exhaustion, interfaces accumulating errors, devices that have gone offline, firmware versions that are out of date compared to peers, and unexpected configuration changes. Maintain a device inventory in your knowledge base and track health trends over time."

**Expected behavior:**
- Spawns a consumer on `network.device.status`
- Maintains per-device health timeseries in its structured tables
- Embeds device health profiles in pgvector for similarity search ("find devices that look like this one that failed last week")
- Emits `device.health.scores` with per-device health score (0-100) and predicted risk
- Cross-references with `network.syslog` for config change correlation
- Learns: after a device actually goes offline, retroactively identifies the precursor signals and weights them higher for future predictions

---

## Operator Interface

### CLI Commands

```bash
# Cell management
agentcell add --directive "Your directive text here" --name "threat-detector"
agentcell remove --name "threat-detector"
agentcell list
agentcell pause --name "threat-detector"
agentcell resume --name "threat-detector"

# Interaction
agentcell chat --name "threat-detector"
# Opens interactive chat with the cell's nucleus
# The nucleus has access to its knowledge base and recent decisions
# Example: "What threats have you detected in the last hour?"
# Example: "Why did you flag 203.0.113.55 as suspicious?"
# Example: "What's the most common attack pattern you've seen?"

# Inspection
agentcell inspect --name "threat-detector"
# Shows: directive, status, spawned consumers, topic subscriptions,
#        knowledge base stats, recent decisions, resource usage

agentcell decisions --name "threat-detector" --last 20
# Shows the last 20 decision log entries from agent.decisions.{cell_id}

agentcell knowledge --name "threat-detector" --query "port scan patterns"
# Semantic search over the cell's pgvector knowledge base

# System
agentcell topics
# Lists all Kafka topics (source, derived, decision logs)

agentcell status
# Shows all cells, their health, consumer lag, events processed
```

### Chat Interface

When chatting with a cell, the nucleus receives:
- Its original directive (system prompt)
- Its recent decisions (last N from the decision log topic)
- Its knowledge base (RAG retrieval from pgvector based on the user's question)
- Current consumer status and statistics

The chat is conversational. The operator can ask questions, give new instructions, or request the cell to explain its reasoning.

```
$ agentcell chat --name "threat-detector"

You are now chatting with threat-detector (active, 3 consumers, 847 events processed)

> What have you detected in the last hour?

I've detected 3 potential threats in the last hour:

1. **Port scan from 203.0.113.55** (severity: high, 14:22 UTC)
   Observed 342 connection attempts across 180 unique ports on 10.0.2.10
   in a 45-second window. Correlated with 312 firewall denies in syslog.
   
2. **Unusual outbound transfer to 198.51.100.12** (severity: medium, 14:35 UTC)
   Device 10.0.1.45 sent 2.3GB to a single external IP over 8 minutes.
   This exceeds the device's normal outbound baseline by 15x.

3. **Cross-VLAN auth attempts** (severity: medium, 14:51 UTC)
   8 failed SSH attempts from VLAN 300 (IoT/guest) targeting devices
   on VLAN 200 (servers). This VLAN pair has no historical auth traffic.

> Why do you think the outbound transfer is suspicious?

The device 10.0.1.45 has a 7-day baseline average of 150MB/hr outbound.
The 2.3GB transfer represents a 15x deviation. The destination IP
198.51.100.12 has not appeared in any previous flows from this device.
The transfer used port 443 (HTTPS), which is normal for the protocol
but unusual for sustained bulk transfer from this device class.

I've added 198.51.100.12 to my watched IP list and will escalate if
I see additional transfers to it from other internal devices.

> Can you also watch for DNS tunneling patterns?

I'll update my detection logic. DNS tunneling typically manifests as:
high-volume DNS queries to a single domain, unusually long subdomain
strings, and DNS traffic volume disproportionate to HTTP traffic.

[DECISION LOGGED: Updating consumer to add DNS flow pattern detection]

I'll spawn an additional consumer focused on flows where dst_port=53
and aggregate by destination domain. I'll flag domains receiving >100
queries/minute from any single source with average query length >50 chars.
```

---

## Database Schema (Postgres/pgvector)

Each cell gets its own schema for isolation:

```sql
-- Shared setup
CREATE EXTENSION IF NOT EXISTS vector;

-- Per-cell schema (created on cell birth)
CREATE SCHEMA cell_{id};

-- Knowledge embeddings
CREATE TABLE cell_{id}.knowledge (
    id SERIAL PRIMARY KEY,
    content TEXT NOT NULL,
    embedding vector(1536) NOT NULL,
    category VARCHAR(50),        -- 'attack_pattern', 'ip_reputation', 'baseline', etc.
    metadata JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    expires_at TIMESTAMPTZ        -- NULL = permanent
);

CREATE INDEX ON cell_{id}.knowledge 
    USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);

-- Structured state (cell-specific, schema varies by directive)
CREATE TABLE cell_{id}.state (
    key VARCHAR(255) PRIMARY KEY,
    value JSONB NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Cell registry (shared, managed by orchestrator)
CREATE TABLE public.cells (
    cell_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(100) UNIQUE NOT NULL,
    directive TEXT NOT NULL,
    status VARCHAR(20) DEFAULT 'active',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    consumers JSONB DEFAULT '[]',
    topics_subscribed JSONB DEFAULT '[]',
    topics_produced JSONB DEFAULT '[]',
    config JSONB DEFAULT '{}'
);
```

---

## Implementation Plan for Claude Code

### Phase 1: Infrastructure (~1 hour)

```
Set up Docker Compose with Kafka, Zookeeper, Postgres (pgvector), and Kafka UI.
Create the database schema (cells registry, pgvector extension).
Verify Kafka is accessible on localhost:9092 and Postgres on localhost:5432.
Create the three source topics: network.flows, network.device.status, network.syslog.
```

### Phase 2: Synthetic Producers (~2 hours)

```
Build three Python producer scripts that generate realistic network telemetry.
Each producer should run as an independent process.
The NetFlow producer should inject anomaly patterns (port scans, exfiltration,
lateral movement, DDoS) at random intervals every 2-5 minutes.
The Device producer should maintain persistent device state across polls.
The Syslog producer should correlate with NetFlow anomalies (when a port scan
is happening in NetFlow, syslog should show corresponding firewall denies).
Verify events are flowing in Kafka UI.
```

### Phase 3: Cell Orchestrator (~2 hours)

```
Build the AgentCell class and CellOrchestrator.
The orchestrator manages cell lifecycle: create, start, pause, resume, destroy.
Each cell gets its own pgvector schema on creation.
Each cell gets a decision log topic (agent.decisions.{cell_id}) on creation.
The nucleus is a Claude Sonnet client that receives the directive as system prompt.
Implement the reasoning loop: the nucleus receives context (available topics,
schemas, current consumers) and outputs decisions (what consumers to spawn,
what knowledge to build).
Decision logging: every nucleus action writes to the decision log topic.
```

### Phase 4: Consumer Spawning (~2 hours)

```
Implement the consumer spawn mechanism.
The nucleus outputs a consumer specification as structured JSON.
The orchestrator translates this into a Python consumer process (using
confluent-kafka or faust) that reads from specified topics, applies the
specified processing logic, and writes to the specified output topic.
For the PoC, consumers run as asyncio tasks within the cell process.
In production this would be Flink jobs deployed via a platform API.
Implement basic windowed aggregation (tumbling windows for event correlation).
```

### Phase 5: Knowledge Base (~1 hour)

```
Implement the pgvector knowledge store.
The nucleus can request embeddings of observed patterns via the Anthropic API
(or a local embedding model).
Implement semantic search over the knowledge base for RAG during chat.
The nucleus periodically reviews its knowledge base and decides what to
retain, update, or expire.
```

### Phase 6: Operator CLI (~2 hours)

```
Build the CLI using Click or Typer.
Implement: add, remove, list, pause, resume, chat, inspect, decisions, knowledge, topics, status.
The chat command opens an interactive session with the cell's nucleus.
Chat context includes: directive, recent decisions, RAG from knowledge base.
The inspect command shows cell state: consumers, topics, knowledge stats, decisions.
The decisions command replays from the Kafka decision log topic.
```

### Phase 7: Integration & Demo (~1 hour)

```
Start all producers.
Add the three suggested agent cells via CLI.
Watch them reason about available data, spawn consumers, and start detecting.
Demonstrate:
- Chatting with the threat cell about detected attacks
- Inspecting the traffic cell's baseline models
- Watching the device cell predict a failure
- Adding a new cell with a custom directive
- Reviewing decision logs for audit
```

---

## Tech Stack

| Component | Technology | Reason |
|-----------|-----------|--------|
| Event substrate | Kafka (Confluent Docker) | Blog post's core thesis |
| Knowledge base | Postgres + pgvector | Per-cell vector storage + structured state |
| Agent nucleus | Claude Sonnet (API) | Cost-effective for PoC reasoning |
| Embeddings | Anthropic voyage or OpenAI ada-002 | For pgvector knowledge embeddings |
| Consumers | Python (confluent-kafka) | PoC simplicity; Flink in production |
| CLI | Python (Typer + Rich) | Interactive, colored output |
| Monitoring | Kafka UI (Provectus) | Visual topic/consumer inspection |
| Containers | Docker Compose | Local development |
| Language | Python 3.12 | Everything except Kafka/Postgres |

---

## Environment Variables

```bash
ANTHROPIC_API_KEY=sk-ant-...        # For Sonnet nucleus calls
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
POSTGRES_URL=postgresql://agentcell:agentcell@localhost:5432/agentcell
EMBEDDING_MODEL=voyage-3             # Or openai/text-embedding-3-small
```

---

## What This Demonstrates

1. **Agent cells as deployable units.** Each cell has a nucleus, consumers, producers, and knowledge base — the full agent cell from the blog post.

2. **Agents authoring their own infrastructure.** The nucleus decides what consumers to spawn based on its directive and available data. No human writes the consumer code.

3. **The event log as source of truth.** All domain events and all agent decisions flow through Kafka. Full audit trail. Full replayability.

4. **Per-cell knowledge bases as derived projections.** Each cell's pgvector store is a materialized view over the log, optimized for that cell's query patterns.

5. **The three Kafka layers from the blog post.** Layer 1: source topics (domain events). Layer 2: decision log topics (orchestration). Layer 3: derived topics (agent-synthesized events).

6. **Interactive operator experience.** Add, remove, chat with, and inspect agent cells. The operator is the human-in-the-loop, not the pipeline author.

---

## Auvik Interview Relevance

This PoC maps directly to Auvik's problem space:

- **Network discovery and monitoring** → Device Health Cell maintains inventory and tracks status
- **Traffic analysis (TrafficInsights)** → Traffic Anomaly Cell builds baselines and detects deviations
- **Alert management** → Threat Detection Cell correlates events and produces scored alerts
- **Configuration management** → Device Health Cell detects config changes and firmware drift
- **Proactive monitoring** → All cells predict issues before they cause outages

The conversation starter: "What if Auvik's monitoring pipeline was a mesh of autonomous agent cells that could independently reason about network health, provision their own detection logic, and evolve their alerting over time without human-authored rules?"
