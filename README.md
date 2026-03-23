# Agent Cell PoC

A proof-of-concept demonstrating the **agent cell architecture** — autonomous AI agents that author and deploy their own streaming infrastructure over a shared Kafka event substrate.

The domain is **network security and observability**: synthetic producers generate realistic network telemetry, and agent cells autonomously build their own Kafka consumers and pgvector knowledge bases to fulfill operator-defined directives.

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                   Operator CLI                       │
│  add cell · chat · inspect · refine · dashboards     │
└──────────────────────┬──────────────────────────────┘
                       │ Unix socket (streaming JSON)
┌──────────────────────▼──────────────────────────────┐
│              Agent Cell Server                       │
│  Cell lifecycle · Nucleus reasoning · Viz server     │
└──────┬───────────────┬───────────────┬──────────────┘
       │               │               │
┌──────▼──────┐ ┌──────▼──────┐ ┌──────▼──────┐
│ Threat Cell │ │ Traffic Cell│ │ Device Cell │
│             │ │             │ │             │
│ ┌─────────┐ │ │ ┌─────────┐ │ │ ┌─────────┐ │
│ │ Claude  │ │ │ │ Claude  │ │ │ │ Claude  │ │
│ │ Nucleus │ │ │ │ Nucleus │ │ │ │ Nucleus │ │
│ └────┬────┘ │ │ └────┬────┘ │ │ └────┬────┘ │
│      │      │ │      │      │ │      │      │
│ ┌────▼────┐ │ │ ┌────▼────┐ │ │ ┌────▼────┐ │
│ │Authored │ │ │ │Authored │ │ │ │Authored │ │
│ │Consumers│ │ │ │Consumers│ │ │ │Consumers│ │
│ └─────────┘ │ │ └─────────┘ │ │ └─────────┘ │
│ ┌─────────┐ │ │ ┌─────────┐ │ │ ┌─────────┐ │
│ │pgvector │ │ │ │pgvector │ │ │ │pgvector │ │
│ │knowledge│ │ │ │knowledge│ │ │ │knowledge│ │
│ └─────────┘ │ │ └─────────┘ │ │ └─────────┘ │
└─────────────┘ └─────────────┘ └─────────────┘
       │               │               │
┌──────▼───────────────▼───────────────▼──────────────┐
│                    Kafka Cluster                      │
│  Source · Derived · Decision Logs · DLQs              │
└──────────────────────────────────────────────────────┘
```

## Key Concepts

**Agent cells author their own infrastructure.** The nucleus (Claude) receives a directive and writes the actual Python code for its Kafka consumers — detection logic, knowledge-building, alert emission. No human writes the consumer code.

**The event log is the source of truth.** All domain events and all agent decisions flow through Kafka. Full audit trail. Full replayability.

**Per-cell knowledge bases are derived projections.** Each cell's pgvector store is a materialized view over the log, with custom tables the nucleus creates dynamically.

**Interactive refinement.** Operators chat with cells, review their detection logic, and approve code changes — similar to a code review workflow.

## Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.12+
- An Anthropic API key

### Setup

```bash
# Clone and install
git clone git@github.com:clusteryieldanalytics/agent-cell-poc.git
cd agent-cell-poc
python3 -m venv .venv
source .venv/bin/activate
pip install -e .

# Configure
cp .env.example .env
# Edit .env and add your ANTHROPIC_API_KEY

# Start infrastructure
docker compose up -d
```

### Run

Three terminals:

**Terminal 1 — Server**
```bash
source .venv/bin/activate
agentcell server
```

**Terminal 2 — Producers**
```bash
source .venv/bin/activate
agentcell producers
```

**Terminal 3 — Operator**
```bash
source .venv/bin/activate

# Add a cell with interactive approval of authored consumer code
agentcell add -n threat-detector -d "You are a network threat detection agent. Your job is to identify potential security threats by correlating network flow data with firewall logs and syslog events. You should detect: port scans, data exfiltration attempts, lateral movement between VLANs, brute force authentication attempts, and DDoS patterns. When you detect a threat, emit an event to your output topic with a severity score, affected devices, and recommended response. Build a knowledge base of known attack patterns and IP reputation scores to improve your detection over time."

# Inspect the cell
agentcell inspect -n threat-detector

# View the authored consumer code
agentcell code -n threat-detector

# Chat with the cell
agentcell chat -n threat-detector

# View the decision audit log
agentcell decisions -n threat-detector --compact

# Check consumer dead letter queues
agentcell dlq -n threat-detector

# View live dashboards
agentcell dashboards
```

## CLI Commands

| Command | Description |
|---------|-------------|
| `agentcell server` | Start the persistent cell server |
| `agentcell producers` | Run synthetic data producers |
| `agentcell add -n NAME -d "DIRECTIVE"` | Add a cell with interactive code approval |
| `agentcell add -n NAME -d "DIRECTIVE" -y` | Add a cell, auto-approve consumers |
| `agentcell remove -n NAME` | Stop a cell (preserves knowledge) |
| `agentcell purge -n NAME` | Delete all resources for a cell |
| `agentcell purge-all` | Delete all cells and resources |
| `agentcell list` | List all cells |
| `agentcell inspect -n NAME` | Inspect cell state, consumers, knowledge |
| `agentcell code -n NAME` | View nucleus-authored consumer code |
| `agentcell chat -n NAME` | Interactive chat with a cell's nucleus |
| `agentcell decisions -n NAME` | View the decision audit log |
| `agentcell dlq -n NAME` | Inspect consumer dead letter queues |
| `agentcell dashboards` | List live visualization dashboards |
| `agentcell topics` | List all Kafka topics |
| `agentcell status` | System-wide status |

## What the Nucleus Can Do

During initial provisioning, the nucleus:
- Reviews available Kafka topics and their schemas
- Authors Python consumer code with detection logic, knowledge-building, and alert emission
- Creates custom Postgres tables in its schema
- Stores design rationale in its knowledge base

During interactive chat, the nucleus can:
- Query its knowledge base (SQL, semantic search, hybrid search)
- Inspect its consumer DLQs to diagnose errors
- Sample raw events from any Kafka topic
- Check topic message counts and rates
- Create live visualization dashboards
- Propose code changes (plan → author → approve → deploy)

## Cell Persistence

Cells survive server restarts:
- Consumer specs (including authored code) are persisted to Postgres
- Event counters carry forward across restarts
- Knowledge base tables persist in pgvector
- Decision logs persist in Kafka

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Event substrate | Kafka (Confluent Docker) |
| Knowledge base | Postgres + pgvector (HNSW indexes) |
| Agent nucleus | Claude (Anthropic API) |
| Embeddings | sentence-transformers/all-MiniLM-L6-v2 |
| Consumers | Python (confluent-kafka, threaded) |
| CLI | Python (Typer + Rich) |
| Dashboards | aiohttp + Chart.js + WebSocket |
| Server | asyncio Unix socket (streaming JSON) |
| Monitoring | Kafka UI (Provectus) |

## Project Structure

```
agent-cell-poc/
├── docker-compose.yml          # Kafka, Zookeeper, Postgres/pgvector, Kafka UI
├── db/init.sql                 # Base schema
├── pyproject.toml              # Dependencies + agentcell CLI entrypoint
├── src/
│   ├── config.py               # Environment config
│   ├── server.py               # Persistent server (Unix socket + streaming)
│   ├── embeddings.py           # Sentence-transformers embedding module
│   ├── cell/
│   │   ├── agent_cell.py       # AgentCell — the core unit
│   │   ├── orchestrator.py     # Cell lifecycle management
│   │   ├── nucleus.py          # Claude reasoning with tool use
│   │   ├── consumer.py         # Dynamic consumer spawning + DLQs
│   │   ├── knowledge.py        # pgvector knowledge store (HNSW, hybrid search)
│   │   └── kafka_tools.py      # Topic sampling and stats
│   ├── cli/
│   │   └── main.py             # Typer CLI (thin client)
│   ├── producers/
│   │   ├── netflow.py          # ~50 events/sec flow records
│   │   ├── device_status.py    # SNMP-style device polls
│   │   ├── syslog.py           # Correlated syslog events
│   │   ├── anomalies.py        # Coordinated anomaly injection
│   │   ├── devices.py          # 20-device inventory
│   │   └── run.py              # Concurrent producer runner
│   └── viz/
│       ├── server.py           # aiohttp dashboard server
│       ├── dashboard.py        # Dashboard data model
│       └── templates.py        # Chart.js HTML templates
```
