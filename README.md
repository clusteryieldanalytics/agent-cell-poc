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
│  Self-audit loop (optional)                          │
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
│ │ Python  │ │ │ │Flink SQL│ │ │ │ Python  │ │
│ │Consumer │ │ │ │  + Py   │ │ │ │Consumer │ │
│ └─────────┘ │ │ └─────────┘ │ │ └─────────┘ │
│ ┌─────────┐ │ │ ┌─────────┐ │ │ ┌─────────┐ │
│ │pgvector │ │ │ │pgvector │ │ │ │pgvector │ │
│ │knowledge│ │ │ │knowledge│ │ │ │knowledge│ │
│ └─────────┘ │ │ └─────────┘ │ │ └─────────┘ │
└─────────────┘ └─────────────┘ └─────────────┘
       │               │               │
┌──────▼───────────────▼───────────────▼──────────────┐
│                    Kafka Cluster                    │
│  Source · Derived · Decision Logs · DLQs            │
└──────────────────────┬──────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────┐
│                 Apache Flink                        │
│  JobManager · TaskManager · SQL Gateway             │
│  Windowed aggregations · Joins · CEP                │
└─────────────────────────────────────────────────────┘
```

## Key Concepts

**Agent cells author their own infrastructure.** The nucleus (Claude) receives a directive and writes the actual code for its Kafka consumers — detection logic, knowledge-building, alert emission. No human writes the consumer code.

**Dual consumer runtimes.** The nucleus chooses the right runtime per consumer:

- **Python** — for consumers that need the knowledge API (custom tables, embeddings, semantic search). Runs in-process with DLQ error tracking.
- **Flink SQL** — for high-throughput stream processing (windowed aggregations, cross-topic joins, `MATCH_RECOGNIZE` pattern detection). Runs as jobs in the Flink cluster.

**Composable pipeline pattern.** Flink SQL handles heavy stream processing and writes to a derived Kafka topic. A Python consumer subscribes to that topic and builds knowledge. This separates throughput from intelligence.

**The event log is the source of truth.** All domain events and all agent decisions flow through Kafka. Full audit trail. Full replayability.

**Per-cell knowledge bases are derived projections.** Each cell's pgvector store is a materialized view over the log, with custom tables the nucleus creates dynamically.

**Interactive refinement.** Operators chat with cells, review their detection logic, and approve code changes — similar to a code review workflow.

**Self-audit.** Cells can periodically review their own consumer health and DLQ errors, autonomously diagnosing and fixing problems without operator intervention.

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

# Start infrastructure (Kafka, Postgres, Flink, Kafka UI)
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

## Provisioning Flow

When you add a cell, the nucleus goes through a structured pipeline:

1. **Reconnaissance** — Samples live events from all source topics and gathers topic stats. The nucleus sees actual field names, value ranges, and event rates before designing anything.

2. **Architecture planning** — The nucleus produces a text-only architecture plan: which consumers, which runtimes, which topics, why each consumer exists. The plan is shown to the operator for approval.

3. **Step-by-step deployment** — Each consumer is proposed, reviewed, approved, spawned, and verified one at a time. Flink consumers are deployed first if Python consumers depend on their output.

4. **Stabilization** — After each consumer is deployed, the server verifies it's actually processing events. For Python consumers, it checks event counts and DLQs. For Flink jobs, it polls the REST API for job status and record throughput. Zero-event consumers trigger an investigation — the nucleus inspects the problem and proposes fixes.

### Adding a Flink-Enabled Cell

To demonstrate the composable pipeline pattern (Flink SQL + Python), use a directive that hints at high-throughput aggregation:

```bash
agentcell add -n traffic-analyst -d "Analyze network traffic patterns. Use Flink SQL for high-throughput windowed aggregations over network flows — group by source IP, compute byte totals and flow counts per minute. Use Python consumers to subscribe to the Flink output, build IP reputation scores in your knowledge base, and detect anomalies using statistical baselines."
```

The nucleus will propose:

- A **Flink SQL consumer** for the windowed aggregation (runs in the Flink cluster)
- A **Python consumer** that subscribes to the Flink output topic for knowledge-building

Flink jobs are visible in the Flink dashboard at `http://localhost:8081`.

## CLI Commands

| Command | Description |
| ------- | ----------- |
| `agentcell server` | Start the persistent cell server |
| `agentcell producers` | Run synthetic data producers |
| `agentcell add -n NAME -d "DIRECTIVE"` | Add a cell (plan → approve → deploy → verify) |
| `agentcell add -n NAME -d "DIRECTIVE" -y` | Add a cell, auto-approve consumers |
| `agentcell remove -n NAME` | Stop a cell (preserves knowledge) |
| `agentcell purge -n NAME` | Delete all resources for a cell (detailed) |
| `agentcell purge-all` | Delete all cells and resources (detailed) |
| `agentcell list` | List all cells |
| `agentcell inspect -n NAME` | Inspect cell state, consumers, topology, knowledge |
| `agentcell code -n NAME` | View nucleus-authored consumer code |
| `agentcell chat -n NAME` | Interactive chat with a cell's nucleus |
| `agentcell decisions -n NAME` | View the decision audit log |
| `agentcell dlq -n NAME` | Inspect consumer dead letter queues |
| `agentcell audit -n NAME` | Trigger a one-shot self-audit (streamed live) |
| `agentcell audit-log` | Stream the autonomous audit log (`tail -f`) |
| `agentcell verify -n NAME` | Run consumer verification/stabilization |
| `agentcell dashboards` | List live visualization dashboards |
| `agentcell topics` | List all Kafka topics |
| `agentcell status` | System-wide status |

## What the Nucleus Can Do

### During Provisioning

- Samples live data from source topics to understand event shapes and value ranges
- Designs an architecture plan with justified threshold choices based on observed data
- Chooses the appropriate runtime (Python or Flink SQL) per consumer
- Authors consumer code one at a time, each reviewed and stabilized before the next
- Creates custom Postgres tables in its schema
- Stores design rationale in its knowledge base

### During Interactive Chat

- Query its knowledge base (SQL, semantic search, hybrid search)
- Inspect its consumer DLQs to diagnose errors
- Check Flink job status, metrics, and exceptions via `flink_job_status`, `flink_inspect`, `flink_cluster`
- Sample raw events from any Kafka topic
- Check topic message counts and rates
- Create and manage live visualization dashboards
- Propose code changes (plan → author → approve → deploy → verify)
- All code changes require operator approval before deployment

### During Self-Audit

- Review consumer health snapshots (events processed, errors, DLQ entries)
- Check Flink job status and throughput metrics
- Investigate zero-event consumers (wrong topic? schema mismatch? Flink config?)
- Diagnose and fix consumer errors autonomously (no operator approval)
- Store audit findings in knowledge base for future reference

## Consumer Runtimes

### Python Runtime (default)

- Full access to the knowledge API: custom tables, vector embeddings, semantic search, key-value state
- Complex per-event Python logic with in-memory state dict
- DLQ error tracking — failed events written to per-consumer dead letter queue topics
- Self-verification: the server checks DLQs and asks the nucleus to fix errors
- Runs in-process via `exec()` in a thread executor

### Flink SQL Runtime

- High-throughput stateless stream processing via Apache Flink
- Windowed aggregations (`TUMBLE`, `HOP`, `SESSION` windows)
- Cross-topic joins within time windows
- Complex event processing via `MATCH_RECOGNIZE`
- Jobs submitted to Flink SQL Gateway REST API, managed via Flink REST API
- No knowledge API access — Flink reads and writes Kafka only
- Job status visible in Flink dashboard at `http://localhost:8081`
- Nucleus can inspect job metrics, exceptions, and cluster state via chat tools

### Composable Pipelines

The recommended pattern for complex workloads:

```
network.flows ──→ [Flink SQL: windowed aggregation] ──→ traffic.summary
                                                              │
traffic.summary ──→ [Python: knowledge-building] ──→ alerts + pgvector
```

Flink handles the heavy lifting (aggregation, joins, filtering). Python handles the intelligence (baselines, embeddings, anomaly detection).

## Self-Audit and Autonomous Repair

### On-Demand Audit

Trigger an audit and watch the nucleus work in real-time:

```bash
agentcell audit -n threat-detector
```

This streams all nucleus activity (API calls, tool use, reasoning, fixes) directly to your terminal. Useful for debugging or verifying the cell is healthy.

### Periodic Autonomous Audit

Enable unattended self-audit via environment variable:

```bash
# In .env or exported before starting the server
SELF_AUDIT_INTERVAL_SECONDS=300  # every 5 minutes (0 = disabled)
```

The server runs audit cycles in the background. During each cycle the nucleus:

- Reviews all consumer health (Python event counts + DLQ, Flink job status + record metrics)
- Investigates any consumer with 0 events or errors
- Fixes broken consumers autonomously (no operator approval needed)
- Logs everything to `/tmp/agentcell-audit.log`

### Monitoring Autonomous Audits

Stream the audit log in a separate terminal:

```bash
# Follow the audit log live (like tail -f)
agentcell audit-log

# Show last 100 lines without following
agentcell audit-log --no-follow -n 100
```

Output shows timestamped nucleus activity per cell:

```
[18:05:00] [threat-detector] ── Audit started ──
[18:05:01] [threat-detector] API call → claude-sonnet-4-6 (chat turn 1)
[18:05:03] [threat-detector] Tool call → flink_job_status
[18:05:04] [threat-detector] Tool call → inspect_dlq
[18:05:06] [threat-detector] All consumers healthy, no issues found.
[18:05:06] [threat-detector] Audit complete: 2 consumers checked, 0 action(s) applied
[18:05:06] [threat-detector] ── Audit ended ──
```

### Reviewing Audit Decisions

After an audit cycle, review what the nucleus decided:

```bash
# Compact decision log — self_audit entries highlighted
agentcell decisions -n threat-detector --compact

# Ask the cell directly
agentcell chat -n threat-detector
> What did your last self-audit find?
```

## Inspect and Topology

`agentcell inspect -n NAME` shows:

- **Consumer table** — ID, runtime (python/flink_sql), topics, description, events in/out, errors, status. Flink consumers show live job state and record counts from the Flink REST API.
- **Flink job IDs** — with links to the Flink dashboard for each Flink consumer.
- **Consumer topology** — visual diagram of the pipeline showing data flow between consumers:

```
Consumer Topology — threat-detector

  ── Data Processing Layer (Flink SQL) ──
    network.flows, network.syslog
      └─→ Flink flow-correlator
            └─→ threats.correlated (derived)

  ── Intelligence Layer (Python) ──
    threats.correlated (from Flink)
      └─→ Python threat-scorer
            └─→ threats.detected + pgvector knowledge
```

- **Knowledge base tables** — row counts, sizes, index types (vector, FTS, GIN).

## Cell Persistence

Cells survive server restarts:

- Consumer specs (including authored code, runtime, and Flink job IDs) are persisted to Postgres
- Event counters carry forward across restarts
- Knowledge base tables persist in pgvector
- Decision logs persist in Kafka
- On reload, Flink jobs are reattached if still running, or resubmitted from persisted SQL

## Environment Variables

| Variable | Default | Description |
| -------- | ------- | ----------- |
| `ANTHROPIC_API_KEY` | (required) | Anthropic API key |
| `ANTHROPIC_MODEL` | `claude-sonnet-4-6` | Claude model to use |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka bootstrap servers |
| `POSTGRES_URL` | `postgresql://agentcell:agentcell@localhost:5432/agentcell` | Postgres connection |
| `FLINK_REST_URL` | `http://localhost:8083` | Flink SQL Gateway URL |
| `FLINK_DASHBOARD_URL` | `http://localhost:8081` | Flink dashboard/REST API URL |
| `SELF_AUDIT_INTERVAL_SECONDS` | `0` | Self-audit interval in seconds (0 = disabled) |

## Tech Stack

| Component | Technology |
| --------- | ---------- |
| Event substrate | Kafka (Confluent Docker) |
| Stream processing | Apache Flink 1.20 (SQL Gateway) |
| Knowledge base | Postgres + pgvector (HNSW indexes) |
| Agent nucleus | Claude (Anthropic API) |
| Embeddings | sentence-transformers/all-MiniLM-L6-v2 |
| Consumers | Python (confluent-kafka) + Flink SQL |
| CLI | Python (Typer + Rich) |
| Dashboards | aiohttp + Chart.js + WebSocket |
| Server | asyncio Unix socket (streaming JSON) |
| Monitoring | Kafka UI + Flink Dashboard |

## Infrastructure Services

| Service | Port | Description |
| ------- | ---- | ----------- |
| Kafka | 9092 | Message broker (host), 29092 (Docker internal) |
| Kafka UI | 8080 | Visual topic/consumer inspection |
| Postgres | 5432 | pgvector knowledge bases |
| Flink Dashboard | 8081 | Flink job monitoring and metrics |
| Flink SQL Gateway | 8083 | SQL statement submission API |
| Viz Server | 3000 | Live cell dashboards |

## Project Structure

```
agent-cell-poc/
├── docker-compose.yml          # Kafka, Zookeeper, Postgres, Kafka UI, Flink
├── docker/flink/Dockerfile     # Custom Flink image with Kafka SQL connector
├── db/init.sql                 # Base schema
├── pyproject.toml              # Dependencies + agentcell CLI entrypoint
├── src/
│   ├── config.py               # Environment config
│   ├── server.py               # Persistent server (Unix socket, streaming, self-audit)
│   ├── embeddings.py           # Sentence-transformers embedding module
│   ├── cell/
│   │   ├── agent_cell.py       # AgentCell — the core unit
│   │   ├── orchestrator.py     # Cell lifecycle management
│   │   ├── nucleus.py          # Claude reasoning with tool use (dual runtime)
│   │   ├── consumer.py         # Consumer spawning, DLQs, Python + Flink dispatch
│   │   ├── flink_runtime.py    # Flink SQL Gateway client (submit, cancel, status, metrics)
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
