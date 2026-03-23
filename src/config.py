import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
POSTGRES_URL = os.getenv("POSTGRES_URL", "postgresql://agentcell:agentcell@localhost:5432/agentcell")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "voyage-3")

# Kafka source topics
TOPIC_FLOWS = "network.flows"
TOPIC_DEVICE_STATUS = "network.device.status"
TOPIC_SYSLOG = "network.syslog"

# Kafka derived topics
TOPIC_THREATS = "threats.detected"
TOPIC_ANOMALIES = "traffic.anomalies"
TOPIC_HEALTH_SCORES = "device.health.scores"

# Producer settings
FLOW_EVENTS_PER_SECOND = 50
DEVICE_POLL_INTERVAL_SECONDS = 30
SYSLOG_EVENTS_PER_SECOND = 20
ANOMALY_INTERVAL_RANGE = (120, 300)  # 2-5 minutes

# Network simulation
VLANS = {100: "corporate", 200: "servers", 300: "iot_guest"}
INTERNAL_SUBNETS = ["10.0.1", "10.0.2", "10.0.3"]  # mapped to VLANs 100, 200, 300
