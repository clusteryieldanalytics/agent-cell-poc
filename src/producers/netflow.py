"""NetFlow producer — generates network flow records to network.flows topic."""

import asyncio
import random
import time
from datetime import datetime, timezone

import orjson
from confluent_kafka import Producer

from src.config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_FLOWS
from src.producers.anomalies import AnomalyScheduler, AnomalyType

APPLICATIONS = ["HTTPS", "HTTP", "DNS", "SSH", "SMTP", "NTP", "SNMP", "RDP", "FTP", "LDAP"]
APP_PORTS = {"HTTPS": 443, "HTTP": 80, "DNS": 53, "SSH": 22, "SMTP": 25, "NTP": 123, "SNMP": 161, "RDP": 3389, "FTP": 21, "LDAP": 389}
DEVICES = [f"switch-core-0{i}" for i in range(1, 3)] + [f"switch-dist-0{i}" for i in range(1, 3)] + [f"switch-access-0{i}" for i in range(1, 3)] + ["router-edge-01", "router-edge-02", "fw-edge-01", "fw-edge-02"]
PROTOCOLS = ["TCP", "UDP"]


def _random_internal_ip() -> str:
    vlan = random.choice([1, 2, 3])
    return f"10.0.{vlan}.{random.randint(10, 254)}"


def _random_external_ip() -> str:
    return f"{random.choice([203, 198, 192, 151, 104])}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}"


def _vlan_for_ip(ip: str) -> int:
    if ip.startswith("10.0.1."): return 100
    if ip.startswith("10.0.2."): return 200
    if ip.startswith("10.0.3."): return 300
    return 0


def _normal_flow() -> dict:
    app = random.choice(APPLICATIONS)
    src_ip = _random_internal_ip()
    dst_ip = _random_external_ip() if random.random() < 0.8 else _random_internal_ip()
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "src_ip": src_ip,
        "dst_ip": dst_ip,
        "src_port": random.randint(1024, 65535),
        "dst_port": APP_PORTS.get(app, random.randint(1024, 65535)),
        "protocol": "UDP" if app in ("DNS", "NTP", "SNMP") else "TCP",
        "bytes_sent": random.randint(100, 50_000),
        "bytes_received": random.randint(100, 200_000),
        "packets": random.randint(1, 200),
        "duration_ms": random.randint(10, 30_000),
        "application": app,
        "device_id": random.choice(DEVICES),
        "vlan_id": _vlan_for_ip(src_ip),
        "direction": "outbound" if not dst_ip.startswith("10.") else "internal",
    }


def _anomaly_flows(anomaly) -> list[dict]:
    """Generate flows for an active anomaly."""
    flows = []
    now_iso = datetime.now(timezone.utc).isoformat()
    p = anomaly.params

    if anomaly.anomaly_type == AnomalyType.PORT_SCAN:
        for _ in range(random.randint(10, 30)):
            flows.append({
                "timestamp": now_iso,
                "src_ip": p["src_ip"],
                "dst_ip": p["target_ip"],
                "src_port": random.randint(40000, 65535),
                "dst_port": random.choice(p["ports"]),
                "protocol": "TCP",
                "bytes_sent": random.randint(40, 120),
                "bytes_received": 0,
                "packets": random.randint(1, 3),
                "duration_ms": random.randint(1, 50),
                "application": "UNKNOWN",
                "device_id": "fw-edge-01",
                "vlan_id": _vlan_for_ip(p["target_ip"]),
                "direction": "inbound",
            })

    elif anomaly.anomaly_type == AnomalyType.DATA_EXFILTRATION:
        flows.append({
            "timestamp": now_iso,
            "src_ip": p["src_ip"],
            "dst_ip": p["dst_ip"],
            "src_port": random.randint(40000, 65535),
            "dst_port": 443,
            "protocol": "TCP",
            "bytes_sent": p["bytes_per_event"],
            "bytes_received": random.randint(100, 1000),
            "packets": p["bytes_per_event"] // 1400 + 1,
            "duration_ms": random.randint(1000, 5000),
            "application": "HTTPS",
            "device_id": random.choice(DEVICES),
            "vlan_id": _vlan_for_ip(p["src_ip"]),
            "direction": "outbound",
        })

    elif anomaly.anomaly_type == AnomalyType.LATERAL_MOVEMENT:
        target = random.choice(p["targets"])
        flows.append({
            "timestamp": now_iso,
            "src_ip": p["src_ip"],
            "dst_ip": target,
            "src_port": random.randint(40000, 65535),
            "dst_port": p["port"],
            "protocol": "TCP",
            "bytes_sent": random.randint(200, 2000),
            "bytes_received": random.randint(100, 500),
            "packets": random.randint(5, 30),
            "duration_ms": random.randint(100, 3000),
            "application": "SSH",
            "device_id": random.choice(DEVICES),
            "vlan_id": _vlan_for_ip(p["src_ip"]),
            "direction": "internal",
        })

    elif anomaly.anomaly_type == AnomalyType.DDOS:
        for _ in range(random.randint(5, 15)):
            src = random.choice(p["sources"])
            flows.append({
                "timestamp": now_iso,
                "src_ip": src,
                "dst_ip": p["target_ip"],
                "src_port": random.randint(1024, 65535),
                "dst_port": 80,
                "protocol": "TCP",
                "bytes_sent": random.randint(500, 5000),
                "bytes_received": 0,
                "packets": p["packets_per_event"],
                "duration_ms": random.randint(10, 100),
                "application": "HTTP",
                "device_id": "fw-edge-01",
                "vlan_id": _vlan_for_ip(p["target_ip"]),
                "direction": "inbound",
            })

    elif anomaly.anomaly_type == AnomalyType.BRUTE_FORCE:
        flows.append({
            "timestamp": now_iso,
            "src_ip": p["src_ip"],
            "dst_ip": p["target_ip"],
            "src_port": random.randint(40000, 65535),
            "dst_port": p["port"],
            "protocol": "TCP",
            "bytes_sent": random.randint(100, 500),
            "bytes_received": random.randint(50, 200),
            "packets": random.randint(5, 15),
            "duration_ms": random.randint(500, 3000),
            "application": "SSH",
            "device_id": "fw-edge-01",
            "vlan_id": _vlan_for_ip(p["target_ip"]),
            "direction": "inbound",
        })

    return flows


async def run_netflow_producer(scheduler: AnomalyScheduler):
    """Main loop for the NetFlow producer."""
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    print("[NetFlow] Producer started")

    try:
        while True:
            anomalies = scheduler.tick()

            # Normal traffic batch
            batch_size = random.randint(3, 8)
            for _ in range(batch_size):
                flow = _normal_flow()
                producer.produce(
                    TOPIC_FLOWS,
                    key=flow["src_ip"].encode(),
                    value=orjson.dumps(flow),
                )

            # Anomaly traffic
            for anomaly in anomalies:
                for flow in _anomaly_flows(anomaly):
                    producer.produce(
                        TOPIC_FLOWS,
                        key=flow["src_ip"].encode(),
                        value=orjson.dumps(flow),
                    )

            producer.poll(0)
            await asyncio.sleep(1.0 / 10)  # ~50 events/sec with batching
    except asyncio.CancelledError:
        pass
    finally:
        producer.flush(5)
        print("[NetFlow] Producer stopped")
