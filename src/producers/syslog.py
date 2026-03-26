"""Syslog producer — generates syslog events to network.syslog topic."""

import asyncio
import random
from datetime import datetime, timezone

import orjson
from confluent_kafka import Producer

from src.config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_SYSLOG
from src.producers.anomalies import AnomalyScheduler, AnomalyType

FIREWALL_DEVICES = ["fw-edge-01", "fw-edge-02", "fw-internal-01"]
ALL_DEVICES = FIREWALL_DEVICES + ["switch-core-01", "switch-core-02", "router-edge-01", "router-edge-02", "vpn-01"]

NORMAL_TEMPLATES = [
    {"severity": "info", "facility": "system", "event_type": "interface_change", "action": "info",
     "message_tpl": "Interface {iface} on {device} changed state to up"},
    {"severity": "info", "facility": "network", "event_type": "firewall_allow", "action": "allow",
     "message_tpl": "Allowed {proto} connection from {src}:{sport} to {dst}:{dport} (rule: {rule})"},
    {"severity": "info", "facility": "auth", "event_type": "auth_success", "action": "allow",
     "message_tpl": "Successful login for user {user} from {src} via SSH"},
    {"severity": "info", "facility": "vpn", "event_type": "vpn_connect", "action": "allow",
     "message_tpl": "VPN tunnel established from {src} (user: {user})"},
    {"severity": "info", "facility": "vpn", "event_type": "vpn_disconnect", "action": "info",
     "message_tpl": "VPN tunnel disconnected from {src} (user: {user}, duration: {dur}m)"},
    {"severity": "warning", "facility": "security", "event_type": "firewall_deny", "action": "deny",
     "message_tpl": "Denied {proto} connection from {src}:{sport} to {dst}:{dport} (rule: {rule})"},
]

RULES = ["allow-web-outbound", "allow-internal-dns", "block-external-ssh", "allow-vpn", "allow-https-inbound", "block-telnet", "allow-snmp-internal"]
USERS = ["admin", "jdoe", "asmith", "mwilson", "schen"]


def _random_internal_ip() -> str:
    return f"10.0.{random.choice([1,2,3])}.{random.randint(10,254)}"


def _random_external_ip() -> str:
    return f"{random.choice([203,198,192,151])}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}"


def _normal_syslog() -> dict:
    tpl = random.choices(NORMAL_TEMPLATES, weights=[5, 30, 10, 5, 5, 15], k=1)[0]
    src = _random_external_ip() if random.random() < 0.6 else _random_internal_ip()
    dst = _random_internal_ip()
    device = random.choice(ALL_DEVICES) if tpl["event_type"] not in ("firewall_allow", "firewall_deny") else random.choice(FIREWALL_DEVICES)

    msg = tpl["message_tpl"].format(
        src=src, dst=dst, sport=random.randint(1024, 65535), dport=random.choice([22, 80, 443, 53, 3389]),
        proto=random.choice(["TCP", "UDP"]), rule=random.choice(RULES),
        user=random.choice(USERS), device=device,
        iface=f"GigabitEthernet1/0/{random.randint(1,48)}", dur=random.randint(5, 480),
    )

    return {
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        "device_id": device,
        "device_type": "firewall" if device in FIREWALL_DEVICES else "router",
        "severity": tpl["severity"],
        "facility": tpl["facility"],
        "message": msg,
        "source_ip": src,
        "destination_ip": dst,
        "rule_name": random.choice(RULES) if "firewall" in tpl["event_type"] else None,
        "action": tpl["action"],
        "event_type": tpl["event_type"],
    }


def _anomaly_syslogs(anomaly) -> list[dict]:
    """Generate correlated syslog events for active anomalies."""
    events = []
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    p = anomaly.params

    if anomaly.anomaly_type == AnomalyType.PORT_SCAN:
        for _ in range(random.randint(5, 15)):
            port = random.choice(p["ports"])
            events.append({
                "timestamp": now_iso,
                "device_id": "fw-edge-01",
                "device_type": "firewall",
                "severity": "warning",
                "facility": "security",
                "message": f"Denied TCP connection from {p['src_ip']}:{random.randint(40000,65535)} to {p['target_ip']}:{port} (rule: block-scan-detected)",
                "source_ip": p["src_ip"],
                "destination_ip": p["target_ip"],
                "rule_name": "block-scan-detected",
                "action": "deny",
                "event_type": "firewall_deny",
            })

    elif anomaly.anomaly_type == AnomalyType.LATERAL_MOVEMENT:
        target = random.choice(p["targets"])
        events.append({
            "timestamp": now_iso,
            "device_id": "fw-internal-01",
            "device_type": "firewall",
            "severity": "warning",
            "facility": "auth",
            "message": f"Failed SSH login attempt from {p['src_ip']} to {target} (user: root)",
            "source_ip": p["src_ip"],
            "destination_ip": target,
            "rule_name": None,
            "action": "deny",
            "event_type": "auth_failure",
        })

    elif anomaly.anomaly_type == AnomalyType.BRUTE_FORCE:
        user = random.choice(p.get("username_list", ["admin"]))
        events.append({
            "timestamp": now_iso,
            "device_id": "fw-edge-01",
            "device_type": "firewall",
            "severity": "error",
            "facility": "auth",
            "message": f"Failed SSH login attempt from {p['src_ip']} to {p['target_ip']} (user: {user})",
            "source_ip": p["src_ip"],
            "destination_ip": p["target_ip"],
            "rule_name": None,
            "action": "deny",
            "event_type": "auth_failure",
        })

    elif anomaly.anomaly_type == AnomalyType.DDOS:
        src = random.choice(p["sources"])
        events.append({
            "timestamp": now_iso,
            "device_id": "fw-edge-01",
            "device_type": "firewall",
            "severity": "critical",
            "facility": "security",
            "message": f"Rate limit exceeded: {src} -> {p['target_ip']}:80 (rule: ddos-protection)",
            "source_ip": src,
            "destination_ip": p["target_ip"],
            "rule_name": "ddos-protection",
            "action": "deny",
            "event_type": "firewall_deny",
        })

    return events


async def run_syslog_producer(scheduler: AnomalyScheduler):
    """Main loop for the syslog producer."""
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    print("[Syslog] Producer started")

    try:
        while True:
            anomalies = scheduler.tick()

            # Normal syslog events
            batch = random.randint(1, 4)
            for _ in range(batch):
                event = _normal_syslog()
                producer.produce(
                    TOPIC_SYSLOG,
                    key=event["device_id"].encode(),
                    value=orjson.dumps(event),
                )

            # Anomaly-correlated events
            for anomaly in anomalies:
                for event in _anomaly_syslogs(anomaly):
                    producer.produce(
                        TOPIC_SYSLOG,
                        key=event["device_id"].encode(),
                        value=orjson.dumps(event),
                    )

            producer.poll(0)
            await asyncio.sleep(1.0 / 5)  # ~20 events/sec with batching
    except asyncio.CancelledError:
        pass
    finally:
        producer.flush(5)
        print("[Syslog] Producer stopped")
