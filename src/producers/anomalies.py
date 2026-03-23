"""Coordinated anomaly injection across producers."""

import random
import time
from dataclasses import dataclass, field
from enum import Enum


class AnomalyType(Enum):
    PORT_SCAN = "port_scan"
    DATA_EXFILTRATION = "data_exfiltration"
    LATERAL_MOVEMENT = "lateral_movement"
    DDOS = "ddos"
    BRUTE_FORCE = "brute_force"
    DEVICE_OFFLINE = "device_offline"
    CPU_SPIKE = "cpu_spike"
    CONFIG_CHANGE = "config_change"


@dataclass
class ActiveAnomaly:
    anomaly_type: AnomalyType
    start_time: float
    duration_seconds: float
    params: dict = field(default_factory=dict)

    @property
    def is_expired(self) -> bool:
        return time.time() > self.start_time + self.duration_seconds


class AnomalyScheduler:
    """Schedules and manages correlated anomalies across all producers."""

    def __init__(self):
        self.active: list[ActiveAnomaly] = []
        self.next_anomaly_time = time.time() + random.uniform(60, 120)  # first anomaly in 1-2 min

    def tick(self) -> list[ActiveAnomaly]:
        """Called periodically. Returns list of currently active anomalies."""
        now = time.time()

        # Expire old anomalies
        self.active = [a for a in self.active if not a.is_expired]

        # Schedule new anomaly
        if now >= self.next_anomaly_time and len(self.active) < 2:
            anomaly = self._generate_anomaly()
            self.active.append(anomaly)
            self.next_anomaly_time = now + random.uniform(120, 300)

        return self.active

    def _generate_anomaly(self) -> ActiveAnomaly:
        atype = random.choice([
            AnomalyType.PORT_SCAN,
            AnomalyType.DATA_EXFILTRATION,
            AnomalyType.LATERAL_MOVEMENT,
            AnomalyType.DDOS,
            AnomalyType.BRUTE_FORCE,
        ])

        src_external = f"{random.choice([203, 198, 192])}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}"
        target_internal = f"10.0.{random.choice([1,2,3])}.{random.randint(10,50)}"

        params: dict = {}
        duration = 30.0

        if atype == AnomalyType.PORT_SCAN:
            params = {
                "src_ip": src_external,
                "target_ip": target_internal,
                "ports": list(range(1, random.randint(100, 500))),
            }
            duration = random.uniform(30, 60)

        elif atype == AnomalyType.DATA_EXFILTRATION:
            params = {
                "src_ip": f"10.0.1.{random.randint(10,50)}",
                "dst_ip": src_external,
                "bytes_per_event": random.randint(500_000, 5_000_000),
            }
            duration = random.uniform(120, 480)

        elif atype == AnomalyType.LATERAL_MOVEMENT:
            params = {
                "src_ip": f"10.0.3.{random.randint(10,50)}",  # IoT VLAN
                "targets": [f"10.0.2.{random.randint(10,30)}" for _ in range(random.randint(3, 8))],
                "port": 22,
            }
            duration = random.uniform(60, 180)

        elif atype == AnomalyType.DDOS:
            params = {
                "sources": [f"{random.randint(1,223)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}" for _ in range(random.randint(20, 100))],
                "target_ip": target_internal,
                "packets_per_event": random.randint(5000, 50000),
            }
            duration = random.uniform(60, 120)

        elif atype == AnomalyType.BRUTE_FORCE:
            params = {
                "src_ip": src_external,
                "target_ip": target_internal,
                "port": 22,
                "username_list": ["admin", "root", "user", "test", "deploy", "ubuntu"],
            }
            duration = random.uniform(60, 180)

        return ActiveAnomaly(
            anomaly_type=atype,
            start_time=time.time(),
            duration_seconds=duration,
            params=params,
        )
