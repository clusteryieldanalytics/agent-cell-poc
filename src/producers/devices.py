"""Simulated network device inventory with persistent state."""

import random

DEVICES = [
    # Core switches
    {"device_id": "switch-core-01", "device_type": "switch", "manufacturer": "Cisco", "model": "Catalyst 9300", "firmware_version": "17.9.4", "ip_address": "10.0.0.1", "location": "Building A, Floor 2, MDF", "vlan_id": 100},
    {"device_id": "switch-core-02", "device_type": "switch", "manufacturer": "Cisco", "model": "Catalyst 9300", "firmware_version": "17.9.4", "ip_address": "10.0.0.2", "location": "Building A, Floor 3, IDF", "vlan_id": 100},
    {"device_id": "switch-dist-01", "device_type": "switch", "manufacturer": "Cisco", "model": "Catalyst 9200", "firmware_version": "17.9.4", "ip_address": "10.0.0.3", "location": "Building B, Floor 1, MDF", "vlan_id": 200},
    {"device_id": "switch-dist-02", "device_type": "switch", "manufacturer": "Arista", "model": "7050X3", "firmware_version": "4.32.0F", "ip_address": "10.0.0.4", "location": "Building B, Floor 2, IDF", "vlan_id": 200},
    {"device_id": "switch-access-01", "device_type": "switch", "manufacturer": "Cisco", "model": "Catalyst 9200L", "firmware_version": "17.9.4", "ip_address": "10.0.0.5", "location": "Building A, Floor 1", "vlan_id": 300},
    {"device_id": "switch-access-02", "device_type": "switch", "manufacturer": "Cisco", "model": "Catalyst 9200L", "firmware_version": "17.9.4", "ip_address": "10.0.0.6", "location": "Building C, Floor 1", "vlan_id": 300},
    # Routers
    {"device_id": "router-edge-01", "device_type": "router", "manufacturer": "Cisco", "model": "ISR 4451", "firmware_version": "17.6.5", "ip_address": "10.0.0.10", "location": "Data Center, Row A", "vlan_id": None},
    {"device_id": "router-edge-02", "device_type": "router", "manufacturer": "Juniper", "model": "MX204", "firmware_version": "22.4R2", "ip_address": "10.0.0.11", "location": "Data Center, Row B", "vlan_id": None},
    # Firewalls
    {"device_id": "fw-edge-01", "device_type": "firewall", "manufacturer": "Palo Alto", "model": "PA-3260", "firmware_version": "11.1.3", "ip_address": "10.0.0.20", "location": "Data Center, Row A", "vlan_id": None},
    {"device_id": "fw-edge-02", "device_type": "firewall", "manufacturer": "Fortinet", "model": "FortiGate 200F", "firmware_version": "7.4.3", "ip_address": "10.0.0.21", "location": "Data Center, Row B", "vlan_id": None},
    {"device_id": "fw-internal-01", "device_type": "firewall", "manufacturer": "Palo Alto", "model": "PA-850", "firmware_version": "11.1.3", "ip_address": "10.0.0.22", "location": "Building A, Floor 2", "vlan_id": None},
    # Wireless APs
    {"device_id": "ap-01", "device_type": "access_point", "manufacturer": "Cisco", "model": "Catalyst 9130AXI", "firmware_version": "17.9.4", "ip_address": "10.0.0.30", "location": "Building A, Floor 1", "vlan_id": 300},
    {"device_id": "ap-02", "device_type": "access_point", "manufacturer": "Cisco", "model": "Catalyst 9130AXI", "firmware_version": "17.9.4", "ip_address": "10.0.0.31", "location": "Building A, Floor 2", "vlan_id": 300},
    {"device_id": "ap-03", "device_type": "access_point", "manufacturer": "Cisco", "model": "Catalyst 9130AXI", "firmware_version": "17.9.3", "ip_address": "10.0.0.32", "location": "Building B, Floor 1", "vlan_id": 300},
    {"device_id": "ap-04", "device_type": "access_point", "manufacturer": "Aruba", "model": "AP-635", "firmware_version": "8.11.2", "ip_address": "10.0.0.33", "location": "Building C, Floor 1", "vlan_id": 300},
    # Servers (monitored via SNMP)
    {"device_id": "server-web-01", "device_type": "server", "manufacturer": "Dell", "model": "PowerEdge R750", "firmware_version": "2.19.1", "ip_address": "10.0.2.10", "location": "Data Center, Row C", "vlan_id": 200},
    {"device_id": "server-web-02", "device_type": "server", "manufacturer": "Dell", "model": "PowerEdge R750", "firmware_version": "2.19.1", "ip_address": "10.0.2.11", "location": "Data Center, Row C", "vlan_id": 200},
    {"device_id": "server-db-01", "device_type": "server", "manufacturer": "Dell", "model": "PowerEdge R750", "firmware_version": "2.19.1", "ip_address": "10.0.2.20", "location": "Data Center, Row D", "vlan_id": 200},
    # VPN concentrator
    {"device_id": "vpn-01", "device_type": "vpn_concentrator", "manufacturer": "Cisco", "model": "ASA 5525-X", "firmware_version": "9.18.4", "ip_address": "10.0.0.40", "location": "Data Center, Row A", "vlan_id": None},
    # Load balancer
    {"device_id": "lb-01", "device_type": "load_balancer", "manufacturer": "F5", "model": "BIG-IP i4800", "firmware_version": "17.1.1", "ip_address": "10.0.0.50", "location": "Data Center, Row C", "vlan_id": 200},
]


class DeviceState:
    """Maintains mutable state for a simulated device."""

    def __init__(self, device_info: dict):
        self.info = device_info
        self.cpu_percent = random.uniform(15, 45)
        self.memory_percent = random.uniform(40, 70)
        self.temperature_celsius = random.uniform(35, 50)
        self.uptime_seconds = random.randint(86400, 31536000)
        self.status = "online"
        self.config_changed = False
        self.last_config_change = "2026-03-15T09:30:00Z"
        self.interfaces = self._init_interfaces()

    def _init_interfaces(self) -> list[dict]:
        dtype = self.info["device_type"]
        if dtype == "switch":
            count = random.randint(24, 48)
            prefix = "GigabitEthernet1/0/"
        elif dtype == "router":
            count = random.randint(4, 8)
            prefix = "GigabitEthernet0/0/"
        elif dtype == "firewall":
            count = random.randint(8, 16)
            prefix = "ethernet1/"
        elif dtype == "access_point":
            count = 2
            prefix = "radio"
        else:
            count = random.randint(2, 4)
            prefix = "eth"

        interfaces = []
        for i in range(count):
            interfaces.append({
                "name": f"{prefix}{i}",
                "status": "up" if random.random() < 0.95 else "down",
                "speed_mbps": random.choice([100, 1000, 10000]),
                "utilization_percent": round(random.uniform(5, 60), 1),
                "errors_in": 0,
                "errors_out": 0,
            })
        return interfaces

    def tick(self):
        """Advance state by one poll interval. Returns True if online."""
        if self.status == "offline":
            return False

        self.uptime_seconds += 30
        # Small random walk for CPU/memory/temp
        self.cpu_percent = max(5, min(95, self.cpu_percent + random.gauss(0, 2)))
        self.memory_percent = max(20, min(95, self.memory_percent + random.gauss(0, 1)))
        self.temperature_celsius = max(30, min(80, self.temperature_celsius + random.gauss(0, 0.5)))

        for iface in self.interfaces:
            if iface["status"] == "up":
                iface["utilization_percent"] = max(0, min(100, iface["utilization_percent"] + random.gauss(0, 3)))
                iface["utilization_percent"] = round(iface["utilization_percent"], 1)

        self.config_changed = False
        return True
