"""Device Status producer — generates SNMP-style device health polls to network.device.status."""

import asyncio
import random
from datetime import datetime, timezone

import orjson
from confluent_kafka import Producer

from src.config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_DEVICE_STATUS
from src.producers.anomalies import AnomalyScheduler, AnomalyType
from src.producers.devices import DEVICES, DeviceState


async def run_device_producer(scheduler: AnomalyScheduler):
    """Main loop for the device status producer."""
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    states = [DeviceState(d) for d in DEVICES]
    print(f"[DeviceStatus] Producer started — tracking {len(states)} devices")

    try:
        while True:
            anomalies = scheduler.tick()

            # Apply anomaly effects to device state
            for anomaly in anomalies:
                if anomaly.anomaly_type == AnomalyType.DEVICE_OFFLINE:
                    device_id = anomaly.params.get("device_id")
                    for s in states:
                        if s.info["device_id"] == device_id:
                            s.status = "offline"

                elif anomaly.anomaly_type == AnomalyType.CPU_SPIKE:
                    device_id = anomaly.params.get("device_id")
                    for s in states:
                        if s.info["device_id"] == device_id:
                            s.cpu_percent = random.uniform(85, 99)
                            s.memory_percent = random.uniform(80, 95)

                elif anomaly.anomaly_type == AnomalyType.CONFIG_CHANGE:
                    device_id = anomaly.params.get("device_id")
                    for s in states:
                        if s.info["device_id"] == device_id:
                            s.config_changed = True
                            s.last_config_change = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

            # Emit status for all devices
            for state in states:
                state.tick()
                event = {
                    "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                    "device_id": state.info["device_id"],
                    "device_type": state.info["device_type"],
                    "manufacturer": state.info["manufacturer"],
                    "model": state.info["model"],
                    "firmware_version": state.info["firmware_version"],
                    "ip_address": state.info["ip_address"],
                    "location": state.info["location"],
                    "status": state.status,
                    "cpu_percent": round(state.cpu_percent, 1),
                    "memory_percent": round(state.memory_percent, 1),
                    "temperature_celsius": round(state.temperature_celsius, 1),
                    "uptime_seconds": state.uptime_seconds,
                    "interfaces": state.interfaces[:4],  # truncate for message size
                    "config_changed": state.config_changed,
                    "last_config_change": state.last_config_change,
                }
                producer.produce(
                    TOPIC_DEVICE_STATUS,
                    key=state.info["device_id"].encode(),
                    value=orjson.dumps(event),
                )

            producer.poll(0)
            producer.flush(2)
            await asyncio.sleep(30)  # poll every 30 seconds
    except asyncio.CancelledError:
        pass
    finally:
        producer.flush(5)
        print("[DeviceStatus] Producer stopped")
