"""Run all synthetic data producers concurrently."""

import asyncio
import signal

from src.producers.anomalies import AnomalyScheduler
from src.producers.device_status import run_device_producer
from src.producers.netflow import run_netflow_producer
from src.producers.syslog import run_syslog_producer


async def main():
    scheduler = AnomalyScheduler()
    tasks = [
        asyncio.create_task(run_netflow_producer(scheduler)),
        asyncio.create_task(run_device_producer(scheduler)),
        asyncio.create_task(run_syslog_producer(scheduler)),
    ]

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: [t.cancel() for t in tasks])

    print("All producers running. Press Ctrl+C to stop.")
    await asyncio.gather(*tasks, return_exceptions=True)
    print("All producers stopped.")


if __name__ == "__main__":
    asyncio.run(main())
