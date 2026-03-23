"""Persistent server process that manages cell lifecycle.

The CLI sends commands over a Unix socket. The server holds cells in memory
so they persist across CLI invocations, with consumers running as asyncio tasks.

Protocol: newline-delimited JSON streaming.
  Client → Server: single JSON line (the command)
  Server → Client: zero or more JSON event lines, then a final response line with "done": true

This lets the CLI display nucleus activity (API calls, tool use) in real-time
instead of waiting for the entire operation to complete.
"""

import asyncio
import json
import logging
import os
import signal
import traceback
from datetime import datetime, timezone

from src.cell.orchestrator import CellOrchestrator
from src.viz.dashboard import DashboardRegistry
from src.viz.server import VizServer

SOCKET_PATH = "/tmp/agentcell.sock"

log = logging.getLogger("agentcell.server")


class CellServer:
    """Async server that manages cells and accepts CLI commands."""

    def __init__(self):
        self.dashboard_registry = DashboardRegistry()
        self.orchestrator = CellOrchestrator(dashboard_registry=self.dashboard_registry)
        self.viz_server = VizServer(
            registry=self.dashboard_registry,
            get_knowledge_store=self._get_knowledge_store,
        )
        self._server = None
        self._started_at = datetime.now(timezone.utc)

    def _get_knowledge_store(self, cell_id: str):
        """Look up a cell's knowledge store by cell_id."""
        for cell in self.orchestrator.cells.values():
            if cell.cell_id == cell_id:
                return cell.knowledge
        return None

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Handle a single CLI command using streaming newline-delimited JSON."""
        try:
            line = await asyncio.wait_for(reader.readline(), timeout=10.0)
            if not line:
                return
            request = json.loads(line.decode())
            command = request.get("command")
            log.info(f"← {command} {request.get('name', '')}")

            # Create a stream helper that sends events to the client in real-time
            async def send_event(event: dict):
                if writer.is_closing():
                    return
                try:
                    writer.write(json.dumps(event, default=str).encode() + b"\n")
                    await writer.drain()
                except Exception:
                    pass

            response = await self._dispatch(command, request, send_event)
            response["done"] = True
            log.info(f"→ {command} ok={response.get('ok', False)}")

        except asyncio.TimeoutError:
            log.error("Timeout reading client request")
            response = {"done": True, "error": "Server timed out reading request"}
        except asyncio.IncompleteReadError:
            log.error("Incomplete client request")
            response = {"done": True, "error": "Incomplete request"}
        except Exception as e:
            log.error(f"Command failed: {type(e).__name__}: {e}")
            response = {"done": True, "error": f"{type(e).__name__}: {e}", "traceback": traceback.format_exc()}

        try:
            writer.write(json.dumps(response, default=str).encode() + b"\n")
            await writer.drain()
        except Exception:
            pass
        finally:
            writer.close()
            await writer.wait_closed()

    async def _dispatch(self, command: str, request: dict, send_event) -> dict:
        if command == "add":
            # Create cell, wire streaming, reason, auto-approve
            cell = self.orchestrator.create_cell(request["name"], request["directive"])
            cell.nucleus.on_event = lambda msg: asyncio.ensure_future(send_event({"event": msg}))
            try:
                decisions = await cell.propose()
                await cell.approve(decisions)
            finally:
                cell.nucleus.on_event = None
            return {"ok": True, "cell": cell.inspect()}

        elif command == "propose":
            # Create cell, wire streaming, reason, return decisions for review
            cell = self.orchestrator.create_cell(request["name"], request["directive"])
            cell.nucleus.on_event = lambda msg: asyncio.ensure_future(send_event({"event": msg}))
            try:
                decisions = await cell.propose()
                cell._pending_decisions = decisions
            finally:
                cell.nucleus.on_event = None
            return {"ok": True, "cell_id": cell.cell_id, "name": cell.name, "decisions": decisions}

        elif command == "approve":
            cell = self.orchestrator.get_cell(request["name"])
            approved_indices = request.get("approved", [])
            all_decisions = cell._pending_decisions
            approved = [all_decisions[i] for i in approved_indices if i < len(all_decisions)]
            await cell.approve(approved)
            cell._pending_decisions = []
            return {"ok": True, "cell": cell.inspect()}

        elif command == "reject":
            name = request["name"]
            if name in self.orchestrator.cells:
                cell = self.orchestrator.cells.pop(name)
                cell._unregister()
            return {"ok": True}

        elif command == "remove":
            await self.orchestrator.remove_cell(request["name"])
            return {"ok": True}

        elif command == "purge":
            actions = await self.orchestrator.purge_cell(request["name"])
            for action in actions:
                await send_event({"event": action})
            return {"ok": True, "actions": actions}

        elif command == "list":
            cells = self.orchestrator.list_cells()
            if not cells:
                try:
                    cells = self.orchestrator.list_cells_from_db()
                except Exception:
                    pass
            return {"ok": True, "cells": cells}

        elif command == "inspect":
            cell = self.orchestrator.get_cell(request["name"])
            return {"ok": True, "cell": cell.inspect()}

        elif command == "pause":
            await self.orchestrator.pause_cell(request["name"])
            return {"ok": True}

        elif command == "resume":
            await self.orchestrator.resume_cell(request["name"])
            return {"ok": True}

        elif command == "chat_status":
            cell = self.orchestrator.get_cell(request["name"])
            return {"ok": True, "info": {
                "status": cell.status.value,
                "consumers": len(cell.consumer_manager.consumers),
                "events_processed": cell.consumer_manager.total_events(),
            }}

        elif command == "chat":
            cell = self.orchestrator.get_cell(request["name"])
            cell.nucleus.on_event = lambda msg: asyncio.ensure_future(send_event({"event": msg}))
            try:
                reply, pending_actions = await cell.chat(request["message"])
            finally:
                cell.nucleus.on_event = None
            return {"ok": True, "reply": reply, "pending_actions": pending_actions}

        elif command == "deploy_action":
            cell = self.orchestrator.get_cell(request["name"])
            action = request["action"]
            log.info(f"Deploying {action.get('type')} for consumer '{action.get('consumer_id')}' (code: {len(action.get('consumer_code', ''))} chars)")
            result = await cell.deploy_action(action)
            log.info(f"Deploy result: {result}")
            await send_event({"event": f"Deploy: {result}"})
            return {"ok": True, "result": result}

        elif command == "decisions":
            cell = self.orchestrator.get_cell(request["name"])
            return {"ok": True, "topic": cell.decision_topic}

        elif command == "consumer_code":
            cell = self.orchestrator.get_cell(request["name"])
            consumer_id = request.get("consumer_id")
            if consumer_id:
                code = cell.consumer_manager.get_consumer_code(consumer_id)
                if code is None:
                    return {"error": f"Consumer '{consumer_id}' not found"}
                return {"ok": True, "consumer_id": consumer_id, "code": code}
            else:
                consumers = cell.consumer_manager.list_consumers(include_code=True)
                return {"ok": True, "consumers": consumers}

        elif command == "purge_all":
            results = await self.orchestrator.purge_all_cells()
            for r in results:
                await send_event({"event": f"Purged {r['name']}: {r.get('actions_count', 0)} resources"})
            return {"ok": True, "results": results}

        elif command == "dlq":
            cell = self.orchestrator.get_cell(request["name"])
            consumer_id = request.get("consumer_id")
            limit = request.get("limit", 20)
            if consumer_id:
                managed = cell.consumer_manager.consumers.get(consumer_id)
                if not managed:
                    return {"error": f"Consumer '{consumer_id}' not found"}
                from src.cell.consumer import ConsumerManager
                entries = await ConsumerManager.read_dlq(managed.dlq_topic, limit)
                return {"ok": True, "consumer_id": consumer_id, "dlq_topic": managed.dlq_topic, "entries": entries}
            else:
                # Return DLQ summary for all consumers
                dlqs = []
                for managed in cell.consumer_manager.consumers.values():
                    from src.cell.consumer import ConsumerManager
                    entries = await ConsumerManager.read_dlq(managed.dlq_topic, limit=1)
                    dlqs.append({
                        "consumer_id": managed.spec.consumer_id,
                        "dlq_topic": managed.dlq_topic,
                        "errors": managed.errors,
                        "latest_error": entries[-1] if entries else None,
                    })
                return {"ok": True, "dlqs": dlqs}

        elif command == "dashboards":
            return {"ok": True, "dashboards": self.dashboard_registry.list_all()}

        elif command == "status":
            cells = self.orchestrator.list_cells()
            return {
                "ok": True,
                "cells": cells,
                "total_cells": len(cells),
                "total_consumers": sum(c.get("consumers", 0) for c in cells),
                "total_events": sum(c.get("events_processed", 0) for c in cells),
                "uptime_seconds": (datetime.now(timezone.utc) - self._started_at).total_seconds(),
            }

        elif command == "ping":
            return {"ok": True}

        else:
            return {"error": f"Unknown command: {command}"}

    async def start(self):
        """Start the server."""
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(name)s] %(message)s",
            datefmt="%H:%M:%S",
        )

        if os.path.exists(SOCKET_PATH):
            os.unlink(SOCKET_PATH)

        # Preload the embedding model so first operations are fast
        try:
            log.info("Preloading embedding model...")
            from src.embeddings import embed_one
            embed_one("warmup")
            log.info("Embedding model ready")
        except Exception as e:
            log.warning(f"Could not preload embedding model: {e}")

        # Start visualization server
        await self.viz_server.start()

        # Reload persisted cells
        try:
            reloaded = await self.orchestrator.reload_cells()
            if reloaded:
                log.info(f"Reloaded {reloaded} cell(s) from persisted state")
        except Exception as e:
            log.warning(f"Could not reload cells: {e}")

        self._server = await asyncio.start_unix_server(
            self.handle_client, path=SOCKET_PATH,
            limit=1 << 20,  # 1MB readline buffer
        )

        log.info(f"Listening on {SOCKET_PATH}")
        print(f"""
╔══════════════════════════════════════════════════╗
║          Agent Cell Server Running               ║
║                                                  ║
║  Socket:     {SOCKET_PATH:<34s}                      ║
║  Dashboards: http://localhost:3000               ║
║  Kafka UI:   http://localhost:8080               ║
║                                                  ║
║  Commands (in another terminal):                 ║
║    agentcell add -n <name> -d "<directive>"      ║
║    agentcell list / inspect / chat / dashboards  ║
║                                                  ║
║  Press Ctrl+C to stop                            ║
╚══════════════════════════════════════════════════╝
""")

        loop = asyncio.get_event_loop()
        stop_event = asyncio.Event()

        def _signal_handler():
            stop_event.set()

        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, _signal_handler)

        try:
            await stop_event.wait()
        finally:
            await self.shutdown()

    async def shutdown(self):
        """Gracefully shut down all cells and the server."""
        log.info("Shutting down...")
        for name, cell in list(self.orchestrator.cells.items()):
            try:
                log.info(f"Stopping cell '{name}' (state persisted)...")
                await cell.stop()
            except Exception as e:
                log.error(f"Error stopping '{name}': {e}")

        await self.viz_server.stop()

        if self._server:
            self._server.close()
            await self._server.wait_closed()

        if os.path.exists(SOCKET_PATH):
            os.unlink(SOCKET_PATH)

        log.info("Server stopped.")


async def send_command(command: dict, timeout: float = 600) -> tuple[list[dict], dict]:
    """Send a command and stream back events + final response.

    Returns (events, response) where events is a list of event dicts
    received before the final response.
    """
    reader, writer = await asyncio.open_unix_connection(SOCKET_PATH)

    # Send command as a single JSON line
    writer.write(json.dumps(command).encode() + b"\n")
    await writer.drain()

    # Read streaming response lines
    events = []
    response = None
    while True:
        line = await asyncio.wait_for(reader.readline(), timeout=timeout)
        if not line:
            break
        data = json.loads(line.decode())
        if data.get("done"):
            response = data
            break
        else:
            events.append(data)

    writer.close()
    await writer.wait_closed()

    if response is None:
        response = {"error": "Connection closed without response"}

    return events, response


def server_is_running() -> bool:
    """Check if the server is running."""
    if not os.path.exists(SOCKET_PATH):
        return False
    try:
        loop = asyncio.new_event_loop()
        _, resp = loop.run_until_complete(send_command({"command": "ping"}, timeout=2))
        loop.close()
        return resp.get("ok", False)
    except Exception:
        return False


if __name__ == "__main__":
    server = CellServer()
    asyncio.run(server.start())
