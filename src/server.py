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
from src.config import SELF_AUDIT_INTERVAL_SECONDS
from src.viz.dashboard import DashboardRegistry
from src.viz.server import VizServer

SOCKET_PATH = "/tmp/agentcell.sock"
AUDIT_LOG_PATH = "/tmp/agentcell-audit.log"

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
        self._audit_task: asyncio.Task | None = None

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
            # Create cell, wire streaming, auto-approve all consumers iteratively
            cell = self.orchestrator.create_cell(request["name"], request["directive"])
            cell.nucleus.on_event = lambda msg: asyncio.ensure_future(send_event(msg if isinstance(msg, dict) else {"event": msg}))
            try:
                await cell.initialize()
                # Plan architecture first, then propose and approve one at a time
                await cell.plan_architecture()
                while True:
                    decisions = await cell.propose_next()
                    has_consumer = any(d.get("decision_type") == "spawn_consumer" or d.get("action", {}).get("type") == "spawn_consumer" for d in decisions)
                    await cell.approve(decisions)
                    if not has_consumer:
                        break
            finally:
                cell.nucleus.on_event = None
            return {"ok": True, "cell": cell.inspect()}

        elif command == "init_cell":
            # Create and initialize (no reasoning yet)
            cell = self.orchestrator.create_cell(request["name"], request["directive"])
            cell.nucleus.on_event = lambda msg: asyncio.ensure_future(send_event(msg if isinstance(msg, dict) else {"event": msg}))
            try:
                await cell.initialize()
            finally:
                cell.nucleus.on_event = None
            return {"ok": True, "cell_id": cell.cell_id, "name": cell.name}

        elif command == "plan":
            # Architecture planning phase
            cell = self.orchestrator.get_cell(request["name"])
            cell.nucleus.on_event = lambda msg: asyncio.ensure_future(send_event(msg if isinstance(msg, dict) else {"event": msg}))
            try:
                plan = await cell.plan_architecture()
            finally:
                cell.nucleus.on_event = None
            return {"ok": True, "plan": plan}

        elif command == "propose_next":
            # Propose the next consumer
            cell = self.orchestrator.get_cell(request["name"])
            cell.nucleus.on_event = lambda msg: asyncio.ensure_future(send_event(msg if isinstance(msg, dict) else {"event": msg}))
            try:
                decisions = await cell.propose_next()
                cell._pending_decisions = decisions
            finally:
                cell.nucleus.on_event = None
            return {"ok": True, "decisions": decisions}

        elif command == "approve":
            cell = self.orchestrator.get_cell(request["name"])
            approved_indices = request.get("approved", [])
            all_decisions = cell._pending_decisions
            approved = [all_decisions[i] for i in approved_indices if i < len(all_decisions)]
            failures_before = len(cell._failed_spawns)
            await cell.approve(approved)
            cell._pending_decisions = []
            # Include any new spawn failures in the response
            new_failures = cell._failed_spawns[failures_before:]
            return {"ok": True, "cell": cell.inspect(), "spawn_failures": new_failures}

        elif command == "verify":
            cell = self.orchestrator.get_cell(request["name"])
            cell.nucleus.on_event = lambda msg: asyncio.ensure_future(send_event(msg if isinstance(msg, dict) else {"event": msg}))
            try:
                summary = await cell.verify(
                    max_iterations=request.get("max_iterations", 5),
                    settle_seconds=request.get("settle_seconds", 15),
                )
            finally:
                cell.nucleus.on_event = None
            return {"ok": True, "summary": summary, "cell": cell.inspect()}

        elif command == "audit":
            cell = self.orchestrator.get_cell(request["name"])
            cell.nucleus.on_event = lambda msg: asyncio.ensure_future(send_event(msg if isinstance(msg, dict) else {"event": msg}))
            try:
                summary = await cell.self_audit()
            finally:
                cell.nucleus.on_event = None
            return {"ok": True, "summary": summary}

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
            cell.nucleus.on_event = lambda msg: asyncio.ensure_future(send_event(msg if isinstance(msg, dict) else {"event": msg}))
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
                await send_event({"event": f"Purging {r['name']}..."})
                for action in r.get("actions", []):
                    await send_event({"event": f"  → {action}"})
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

        # Start self-audit loop if configured
        if SELF_AUDIT_INTERVAL_SECONDS > 0:
            self._audit_task = asyncio.create_task(self._audit_loop())
            log.info(f"Self-audit enabled — every {SELF_AUDIT_INTERVAL_SECONDS}s")

        log.info(f"Listening on {SOCKET_PATH}")

        # Build the box with consistent 48-char inner width
        W = 48
        def _line(text=""):
            return f"║  {text:<{W-4}}  ║" if text else f"║{' ' * W}║"

        if SELF_AUDIT_INTERVAL_SECONDS > 0:
            audit_text = f"Self-audit: every {SELF_AUDIT_INTERVAL_SECONDS}s"
        else:
            audit_text = "Self-audit: disabled (set SELF_AUDIT_INTERVAL)"

        print(f"""
{'═' * (W + 2)}
{_line('Agent Cell Server Running'.center(W - 4))}
{_line()}
{_line(f'Socket:     {SOCKET_PATH}')}
{_line('Dashboards: http://localhost:3000')}
{_line('Kafka UI:   http://localhost:8080')}
{_line('Flink:      http://localhost:8081')}
{_line(audit_text)}
{_line()}
{_line('Commands (in another terminal):')}
{_line('  agentcell add -n <name> -d "<directive>"')}
{_line('  agentcell list / inspect / chat / audit')}
{_line()}
{_line('Press Ctrl+C to stop')}
{'═' * (W + 2)}
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

    def _write_audit_log(self, cell_name: str, line: str):
        """Append a line to the audit log file."""
        try:
            ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
            with open(AUDIT_LOG_PATH, "a") as f:
                f.write(f"[{ts}] [{cell_name}] {line}\n")
        except Exception:
            pass

    def _init_audit_text_buffer(self):
        """Initialize the text delta accumulator for audit logging."""
        self._audit_text_buf = ""

    def _flush_audit_text_buffer(self, cell_name: str):
        """Flush accumulated reasoning text to the audit log as clean lines."""
        if hasattr(self, "_audit_text_buf") and self._audit_text_buf.strip():
            for line in self._audit_text_buf.strip().split("\n"):
                line = line.strip()
                if line:
                    self._write_audit_log(cell_name, f"  ▸ {line}")
            self._audit_text_buf = ""

    def _audit_event_handler(self, cell_name: str, msg):
        """Handle nucleus events for the audit log.

        Text deltas are accumulated into complete lines. Event log strings
        are written immediately (after flushing any pending text).
        """
        if isinstance(msg, dict):
            if msg.get("type") == "text_delta":
                # Accumulate streaming text into a buffer
                self._audit_text_buf += msg.get("text", "")
                return
        elif isinstance(msg, str):
            text = msg.strip()
            if not text:
                return
            # Flush any pending reasoning text before writing the event
            self._flush_audit_text_buffer(cell_name)
            self._write_audit_log(cell_name, text)

    def _write_audit_summary(self, cell_name: str, summary: dict):
        """Write a clean structured audit summary to the log."""
        actions = summary.get("actions_applied", [])
        n_consumers = summary.get("consumers_audited", 0)

        self._write_audit_log(cell_name, f"Consumers checked: {n_consumers}")

        if actions:
            self._write_audit_log(cell_name, f"Actions applied: {len(actions)}")
            for a in actions:
                action_type = a.get("type", "?")
                consumer_id = a.get("consumer_id", "?")
                result = a.get("result", "")
                self._write_audit_log(cell_name, f"  {action_type} '{consumer_id}' → {result}")
        else:
            self._write_audit_log(cell_name, "No fixes needed — all consumers healthy")

        # Log the nucleus's assessment (the reply text, cleaned up)
        reply = summary.get("reply", "")
        if reply:
            # Write the reply as a clean block, not streaming chunks
            for line in reply.strip().split("\n"):
                line = line.strip()
                if line:
                    self._write_audit_log(cell_name, f"  {line}")

    async def _audit_loop(self):
        """Periodically trigger self-audit on all active cells."""
        # Wait for initial settle time before first audit
        await asyncio.sleep(min(SELF_AUDIT_INTERVAL_SECONDS, 120))

        while True:
            try:
                for name, cell in list(self.orchestrator.cells.items()):
                    try:
                        # Wire nucleus events to the audit log
                        self._init_audit_text_buffer()
                        cell.nucleus.on_event = lambda msg, n=name: self._audit_event_handler(n, msg)
                        self._write_audit_log(name, "── Audit started ──")

                        summary = await cell.self_audit()

                        # Flush any remaining reasoning text
                        self._flush_audit_text_buffer(name)

                        if summary.get("skipped"):
                            self._write_audit_log(name, f"Skipped: {summary.get('reason')}")
                            log.debug(f"Audit skipped for '{name}': {summary.get('reason')}")
                        else:
                            self._write_audit_summary(name, summary)
                            self._write_audit_log(name, "── Audit ended ──")
                            actions = summary.get("actions_applied", [])
                            log.info(
                                f"Audit complete for '{name}': "
                                f"{summary.get('consumers_audited', 0)} consumers checked, "
                                f"{len(actions)} action(s) applied"
                            )
                    except Exception as e:
                        self._write_audit_log(name, f"ERROR: {e}")
                        log.error(f"Audit failed for '{name}': {e}")
                    finally:
                        cell.nucleus.on_event = None
            except asyncio.CancelledError:
                return
            except Exception as e:
                log.error(f"Audit loop error: {e}")

            try:
                await asyncio.sleep(SELF_AUDIT_INTERVAL_SECONDS)
            except asyncio.CancelledError:
                return

    async def shutdown(self):
        """Gracefully shut down all cells and the server."""
        log.info("Shutting down...")
        if self._audit_task and not self._audit_task.done():
            self._audit_task.cancel()
            try:
                await self._audit_task
            except asyncio.CancelledError:
                pass
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
