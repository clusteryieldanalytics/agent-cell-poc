"""Cell Orchestrator — manages the lifecycle of all agent cells."""

import json
import uuid

import psycopg

from src.config import POSTGRES_URL
from src.cell.agent_cell import AgentCell, CellStatus


class CellOrchestrator:
    """Manages agent cell lifecycle: create, start, pause, resume, destroy, reload."""

    def __init__(self, dashboard_registry=None):
        self.cells: dict[str, AgentCell] = {}
        self.dashboard_registry = dashboard_registry

    def create_cell(self, name: str, directive: str) -> AgentCell:
        """Create a cell and register it, but don't start reasoning yet.
        This allows the caller to wire event callbacks before propose()."""
        cell_id = f"{name}-{uuid.uuid4().hex[:8]}"
        cell = AgentCell(cell_id=cell_id, name=name, directive=directive)
        cell.dashboard_registry = self.dashboard_registry
        self.cells[name] = cell
        return cell

    async def add_cell(self, name: str, directive: str) -> AgentCell:
        """Create and start a new agent cell (auto-approve, no review)."""
        cell = self.create_cell(name, directive)
        await cell.start()
        return cell

    async def remove_cell(self, name: str):
        """Stop and destroy a cell."""
        if name not in self.cells:
            raise KeyError(f"Cell '{name}' not found")
        cell = self.cells.pop(name)
        await cell.destroy()

    async def purge_cell(self, name: str) -> list[str]:
        """Purge all resources for a cell — Postgres, Kafka topics, registry."""
        # If cell is live in memory, purge it directly
        if name in self.cells:
            cell = self.cells.pop(name)
            return await cell.purge()

        # Cell not in memory — reconstruct from DB to purge its resources
        with psycopg.connect(POSTGRES_URL) as conn:
            row = conn.execute(
                "SELECT cell_id, name, directive FROM public.cells WHERE name = %s", (name,)
            ).fetchone()
        if not row:
            raise KeyError(f"Cell '{name}' not found in memory or database")

        cell = AgentCell(cell_id=row[0], name=row[1], directive=row[2])
        return await cell.purge()

    def get_cell(self, name: str) -> AgentCell:
        if name not in self.cells:
            raise KeyError(f"Cell '{name}' not found")
        return self.cells[name]

    def list_cells(self) -> list[dict]:
        results = []
        for name, cell in self.cells.items():
            results.append({
                "name": name,
                "cell_id": cell.cell_id,
                "status": cell.status.value,
                "consumers": len(cell.consumer_manager.consumers),
                "events_processed": cell.consumer_manager.total_events(),
            })
        return results

    def list_cells_from_db(self) -> list[dict]:
        with psycopg.connect(POSTGRES_URL) as conn:
            rows = conn.execute(
                "SELECT cell_id, name, directive, status, created_at, consumers, topics_subscribed, topics_produced FROM public.cells ORDER BY created_at"
            ).fetchall()
            return [
                {
                    "cell_id": r[0],
                    "name": r[1],
                    "directive": r[2][:80] + "..." if len(r[2]) > 80 else r[2],
                    "status": r[3],
                    "created_at": str(r[4]),
                    "consumers": len(r[5]) if isinstance(r[5], list) else 0,
                    "topics_subscribed": r[6],
                    "topics_produced": r[7],
                }
                for r in rows
            ]

    async def reload_cells(self) -> int:
        """Reload all persisted cells from Postgres. Returns count of reloaded cells."""
        with psycopg.connect(POSTGRES_URL) as conn:
            rows = conn.execute(
                "SELECT cell_id, name, directive, status, consumers FROM public.cells WHERE status != 'terminated'"
            ).fetchall()

        reloaded = 0
        for cell_id, name, directive, status, consumers in rows:
            # Skip if already loaded
            if name in self.cells:
                continue

            # Only reload cells that have persisted consumer specs
            specs = consumers if isinstance(consumers, list) else []
            has_code = any(s.get("consumer_code") for s in specs)
            if not has_code:
                print(f"  Skipping '{name}' — no persisted consumer code")
                continue

            cell = AgentCell(cell_id=cell_id, name=name, directive=directive)
            cell.dashboard_registry = self.dashboard_registry
            self.cells[name] = cell
            await cell.reload()
            reloaded += 1

        return reloaded

    async def purge_all_cells(self) -> list[dict]:
        """Purge all cells — in-memory and DB-only."""
        results = []

        # Purge in-memory cells
        for name in list(self.cells.keys()):
            try:
                actions = await self.purge_cell(name)
                results.append({"name": name, "actions": actions, "actions_count": len(actions)})
            except Exception as e:
                results.append({"name": name, "error": str(e), "actions_count": 0})

        # Purge any remaining DB-only cells
        with psycopg.connect(POSTGRES_URL) as conn:
            rows = conn.execute("SELECT cell_id, name, directive FROM public.cells").fetchall()
        for cell_id, name, directive in rows:
            try:
                cell = AgentCell(cell_id=cell_id, name=name, directive=directive)
                actions = await cell.purge()
                results.append({"name": name, "actions": actions, "actions_count": len(actions)})
            except Exception as e:
                results.append({"name": name, "error": str(e), "actions_count": 0})

        return results

    async def pause_cell(self, name: str):
        cell = self.get_cell(name)
        cell.pause()

    async def resume_cell(self, name: str):
        cell = self.get_cell(name)
        cell.resume()
