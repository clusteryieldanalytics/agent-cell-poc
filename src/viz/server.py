"""Visualization web server — serves live dashboards via HTTP + WebSocket.

Runs as part of the main server process on port 3000.
Dashboards are created by agent cells via the create_dashboard tool.
Each panel polls its data source and pushes updates to connected browsers via WebSocket.
"""

import asyncio
import json
import logging
import traceback

from aiohttp import web

from src.viz.dashboard import DashboardRegistry, DashboardSpec
from src.viz.templates import render_dashboard_page, render_index_page

log = logging.getLogger("agentcell.viz")

VIZ_PORT = 3000


class VizServer:
    """HTTP + WebSocket server for live dashboards."""

    def __init__(self, registry: DashboardRegistry, get_knowledge_store):
        self.registry = registry
        self.get_knowledge_store = get_knowledge_store  # callable(cell_id) -> KnowledgeStore
        self._app = web.Application()
        self._app.router.add_get("/", self.handle_index)
        self._app.router.add_get("/dashboard/{dashboard_id}", self.handle_dashboard)
        self._app.router.add_get("/ws/{dashboard_id}", self.handle_ws)
        self._app.router.add_get("/api/dashboards", self.handle_api_list)
        self._runner = None

    async def start(self):
        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "0.0.0.0", VIZ_PORT)
        await site.start()
        log.info(f"Visualization server running on http://localhost:{VIZ_PORT}")

    async def stop(self):
        if self._runner:
            await self._runner.cleanup()

    async def handle_index(self, request):
        dashboards = self.registry.list_all()
        html = render_index_page(dashboards)
        return web.Response(text=html, content_type="text/html")

    async def handle_dashboard(self, request):
        dashboard_id = request.match_info["dashboard_id"]
        dashboard = self.registry.get(dashboard_id)
        if not dashboard:
            return web.Response(text="Dashboard not found", status=404)
        html = render_dashboard_page(dashboard)
        return web.Response(text=html, content_type="text/html")

    async def handle_api_list(self, request):
        return web.json_response(self.registry.list_all())

    async def handle_ws(self, request):
        dashboard_id = request.match_info["dashboard_id"]
        dashboard = self.registry.get(dashboard_id)
        if not dashboard:
            return web.Response(text="Dashboard not found", status=404)

        ws = web.WebSocketResponse()
        await ws.prepare(request)
        log.info(f"WebSocket connected for dashboard {dashboard_id}")

        try:
            knowledge = self.get_knowledge_store(dashboard.cell_id)
            if not knowledge:
                await ws.send_json({"error": "Cell not found"})
                return ws

            # Send initial data for all panels
            await self._push_all_panels(ws, dashboard, knowledge)

            # Then push updates on each panel's refresh interval
            while not ws.closed:
                await asyncio.sleep(min(p.refresh_seconds for p in dashboard.panels))
                await self._push_all_panels(ws, dashboard, knowledge)

        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.error(f"WebSocket error: {e}")
        finally:
            log.info(f"WebSocket disconnected for dashboard {dashboard_id}")

        return ws

    async def _push_all_panels(self, ws, dashboard: DashboardSpec, knowledge):
        for panel in dashboard.panels:
            try:
                data = self._query_panel(panel, knowledge)
                await ws.send_json({
                    "panel_id": panel.panel_id,
                    "chart_type": panel.chart_type,
                    "title": panel.title,
                    "data": data,
                    "config": panel.config,
                })
                self.registry.record_panel_success(dashboard.dashboard_id, panel.panel_id)
            except Exception as e:
                self.registry.record_panel_error(dashboard.dashboard_id, panel.panel_id, str(e))
                await ws.send_json({
                    "panel_id": panel.panel_id,
                    "error": str(e),
                })

    def _query_panel(self, panel, knowledge) -> dict:
        if panel.data_source == "sql":
            rows = knowledge.execute(panel.query)
            if not rows:
                return {"columns": [], "rows": []}
            # Convert to serializable format
            return {
                "rows": [[self._serialize(v) for v in row] for row in rows],
            }
        elif panel.data_source == "static":
            return panel.config.get("static_data", {})
        return {"error": f"Unknown data source: {panel.data_source}"}

    @staticmethod
    def _serialize(v):
        if v is None:
            return None
        if isinstance(v, (int, float, bool, str)):
            return v
        return str(v)
