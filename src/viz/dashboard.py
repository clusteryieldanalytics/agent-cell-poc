"""Dashboard data model and registry."""

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass
class PanelSpec:
    """A single visualization panel within a dashboard."""
    panel_id: str
    title: str
    chart_type: str  # line, bar, gauge, table, stat
    data_source: str  # "sql" or "kafka"
    query: str  # SQL query (for sql source) or topic name (for kafka source)
    refresh_seconds: int = 5
    config: dict = field(default_factory=dict)  # chart-specific config (colors, labels, etc.)
    last_error: str | None = None
    error_count: int = 0
    success_count: int = 0


@dataclass
class DashboardSpec:
    """A complete dashboard with multiple panels."""
    dashboard_id: str
    cell_id: str
    cell_name: str
    title: str
    description: str
    panels: list[PanelSpec] = field(default_factory=list)
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class DashboardRegistry:
    """Global registry of active dashboards."""

    def __init__(self):
        self.dashboards: dict[str, DashboardSpec] = {}

    def create(self, cell_id: str, cell_name: str, title: str, description: str,
               panels: list[dict]) -> DashboardSpec:
        dashboard_id = f"dash-{uuid.uuid4().hex[:8]}"
        panel_specs = []
        for i, p in enumerate(panels):
            panel_specs.append(PanelSpec(
                panel_id=p.get("panel_id", f"panel-{i}"),
                title=p.get("title", f"Panel {i}"),
                chart_type=p.get("chart_type", "line"),
                data_source=p.get("data_source", "sql"),
                query=p.get("query", ""),
                refresh_seconds=p.get("refresh_seconds", 5),
                config=p.get("config", {}),
            ))

        dashboard = DashboardSpec(
            dashboard_id=dashboard_id,
            cell_id=cell_id,
            cell_name=cell_name,
            title=title,
            description=description,
            panels=panel_specs,
        )
        self.dashboards[dashboard_id] = dashboard
        return dashboard

    def get(self, dashboard_id: str) -> DashboardSpec | None:
        return self.dashboards.get(dashboard_id)

    def list_all(self) -> list[dict]:
        return [
            {
                "dashboard_id": d.dashboard_id,
                "cell_name": d.cell_name,
                "title": d.title,
                "panels": len(d.panels),
                "created_at": d.created_at,
            }
            for d in self.dashboards.values()
        ]

    def get_detail(self, dashboard_id: str) -> dict | None:
        """Get full dashboard details including panel health."""
        d = self.dashboards.get(dashboard_id)
        if not d:
            return None
        return {
            "dashboard_id": d.dashboard_id,
            "cell_id": d.cell_id,
            "cell_name": d.cell_name,
            "title": d.title,
            "description": d.description,
            "created_at": d.created_at,
            "panels": [
                {
                    "panel_id": p.panel_id,
                    "title": p.title,
                    "chart_type": p.chart_type,
                    "data_source": p.data_source,
                    "query": p.query,
                    "refresh_seconds": p.refresh_seconds,
                    "config": p.config,
                    "last_error": p.last_error,
                    "error_count": p.error_count,
                    "success_count": p.success_count,
                }
                for p in d.panels
            ],
        }

    def update_panel(self, dashboard_id: str, panel_id: str, updates: dict) -> str:
        """Update a panel's query, chart_type, config, or title."""
        d = self.dashboards.get(dashboard_id)
        if not d:
            return f"Dashboard '{dashboard_id}' not found"
        for panel in d.panels:
            if panel.panel_id == panel_id:
                if "query" in updates:
                    panel.query = updates["query"]
                if "chart_type" in updates:
                    panel.chart_type = updates["chart_type"]
                if "title" in updates:
                    panel.title = updates["title"]
                if "config" in updates:
                    panel.config = updates["config"]
                if "refresh_seconds" in updates:
                    panel.refresh_seconds = updates["refresh_seconds"]
                # Reset error state on update
                panel.last_error = None
                panel.error_count = 0
                panel.success_count = 0
                return f"Panel '{panel_id}' updated"
        return f"Panel '{panel_id}' not found in dashboard '{dashboard_id}'"

    def add_panel(self, dashboard_id: str, panel: dict) -> str:
        """Add a new panel to an existing dashboard."""
        d = self.dashboards.get(dashboard_id)
        if not d:
            return f"Dashboard '{dashboard_id}' not found"
        d.panels.append(PanelSpec(
            panel_id=panel.get("panel_id", f"panel-{len(d.panels)}"),
            title=panel.get("title", "New Panel"),
            chart_type=panel.get("chart_type", "line"),
            data_source=panel.get("data_source", "sql"),
            query=panel.get("query", ""),
            refresh_seconds=panel.get("refresh_seconds", 5),
            config=panel.get("config", {}),
        ))
        return f"Panel added to dashboard '{dashboard_id}'"

    def remove_panel(self, dashboard_id: str, panel_id: str) -> str:
        d = self.dashboards.get(dashboard_id)
        if not d:
            return f"Dashboard '{dashboard_id}' not found"
        d.panels = [p for p in d.panels if p.panel_id != panel_id]
        return f"Panel '{panel_id}' removed"

    def record_panel_error(self, dashboard_id: str, panel_id: str, error: str):
        """Called by viz server when a panel query fails."""
        d = self.dashboards.get(dashboard_id)
        if not d:
            return
        for p in d.panels:
            if p.panel_id == panel_id:
                p.last_error = error
                p.error_count += 1
                break

    def record_panel_success(self, dashboard_id: str, panel_id: str):
        d = self.dashboards.get(dashboard_id)
        if not d:
            return
        for p in d.panels:
            if p.panel_id == panel_id:
                p.success_count += 1
                break

    def remove(self, dashboard_id: str):
        self.dashboards.pop(dashboard_id, None)
