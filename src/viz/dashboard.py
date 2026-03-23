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

    def remove(self, dashboard_id: str):
        self.dashboards.pop(dashboard_id, None)
