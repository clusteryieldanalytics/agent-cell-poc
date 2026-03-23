"""HTML templates for visualization dashboards."""

import json
from src.viz.dashboard import DashboardSpec


def render_index_page(dashboards: list[dict]) -> str:
    rows = ""
    for d in dashboards:
        rows += f"""
        <tr>
            <td><a href="/dashboard/{d['dashboard_id']}">{d['title']}</a></td>
            <td>{d['cell_name']}</td>
            <td>{d['panels']}</td>
            <td>{d['created_at'][:19]}</td>
        </tr>"""

    return f"""<!DOCTYPE html>
<html><head>
<title>Agent Cell Dashboards</title>
<style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #0d1117; color: #c9d1d9; margin: 0; padding: 20px; }}
    h1 {{ color: #58a6ff; }}
    table {{ width: 100%; border-collapse: collapse; margin-top: 20px; }}
    th, td {{ padding: 12px 16px; text-align: left; border-bottom: 1px solid #21262d; }}
    th {{ color: #8b949e; font-weight: 600; }}
    a {{ color: #58a6ff; text-decoration: none; }}
    a:hover {{ text-decoration: underline; }}
    .empty {{ color: #8b949e; padding: 40px; text-align: center; }}
</style>
</head><body>
<h1>Agent Cell Dashboards</h1>
{"<table><thead><tr><th>Dashboard</th><th>Cell</th><th>Panels</th><th>Created</th></tr></thead><tbody>" + rows + "</tbody></table>" if dashboards else '<div class="empty">No dashboards yet. Use <code>agentcell chat</code> and ask the agent to create a visualization.</div>'}
<script>setTimeout(() => location.reload(), 10000);</script>
</body></html>"""


def render_dashboard_page(dashboard: DashboardSpec) -> str:
    panels_config = json.dumps([
        {
            "panel_id": p.panel_id,
            "title": p.title,
            "chart_type": p.chart_type,
            "config": p.config,
        }
        for p in dashboard.panels
    ])

    # Generate grid items for each panel
    panel_divs = ""
    for p in dashboard.panels:
        panel_divs += f"""
        <div class="panel" id="panel-{p.panel_id}">
            <div class="panel-header">
                <span class="panel-title">{p.title}</span>
                <span class="panel-badge" id="badge-{p.panel_id}">waiting</span>
            </div>
            <div class="panel-body">
                <canvas id="chart-{p.panel_id}"></canvas>
                <div class="table-container" id="table-{p.panel_id}" style="display:none;"></div>
                <div class="stat-container" id="stat-{p.panel_id}" style="display:none;"></div>
            </div>
        </div>"""

    return f"""<!DOCTYPE html>
<html><head>
<title>{dashboard.title} — Agent Cell Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #0d1117; color: #c9d1d9; }}
    .header {{ background: #161b22; border-bottom: 1px solid #21262d; padding: 16px 24px; }}
    .header h1 {{ color: #58a6ff; font-size: 20px; }}
    .header p {{ color: #8b949e; font-size: 14px; margin-top: 4px; }}
    .header .meta {{ color: #8b949e; font-size: 12px; margin-top: 8px; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(450px, 1fr)); gap: 16px; padding: 16px; }}
    .panel {{ background: #161b22; border: 1px solid #21262d; border-radius: 8px; overflow: hidden; }}
    .panel-header {{ display: flex; justify-content: space-between; align-items: center; padding: 12px 16px; border-bottom: 1px solid #21262d; }}
    .panel-title {{ font-weight: 600; font-size: 14px; }}
    .panel-badge {{ font-size: 11px; padding: 2px 8px; border-radius: 10px; background: #21262d; color: #8b949e; }}
    .panel-badge.live {{ background: #238636; color: #fff; }}
    .panel-body {{ padding: 16px; min-height: 250px; }}
    .table-container {{ overflow-x: auto; }}
    .table-container table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
    .table-container th {{ color: #8b949e; text-align: left; padding: 6px 8px; border-bottom: 1px solid #21262d; }}
    .table-container td {{ padding: 6px 8px; border-bottom: 1px solid #21262d; }}
    .stat-container {{ display: flex; flex-direction: column; align-items: center; justify-content: center; height: 200px; }}
    .stat-value {{ font-size: 48px; font-weight: 700; color: #58a6ff; }}
    .stat-label {{ font-size: 14px; color: #8b949e; margin-top: 8px; }}
    .status {{ position: fixed; bottom: 16px; right: 16px; font-size: 12px; padding: 6px 12px; border-radius: 6px; }}
    .status.connected {{ background: #238636; color: #fff; }}
    .status.disconnected {{ background: #da3633; color: #fff; }}
</style>
</head><body>
<div class="header">
    <h1>{dashboard.title}</h1>
    <p>{dashboard.description}</p>
    <div class="meta">Cell: {dashboard.cell_name} ({dashboard.cell_id}) &middot; {len(dashboard.panels)} panels</div>
</div>
<div class="grid">{panel_divs}</div>
<div class="status disconnected" id="ws-status">Connecting...</div>

<script>
const PANELS_CONFIG = {panels_config};
const charts = {{}};
const chartData = {{}};

// Initialize charts
PANELS_CONFIG.forEach(p => {{
    if (p.chart_type === 'table' || p.chart_type === 'stat') return;

    const ctx = document.getElementById('chart-' + p.panel_id);
    if (!ctx) return;

    const colors = p.config.colors || ['#58a6ff', '#f78166', '#3fb950', '#d2a8ff', '#f0883e', '#a5d6ff'];
    const isStacked = p.config.stacked || false;

    chartData[p.panel_id] = {{ labels: [], datasets: [] }};

    charts[p.panel_id] = new Chart(ctx, {{
        type: p.chart_type === 'gauge' ? 'doughnut' : p.chart_type,
        data: chartData[p.panel_id],
        options: {{
            responsive: true,
            maintainAspectRatio: false,
            animation: {{ duration: 300 }},
            plugins: {{
                legend: {{ display: true, labels: {{ color: '#8b949e', font: {{ size: 11 }} }} }},
            }},
            scales: p.chart_type === 'line' || p.chart_type === 'bar' ? {{
                x: {{ ticks: {{ color: '#8b949e', font: {{ size: 10 }}, maxTicksLimit: 10 }}, grid: {{ color: '#21262d' }} }},
                y: {{ ticks: {{ color: '#8b949e' }}, grid: {{ color: '#21262d' }}, stacked: isStacked, beginAtZero: true }},
            }} : undefined,
        }},
    }});
}});

// WebSocket connection
const dashboardId = '{dashboard.dashboard_id}';
let ws;

function connect() {{
    ws = new WebSocket(`ws://${{location.host}}/ws/${{dashboardId}}`);

    ws.onopen = () => {{
        document.getElementById('ws-status').className = 'status connected';
        document.getElementById('ws-status').textContent = 'Live';
    }};

    ws.onclose = () => {{
        document.getElementById('ws-status').className = 'status disconnected';
        document.getElementById('ws-status').textContent = 'Reconnecting...';
        setTimeout(connect, 2000);
    }};

    ws.onmessage = (event) => {{
        const msg = JSON.parse(event.data);
        updatePanel(msg);
    }};
}}

function updatePanel(msg) {{
    const badge = document.getElementById('badge-' + msg.panel_id);
    if (badge) {{
        badge.textContent = 'live';
        badge.className = 'panel-badge live';
    }}

    if (msg.error) {{
        const container = document.getElementById('panel-' + msg.panel_id);
        if (container) {{
            const body = container.querySelector('.panel-body');
            body.innerHTML = `<div style="color: #da3633; padding: 20px;">${{msg.error}}</div>`;
        }}
        return;
    }}

    const panelConfig = PANELS_CONFIG.find(p => p.panel_id === msg.panel_id);
    if (!panelConfig) return;

    if (panelConfig.chart_type === 'table') {{
        renderTable(msg);
    }} else if (panelConfig.chart_type === 'stat') {{
        renderStat(msg);
    }} else {{
        renderChart(msg, panelConfig);
    }}
}}

function renderChart(msg, panelConfig) {{
    const chart = charts[msg.panel_id];
    if (!chart || !msg.data || !msg.data.rows) return;

    const rows = msg.data.rows;
    if (rows.length === 0) return;

    const config = msg.config || {{}};
    const colors = config.colors || ['#58a6ff', '#f78166', '#3fb950', '#d2a8ff', '#f0883e', '#a5d6ff'];

    // Convention: first column = labels, remaining columns = data series
    const labels = rows.map(r => r[0]);

    // Check if we should append or replace
    if (config.append) {{
        // Time-series: append new data points
        const cd = chartData[msg.panel_id];
        rows.forEach(row => {{
            cd.labels.push(row[0]);
            for (let i = 1; i < row.length; i++) {{
                if (!cd.datasets[i-1]) {{
                    cd.datasets.push({{
                        label: config.series_labels ? config.series_labels[i-1] : `Series ${{i}}`,
                        data: [],
                        borderColor: colors[(i-1) % colors.length],
                        backgroundColor: colors[(i-1) % colors.length] + '33',
                        fill: config.fill || false,
                        tension: 0.3,
                    }});
                }}
                cd.datasets[i-1].data.push(row[i]);
            }}
        }});
        // Keep last 100 points
        const maxPoints = config.max_points || 100;
        if (cd.labels.length > maxPoints) {{
            const trim = cd.labels.length - maxPoints;
            cd.labels.splice(0, trim);
            cd.datasets.forEach(ds => ds.data.splice(0, trim));
        }}
    }} else {{
        // Snapshot: replace all data
        chart.data.labels = labels;
        chart.data.datasets = [];
        for (let i = 1; i < (rows[0] || []).length; i++) {{
            chart.data.datasets.push({{
                label: config.series_labels ? config.series_labels[i-1] : `Series ${{i}}`,
                data: rows.map(r => r[i]),
                borderColor: colors[(i-1) % colors.length],
                backgroundColor: panelConfig.chart_type === 'bar' ? colors[(i-1) % colors.length] + '99' : colors[(i-1) % colors.length] + '33',
                fill: config.fill || false,
                tension: 0.3,
            }});
        }}
    }}

    chart.update();
}}

function renderTable(msg) {{
    const container = document.getElementById('table-' + msg.panel_id);
    const canvas = document.getElementById('chart-' + msg.panel_id);
    if (!container || !msg.data || !msg.data.rows) return;

    canvas.style.display = 'none';
    container.style.display = 'block';

    const config = msg.config || {{}};
    const headers = config.column_names || [];
    const rows = msg.data.rows;

    let html = '<table>';
    if (headers.length) {{
        html += '<thead><tr>' + headers.map(h => `<th>${{h}}</th>`).join('') + '</tr></thead>';
    }}
    html += '<tbody>';
    rows.forEach(row => {{
        html += '<tr>' + row.map(v => `<td>${{v !== null ? v : '-'}}</td>`).join('') + '</tr>';
    }});
    html += '</tbody></table>';
    container.innerHTML = html;
}}

function renderStat(msg) {{
    const container = document.getElementById('stat-' + msg.panel_id);
    const canvas = document.getElementById('chart-' + msg.panel_id);
    if (!container || !msg.data || !msg.data.rows) return;

    canvas.style.display = 'none';
    container.style.display = 'flex';

    const config = msg.config || {{}};
    const value = msg.data.rows[0] ? msg.data.rows[0][0] : '-';
    const label = config.label || msg.title;
    const color = config.color || '#58a6ff';

    container.innerHTML = `<div class="stat-value" style="color:${{color}}">${{value}}</div><div class="stat-label">${{label}}</div>`;
}}

connect();
</script>
</body></html>"""
