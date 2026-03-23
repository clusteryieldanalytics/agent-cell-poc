"""Operator CLI for the Agent Cell PoC.

Commands talk to a persistent server process over a Unix socket.
The server holds cells in memory with their consumers running as asyncio tasks.
"""

import asyncio
import json
import sys

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Prompt
from rich.syntax import Syntax

SOCKET_PATH = "/tmp/agentcell.sock"

app = typer.Typer(name="agentcell", help="Agent Cell PoC — Network Security & Observability")
console = Console()


def _send(command: dict, timeout: float = 600, stream: bool = False) -> dict:
    """Send a command to the server.

    If stream=True, prints event lines in real-time as they arrive.
    Returns the final response dict.
    """
    async def _run():
        reader, writer = await asyncio.open_unix_connection(SOCKET_PATH)
        writer.write(json.dumps(command).encode() + b"\n")
        await writer.drain()

        response = None
        while True:
            line = await asyncio.wait_for(reader.readline(), timeout=timeout)
            if not line:
                break
            data = json.loads(line.decode())
            if data.get("done"):
                response = data
                break
            elif stream and data.get("event"):
                _print_event(data["event"])

        writer.close()
        await writer.wait_closed()
        return response or {"error": "Connection closed without response"}

    try:
        return asyncio.run(_run())
    except (ConnectionRefusedError, FileNotFoundError):
        console.print("[red]Server is not running.[/] Start it with: [cyan]agentcell server[/]")
        raise typer.Exit(1)
    except asyncio.TimeoutError:
        console.print("[red]Request timed out.[/] The nucleus may still be reasoning on the server.")
        raise typer.Exit(1)


def _check_error(response: dict):
    """Check for errors in the server response."""
    if "error" in response:
        console.print(f"[red]Error:[/] {response['error']}")
        if "traceback" in response:
            console.print(f"[dim]{response['traceback']}[/]")
        raise typer.Exit(1)


def _print_event(event: str):
    """Print a single nucleus event log line with color coding."""
    if "API call →" in event:
        console.print(f"  [dim cyan]▸ {event}[/]")
    elif "API response ←" in event:
        console.print(f"  [dim green]▸ {event}[/]")
    elif "Tool call →" in event:
        console.print(f"  [dim yellow]▸ {event}[/]")
    elif "Tool result ←" in event:
        console.print(f"  [dim]▸ {event}[/]")
    elif "Reasoning:" in event:
        console.print(f"  [dim magenta]▸ {event}[/]")
    else:
        console.print(f"  [dim]▸ {event}[/]")


def _print_events(events: list[str]):
    """Print multiple nucleus event log lines."""
    for event in events:
        _print_event(event)


@app.command()
def server():
    """Start the persistent cell server (run this first)."""
    from src.server import CellServer
    srv = CellServer()
    asyncio.run(srv.start())


@app.command()
def add(
    name: str = typer.Option(..., "--name", "-n", help="Cell name"),
    directive: str = typer.Option(..., "--directive", "-d", help="Cell directive"),
    auto_approve: bool = typer.Option(False, "--yes", "-y", help="Skip approval and auto-approve all consumers"),
):
    """Add a new agent cell. Shows proposed consumer code for approval."""
    # Phase 1: Propose — nucleus reasons about what consumers to author
    console.print(Panel(
        f"[bold]Directive:[/] {directive[:200]}{'...' if len(directive) > 200 else ''}",
        title=f"Creating cell '{name}'",
    ))
    console.print(f"\n[bold cyan]Nucleus reasoning...[/]\n")

    response = _send({"command": "propose", "name": name, "directive": directive}, stream=True)
    _check_error(response)
    console.print()

    decisions = response.get("decisions", [])
    consumer_decisions = [d for d in decisions if d.get("decision_type") == "spawn_consumer"]

    if not consumer_decisions:
        console.print("[yellow]Nucleus did not propose any consumers.[/]")
        for d in decisions:
            if d.get("reasoning"):
                console.print(f"  [dim]{d['reasoning'][:200]}[/]")
        _send({"command": "reject", "name": name})
        return

    console.print(f"[bold]Proposed {len(consumer_decisions)} consumer(s):[/]\n")

    # Auto-approve mode
    if auto_approve:
        console.print("[dim]Auto-approving all consumers (--yes)[/]\n")
        response = _send({"command": "approve", "name": name, "approved": list(range(len(decisions)))})
        _check_error(response)
        info = response["cell"]
        for d in consumer_decisions:
            action = d.get("action", {})
            console.print(f"  [green]✓[/] {action.get('consumer_id', '?')}  {', '.join(action.get('source_topics', []))} → {action.get('output_topic', '?')}")
        console.print(Panel(
            f"[bold green]Cell '{name}' active[/]\n"
            f"Cell ID: {info['cell_id']}\n"
            f"Consumers: {len(info['consumers'])}",
            title="Cell Created",
        ))
        return

    # Interactive approval — show each consumer for review
    approved_indices = []
    for i, d in enumerate(decisions):
        if d.get("decision_type") != "spawn_consumer":
            approved_indices.append(i)
            continue

        action = d.get("action", {})
        consumer_id = action.get("consumer_id", "?")
        topics = action.get("source_topics", [])
        output = action.get("output_topic", "?")
        code = action.get("consumer_code", "")

        # Header with consumer info
        console.print(f"{'─' * 60}")
        console.print(f"  Consumer:  [bold cyan]{consumer_id}[/]")
        console.print(f"  Topics:    {', '.join(topics)} → [green]{output}[/]")

        # Show reasoning
        if d.get("reasoning"):
            console.print(f"  Reasoning: [dim]{d['reasoning'][:200]}{'...' if len(d.get('reasoning', '')) > 200 else ''}[/]")
        console.print()

        # Show the authored code
        console.print(Syntax(code, "python", theme="monokai", line_numbers=True))
        console.print()

        # Prompt
        choice = Prompt.ask(
            f"  Approve '[cyan]{consumer_id}[/]'? [green]y[/]es / [red]n[/]o / [yellow]q[/]uit",
            choices=["y", "n", "q"],
            default="y",
        )

        if choice == "q":
            console.print("[yellow]Aborting — rejecting all.[/]")
            _send({"command": "reject", "name": name})
            return
        elif choice == "y":
            approved_indices.append(i)
            console.print(f"  [green]✓ Approved[/]\n")
        else:
            console.print(f"  [red]✗ Rejected[/]\n")

    if not any(decisions[i].get("decision_type") == "spawn_consumer" for i in approved_indices):
        console.print("[yellow]No consumers approved. Cancelling cell.[/]")
        _send({"command": "reject", "name": name})
        return

    # Phase 2: Spawn approved consumers
    console.print(f"\n[bold cyan]Spawning approved consumers...[/]")
    response = _send({"command": "approve", "name": name, "approved": approved_indices})
    _check_error(response)

    info = response["cell"]
    console.print(Panel(
        f"[bold green]Cell '{name}' active[/]\n"
        f"Cell ID: {info['cell_id']}\n"
        f"Consumers: {len(info['consumers'])}\n"
        f"Decision topic: {info['decision_topic']}",
        title="Cell Created",
    ))


@app.command()
def remove(name: str = typer.Option(..., "--name", "-n", help="Cell name")):
    """Remove and destroy a cell (keeps Kafka topics and knowledge base)."""
    response = _send({"command": "remove", "name": name})
    _check_error(response)
    console.print(f"[bold red]Cell '{name}' destroyed.[/]")


@app.command()
def purge(name: str = typer.Option(..., "--name", "-n", help="Cell name")):
    """Purge ALL resources for a cell — Postgres schema, Kafka topics, registry.

    This is destructive and irreversible. Deletes:
    - All knowledge base tables (built-in + custom)
    - Kafka decision log topic
    - Kafka derived output topics
    - Cell registry entry
    """
    console.print(f"[bold red]This will permanently delete all resources for '{name}'.[/]")
    choice = Prompt.ask("  Continue?", choices=["y", "n"], default="n")
    if choice != "y":
        console.print("[dim]Aborted.[/]")
        return

    console.print(f"\n[bold red]Purging '{name}'...[/]\n")
    response = _send({"command": "purge", "name": name}, stream=True)
    _check_error(response)

    actions = response.get("actions", [])
    console.print()
    for action in actions:
        console.print(f"  [red]✗[/] {action}")
    console.print(f"\n[bold red]Cell '{name}' purged — {len(actions)} resources deleted.[/]")


@app.command(name="list")
def list_cells():
    """List all agent cells."""
    response = _send({"command": "list"})
    _check_error(response)
    cells = response.get("cells", [])

    if not cells:
        console.print("[dim]No cells found.[/]")
        return

    table = Table(title="Agent Cells")
    table.add_column("Name", style="cyan")
    table.add_column("Cell ID", style="dim")
    table.add_column("Status", style="green")
    table.add_column("Consumers", justify="right")
    table.add_column("Events", justify="right")

    for c in cells:
        status_style = {
            "active": "green", "paused": "yellow",
            "terminated": "red", "initializing": "blue",
        }.get(c.get("status", ""), "white")
        consumers = c.get("consumers", 0)
        if isinstance(consumers, list):
            consumers = len(consumers)
        table.add_row(
            c.get("name", ""),
            c.get("cell_id", ""),
            f"[{status_style}]{c.get('status', '')}[/]",
            str(consumers),
            str(c.get("events_processed", "-")),
        )

    console.print(table)


@app.command()
def pause(name: str = typer.Option(..., "--name", "-n")):
    """Pause a cell."""
    response = _send({"command": "pause", "name": name})
    _check_error(response)
    console.print(f"[yellow]Cell '{name}' paused.[/]")


@app.command()
def resume(name: str = typer.Option(..., "--name", "-n")):
    """Resume a paused cell."""
    response = _send({"command": "resume", "name": name})
    _check_error(response)
    console.print(f"[green]Cell '{name}' resumed.[/]")


@app.command()
def chat(name: str = typer.Option(..., "--name", "-n")):
    """Interactive chat with a cell's nucleus."""
    # Quick status check (no knowledge base stats)
    response = _send({"command": "chat_status", "name": name})
    _check_error(response)
    info = response.get("info", {})

    console.print(Panel(
        f"Chatting with [bold cyan]{name}[/] "
        f"({info.get('status', '?')}, {info.get('consumers', '?')} consumers, "
        f"{info.get('events_processed', '?')} events processed)\n"
        f"Type 'exit' or 'quit' to end the chat.",
        title="Agent Cell Chat",
    ))

    while True:
        try:
            user_input = Prompt.ask("\n[bold cyan]>[/]")
        except (EOFError, KeyboardInterrupt):
            break

        if user_input.lower() in ("exit", "quit", "q"):
            break

        if not user_input.strip():
            continue

        console.print("[dim]Thinking...[/]")
        response = _send({"command": "chat", "name": name, "message": user_input}, stream=True)
        _check_error(response)

        # If the nucleus wants to deploy code changes, require confirmation
        pending = response.get("pending_actions", [])
        if pending:
            for action in pending:
                action_type = action.get("type")
                consumer_id = action.get("consumer_id", "?")
                code = action.get("consumer_code", "")

                if action_type in ("replace_consumer", "spawn_consumer"):
                    console.print(f"\n{'─' * 60}")
                    console.print(f"  [bold yellow]Proposed code change:[/] [cyan]{action_type}[/] → [cyan]{consumer_id}[/]")
                    if action_type == "spawn_consumer":
                        console.print(f"  Topics: {', '.join(action.get('source_topics', []))} → {action.get('output_topic', '?')}")
                    console.print()
                    console.print(Syntax(code, "python", theme="monokai", line_numbers=True))
                    console.print()

                    choice = Prompt.ask(
                        f"  Deploy this change?",
                        choices=["y", "n"],
                        default="y",
                    )
                    if choice == "y":
                        deploy_resp = _send({"command": "deploy_action", "name": name, "action": action}, stream=True)
                        _check_error(deploy_resp)
                        console.print(f"  [green]✓ Deployed[/]")
                    else:
                        console.print(f"  [red]✗ Skipped[/]")
                elif action_type == "remove_consumer":
                    choice = Prompt.ask(
                        f"  Remove consumer '[cyan]{consumer_id}[/]'?",
                        choices=["y", "n"],
                        default="n",
                    )
                    if choice == "y":
                        deploy_resp = _send({"command": "deploy_action", "name": name, "action": action}, stream=True)
                        _check_error(deploy_resp)
                        console.print(f"  [green]✓ Removed[/]")
                    else:
                        console.print(f"  [red]✗ Skipped[/]")

        console.print(f"\n{response['reply']}")


@app.command()
def inspect(name: str = typer.Option(..., "--name", "-n")):
    """Inspect a cell's full state."""
    response = _send({"command": "inspect", "name": name})
    _check_error(response)
    info = response["cell"]

    # Collect output topics
    output_topics = list({c.get("output_topic", "") for c in info.get("consumers", []) if c.get("output_topic")})

    console.print(Panel(
        f"[bold]{info['name']}[/] ({info['cell_id']})\n"
        f"Status: {info['status']}\n"
        f"Decision topic: {info['decision_topic']}\n"
        f"Output topics: {', '.join(output_topics) if output_topics else 'none'}",
        title="Cell Info",
    ))

    console.print(Panel(info["directive"], title="Directive"))

    if info["consumers"]:
        table = Table(title="Consumers")
        table.add_column("ID", style="cyan")
        table.add_column("Description")
        table.add_column("Topics")
        table.add_column("Output", style="green")
        table.add_column("Detects")
        table.add_column("Events", justify="right")
        table.add_column("Alerts", justify="right")
        table.add_column("Errors", justify="right")
        table.add_column("Running")
        for c in info["consumers"]:
            patterns = c.get("detection_patterns", [])
            table.add_row(
                c["consumer_id"],
                (c.get("description", "") or "")[:60] + ("..." if len(c.get("description", "") or "") > 60 else ""),
                ", ".join(c["source_topics"]),
                c.get("output_topic", "-") or "-",
                ", ".join(patterns[:3]) + (f" +{len(patterns)-3}" if len(patterns) > 3 else "") if patterns else "-",
                str(c["events_processed"]),
                str(c.get("alerts_emitted", 0)),
                str(c.get("errors", 0)),
                "[green]yes[/]" if c["running"] else "[red]no[/]",
            )
        console.print(table)
    else:
        console.print("[dim]No consumers spawned.[/]")

    kb = info.get("knowledge_base", {})
    tables = kb.get("tables", {})

    if tables:
        kb_table = Table(title="Knowledge Base Tables")
        kb_table.add_column("Table", style="cyan")
        kb_table.add_column("Rows", justify="right")
        kb_table.add_column("Size", justify="right")
        kb_table.add_column("Type")
        for tname, tinfo in tables.items():
            ttype = "[dim]built-in[/]" if tname in ("knowledge", "state") else "[green]custom[/]"
            kb_table.add_row(tname, str(tinfo["rows"]), tinfo["size"], ttype)
        console.print(kb_table)
    else:
        console.print("[dim]No knowledge base tables found.[/]")

    categories = kb.get("categories", {})
    if categories:
        console.print(Panel(
            "\n".join(f"  {cat}: {count}" for cat, count in categories.items()),
            title="Embedding Categories",
        ))


@app.command()
def code(
    name: str = typer.Option(..., "--name", "-n"),
    consumer_id: str = typer.Option(None, "--consumer", "-c", help="Specific consumer ID (omit for all)"),
):
    """Show the nucleus-authored code running in a cell's consumers."""
    cmd = {"command": "consumer_code", "name": name}
    if consumer_id:
        cmd["consumer_id"] = consumer_id
    response = _send(cmd)
    _check_error(response)

    if consumer_id:
        console.print(Panel(
            f"Consumer: [cyan]{response['consumer_id']}[/]",
            title=f"{name} — Authored Consumer Code",
        ))
        console.print(Syntax(response["code"], "python", theme="monokai", line_numbers=True))
    else:
        consumers = response.get("consumers", [])
        if not consumers:
            console.print("[dim]No consumers found.[/]")
            return
        for c in consumers:
            console.print(Panel(
                f"Consumer: [cyan]{c['consumer_id']}[/]\n"
                f"Topics: {', '.join(c['source_topics'])} → {c.get('output_topic', '-')}\n"
                f"Events: {c['events_processed']} | Alerts: {c.get('alerts_emitted', 0)} | Errors: {c.get('errors', 0)}",
                title=f"Consumer",
            ))
            if c.get("consumer_code"):
                console.print(Syntax(c["consumer_code"], "python", theme="monokai", line_numbers=True))
            console.print()


@app.command()
def decisions(
    name: str = typer.Option(..., "--name", "-n"),
    last: int = typer.Option(20, "--last", "-l", help="Number of decisions to show"),
    compact: bool = typer.Option(False, "--compact", help="Compact output — one line per decision, no code"),
    full: bool = typer.Option(False, "--full", help="Full output — show complete reasoning and code"),
):
    """Show recent decisions from a cell's decision log."""
    response = _send({"command": "decisions", "name": name})
    _check_error(response)

    topic = response["topic"]

    from confluent_kafka import Consumer as KConsumer, TopicPartition

    consumer = KConsumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": "cli-decisions-ephemeral",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })

    metadata = consumer.list_topics(topic, timeout=5)
    if topic not in metadata.topics:
        console.print(f"[dim]Topic {topic} not found.[/]")
        consumer.close()
        return

    partitions = [
        TopicPartition(topic, p, 0)
        for p in metadata.topics[topic].partitions.keys()
    ]
    consumer.assign(partitions)

    console.print(f"[dim]Reading from {topic}...[/]")
    messages = []
    empty_polls = 0
    while empty_polls < 3:
        msg = consumer.poll(1.0)
        if msg is None:
            empty_polls += 1
            continue
        if msg.error():
            continue
        empty_polls = 0
        try:
            messages.append(json.loads(msg.value()))
        except Exception:
            pass

    consumer.close()

    if not messages:
        console.print("[dim]No decisions found.[/]")
        return

    console.print(f"[dim]{len(messages)} total decisions[/]\n")

    for d in messages[-last:]:
        dtype = d.get("decision_type", "")
        style = "green" if dtype == "spawn_consumer" else "yellow" if dtype == "store_knowledge" else "blue"
        action = d.get("action", {})

        if compact:
            # One line per decision
            ts = d.get("timestamp", "")[:19]
            summary = ""
            if dtype == "spawn_consumer":
                summary = f"→ {action.get('consumer_id', '?')} on {action.get('source_topics', [])} → {action.get('output_topic', '?')}"
            elif dtype == "store_knowledge":
                summary = f"→ {action.get('category', '?')}: {action.get('content', '')[:80]}"
            elif dtype == "reasoning":
                summary = f"→ {d.get('reasoning', '')[:80]}"
            console.print(f"[dim]{ts}[/] [{style}]{dtype:<16}[/] {summary}")
        else:
            # Full or default output
            reasoning = d.get("reasoning", "")
            if not full and len(reasoning) > 300:
                reasoning = reasoning[:300] + "..."

            body = f"[dim]{d.get('timestamp', '')}[/]\nType: [{style}]{dtype}[/]\n"
            if reasoning:
                body += f"Reasoning: {reasoning}\n"

            if action.get("consumer_code"):
                if full:
                    console.print(Panel(body, title="Decision"))
                    console.print(Syntax(action["consumer_code"], "python", theme="monokai", line_numbers=True))
                else:
                    code_lines = action["consumer_code"].strip().split("\n")
                    body += f"\nAuthored code: ({len(code_lines)} lines, use --full to see)\n"
                    # Show first 5 and last 3 lines as preview
                    preview = code_lines[:5]
                    if len(code_lines) > 8:
                        preview.append(f"    # ... {len(code_lines) - 8} more lines ...")
                        preview.extend(code_lines[-3:])
                    elif len(code_lines) > 5:
                        preview.extend(code_lines[5:])
                    console.print(Panel(body, title="Decision"))
                    console.print(Syntax("\n".join(preview), "python", theme="monokai"))
            else:
                if full:
                    body += f"Action: {json.dumps(action, indent=2)}"
                else:
                    action_summary = {k: v for k, v in action.items() if k != "consumer_code"}
                    body += f"Action: {json.dumps(action_summary, indent=2)}"
                console.print(Panel(body, title="Decision"))


@app.command()
def status():
    """Show system-wide status."""
    response = _send({"command": "status"})
    _check_error(response)

    console.print(Panel(
        f"Active cells: {response['total_cells']}\n"
        f"Total consumers: {response['total_consumers']}\n"
        f"Total events: {response['total_events']}",
        title="System Status",
    ))

    if response.get("cells"):
        list_cells()


@app.command()
def topics():
    """List all Kafka topics."""
    from confluent_kafka.admin import AdminClient

    admin = AdminClient({"bootstrap.servers": "localhost:9092"})
    metadata = admin.list_topics(timeout=5)

    table = Table(title="Kafka Topics")
    table.add_column("Topic", style="cyan")
    table.add_column("Partitions", justify="right")
    table.add_column("Type")

    for topic_name in sorted(metadata.topics.keys()):
        if topic_name.startswith("_"):
            continue
        topic_info = metadata.topics[topic_name]
        if topic_name.startswith("network."):
            ttype = "[blue]source[/]"
        elif topic_name.startswith("agent.decisions"):
            ttype = "[yellow]decisions[/]"
        elif topic_name in ("threats.detected", "traffic.anomalies", "device.health.scores"):
            ttype = "[green]derived[/]"
        else:
            ttype = "other"
        table.add_row(topic_name, str(len(topic_info.partitions)), ttype)

    console.print(table)


@app.command()
def producers():
    """Run all synthetic data producers."""
    from src.producers.run import main as run_producers
    console.print("[bold green]Starting synthetic data producers...[/]")
    asyncio.run(run_producers())


if __name__ == "__main__":
    app()
