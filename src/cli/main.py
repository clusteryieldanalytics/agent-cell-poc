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
        reader, writer = await asyncio.open_unix_connection(SOCKET_PATH, limit=1 << 20)
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
            elif stream:
                if data.get("type") == "text_delta":
                    # Stream of thought — render with dim styling and line prefix
                    text = data["text"]
                    # Prefix newlines with the continuation marker
                    text = text.replace("\n", "\n  [dim italic]│[/dim italic] ")
                    console.print(f"[dim italic]{text}[/dim italic]", end="", highlight=False)
                elif data.get("event"):
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


def _run_verification(name: str, max_iterations: int = 5, settle_seconds: int = 15):
    """Run the self-verification loop, streaming events to the console."""
    console.print(f"\n[bold cyan]Verifying consumers (up to {max_iterations} iterations, {settle_seconds}s settle)...[/]\n")
    response = _send(
        {"command": "verify", "name": name, "max_iterations": max_iterations, "settle_seconds": settle_seconds},
        stream=True,
    )
    _check_error(response)

    summary = response.get("summary", {})
    fixes = summary.get("fixes", [])
    stable = summary.get("stable", False)

    if stable:
        console.print(f"\n[bold green]Verification passed[/] — all consumers stable after {summary.get('iterations', '?')} iteration(s)")
    else:
        console.print(f"\n[bold yellow]Verification incomplete[/] — {summary.get('errors_found', 0)} errors, {len(fixes)} fixes applied")

    if fixes:
        console.print()
        for f in fixes:
            status = "[green]fixed[/]" if "replaced and running" in f.get("result", "") else f"[red]{f.get('result', '?')}[/]"
            console.print(f"  Iteration {f['iteration']}: {f['consumer_id']} — {f.get('error', '?')[:60]} → {status}")

    console.print()


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
    verify: bool = typer.Option(True, "--verify/--no-verify", help="Run self-verification after each consumer"),
    max_iterations: int = typer.Option(3, "--max-iters", help="Max verification fix iterations per consumer"),
    settle_seconds: int = typer.Option(15, "--settle", help="Seconds to wait between verification checks"),
):
    """Add a new agent cell. Proposes consumers one at a time: author → approve → verify → next."""
    console.print(Panel(
        f"[bold]Directive:[/] {directive[:200]}{'...' if len(directive) > 200 else ''}",
        title=f"Creating cell '{name}'",
    ))

    # Initialize the cell
    console.print(f"\n[bold cyan]Initializing cell...[/]\n")
    response = _send({"command": "init_cell", "name": name, "directive": directive}, stream=True)
    _check_error(response)

    consumer_num = 0

    # Iterative loop: propose one consumer at a time
    while True:
        consumer_num += 1
        console.print(f"\n[bold cyan]{'─' * 60}[/]")
        console.print(f"[bold cyan]Proposing consumer #{consumer_num}...[/]\n")

        response = _send({"command": "propose_next", "name": name}, stream=True)
        _check_error(response)

        decisions = response.get("decisions", [])
        consumer_decisions = [d for d in decisions if d.get("decision_type") == "spawn_consumer" or d.get("action", {}).get("type") == "spawn_consumer"]

        if not consumer_decisions:
            # Nucleus says it's done
            console.print("[dim]Nucleus has no more consumers to propose.[/]")
            for d in decisions:
                reasoning = d.get("reasoning", "")
                if reasoning:
                    console.print(f"  [dim]{reasoning[:200]}[/]")
            break

        # Show the proposed consumer
        d = consumer_decisions[0]
        action = d.get("action", {})
        consumer_id = action.get("consumer_id", "?")
        topics = action.get("source_topics", [])
        output = action.get("output_topic", "?")
        code = action.get("consumer_code", "")
        description = action.get("description", "")
        patterns = action.get("detection_patterns", [])

        console.print(f"  Consumer:  [bold cyan]{consumer_id}[/]")
        console.print(f"  Topics:    {', '.join(topics)} → [green]{output}[/]")
        if description:
            console.print(f"  Purpose:   {description[:120]}")
        if patterns:
            console.print(f"  Detects:   {', '.join(patterns)}")
        if d.get("reasoning"):
            console.print(f"  Reasoning: [dim]{d['reasoning'][:200]}{'...' if len(d.get('reasoning', '')) > 200 else ''}[/]")
        console.print()

        if code:
            console.print(Syntax(code, "python", theme="monokai", line_numbers=True))
            console.print()

        # Approval gate
        if auto_approve:
            console.print(f"  [green]✓ Auto-approved[/]")
            approved_indices = list(range(len(decisions)))
        else:
            choice = Prompt.ask(
                f"  Approve '[cyan]{consumer_id}[/]'? [green]y[/]es / [red]n[/]o / [yellow]d[/]one (stop adding)",
                choices=["y", "n", "d"],
                default="y",
            )

            if choice == "d":
                console.print("[dim]Stopping — no more consumers will be added.[/]")
                break
            elif choice == "n":
                console.print(f"  [red]✗ Rejected[/]")
                continue  # Ask for the next proposal
            else:
                approved_indices = list(range(len(decisions)))
                console.print(f"  [green]✓ Approved[/]")

        # Spawn the approved consumer
        console.print(f"\n  [cyan]Spawning '{consumer_id}'...[/]")
        response = _send({"command": "approve", "name": name, "approved": approved_indices})
        _check_error(response)

        # Verify this consumer
        if verify:
            _run_verification(name, max_iterations, settle_seconds)

    # Final summary
    info = _send({"command": "inspect", "name": name}).get("cell", {})
    console.print(Panel(
        f"[bold green]Cell '{name}' active[/]\n"
        f"Cell ID: {info.get('cell_id', '?')}\n"
        f"Consumers: {len(info.get('consumers', []))}\n"
        f"Decision topic: {info.get('decision_topic', '?')}",
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


@app.command(name="purge-all")
def purge_all():
    """Purge ALL cells and their resources. Nuclear option."""
    # First list what exists
    response = _send({"command": "list"})
    _check_error(response)
    cells = response.get("cells", [])

    if not cells:
        console.print("[dim]No cells to purge.[/]")
        return

    console.print(f"[bold red]This will permanently delete ALL {len(cells)} cell(s) and their resources:[/]\n")
    for c in cells:
        console.print(f"  [red]✗[/] {c.get('name', c.get('cell_id', '?'))}")

    console.print()
    choice = Prompt.ask("  Continue?", choices=["y", "n"], default="n")
    if choice != "y":
        console.print("[dim]Aborted.[/]")
        return

    console.print()
    response = _send({"command": "purge_all"}, stream=True)
    _check_error(response)

    results = response.get("results", [])
    for r in results:
        console.print(f"  [red]✗[/] {r['name']}: {r.get('actions_count', 0)} resources deleted")

    console.print(f"\n[bold red]All cells purged.[/]")


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
                        result = deploy_resp.get("result", "")
                        if "Failed" in result or "error" in result.lower() or "gone" in result.lower():
                            console.print(f"  [red]✗ {result}[/]")
                        else:
                            console.print(f"  [green]✓ {result}[/]")
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
                        result = deploy_resp.get("result", "")
                        console.print(f"  [green]✓ {result}[/]")
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
        kb_table.add_column("Indexes")
        for tname, tinfo in tables.items():
            ttype = "[dim]built-in[/]" if tname in ("knowledge", "state") else "[green]custom[/]"
            idx_list = tinfo.get("indexes", [])
            idx_display = ""
            for idx in idx_list:
                if idx == "vector":
                    idx_display += "[magenta]vector[/] "
                elif idx == "vector*":
                    idx_display += "[dim magenta]vector*[/] "
                elif idx == "fts":
                    idx_display += "[cyan]fts[/] "
                elif idx == "fts*":
                    idx_display += "[dim cyan]fts*[/] "
                elif idx == "gin":
                    idx_display += "[yellow]gin[/] "
                else:
                    idx_display += f"[dim]{idx}[/] "
            kb_table.add_row(tname, str(tinfo["rows"]), tinfo["size"], ttype, idx_display.strip() or "[dim]-[/]")
        console.print(kb_table)
        console.print("[dim]  * = column exists but no index[/]")
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

    console.print(f"[dim]{len(messages)} total entries[/]\n")

    for d in messages[-last:]:
        ts = d.get("timestamp", "")[:19]
        # Support both old format (decision_type) and new format (entry_type)
        entry_type = d.get("entry_type") or d.get("decision_type", "unknown")

        # Color map
        type_styles = {
            "chat_user_message": "bold cyan",
            "chat_response": "bold green",
            "api_call": "cyan",
            "api_response": "green",
            "tool_call": "yellow",
            "tool_result": "dim",
            "assistant_text": "magenta",
            "decision": "bold green",
            "spawn_consumer": "bold green",
            "store_knowledge": "yellow",
            "replace_consumer": "bold yellow",
            "reasoning": "blue",
            "reason_start": "bold cyan",
            "reason_complete": "bold green",
            "tool_loop_limit": "bold red",
        }
        style = type_styles.get(entry_type, "dim")

        if compact:
            summary = ""
            if entry_type == "chat_user_message":
                summary = d.get("message", "")[:80]
            elif entry_type == "chat_response":
                summary = d.get("reply", "")[:80]
            elif entry_type == "api_call":
                summary = f"turn {d.get('turn', '?')} ({d.get('mode', '?')})"
            elif entry_type == "api_response":
                summary = f"in={d.get('input_tokens', '?')} out={d.get('output_tokens', '?')} stop={d.get('stop_reason', '?')}"
            elif entry_type == "tool_call":
                inp = d.get("input", {})
                summary = f"{d.get('tool', '?')}({json.dumps(inp, default=str)[:60]})"
            elif entry_type == "tool_result":
                summary = f"{d.get('tool', '?')} → {d.get('result', '')[:60]}"
            elif entry_type == "assistant_text":
                summary = d.get("text", "")[:80]
            elif entry_type == "decision":
                inner = d.get("decision_type", d.get("action", {}).get("type", "?"))
                summary = f"{inner}: {d.get('reasoning', '')[:60]}"
            elif entry_type in ("spawn_consumer", "store_knowledge", "reasoning"):
                # Old format
                action = d.get("action", {})
                summary = f"{action.get('consumer_id', d.get('reasoning', '')[:60])}"
            else:
                summary = json.dumps({k: v for k, v in d.items() if k not in ("timestamp", "cell_id", "entry_type")}, default=str)[:80]

            console.print(f"[dim]{ts}[/] [{style}]{entry_type:<20}[/] {summary}")

        else:
            # Detailed output
            # Filter out noise for non-full mode
            if not full and entry_type in ("api_call", "api_response"):
                tokens = f"in={d.get('input_tokens', '')} out={d.get('output_tokens', '')}" if entry_type == "api_response" else f"turn {d.get('turn', '?')}"
                console.print(f"  [{style}]▸ {entry_type}: {tokens}[/]")
                continue

            body = f"[dim]{d.get('timestamp', '')}[/]\nType: [{style}]{entry_type}[/]\n"

            if entry_type == "chat_user_message":
                body += f"Message: {d.get('message', '')}\n"
            elif entry_type == "chat_response":
                reply = d.get("reply", "")
                if not full and len(reply) > 300:
                    reply = reply[:300] + "..."
                body += f"Reply: {reply}\n"
                if d.get("forced"):
                    body += "[yellow](forced — tool loop limit reached)[/]\n"
            elif entry_type == "tool_call":
                body += f"Tool: [yellow]{d.get('tool', '?')}[/]\n"
                inp = d.get("input", {})
                body += f"Input: {json.dumps(inp, indent=2, default=str)}\n"
            elif entry_type == "tool_result":
                body += f"Tool: {d.get('tool', '?')}\n"
                result = d.get("result", "")
                if not full and len(result) > 200:
                    result = result[:200] + f"... ({d.get('result_length', '?')} chars total)"
                body += f"Result: {result}\n"
            elif entry_type == "assistant_text":
                text = d.get("text", "")
                if not full and len(text) > 300:
                    text = text[:300] + "..."
                body += f"Text: {text}\n"
            elif entry_type == "decision":
                action = d.get("action", {})
                reasoning = d.get("reasoning", "")
                if not full and len(reasoning) > 300:
                    reasoning = reasoning[:300] + "..."
                if reasoning:
                    body += f"Reasoning: {reasoning}\n"
                code = action.get("consumer_code", "")
                if code:
                    if full:
                        console.print(Panel(body, title=entry_type))
                        console.print(Syntax(code, "python", theme="monokai", line_numbers=True))
                        continue
                    else:
                        lines = code.strip().split("\n")
                        body += f"\nAuthored code: ({len(lines)} lines, use --full to see)\n"
                else:
                    action_clean = {k: v for k, v in action.items() if k != "consumer_code"}
                    body += f"Action: {json.dumps(action_clean, indent=2, default=str)}\n"
            elif entry_type in ("spawn_consumer", "store_knowledge", "reasoning"):
                # Old format entries
                reasoning = d.get("reasoning", "")
                if not full and len(reasoning) > 300:
                    reasoning = reasoning[:300] + "..."
                if reasoning:
                    body += f"Reasoning: {reasoning}\n"
                action = d.get("action", {})
                code = action.get("consumer_code", "")
                if code and full:
                    console.print(Panel(body, title=entry_type))
                    console.print(Syntax(code, "python", theme="monokai", line_numbers=True))
                    continue
                elif code:
                    body += f"Code: ({len(code.split(chr(10)))} lines, use --full)\n"
                else:
                    body += f"Action: {json.dumps(action, indent=2, default=str)}\n"
            else:
                extra = {k: v for k, v in d.items() if k not in ("timestamp", "cell_id", "entry_type")}
                if extra:
                    body += json.dumps(extra, indent=2, default=str) + "\n"

            console.print(Panel(body, title=entry_type))


@app.command()
def dlq(
    name: str = typer.Option(..., "--name", "-n", help="Cell name"),
    consumer_id: str = typer.Option(None, "--consumer", "-c", help="Specific consumer (omit for summary)"),
    last: int = typer.Option(20, "--last", "-l", help="Number of DLQ entries to show"),
):
    """Inspect consumer dead letter queues."""
    cmd = {"command": "dlq", "name": name, "limit": last}
    if consumer_id:
        cmd["consumer_id"] = consumer_id
    response = _send(cmd)
    _check_error(response)

    if consumer_id:
        entries = response.get("entries", [])
        dlq_topic = response.get("dlq_topic", "?")
        console.print(f"[dim]DLQ topic: {dlq_topic}[/]")
        if not entries:
            console.print(f"[green]No errors in DLQ for '{consumer_id}'[/]")
            return
        console.print(f"[red]{len(entries)} error(s):[/]\n")
        for e in entries:
            console.print(Panel(
                f"[dim]{e.get('timestamp', '')}[/]\n"
                f"Error: [red]{e.get('error_type', '?')}: {e.get('error', '?')}[/]\n"
                f"Traceback:\n[dim]{e.get('traceback', '')[-500:]}[/]\n"
                f"Event: [dim]{json.dumps(e.get('event', {}), default=str)[:200]}[/]",
                title="DLQ Entry",
            ))
    else:
        dlqs = response.get("dlqs", [])
        table = Table(title="Consumer Dead Letter Queues")
        table.add_column("Consumer", style="cyan")
        table.add_column("Errors", justify="right")
        table.add_column("DLQ Topic", style="dim")
        table.add_column("Latest Error")
        for d in dlqs:
            latest = d.get("latest_error")
            latest_str = f"[red]{latest.get('error_type', '?')}: {latest.get('error', '')[:50]}[/]" if latest else "[green]none[/]"
            table.add_row(
                d["consumer_id"],
                str(d["errors"]),
                d["dlq_topic"],
                latest_str,
            )
        console.print(table)


@app.command(name="verify")
def verify_cell(
    name: str = typer.Option(..., "--name", "-n", help="Cell name"),
    max_iterations: int = typer.Option(5, "--max-iters", help="Max fix iterations"),
    settle_seconds: int = typer.Option(15, "--settle", help="Seconds between checks"),
):
    """Run self-verification on a cell's consumers. Checks DLQs and auto-fixes errors."""
    _run_verification(name, max_iterations, settle_seconds)


@app.command()
def dashboards():
    """List active visualization dashboards."""
    response = _send({"command": "dashboards"})
    _check_error(response)
    dashes = response.get("dashboards", [])

    if not dashes:
        console.print("[dim]No dashboards. Chat with a cell and ask it to create a visualization.[/]")
        return

    table = Table(title="Live Dashboards")
    table.add_column("Dashboard", style="cyan")
    table.add_column("Cell")
    table.add_column("Panels", justify="right")
    table.add_column("Created")
    table.add_column("URL", style="green")

    for d in dashes:
        table.add_row(
            d["title"],
            d["cell_name"],
            str(d["panels"]),
            d["created_at"][:19],
            f"http://localhost:3000/dashboard/{d['dashboard_id']}",
        )

    console.print(table)
    console.print(f"\n[dim]Dashboard index: http://localhost:3000[/]")


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
