from __future__ import annotations

import sys
from datetime import datetime
from typing import Optional

import click
from rich import box
from rich.console import Console
from rich.markdown import Markdown
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table
from rich.text import Text

console = Console()


def _get_store():
    from quarantine.stores.factory import get_store
    return get_store()


def _severity_color(severity: str) -> str:
    return {"critical": "red", "warning": "yellow", "info": "cyan"}.get(severity, "white")


def _type_color(ft: str) -> str:
    colors = {
        "schema_violation": "magenta",
        "null_violation": "red",
        "uniqueness_violation": "orange3",
        "range_violation": "yellow",
        "pattern_violation": "blue",
        "volume_anomaly": "cyan",
        "business_rule": "green",
        "unknown": "white",
    }
    return colors.get(ft, "white")


@click.group()
@click.version_option(package_name="quarantine-dq")
def cli():
    """quarantine-dq — dead letter queue for data pipelines."""


@cli.command("inspect")
@click.option("--pipeline", "-p", default=None, help="Filter by pipeline name")
@click.option("--failure-type", "-t", default=None, help="Filter by failure type")
@click.option("--since", "-s", default=None, help="Filter records after date (YYYY-MM-DD)")
@click.option("--limit", "-l", default=50, show_default=True, help="Maximum records to show")
@click.option("--show-data", is_flag=True, default=False, help="Show original record data")
def inspect(pipeline, failure_type, since, limit, show_data):
    """List quarantined records."""
    store = _get_store()

    since_dt = None
    if since:
        try:
            since_dt = datetime.fromisoformat(since)
        except ValueError:
            console.print(f"[red]Invalid date format: {since}. Use YYYY-MM-DD.[/red]")
            sys.exit(1)

    with Progress(SpinnerColumn(), TextColumn("Fetching records…"), transient=True, console=console):
        records = store.list(
            pipeline_name=pipeline,
            failure_type=failure_type,
            since=since_dt,
            limit=limit,
        )

    if not records:
        console.print(Panel("[green]No quarantined records found.[/green]", title="quarantine inspect"))
        return

    table = Table(
        title=f"Quarantined Records ({len(records)} shown)",
        box=box.ROUNDED,
        show_header=True,
        header_style="bold blue",
    )
    table.add_column("ID", style="dim", width=10)
    table.add_column("Pipeline", max_width=20)
    table.add_column("Stage", max_width=15)
    table.add_column("Failure Type", max_width=22)
    table.add_column("Sev", width=5)
    table.add_column("Reason", max_width=50)
    table.add_column("Created At", max_width=20)
    table.add_column("Replay", width=8)

    for rec in records:
        sev_color = _severity_color(rec.failure_severity)
        type_color = _type_color(rec.failure_type)
        replay = rec.replay_status or "—"
        replay_color = {"success": "green", "failed": "red", "pending": "yellow"}.get(
            rec.replay_status, "dim"
        )

        table.add_row(
            rec.id[:8] + "…",
            rec.pipeline_name,
            rec.stage_name,
            f"[{type_color}]{rec.failure_type}[/{type_color}]",
            f"[{sev_color}]{rec.failure_severity[0].upper()}[/{sev_color}]",
            rec.failure_reason[:80],
            rec.created_at.strftime("%Y-%m-%d %H:%M:%S") if rec.created_at else "—",
            f"[{replay_color}]{replay}[/{replay_color}]",
        )

    console.print(table)

    if show_data:
        for rec in records:
            console.print(
                Panel(
                    str(rec.record_data),
                    title=f"[dim]{rec.id[:8]}…[/dim] record data",
                    border_style="dim",
                )
            )


@cli.command("replay")
@click.option("--pipeline", "-p", required=True, help="Pipeline name to replay")
@click.option("--failure-type", "-t", default=None, help="Filter by failure type")
@click.option("--since", "-s", default=None, help="Only replay records after date (YYYY-MM-DD)")
@click.option("--dry-run", is_flag=True, default=False, help="Show what would be replayed without doing it")
@click.option("--limit", "-l", default=1000, show_default=True)
def replay(pipeline, failure_type, since, dry_run, limit):
    """Replay quarantined records through the identity transform (pass-through)."""
    from quarantine.core.replay import ReplayEngine

    since_dt = None
    if since:
        try:
            since_dt = datetime.fromisoformat(since)
        except ValueError:
            console.print(f"[red]Invalid date format: {since}.[/red]")
            sys.exit(1)

    engine = ReplayEngine()

    def passthrough(record):
        return record

    result = engine.replay(
        pipeline_name=pipeline,
        transform_fn=passthrough,
        failure_type=failure_type,
        since=since_dt,
        dry_run=dry_run,
        limit=limit,
    )

    if not dry_run:
        console.print(
            Panel(
                f"[green]succeeded:[/green] {result.succeeded}   "
                f"[red]failed:[/red] {result.failed}   "
                f"attempted: {result.attempted}",
                title=f"Replay — {pipeline}",
            )
        )


@cli.command("stats")
@click.option("--pipeline", "-p", default=None, help="Filter to a single pipeline")
@click.option("--fail-on-critical", is_flag=True, default=False,
              help="Exit with code 1 if unresolved CRITICAL records exist")
def stats(pipeline, fail_on_critical):
    """Show failure statistics dashboard."""
    store = _get_store()
    data = store.stats(pipeline_name=pipeline)

    title = f"Quarantine Stats — {pipeline}" if pipeline else "Quarantine Stats — All Pipelines"
    console.print(Panel(
        f"[bold]Total records:[/bold] {data['total']}   "
        f"[bold]Unresolved:[/bold] [red]{data['unresolved']}[/red]",
        title=title,
        border_style="blue",
    ))

    if data["by_failure_type"]:
        type_table = Table(title="By Failure Type", box=box.SIMPLE)
        type_table.add_column("Failure Type")
        type_table.add_column("Count", justify="right")
        for ft, count in sorted(data["by_failure_type"].items(), key=lambda x: -x[1]):
            color = _type_color(ft)
            type_table.add_row(f"[{color}]{ft}[/{color}]", str(count))
        console.print(type_table)

    if data["by_severity"]:
        sev_table = Table(title="By Severity", box=box.SIMPLE)
        sev_table.add_column("Severity")
        sev_table.add_column("Count", justify="right")
        for sev, count in sorted(data["by_severity"].items(), key=lambda x: -x[1]):
            color = _severity_color(sev)
            sev_table.add_row(f"[{color}]{sev}[/{color}]", str(count))
        console.print(sev_table)

    if data["by_pipeline"] and not pipeline:
        pipe_table = Table(title="By Pipeline", box=box.SIMPLE)
        pipe_table.add_column("Pipeline")
        pipe_table.add_column("Count", justify="right")
        for pname, count in sorted(data["by_pipeline"].items(), key=lambda x: -x[1]):
            pipe_table.add_row(pname, str(count))
        console.print(pipe_table)

    if fail_on_critical:
        critical_count = store.count(
            pipeline_name=pipeline,
            failure_type=None,
            replay_status="unresolved",
        )
        # Check for critical severity unresolved
        records = store.list(pipeline_name=pipeline, limit=10000)
        critical_unresolved = [
            r for r in records
            if r.failure_severity == "critical" and r.replay_status is None
        ]
        if critical_unresolved:
            console.print(
                f"[red bold]CRITICAL:[/red bold] {len(critical_unresolved)} unresolved critical "
                f"records — failing CI gate."
            )
            sys.exit(1)


@cli.command("report")
@click.option("--pipeline", "-p", default=None)
@click.option("--output", "-o", default="quarantine_report.md", show_default=True)
def report(pipeline, output):
    """Generate a markdown failure report."""
    store = _get_store()
    data = store.stats(pipeline_name=pipeline)
    records = store.list(pipeline_name=pipeline, limit=500)

    lines = [
        f"# Quarantine Report\n",
        f"**Generated:** {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}  ",
        f"**Pipeline:** {pipeline or 'All'}  ",
        f"**Total records:** {data['total']}  ",
        f"**Unresolved:** {data['unresolved']}\n",
        "## Failures by Type\n",
        "| Failure Type | Count |",
        "| --- | --- |",
    ]
    for ft, count in sorted(data["by_failure_type"].items(), key=lambda x: -x[1]):
        lines.append(f"| {ft} | {count} |")

    lines += ["\n## Recent Records\n", "| ID | Pipeline | Stage | Type | Severity | Reason |",
               "| --- | --- | --- | --- | --- | --- |"]
    for rec in records[:50]:
        lines.append(
            f"| {rec.id[:8]}… | {rec.pipeline_name} | {rec.stage_name} | "
            f"{rec.failure_type} | {rec.failure_severity} | {rec.failure_reason[:60]} |"
        )

    content = "\n".join(lines)
    with open(output, "w", encoding="utf-8") as f:
        f.write(content)

    console.print(f"[green]Report written to [bold]{output}[/bold][/green]")


@cli.command("clear")
@click.option("--pipeline", "-p", default=None, help="Only clear records from this pipeline")
@click.option("--replayed-only", is_flag=True, default=False,
              help="Only delete successfully replayed records")
@click.confirmation_option(prompt="This will delete quarantined records. Continue?")
def clear(pipeline, replayed_only):
    """Delete quarantined records."""
    store = _get_store()

    if hasattr(store, "delete_where"):
        count = store.delete_where(pipeline_name=pipeline, replayed_only=replayed_only)
        console.print(f"[green]Deleted {count} records.[/green]")
    else:
        records = store.list(pipeline_name=pipeline, limit=100000)
        deleted = 0
        for rec in records:
            if replayed_only and rec.replay_status != "success":
                continue
            store.delete(rec.id)
            deleted += 1
        console.print(f"[green]Deleted {deleted} records.[/green]")
