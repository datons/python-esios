"""CLI subcommands for indicators."""

from __future__ import annotations

import sys
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

indicators_app = typer.Typer(no_args_is_help=True)
console = Console()


def _resolve_geos(handle, geo_refs: list[str] | None) -> list[int] | None:
    """Resolve --geo references (IDs or names) to geo_id list."""
    if not geo_refs:
        return None
    try:
        return [handle.resolve_geo(ref) for ref in geo_refs]
    except ValueError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)


@indicators_app.command("list")
def list_indicators(
    token: Optional[str] = typer.Option(None, "--token", "-t", help="ESIOS API key"),
    format: str = typer.Option("table", "--format", "-f", help="Output format: table, csv, json"),
):
    """List all available ESIOS indicators."""
    from esios.cli.app import get_client

    client = get_client(token)
    df = client.indicators.list()
    _output(df, format, title="ESIOS Indicators")


@indicators_app.command("search")
def search_indicators(
    query: str = typer.Argument(..., help="Search query (name substring)"),
    token: Optional[str] = typer.Option(None, "--token", "-t"),
    format: str = typer.Option("table", "--format", "-f"),
):
    """Search indicators by name."""
    from esios.cli.app import get_client

    client = get_client(token)
    df = client.indicators.search(query)
    if df.empty:
        typer.echo(f"No indicators matching '{query}'")
        raise typer.Exit(0)
    _output(df, format, title=f"Indicators matching '{query}'")


@indicators_app.command("meta")
def meta(
    indicator_id: int = typer.Argument(..., help="Indicator ID"),
    token: Optional[str] = typer.Option(None, "--token", "-t"),
    format: str = typer.Option("table", "--format", "-f", help="table, json"),
):
    """Show metadata for an indicator (unit, granularity, geos, etc.)."""
    from rich.panel import Panel
    from rich.table import Table as RichTable
    from rich.text import Text

    from esios.cli.app import get_client

    client = get_client(token)
    handle = client.indicators.get(indicator_id)
    md = handle.metadata

    if format == "json":
        import json
        # Build a clean dict without values
        clean = {k: v for k, v in md.items() if k != "values"}
        typer.echo(json.dumps(clean, indent=2, ensure_ascii=False, default=str))
        return

    # -- Rich panel output -----------------------------------------------------
    magnitud = md.get("magnitud", [{}])
    unit = magnitud[0].get("name", "—") if magnitud else "—"
    tiempo = md.get("tiempo", [{}])
    granularity = tiempo[0].get("name", "—") if tiempo else "—"
    updated = md.get("values_updated_at", "—")

    lines = Text()
    lines.append("Name:          ", style="bold")
    lines.append(f"{md.get('name', '—')}\n")
    lines.append("Short name:    ", style="bold")
    lines.append(f"{md.get('short_name', '—')}\n")
    lines.append("ID:            ", style="bold")
    lines.append(f"{md.get('id', '—')}\n")
    lines.append("Unit:          ", style="bold")
    lines.append(f"{unit}\n")
    lines.append("Granularity:   ", style="bold")
    lines.append(f"{granularity}\n")
    lines.append("Step type:     ", style="bold")
    lines.append(f"{md.get('step_type', '—')}\n")
    lines.append("Composited:    ", style="bold")
    lines.append(f"{'Yes' if md.get('composited') else 'No'}\n")
    lines.append("Disaggregated: ", style="bold")
    lines.append(f"{'Yes' if md.get('disaggregated') else 'No'}\n")
    lines.append("Last updated:  ", style="bold")
    lines.append(f"{updated}\n")

    geos = handle.geos
    if geos:
        lines.append("\n")
        lines.append("Geographies:\n", style="bold")
        for g in geos:
            lines.append(f"  {g['geo_id']:<8} {g['geo_name']}\n")

    console.print(Panel(lines, title=f"Indicator {indicator_id}", border_style="cyan"))


@indicators_app.command("history")
def history(
    indicator_id: int = typer.Argument(..., help="Indicator ID"),
    start: str = typer.Option(..., "--start", "-s", help="Start date (YYYY-MM-DD)"),
    end: str = typer.Option(..., "--end", "-e", help="End date (YYYY-MM-DD)"),
    geo: Optional[list[str]] = typer.Option(None, "--geo", "-g", help="Filter by geo ID or name (e.g. --geo 3 or --geo España)"),
    token: Optional[str] = typer.Option(None, "--token", "-t"),
    format: str = typer.Option("table", "--format", "-f", help="table, csv, json, parquet"),
    output: Optional[str] = typer.Option(None, "--output", "-o", help="Output file path"),
):
    """Get historical data for an indicator."""
    from esios.cli.app import get_client

    client = get_client(token)
    handle = client.indicators.get(indicator_id)
    geo_ids = _resolve_geos(handle, geo)
    df = handle.historical(start, end, geo_ids=geo_ids)

    if output and format == "parquet":
        df.to_parquet(output)
        typer.echo(f"Written to {output}")
        return

    _output(df, format, title=f"Indicator {indicator_id}: {handle.name}", output_path=output)


# Register exec under indicators
from esios.cli.exec_cmd import exec_command  # noqa: E402

indicators_app.command("exec", help="Fetch data and evaluate a Python expression on it")(exec_command)


def _output(df, format: str, title: str = "", output_path: str | None = None):
    """Render DataFrame in the requested format."""
    import pandas as pd

    if df.empty:
        typer.echo("No data.")
        return

    if format == "csv":
        text = df.to_csv()
        if output_path:
            with open(output_path, "w") as f:
                f.write(text)
            typer.echo(f"Written to {output_path}")
        else:
            typer.echo(text)

    elif format == "json":
        text = df.to_json(orient="records", indent=2, date_format="iso")
        if output_path:
            with open(output_path, "w") as f:
                f.write(text)
            typer.echo(f"Written to {output_path}")
        else:
            typer.echo(text)

    else:  # table
        table = Table(title=title)
        cols = list(df.columns)[:10]  # Limit columns for readability
        if df.index.name:
            table.add_column(df.index.name, style="cyan")
        for col in cols:
            table.add_column(str(col))

        for idx, row in df.head(50).iterrows():
            values = [str(idx)] if df.index.name else []
            values += [str(row[c]) for c in cols]
            table.add_row(*values)

        if len(df) > 50:
            table.caption = f"Showing 50 of {len(df)} rows"

        console.print(table)
