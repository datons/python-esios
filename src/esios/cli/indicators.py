"""CLI subcommands for indicators."""

from __future__ import annotations

import sys
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

indicators_app = typer.Typer(no_args_is_help=True)
console = Console()


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


@indicators_app.command("history")
def history(
    indicator_id: int = typer.Argument(..., help="Indicator ID"),
    start: str = typer.Option(..., "--start", "-s", help="Start date (YYYY-MM-DD)"),
    end: str = typer.Option(..., "--end", "-e", help="End date (YYYY-MM-DD)"),
    token: Optional[str] = typer.Option(None, "--token", "-t"),
    format: str = typer.Option("table", "--format", "-f", help="table, csv, json, parquet"),
    output: Optional[str] = typer.Option(None, "--output", "-o", help="Output file path"),
):
    """Get historical data for an indicator."""
    from esios.cli.app import get_client

    client = get_client(token)
    handle = client.indicators.get(indicator_id)
    df = handle.historical(start, end)

    if output and format == "parquet":
        df.to_parquet(output)
        typer.echo(f"Written to {output}")
        return

    _output(df, format, title=f"Indicator {indicator_id}: {handle.name}", output_path=output)


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
