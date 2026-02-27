"""CLI subcommands for archives."""

from __future__ import annotations

from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

archives_app = typer.Typer(no_args_is_help=True)
console = Console()


@archives_app.command("list")
def list_archives(
    token: Optional[str] = typer.Option(None, "--token", "-t"),
    format: str = typer.Option("table", "--format", "-f"),
):
    """List all available ESIOS archives."""
    from esios.cli.app import get_client

    client = get_client(token)
    df = client.archives.list()

    if format == "csv":
        typer.echo(df.to_csv())
    elif format == "json":
        typer.echo(df.to_json(orient="records", indent=2))
    else:
        table = Table(title="ESIOS Archives")
        cols = list(df.columns)[:8]
        for col in cols:
            table.add_column(str(col))
        for _, row in df.head(50).iterrows():
            table.add_row(*[str(row[c]) for c in cols])
        if len(df) > 50:
            table.caption = f"Showing 50 of {len(df)} rows"
        console.print(table)


@archives_app.command("download")
def download_archive(
    archive_id: int = typer.Argument(..., help="Archive ID"),
    start: Optional[str] = typer.Option(None, "--start", "-s", help="Start date (YYYY-MM-DD)"),
    end: Optional[str] = typer.Option(None, "--end", "-e", help="End date (YYYY-MM-DD)"),
    date: Optional[str] = typer.Option(None, "--date", "-d", help="Single date (YYYY-MM-DD)"),
    output: str = typer.Option(".", "--output", "-o", help="Output directory"),
    token: Optional[str] = typer.Option(None, "--token", "-t"),
):
    """Download an archive for a date or date range."""
    from esios.cli.app import get_client

    if not date and not (start and end):
        typer.echo("Provide --date or both --start and --end.", err=True)
        raise typer.Exit(1)

    client = get_client(token)
    client.archives.download(
        archive_id,
        start=start,
        end=end,
        date=date,
        output_dir=output,
    )
    typer.echo(f"Download complete → {output}")
