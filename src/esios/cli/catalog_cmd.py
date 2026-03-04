"""CLI subcommands for the ESIOS catalog."""

from __future__ import annotations

from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

console = Console()
catalog_app = typer.Typer(no_args_is_help=True)

# -- list ------------------------------------------------------------------

list_app = typer.Typer(no_args_is_help=True)
catalog_app.add_typer(list_app, name="list", help="List catalog entries")


@list_app.command("indicators")
def list_indicators(
    query: Optional[str] = typer.Argument(None, help="Filter by name (substring)"),
) -> None:
    """List indicators in the catalog."""
    from esios.catalog import ESIOSCatalog

    cat = ESIOSCatalog()
    df = cat.indicators.list(query=query)

    if df.empty:
        typer.echo("No indicators found.")
        raise typer.Exit()

    table = Table(title=f"Indicators ({len(df)})")
    table.add_column("ID", style="cyan")
    table.add_column("Name")
    table.add_column("Short Name")
    table.add_column("Tags")

    for idx, row in df.iterrows():
        table.add_row(str(idx), row["name"], row.get("short_name", ""), row.get("tags", ""))

    console.print(table)


@list_app.command("archives")
def list_archives(
    query: Optional[str] = typer.Argument(None, help="Filter by name/description (substring)"),
) -> None:
    """List archives in the catalog."""
    from esios.catalog import ESIOSCatalog

    cat = ESIOSCatalog()
    df = cat.archives.list(query=query)

    if df.empty:
        typer.echo("No archives found.")
        raise typer.Exit()

    table = Table(title=f"Archives ({len(df)})")
    table.add_column("ID", style="cyan")
    table.add_column("Name")
    table.add_column("Description", max_width=50)
    table.add_column("Horizon")
    table.add_column("Type")

    for idx, row in df.iterrows():
        table.add_row(
            str(idx), row["name"], row.get("description", ""),
            row.get("horizon", ""), row.get("archive_type", ""),
        )

    console.print(table)


# -- show ------------------------------------------------------------------

show_app = typer.Typer(no_args_is_help=True)
catalog_app.add_typer(show_app, name="show", help="Show a single catalog entry")


@show_app.command("indicator")
def show_indicator(
    indicator_id: int = typer.Argument(..., help="Indicator ID"),
) -> None:
    """Show details of a single indicator."""
    from esios.catalog import ESIOSCatalog

    cat = ESIOSCatalog()
    try:
        entry = cat.indicators.get(indicator_id)
    except KeyError:
        typer.echo(f"Indicator {indicator_id} not found in catalog.", err=True)
        raise typer.Exit(1)

    from esios.catalog import load_reference

    typer.echo(f"ID:          {entry.id}")
    typer.echo(f"Name:        {entry.name}")
    typer.echo(f"Short Name:  {entry.extra.get('short_name', '')}")
    typer.echo(f"Notes:       {entry.extra.get('notes', '')}")
    typer.echo(f"Tags:        {', '.join(entry.extra.get('tags', []))}")

    # Resolve magnitude → units
    mag_id = entry.extra.get("magnitude_id")
    if mag_id is not None:
        magnitudes = load_reference("magnitudes")
        typer.echo(f"Units:       {magnitudes.get(mag_id, f'(magnitude {mag_id})')}")

    # Resolve time_period → granularity
    tp_id = entry.extra.get("time_period_id")
    if tp_id is not None:
        time_periods = load_reference("time_periods")
        typer.echo(f"Granularity: {time_periods.get(tp_id, f'(time_period {tp_id})')}")

    # Resolve geo_ids → geo names
    geo_ids = entry.extra.get("geo_ids", [])
    if geo_ids:
        geos = load_reference("geos")
        typer.echo("Geos:")
        for gid in geo_ids:
            typer.echo(f"  {gid}: {geos.get(gid, '?')}")


@show_app.command("archive")
def show_archive(
    archive_id: int = typer.Argument(..., help="Archive ID"),
) -> None:
    """Show details of a single archive."""
    from esios.catalog import ESIOSCatalog

    cat = ESIOSCatalog()
    try:
        entry = cat.archives.get(archive_id)
    except KeyError:
        typer.echo(f"Archive {archive_id} not found in catalog.", err=True)
        raise typer.Exit(1)

    typer.echo(f"ID:          {entry.id}")
    typer.echo(f"Name:        {entry.name}")
    typer.echo(f"Description: {entry.extra.get('description', '')}")
    typer.echo(f"Horizon:     {entry.extra.get('horizon', '')}")
    typer.echo(f"Type:        {entry.extra.get('archive_type', '')}")
    typer.echo(f"Notes:       {entry.extra.get('notes', '')}")


# -- refresh ---------------------------------------------------------------


@catalog_app.command("refresh")
def refresh(
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview changes without writing"),
    token: Optional[str] = typer.Option(None, "--token", "-t", envvar="ESIOS_API_KEY", help="API token"),
) -> None:
    """Refresh catalog from the live ESIOS API."""
    from esios.cli.app import get_client

    client = get_client(token)

    from esios.catalog import ESIOSCatalog

    cat = ESIOSCatalog(client)

    typer.echo("Refreshing indicators catalog...")
    ind_result = cat.indicators.refresh(dry_run=dry_run)
    typer.echo(f"  Indicators: +{ind_result.added} added, ~{ind_result.updated} updated, -{ind_result.removed} removed")

    typer.echo("Refreshing archives catalog...")
    arc_result = cat.archives.refresh(dry_run=dry_run)
    typer.echo(f"  Archives: +{arc_result.added} added, ~{arc_result.updated} updated, -{arc_result.removed} removed")

    if dry_run:
        typer.echo("\n(dry run — no files were modified)")
    else:
        typer.echo("\nCatalog updated.")

    client.close()
