"""CLI subcommands for cache management."""

from __future__ import annotations

from typing import Optional

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

cache_app = typer.Typer(no_args_is_help=True)
console = Console()


def _get_cache():
    """Get the CacheStore from a client instance."""
    from esios.cache import CacheConfig, CacheStore

    return CacheStore(CacheConfig())


@cache_app.command("status")
def cache_status():
    """Show cache statistics: path, files, size, geos, and catalog info."""
    store = _get_cache()
    info = store.status()

    lines = Text()
    lines.append("Path:       ", style="bold")
    lines.append(f"{info['path']}\n")
    lines.append("Files:      ", style="bold")
    lines.append(f"{info['files']}\n")
    lines.append("Size:       ", style="bold")
    lines.append(f"{info['size_mb']} MB\n")

    if info.get("endpoints"):
        lines.append("\nEndpoints:\n", style="bold")
        for ep, count in sorted(info["endpoints"].items()):
            lines.append(f"  {ep}: {count} files\n")

    # Geos registry
    geos = store.read_geos()
    if geos:
        lines.append(f"\nGeos registry: ", style="bold")
        lines.append(f"{len(geos)} geographies\n")
        for gid, gname in sorted(geos.items(), key=lambda x: int(x[0]) if x[0].isdigit() else 0):
            lines.append(f"  {gid:<8} {gname}\n")

    # Indicator catalog
    catalog = store.read_catalog("indicators")
    if catalog:
        lines.append(f"\nIndicator catalog: ", style="bold")
        lines.append(f"{len(catalog)} indicators cached\n")

    console.print(Panel(lines, title="ESIOS Cache", border_style="cyan"))


@cache_app.command("path")
def cache_path():
    """Print the cache directory path."""
    store = _get_cache()
    typer.echo(store.config.cache_dir)


@cache_app.command("clear")
def cache_clear(
    indicator: Optional[int] = typer.Option(None, "--indicator", "-i", help="Clear a specific indicator/archive ID"),
    endpoint: str = typer.Option("indicators", "--endpoint", "-e", help="Endpoint: indicators, archives"),
    all_: bool = typer.Option(False, "--all", "-a", help="Clear entire cache (including geos and catalogs)"),
):
    """Remove cached files (indicators, archives, or all)."""
    store = _get_cache()

    if all_:
        count = store.clear()
    elif indicator is not None:
        count = store.clear(endpoint=endpoint, indicator_id=indicator)
    else:
        count = store.clear(endpoint=endpoint)

    typer.echo(f"Removed {count} cached file(s).")


@cache_app.command("geos")
def cache_geos():
    """Show the global geos registry (geo_id → geo_name)."""
    store = _get_cache()
    geos = store.read_geos()

    if not geos:
        typer.echo("No geos cached yet. Fetch an indicator to populate.")
        return

    table = Table(title="Cached Geos Registry")
    table.add_column("geo_id", style="cyan")
    table.add_column("geo_name")

    for gid, gname in sorted(geos.items(), key=lambda x: int(x[0]) if x[0].isdigit() else 0):
        table.add_row(gid, gname)

    console.print(table)
