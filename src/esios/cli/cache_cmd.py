"""CLI subcommands for cache management."""

from __future__ import annotations

from typing import Optional

import typer
from rich.console import Console

cache_app = typer.Typer(no_args_is_help=True)
console = Console()


def _get_cache():
    """Get the CacheStore from a client instance."""
    from esios.cache import CacheConfig, CacheStore

    return CacheStore(CacheConfig())


@cache_app.command("status")
def cache_status():
    """Show cache path, file count, and total size."""
    store = _get_cache()
    info = store.status()
    console.print(f"Path:      {info['path']}")
    console.print(f"Files:     {info['files']}")
    console.print(f"Size:      {info['size_mb']} MB")
    if info.get("endpoints"):
        console.print("Breakdown:")
        for ep, count in sorted(info["endpoints"].items()):
            console.print(f"  {ep}: {count} files")


@cache_app.command("path")
def cache_path():
    """Print the cache directory path."""
    store = _get_cache()
    typer.echo(store.config.cache_dir)


@cache_app.command("clear")
def cache_clear(
    indicator: Optional[int] = typer.Option(None, "--indicator", "-i", help="Clear a specific indicator/archive ID"),
    endpoint: str = typer.Option("indicators", "--endpoint", "-e", help="Endpoint: indicators, offer_indicators, archives"),
    all_: bool = typer.Option(False, "--all", "-a", help="Clear entire cache"),
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
