"""CLI subcommands for configuration management."""

from __future__ import annotations

import typer

config_app = typer.Typer(no_args_is_help=True)


@config_app.command("set")
def config_set(
    key: str = typer.Argument(..., help="Configuration key (e.g., 'token')"),
    value: str = typer.Argument(..., help="Configuration value"),
):
    """Set a configuration value."""
    from esios.cli.config import set_config

    set_config(key, value)
    typer.echo(f"Set {key} = {'***' if 'token' in key.lower() else value}")


@config_app.command("get")
def config_get(
    key: str = typer.Argument(..., help="Configuration key to read"),
):
    """Get a configuration value."""
    from esios.cli.config import get_config

    val = get_config(key)
    if val is None:
        typer.echo(f"{key}: (not set)")
    else:
        display = "***" if "token" in key.lower() else val
        typer.echo(f"{key} = {display}")
