"""ESIOS CLI — main Typer application."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

import typer

from esios.cli.config import get_token

app = typer.Typer(
    name="esios",
    help="CLI for the Spanish electricity market (ESIOS/REE) API.",
    no_args_is_help=True,
)


def get_client(token: str | None = None):
    """Lazy import + construct client."""
    from esios.client import ESIOSClient

    resolved = token or get_token()
    if not resolved:
        typer.echo("Error: No API token. Set ESIOS_API_KEY or run: esios config set token <KEY>", err=True)
        raise typer.Exit(1)
    return ESIOSClient(token=resolved)


# -- Register sub-commands ---------------------------------------------------

from esios.cli.indicators import indicators_app  # noqa: E402
from esios.cli.archives import archives_app  # noqa: E402
from esios.cli.cache_cmd import cache_app  # noqa: E402
from esios.cli.config_cmd import config_app  # noqa: E402
from esios.cli.exec_cmd import exec_command  # noqa: E402

app.add_typer(indicators_app, name="indicators", help="Indicator operations")
app.add_typer(archives_app, name="archives", help="Archive operations")
app.add_typer(cache_app, name="cache", help="Cache management")
app.add_typer(config_app, name="config", help="Configuration management")
app.command("exec", help="Fetch data and evaluate a Python expression on it")(exec_command)


def main() -> None:
    app()


if __name__ == "__main__":
    main()
