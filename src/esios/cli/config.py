"""Configuration management — read/write config.toml and env vars."""

from __future__ import annotations

import os
from pathlib import Path

CONFIG_DIR = Path.home() / ".config" / "esios"
CONFIG_FILE = CONFIG_DIR / "config.toml"


def get_token() -> str | None:
    """Resolve API token: CLI flag > config file > env var."""
    # Try config file first
    if CONFIG_FILE.exists():
        try:
            import tomllib
        except ImportError:
            import tomli as tomllib  # type: ignore[no-redef]

        with open(CONFIG_FILE, "rb") as f:
            config = tomllib.load(f)
        token = config.get("token")
        if token:
            return token

    # Fall back to environment variable
    return os.getenv("ESIOS_API_KEY")


def set_config(key: str, value: str) -> None:
    """Write a key-value pair to config.toml."""
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)

    config: dict = {}
    if CONFIG_FILE.exists():
        try:
            import tomllib
        except ImportError:
            import tomli as tomllib  # type: ignore[no-redef]

        with open(CONFIG_FILE, "rb") as f:
            config = tomllib.load(f)

    config[key] = value

    # Write as simple TOML
    lines = [f'{k} = "{v}"' for k, v in config.items()]
    CONFIG_FILE.write_text("\n".join(lines) + "\n")


def get_config(key: str) -> str | None:
    """Read a value from config.toml."""
    if not CONFIG_FILE.exists():
        return None
    try:
        import tomllib
    except ImportError:
        import tomli as tomllib  # type: ignore[no-redef]

    with open(CONFIG_FILE, "rb") as f:
        config = tomllib.load(f)
    return config.get(key)
