"""ESIOS CLI — command-line interface for the ESIOS API."""

try:
    from esios.cli.app import app
except ImportError:
    raise ImportError(
        "CLI dependencies not installed. Install with: pip install python-esios[cli]"
    )

__all__ = ["app"]
