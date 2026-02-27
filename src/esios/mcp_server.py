"""MCP server exposing ESIOS operations as tools.

Run with: uv run python -m esios.mcp_server
"""

from __future__ import annotations

import os

from fastmcp import FastMCP

mcp = FastMCP("esios", description="Spanish electricity market data (ESIOS/REE)")

_client = None


def get_client():
    """Lazy-init a shared ESIOSClient."""
    global _client
    if _client is None:
        from esios.client import ESIOSClient

        _client = ESIOSClient()
    return _client


@mcp.tool()
def search_indicators(query: str) -> str:
    """Search ESIOS indicators by name or description.

    Returns a table of matching indicators with their IDs and names.
    """
    client = get_client()
    df = client.indicators.search(query)
    if df.empty:
        return f"No indicators found matching '{query}'."
    # Return a concise table
    cols = [c for c in ["id", "name", "short_name"] if c in df.columns]
    return df[cols].head(20).to_string(index=False)


@mcp.tool()
def get_indicator_data(indicator_id: int, start_date: str, end_date: str) -> str:
    """Get historical data for an ESIOS indicator.

    Args:
        indicator_id: The ESIOS indicator ID (e.g. 600 for PVPC).
        start_date: Start date in YYYY-MM-DD format.
        end_date: End date in YYYY-MM-DD format.

    Returns CSV-formatted data with a datetime index.
    """
    client = get_client()
    handle = client.indicators.get(indicator_id)
    df = handle.historical(start_date, end_date)
    if df.empty:
        return f"No data for indicator {indicator_id} between {start_date} and {end_date}."
    return df.to_csv()


@mcp.tool()
def get_indicator_metadata(indicator_id: int) -> str:
    """Get metadata (name, description, units) for an ESIOS indicator.

    Args:
        indicator_id: The ESIOS indicator ID.
    """
    client = get_client()
    handle = client.indicators.get(indicator_id)
    meta = handle.metadata
    lines = [
        f"ID: {handle.id}",
        f"Name: {handle.name}",
        f"Description: {meta.get('description', 'N/A')}",
    ]
    return "\n".join(lines)


@mcp.tool()
def list_archives() -> str:
    """List available ESIOS archives/files."""
    client = get_client()
    df = client.archives.list()
    if df.empty:
        return "No archives available."
    cols = [c for c in ["id", "name"] if c in df.columns]
    return df[cols].head(30).to_string(index=False)


@mcp.tool()
def download_archive(
    archive_id: int,
    start_date: str,
    end_date: str,
    output_dir: str = ".",
) -> str:
    """Download an ESIOS archive file for a date range.

    Args:
        archive_id: The archive ID.
        start_date: Start date in YYYY-MM-DD format.
        end_date: End date in YYYY-MM-DD format.
        output_dir: Directory to save downloaded files.
    """
    client = get_client()
    client.archives.download(
        archive_id,
        start=start_date,
        end=end_date,
        output_dir=output_dir,
    )
    return f"Downloaded archive {archive_id} ({start_date} to {end_date}) → {output_dir}"


if __name__ == "__main__":
    mcp.run()
