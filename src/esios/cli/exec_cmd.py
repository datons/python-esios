"""CLI command: evaluate Python expressions against indicator data."""

from __future__ import annotations

from typing import Optional

import typer
from rich.console import Console

console = Console()


def exec_command(
    indicator_ids: list[int] = typer.Argument(..., help="One or more indicator IDs"),
    start: str = typer.Option(..., "--start", "-s", help="Start date (YYYY-MM-DD)"),
    end: str = typer.Option(..., "--end", "-e", help="End date (YYYY-MM-DD)"),
    expr: str = typer.Option("df", "--expr", "-x", help="Python expression to evaluate (df, pd, np available)"),
    format: str = typer.Option("table", "--format", "-f", help="Output format: table, csv, json"),
    output: Optional[str] = typer.Option(None, "--output", "-o", help="Output file path"),
    token: Optional[str] = typer.Option(None, "--token", "-t", help="ESIOS API key"),
):
    """Fetch indicator data and evaluate a Python expression on it.

    The fetched data is available as `df` (pandas DataFrame).
    `pd` (pandas) and `np` (numpy) are also available.

    \b
    Examples:
        esios exec 600 -s 2025-01-01 -e 2025-01-31 -x "df.describe()"
        esios exec 600 -s 2025-01-01 -e 2025-01-31 -x "df.resample('D').mean()"
        esios exec 600 10034 -s 2025-01-01 -e 2025-01-31 -x "df.corr()"
    """
    import numpy as np
    import pandas as pd

    from esios.cli.app import get_client

    client = get_client(token)

    # Fetch data
    if len(indicator_ids) == 1:
        handle = client.indicators.get(indicator_ids[0])
        df = handle.historical(start, end)
    else:
        df = client.indicators.compare(indicator_ids, start, end)

    if df.empty:
        typer.echo("No data returned.")
        raise typer.Exit(0)

    # Evaluate expression
    namespace = {"df": df, "pd": pd, "np": np}
    try:
        result = eval(expr, {"__builtins__": {}}, namespace)  # noqa: S307
    except Exception as exc:
        typer.echo(f"Error evaluating expression: {exc}", err=True)
        raise typer.Exit(1)

    # Output result
    _render(result, format, output)


def _render(result, format: str, output: str | None) -> None:
    """Render the eval result in the requested format."""
    import pandas as pd
    from rich.table import Table

    # Convert Series to DataFrame for consistent handling
    if isinstance(result, pd.Series):
        result = result.to_frame()

    if isinstance(result, pd.DataFrame):
        if format == "csv":
            text = result.to_csv()
        elif format == "json":
            text = result.to_json(orient="records", indent=2, date_format="iso")
        else:
            # Rich table — always show index
            table = Table()
            idx_name = result.index.name or ""
            table.add_column(str(idx_name), style="cyan")
            for col in result.columns:
                table.add_column(str(col))

            for idx, row in result.head(100).iterrows():
                values = [str(idx)]
                values += [_fmt(row[c]) for c in result.columns]
                table.add_row(*values)

            if len(result) > 100:
                table.caption = f"Showing 100 of {len(result)} rows"
            console.print(table)
            return

        if output:
            with open(output, "w") as f:
                f.write(text)
            typer.echo(f"Written to {output}")
        else:
            typer.echo(text)
    else:
        # Scalar or string result
        typer.echo(result)


def _fmt(val) -> str:
    """Format a value for table display."""
    if isinstance(val, float):
        return f"{val:.4f}"
    return str(val)
