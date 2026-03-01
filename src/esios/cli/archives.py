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


def _download_files(
    archive_id: int,
    token: str | None,
    date: str | None,
    start: str | None,
    end: str | None,
) -> list:
    """Download archive files (shared by sheets and exec commands)."""
    from esios.cli.app import get_client

    if not date and not (start and end):
        typer.echo("Provide --date or both --start and --end.", err=True)
        raise typer.Exit(1)

    client = get_client(token)

    if date:
        files = client.archives.download(archive_id, date=date)
    else:
        files = client.archives.download(archive_id, start=start, end=end)

    if not files:
        typer.echo("No files found for the given date range.", err=True)
        raise typer.Exit(1)

    return files


@archives_app.command("sheets")
def sheets(
    archive_id: int = typer.Argument(..., help="Archive ID (e.g. 34 for I90DIA)"),
    date: Optional[str] = typer.Option(None, "--date", "-d", help="Single date (YYYY-MM-DD)"),
    start: Optional[str] = typer.Option(None, "--start", "-s", help="Start date (YYYY-MM-DD)"),
    end: Optional[str] = typer.Option(None, "--end", "-e", help="End date (YYYY-MM-DD)"),
    token: Optional[str] = typer.Option(None, "--token", "-t"),
):
    """List sheets (table of contents) in an archive file.

    Downloads and parses the first file to show available sheets.

    \b
    Examples:
        esios archives sheets 34 --date 2025-06-01
        esios archives sheets 34 --start 2025-06-01 --end 2025-06-01
    """
    from esios.processing.i90 import I90Book

    files = _download_files(archive_id, token, date, start, end)

    try:
        book = I90Book(files[0])
    except Exception as exc:
        typer.echo(f"Error parsing {files[0].name}: {exc}", err=True)
        raise typer.Exit(1)

    table = Table(title=f"Sheets in {files[0].name}")
    table.add_column("Sheet", style="cyan")
    table.add_column("Description")

    for sheet_name, description in book.table_of_contents.items():
        if not isinstance(sheet_name, str) or not sheet_name.strip():
            continue
        table.add_row(str(sheet_name), str(description))

    console.print(table)


@archives_app.command("exec")
def exec_archive(
    archive_id: int = typer.Argument(..., help="Archive ID (e.g. 34 for I90DIA)"),
    sheet: str = typer.Option(..., "--sheet", help="Sheet name (e.g. I90DIA03, I90DIA26)"),
    date: Optional[str] = typer.Option(None, "--date", "-d", help="Single date (YYYY-MM-DD)"),
    start: Optional[str] = typer.Option(None, "--start", "-s", help="Start date (YYYY-MM-DD)"),
    end: Optional[str] = typer.Option(None, "--end", "-e", help="End date (YYYY-MM-DD)"),
    expr: str = typer.Option("df", "--expr", "-x", help="Python expression to evaluate (df, pd, np available)"),
    format: str = typer.Option("table", "--format", "-f", help="Output format: table, csv, json"),
    output: Optional[str] = typer.Option(None, "--output", "-o", help="Output file path"),
    token: Optional[str] = typer.Option(None, "--token", "-t"),
):
    """Parse archive files and evaluate a Python expression on the data.

    Downloads archive files (cached), parses the specified sheet from each
    file using I90Book, concatenates the DataFrames, and evaluates the
    expression. The parsed data is available as `df` (pandas DataFrame).
    `pd` (pandas) and `np` (numpy) are also available.

    \b
    Examples:
        # Show curtailment data for a single day
        esios archives exec 34 --sheet I90DIA03 --date 2025-06-01

        # Curtailment by technology over a month
        esios archives exec 34 --sheet I90DIA03 -s 2025-05-05 -e 2025-06-08 \\
          -x "df[df['Sentido']=='Bajar'].groupby('Tecnología')['value'].sum().sort_values()"

        # Export PBF generation program to CSV
        esios archives exec 34 --sheet I90DIA26 --date 2025-06-01 -f csv -o pbf.csv

        # Descriptive statistics
        esios archives exec 34 --sheet I90DIA03 --date 2025-06-01 -x "df.describe()"
    """
    import numpy as np
    import pandas as pd

    from esios.processing.i90 import I90Book

    files = _download_files(archive_id, token, date, start, end)

    # Parse all files and extract the requested sheet
    all_dfs = []
    for f in files:
        try:
            book = I90Book(f)
            s = book[sheet]
            if s.df is not None and not s.df.empty:
                all_dfs.append(s.df.reset_index())
        except KeyError:
            # Sheet not found — show available sheets
            try:
                book = I90Book(f)
                available = list(book.table_of_contents.keys())
            except Exception:
                available = []
            typer.echo(f"Sheet '{sheet}' not found in {f.name}.", err=True)
            if available:
                typer.echo(f"Available sheets: {', '.join(str(s) for s in available)}", err=True)
            raise typer.Exit(1)
        except Exception as exc:
            typer.echo(f"Warning: skipping {f.name}: {exc}", err=True)
            continue

    if not all_dfs:
        typer.echo("No data extracted from the specified sheet.", err=True)
        raise typer.Exit(1)

    df = pd.concat(all_dfs, ignore_index=True)

    # Evaluate expression
    namespace = {"df": df, "pd": pd, "np": np}
    try:
        result = eval(expr, {"__builtins__": {}}, namespace)  # noqa: S307
    except Exception as exc:
        typer.echo(f"Error evaluating expression: {exc}", err=True)
        raise typer.Exit(1)

    # Render output — reuse _render from exec_cmd
    from esios.cli.exec_cmd import _render

    _render(result, format, output)
