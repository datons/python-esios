"""I90 file processing — parse I90DIA XLS files into DataFrames.

Migrated from esios/archives/preprocessing.py with added type hints
and improved error handling.
"""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd
import python_calamine

logger = logging.getLogger("esios")


def _get_idx_column_start(columns: np.ndarray) -> int:
    """Find the first column whose value starts with a digit (time columns)."""
    for i, col in enumerate(columns):
        if col and str(col)[0].isdigit():
            return i
    return -1


def _any_value_greater_than_30(series: np.ndarray) -> bool:
    """Check if any numeric value exceeds 30 (quarter-hourly indicator)."""
    return any(v > 30 for v in series if isinstance(v, (int, float)) and not np.isnan(v))


class I90Book:
    """Represents an I90DIA workbook (XLS) with lazy sheet preprocessing.

    Usage::

        book = I90Book("I90DIA_20250101.xls")
        sheet = book["3.1"]
        df = sheet.df
    """

    def __init__(self, path: Path | str):
        self.path = Path(path)
        self.metadata: dict = {}
        self.table_of_contents: dict[str, str] = {}
        self.sheets: dict[str, I90Sheet] = {}
        self._extract_metadata_and_toc()

    def _read_excel(self) -> None:
        with open(self.path, "rb") as f:
            self.workbook = python_calamine.CalamineWorkbook.from_filelike(f)
        self.sheets = {
            name: I90Sheet(name, self.workbook, self.path, self.metadata)
            for name in self.workbook.sheet_names
        }

    def _extract_metadata_and_toc(self) -> None:
        """Extract dates and table of contents from the first sheet."""
        self._read_excel()
        first_sheet = self.sheets[next(iter(self.sheets))]
        rows = first_sheet.rows

        # Dates from row 4
        self.metadata["date_data"] = pd.to_datetime(rows[3][0])
        self.metadata["date_publication"] = pd.to_datetime(rows[3][2])

        # Table of contents from row 10 onwards
        df = pd.read_excel(self.path, sheet_name=0, header=None, skiprows=9, usecols="A,B", engine="calamine")
        df.columns = ["sheet_name", "description"]
        self.table_of_contents = df.set_index("sheet_name")["description"].to_dict()

    def get_sheet(self, sheet_name: str) -> I90Sheet:
        """Get and preprocess a specific sheet by name."""
        if sheet_name not in self.sheets:
            self._read_excel()
        sheet = self.sheets.get(sheet_name)
        if sheet is None:
            raise KeyError(f"Sheet '{sheet_name}' not found in {self.path.name}")
        if sheet.df is None:
            sheet.df = sheet._preprocess()
        return sheet

    def __getitem__(self, sheet_name: str) -> I90Sheet:
        return self.get_sheet(sheet_name)

    def __repr__(self) -> str:
        return f"<I90Book {self.path.name} sheets={len(self.sheets)}>"


class I90Sheet:
    """A single sheet within an I90 workbook.

    Preprocessing detects hourly vs quarter-hourly frequency and produces
    a DataFrame with a UTC DatetimeIndex.
    """

    def __init__(
        self,
        sheet_name: str,
        workbook: python_calamine.CalamineWorkbook,
        path: Path,
        metadata: dict,
    ):
        self.sheet_name = sheet_name
        self.workbook = workbook
        self.path = path
        self.metadata = metadata
        self.sheet = workbook.get_sheet_by_name(sheet_name)
        self.df: pd.DataFrame | None = None
        self.frequency: str | None = None
        self.rows = self._get_rows()
        self._n_columns_totals: int = 2

    def __repr__(self) -> str:
        return f"<I90Sheet {self.sheet_name}>"

    def _get_rows(self) -> np.ndarray:
        rows = self.sheet.to_python()
        arr = np.array(rows, dtype=object)
        arr[arr == ""] = np.nan
        return arr

    def _normalize_datetime_columns(self, columns: np.ndarray) -> np.ndarray:
        """Normalize time column headers to integer period indices."""
        if any(pd.isna(columns)):
            self._n_columns_totals = 3
        else:
            self._n_columns_totals = 2

        series = pd.Series(columns, dtype=str).ffill()
        series = series.str.split("-").str[0]
        return series.astype(float).astype(int).values

    def _preprocess_double_index(
        self, idx: int, columns: np.ndarray
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray] | pd.DataFrame:
        idx_prior = idx - 1
        columns_prior = self.rows[idx_prior]
        idx_col_start = _get_idx_column_start(columns_prior)

        if idx_col_start == -1:
            return pd.DataFrame()

        columns_date = self._normalize_datetime_columns(columns_prior[idx_col_start:])
        columns_variable = columns[idx_col_start:]
        columns_index = columns[: idx_col_start - self._n_columns_totals]

        return columns, columns_index, columns_date, columns_variable

    def _preprocess_single_index(
        self, idx_col_start: int, columns: np.ndarray
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray, None]:
        columns_date = self._normalize_datetime_columns(columns[idx_col_start:])
        columns_index = columns[: idx_col_start - self._n_columns_totals]
        return columns, columns_index, columns_date, None

    def _preprocess(self) -> pd.DataFrame:
        """Parse the sheet into a tidy DataFrame with a datetime index."""
        if len(self.rows) <= 1:
            return pd.DataFrame()

        try:
            # Find the header row (most non-null cells in first 4 rows)
            len_rows = np.array([
                sum(not pd.isna(cell) for cell in row) for row in self.rows[:4]
            ])
            idx = int(len_rows.argmax())
            columns = self.rows[idx]

            idx_col_start = _get_idx_column_start(columns)

            if idx_col_start == -1:
                result = self._preprocess_double_index(idx, columns)
                if isinstance(result, pd.DataFrame):
                    return result
                columns, columns_index, columns_date, columns_variable = result
            else:
                columns, columns_index, columns_date, columns_variable = (
                    self._preprocess_single_index(idx_col_start, columns)
                )

            # Detect frequency
            if _any_value_greater_than_30(columns_date):
                self.frequency = "hourly-quarterly"
                time_deltas = (columns_date - 1) * 15  # minutes
            else:
                self.frequency = "hourly"
                time_deltas = columns_date * 60  # minutes

            # Build datetime index
            base_date = pd.to_datetime(self.metadata["date_data"])
            columns_datetime = base_date + pd.to_timedelta(time_deltas, unit="m")
            columns_datetime = pd.DatetimeIndex(columns_datetime).tz_localize(
                "Europe/Madrid", ambiguous="infer"
            ).tz_convert("UTC")

            data = pd.DataFrame(self.rows[idx + 1 :], columns=columns)

            if columns_variable is not None:
                columns_data = pd.MultiIndex.from_arrays(
                    [columns_datetime, columns_variable],
                    names=["datetime", "variable"],
                )
            else:
                columns_data = columns_datetime
                columns_data.name = "datetime"

            data = data.set_index(columns_index.tolist()).iloc[:, self._n_columns_totals :]
            data.columns = columns_data
            data = data.stack(level="datetime", future_stack=True).astype(float)

            if isinstance(data, pd.DataFrame):
                return data
            return data.to_frame(name="value")

        except Exception as e:
            logger.error("Error preprocessing sheet %s: %s", self.sheet_name, e)
            return pd.DataFrame()
