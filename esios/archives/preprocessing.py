import python_calamine
import pandas as pd
import numpy as np
from pathlib import Path

class I90Book:
    def __init__(self, path: Path | str):
        self.path = path if isinstance(path, Path) else Path(path)
        self.metadata = {}
        self.table_of_contents = {}
        self.sheets = {}
        self._extract_metadata_and_toc()

    def _read_excel(self):
        with open(self.path, "rb") as f:
            self.workbook = python_calamine.CalamineWorkbook.from_filelike(f)
        self.sheets = {name: I90Sheet(name, self.workbook, self.path) for name in self.workbook.sheet_names}

    def _extract_metadata_and_toc(self):
        # Read the workbook to extract metadata and table of contents from the first sheet
        self._read_excel()
        first_sheet = self.sheets[next(iter(self.sheets))]  # Get the first sheet
        first_sheet_rows = first_sheet.rows

        # Extract dates
        self.metadata["date_of_data"] = first_sheet_rows[3][0]  # Row 4 (0-indexed), Column A
        self.metadata["date_of_publication"] = first_sheet_rows[3][2]  # Row 4, Column C

        # Extract table of contents (columns A and B starting from row 10)
        df = pd.read_excel(self.path, sheet_name=0, header=None, skiprows=9, usecols="A,B")
        df.columns = ["sheet_name", "description"]
        self.table_of_contents = df.set_index("sheet_name")["description"].to_dict()

    def get_sheet(self, sheet_name: str):
        if sheet_name not in self.sheets:
            self._read_excel()
        sheet = self.sheets.get(sheet_name)
        if sheet and sheet.df is None:  # Preprocess if not already done
            sheet.df = sheet._preprocess()
        return sheet

    def __getitem__(self, sheet_name: str):
        return self.get_sheet(sheet_name)


def get_idx_column_start(columns):
    return next((i for i, column in enumerate(columns) if column and str(column)[0].isdigit()), -1)

def any_value_greater_than_30(series):
    """
    Check if any value in the series is greater than 30.

    Parameters:
    series (pd.Series): A pandas Series of values.

    Returns:
    bool: True if any value is greater than 30, False otherwise.
    """
    return any(value > 30 for value in series)


import numpy as np
import pandas as pd

def normalize_datetime_columns(columns):
    columns = np.array(columns, dtype=str)  # Ensure all elements are strings
    if np.any(['-' in col for col in columns]):  # Check if any element contains '-'
        mask_str = np.char.find(columns, '-') > -1
        split_values = [col.split('-')[0] for col in columns[mask_str]]
        columns[mask_str] = split_values
    return pd.Series(columns, dtype=float).astype(int)
    

class I90Sheet:
    def __init__(self, sheet_name: str, workbook, path: Path):
        self.sheet_name = sheet_name
        self.workbook = workbook
        self.path = path
        self.metadata = {}
        self.sheet = workbook.get_sheet_by_name(sheet_name)
        self.rows = self._get_rows()
        self.df = None  # Initialize to None for lazy preprocessing
        self.frequency = None

    def __repr__(self):
        return f"<I90Sheet {self.sheet_name}>"

    def _get_rows(self):
        return self.sheet.to_python()

    def _preprocess(self):
        if len(self.rows) <= 1:
            return pd.DataFrame()

        len_rows = np.array([sum([cell != "" for cell in row]) for row in self.rows[:4]])
        idx = len_rows.argmax()
        columns = self.rows[idx]

        idx_column_data_start = get_idx_column_start(columns)

        date = self.path.stem.split("_")[-1]
        
        if idx_column_data_start == -1:
            return self._preprocess_double_index(idx, columns, date)
        else:
            return self._preprocess_single_index(idx, idx_column_data_start, columns, date)

    def _preprocess_double_index(self, idx, columns, date):
        
        n_columns_totals = 3
        idx_prior = idx - 1
        
        columns_prior = self.rows[idx_prior]
        idx_column_start = get_idx_column_start(columns_prior)

        if idx_column_start == -1:
            return pd.DataFrame()

        columns_index = columns[: idx_column_start - n_columns_totals]
        df_data = (
            pd.DataFrame(self.rows[idx + 1:], columns=columns)
            .set_index(columns_index)
            .iloc[:, n_columns_totals:]
        )
        
        df_columns = pd.DataFrame(
            [
                columns_prior[idx_column_start:],
                columns[idx_column_start:],
            ]
        ).replace("", np.nan).ffill(axis=1)
        
        s = normalize_datetime_columns(df_columns.loc[0, :])

        # Precompute the base datetime
        base_datetime = pd.to_datetime(date)
        
        # Use NumPy to compute the time deltas
        if any_value_greater_than_30(s):
            self.frequency = "hourly-quarterly"
            time_deltas = (s - 1) * 15  # Compute time deltas in minutes
        else:
            self.frequency = "hourly"
            time_deltas = s * 60  # Compute time deltas in minutes for hourly

        # Vectorized datetime computation
        datetime = base_datetime + pd.to_timedelta(time_deltas, unit='m')
        
        df_columns.loc[0, :] = datetime
        columns = pd.MultiIndex.from_arrays(df_columns.values, names=["datetime", "variable"])

        df_data.columns = columns
        df_data = df_data.replace("", np.nan)
        df_data = df_data.stack(level="datetime", future_stack=True).astype(float)

        return df_data

    def _preprocess_single_index(self, idx, idx_column_start, columns, date):
        n_columns_totals = 2
        
        columns_index = columns[:idx_column_start - n_columns_totals]
        df_data = pd.DataFrame(self.rows[idx + 1:], columns=columns)
        
        df_data = df_data.set_index(columns_index)
        df_data = df_data.iloc[:, n_columns_totals:]
        
        columns_data = columns[idx_column_start:]
        s = normalize_datetime_columns(columns_data)

        # Precompute the base datetime
        base_datetime = pd.to_datetime(date)
        
        # Use NumPy to compute the time deltas
        if any_value_greater_than_30(s):
            self.frequency = "hourly-quarterly"
            time_deltas = (s - 1) * 15  # Compute time deltas in minutes
        else:
            self.frequency = "hourly"
            time_deltas = s * 60  # Compute time deltas in minutes for hourly

        # Vectorized datetime computation
        datetime = base_datetime + pd.to_timedelta(time_deltas, unit='m')
        
        columns = pd.to_datetime(datetime)
        columns.name = "datetime"
        
        df_data = df_data.replace("", np.nan)
        df_data.columns = columns
        df_data = df_data.stack(level="datetime", future_stack=True).to_frame(name="value").astype(float)
        
        return df_data