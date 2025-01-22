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
        
        self.sheets = {name: I90Sheet(name, self.workbook, self.path, self.metadata) for name in self.workbook.sheet_names}

    def _extract_metadata_and_toc(self):
        # Read the workbook to extract metadata and table of contents from the first sheet
        self._read_excel()
        first_sheet = self.sheets[next(iter(self.sheets))]  # Get the first sheet
        first_sheet_rows = first_sheet.rows

        # Extract dates
        self.metadata["date_data"] = pd.to_datetime(first_sheet_rows[3][0])  # Row 4 (0-indexed), Column A
        self.metadata["date_publication"] = pd.to_datetime(first_sheet_rows[3][2])  # Row 4, Column C

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




    

class I90Sheet:
    def __init__(self, sheet_name: str, workbook, path: Path, metadata: dict):
        self.sheet_name = sheet_name
        self.workbook = workbook
        self.path = path
        self.metadata = metadata
        self.sheet = workbook.get_sheet_by_name(sheet_name)
        self.df = None  # Initialize to None for lazy preprocessing
        self.frequency = None
        self.rows = self._get_rows()

    def __repr__(self):
        return f"<I90Sheet {self.sheet_name}>"


    def _normalize_datetime_columns(self, columns):
        if any(pd.isna(columns)):
            self._n_columns_totals = 3
        else:
            self._n_columns_totals = 2
            
        columns = pd.Series(columns, dtype=str).ffill()
        columns = columns.str.split('-').str[0]
        
        return columns.astype(float).astype(int)
    def _get_rows(self):
        rows = self.sheet.to_python()
        rows = np.array(rows)
        rows[rows == ''] = np.nan
        return rows

    def _preprocess_double_index(self, idx, columns):
        
        idx_prior = idx - 1
        
        columns_prior = self.rows[idx_prior]
        idx_column_start = get_idx_column_start(columns_prior)

        if idx_column_start == -1:
            return pd.DataFrame()

        columns_date = columns_prior[idx_column_start:]
        columns_variable = columns[idx_column_start:]
        
        columns_date = self._normalize_datetime_columns(columns_date)
        
        columns_index = columns[: idx_column_start - self._n_columns_totals]
        
        return columns, columns_index, columns_date, columns_variable
    
    
    def _preprocess_single_index(self, idx_column_start, columns):
        
        columns_date = columns[idx_column_start:]
        columns_date = self._normalize_datetime_columns(columns_date)
        
        columns_index = columns[:idx_column_start - self._n_columns_totals]
        
        return columns, columns_index, columns_date, None
    
    def _preprocess(self):
        if len(self.rows) <= 1:
            return pd.DataFrame()
        
        try:
            len_rows = np.array([sum([not pd.isna(cell) for cell in row]) for row in self.rows[:4]])  # Check for non-missing values
            idx = len_rows.argmax()
            columns = self.rows[idx]

            idx_column_data_start = get_idx_column_start(columns)

            if idx_column_data_start == -1:
                columns, columns_index, columns_date, columns_variable = self._preprocess_double_index(idx, columns)
            else:
                columns, columns_index, columns_date, columns_variable = self._preprocess_single_index(idx_column_data_start, columns)
            
            if any_value_greater_than_30(columns_date):
                self.frequency = "hourly-quarterly"
                time_deltas = (columns_date - 1) * 15  # Compute time deltas in minutes
            else:
                self.frequency = "hourly"
                time_deltas = columns_date * 60  # Compute time deltas in minutes for hourly

            # Vectorized datetime computation
            columns_datetime = pd.to_datetime(self.metadata["date_data"]) + pd.to_timedelta(time_deltas, unit='m')
            columns_datetime = pd.DatetimeIndex(columns_datetime).tz_localize('Europe/Madrid', ambiguous='infer').tz_convert('UTC')
            
            data = pd.DataFrame(self.rows[idx + 1:], columns=columns)
            
            if columns_variable is not None:
                columns_data = pd.MultiIndex.from_arrays([columns_datetime, columns_variable], names=["datetime", "variable"])
            else:
                columns_data = columns_datetime
                columns_data.name = "datetime"
            
            data = data.set_index(columns_index.tolist()).iloc[:, self._n_columns_totals:]
            data.columns = columns_data
            
            data = data.stack(level="datetime", future_stack=True).astype(float)
            
            if isinstance(data, pd.DataFrame):
                return data
            else:
                return data.to_frame(name="value")
            
        except Exception as e:
            
            print(f"Error in _preprocess: {e}")
            return pd.DataFrame()
            
    

