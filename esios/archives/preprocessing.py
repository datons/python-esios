from pathlib import Path
import pandas as pd


def process_excel_file(path, sheet_name):
    """
    Process an Excel file and return a melted dataframe.
    """

    if not isinstance(path, Path):
        path = Path(path)

    sheet_data = pd.read_excel(path, sheet_name=sheet_name, skiprows=0, nrows=1)
    position = sheet_data.columns.get_loc("Indicadores")

    df = pd.read_excel(
        path, sheet_name=sheet_name, index_col=list(range(position)), skiprows=2
    ).iloc[:, 2:]

    date = path.stem.split("_")[1]

    if df.shape[1] > 30:
        date_ = pd.to_datetime(date).tz_localize("Europe/Madrid")
        date_range = pd.date_range(start=date_, periods=df.shape[1], freq="15min")
        df.columns = date_range
    else:
        hour = df.columns.str.split("-", expand=True).get_level_values(0)
        df.columns = pd.to_datetime(date + " " + hour).tz_localize("Europe/Madrid")

    df = df.melt(ignore_index=False, var_name="datetime").reset_index()

    return df
