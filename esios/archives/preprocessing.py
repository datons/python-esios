from pathlib import Path
import pandas as pd
import python_calamine
from datetime import timedelta


def process_excel_file(file, sheet_name):
    if isinstance(file, str):
        file = Path(file)

    with open(file, "rb") as f:
        workbook = python_calamine.CalamineWorkbook.from_filelike(f)
        sheet = workbook.get_sheet_by_name(sheet_name)
        rows = sheet.to_python()

    if len(rows) <= 1:
        return pd.DataFrame()

    flag_index = False
    for i, row in enumerate(rows, 1):
        cell = row[0]
        if cell == "" and not flag_index:
            for j, cell in enumerate(row):
                if cell != "":
                    break
        elif cell != "":
            flag_index = True
            break

    row = [row.strip() if isinstance(row, str) else row for row in row]

    df = pd.DataFrame(rows[i:], columns=row).set_index(row[:j]).iloc[:, 2:]

    if row[j + 1] != "Total":
        i -= 2
        row_columns = rows[i][j + 2 :]
        df.columns = row_columns

    n_columns = df.shape[1]
    if n_columns > 30:
        df.columns = df.columns.astype(int)
    else:
        df.columns = df.columns.str.split("-").str[-1].astype(int)

    df.columns -= 1
    date = pd.to_datetime(file.stem.split("_")[1]).tz_localize("Europe/Madrid")

    if n_columns > 30:
        df.columns = [date + timedelta(minutes=15 * i) for i in df.columns]
    else:
        df.columns = [date + timedelta(minutes=60 * i) for i in df.columns]

    df = df.melt(ignore_index=False, var_name="datetime").reset_index()
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df.dropna(subset=["value"], inplace=True)

    return df
