"""Shared DataFrame utilities for ESIOS data."""

from __future__ import annotations

import pandas as pd

from esios.constants import TIMEZONE


def to_dataframe(
    data: list[dict],
    *,
    timezone: str = TIMEZONE,
) -> pd.DataFrame:
    """Convert a list of ESIOS value dicts to a DataFrame.

    - Parses the ``datetime`` field to a DatetimeIndex.
    - Converts from UTC to the target timezone.
    - Drops time-related columns that clutter the output.
    """
    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)

    if "datetime" in df.columns:
        df["datetime"] = pd.to_datetime(df["datetime"], utc=True)
        df = df.set_index("datetime")
        df.index = df.index.tz_convert(timezone)

    # Drop auxiliary time columns
    time_cols = [c for c in df.columns if "time" in c.lower() and c != "datetime"]
    df = df.drop(columns=time_cols, errors="ignore")

    return df


def convert_timezone(
    df: pd.DataFrame,
    target_tz: str = TIMEZONE,
) -> pd.DataFrame:
    """Convert a DataFrame's DatetimeIndex to another timezone."""
    if isinstance(df.index, pd.DatetimeIndex):
        if df.index.tz is None:
            df.index = df.index.tz_localize("UTC")
        df.index = df.index.tz_convert(target_tz)
    return df
