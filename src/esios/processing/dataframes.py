"""Shared DataFrame utilities for ESIOS data."""

from __future__ import annotations

import pandas as pd

from esios.constants import TIMEZONE


def to_dataframe(
    data: list[dict],
    *,
    timezone: str = TIMEZONE,
    pivot_geo: bool = True,
) -> pd.DataFrame:
    """Convert a list of ESIOS value dicts to a DataFrame.

    - Parses the ``datetime`` field to a DatetimeIndex.
    - Converts from UTC to the target timezone.
    - When multiple geo_ids are present and *pivot_geo* is True,
      pivots so each geo becomes a column (wide format).
    - Drops time-related columns that clutter the output.
    """
    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)

    if "datetime" in df.columns:
        df["datetime"] = pd.to_datetime(df["datetime"], utc=True)
        df["datetime"] = df["datetime"].dt.tz_convert(timezone)

    # Drop auxiliary time columns
    drop_cols = [c for c in df.columns if "time" in c.lower() and c != "datetime"]
    df = df.drop(columns=drop_cols, errors="ignore")

    # -- Multi-geo pivot -------------------------------------------------------
    has_geo = "geo_id" in df.columns
    n_geos = df["geo_id"].nunique() if has_geo else 0

    if has_geo and n_geos > 1 and pivot_geo and "value" in df.columns:
        # Use geo_name as column label, fall back to geo_id
        col_key = "geo_name" if "geo_name" in df.columns else "geo_id"
        pivot = df.pivot_table(
            index="datetime",
            columns=col_key,
            values="value",
            aggfunc="first",
        )
        pivot.columns.name = None
        pivot.index.name = "datetime"
        pivot = pivot.sort_index()
        return pivot

    # When pivot_geo=False, keep geo columns intact (used for cache storage)
    if not pivot_geo:
        if "datetime" in df.columns:
            df = df.set_index("datetime")
        return df

    # Single-geo or no-geo: drop geo columns, set datetime index
    geo_drop = [c for c in df.columns if c in ("geo_id", "geo_name")]
    df = df.drop(columns=geo_drop, errors="ignore")

    if "datetime" in df.columns:
        df = df.set_index("datetime")

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
