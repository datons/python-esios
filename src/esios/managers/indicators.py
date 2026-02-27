"""Indicator manager — list, search, get, and historical data retrieval."""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any

import pandas as pd

from esios.cache import CacheStore
from esios.constants import CHUNK_SIZE_DAYS, TIMEZONE
from esios.managers.base import BaseManager
from esios.models.indicator import Indicator
from esios.processing.dataframes import to_dataframe

logger = logging.getLogger("esios")


class IndicatorHandle:
    """Bound handle to a single indicator, returned by ``IndicatorsManager.get()``."""

    def __init__(self, manager: IndicatorsManager, indicator: Indicator):
        self._manager = manager
        self.indicator = indicator
        self.id = indicator.id
        self.name = indicator.name
        self.metadata = indicator.raw

    def __repr__(self) -> str:
        return f"<IndicatorHandle id={self.id} name={self.name!r}>"

    @property
    def _cache(self) -> CacheStore:
        return self._manager._client.cache

    def historical(
        self,
        start: str,
        end: str,
        *,
        geo_ids: list[int] | None = None,
        locale: str = "es",
        time_agg: str | None = None,
        geo_agg: str | None = None,
        time_trunc: str | None = None,
        geo_trunc: str | None = None,
        column_name: str = "id",
    ) -> pd.DataFrame:
        """Fetch historical values as a DataFrame with DatetimeIndex.

        Uses local parquet cache when enabled. Only fetches missing date ranges
        from the API. Automatically chunks requests exceeding ~3 weeks.
        """
        base_params: dict[str, Any] = {
            "locale": locale,
        }
        if geo_ids:
            base_params["geo_ids[]"] = ",".join(map(str, geo_ids))
        for key, val in [
            ("time_agg", time_agg),
            ("geo_agg", geo_agg),
            ("time_trunc", time_trunc),
            ("geo_trunc", geo_trunc),
        ]:
            if val is not None:
                base_params[key] = val

        start_date = pd.to_datetime(start)
        end_date = pd.to_datetime(end)

        # Determine geo_id for cache key (None if multiple or unset)
        geo_id = geo_ids[0] if geo_ids and len(geo_ids) == 1 else None

        # -- Cache-aware fetch -------------------------------------------------
        cache = self._cache
        use_cache = cache.config.enabled and not time_agg and not geo_agg

        if use_cache:
            cached_df = cache.read("indicators", self.id, start_date, end_date, geo_id=geo_id)
            gaps = cache.find_gaps(cached_df, start_date, end_date)

            if not gaps:
                logger.debug("Cache hit for indicator %d [%s → %s]", self.id, start, end)
                df = cached_df
                if column_name in self.metadata and column_name != "value":
                    label = str(self.metadata[column_name])
                    df.rename(columns={"value": label}, inplace=True)
                return df

            logger.debug("Cache partial — fetching %d gap(s) for indicator %d", len(gaps), self.id)
        else:
            from esios.cache import DateRange
            gaps = [DateRange(start_date, end_date)]

        # -- Fetch missing ranges ----------------------------------------------
        all_values: list[dict] = []
        chunk_delta = timedelta(days=CHUNK_SIZE_DAYS)

        for gap in gaps:
            current = gap.start
            gap_end = gap.end
            while current <= gap_end:
                chunk_end = min(current + chunk_delta, gap_end)
                params = {
                    **base_params,
                    "start_date": current.strftime("%Y-%m-%d"),
                    "end_date": chunk_end.strftime("%Y-%m-%d") + "T23:59:59",
                }
                logger.debug("Fetch %s → %s", params["start_date"], params["end_date"])
                data = self._manager._get(f"indicators/{self.id}", params=params)
                all_values.extend(data.get("indicator", {}).get("values", []))
                current = chunk_end + timedelta(days=1)

        new_df = self._to_dataframe(all_values, column_name="value")

        # -- Merge with cache and persist --------------------------------------
        if use_cache and not new_df.empty:
            cache.write("indicators", self.id, new_df, geo_id=geo_id)

        # Combine cached + new (use string slicing to avoid tz-naive/aware mismatch)
        if use_cache and not cached_df.empty and not new_df.empty:
            result = pd.concat([cached_df, new_df])
            result = result[~result.index.duplicated(keep="last")].sort_index()
            result = result[start:end]
        elif not new_df.empty:
            result = new_df
        elif use_cache:
            result = cached_df
        else:
            result = pd.DataFrame()

        # Rename column
        if column_name in self.metadata and column_name != "value" and "value" in result.columns:
            label = str(self.metadata[column_name])
            result.rename(columns={"value": label}, inplace=True)

        return result

    def _to_dataframe(self, data: list[dict], column_name: str = "value") -> pd.DataFrame:
        """Convert raw value dicts to a DataFrame with Europe/Madrid DatetimeIndex."""
        if not data:
            return pd.DataFrame()

        df = to_dataframe(data, timezone=TIMEZONE)

        # Rename 'value' column using indicator metadata if requested
        if column_name in self.metadata and column_name != "value":
            label = str(self.metadata[column_name])
            df.rename(columns={"value": label}, inplace=True)

        return df


class IndicatorsManager(BaseManager):
    """Manager for ``/indicators`` endpoints."""

    def list(self) -> pd.DataFrame:
        """List all available indicators as a DataFrame."""
        data = self._get("indicators")
        indicators = data.get("indicators", [])
        for ind in indicators:
            ind["description"] = self._html_to_text(ind.get("description", ""))
        return pd.DataFrame(indicators)

    def search(self, query: str) -> pd.DataFrame:
        """Search indicators by name (case-insensitive substring match)."""
        df = self.list()
        if df.empty:
            return df
        mask = df["name"].str.contains(query, case=False, na=False)
        return df[mask].reset_index(drop=True)

    def get(self, indicator_id: int) -> IndicatorHandle:
        """Get an indicator by ID — returns a handle with ``.historical()``."""
        data = self._get(f"indicators/{indicator_id}")
        raw = data.get("indicator", {})
        indicator = Indicator.from_api(raw)
        return IndicatorHandle(self, indicator)

    def compare(
        self,
        indicator_ids: list[int],
        start: str,
        end: str,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """Fetch multiple indicators and merge into a single DataFrame.

        Each indicator's value column is named by its ID.
        """
        frames: dict[str, pd.DataFrame] = {}
        for iid in indicator_ids:
            handle = self.get(iid)
            df = handle.historical(start, end, **kwargs)
            # Use the 'value' column, rename to indicator name or ID
            col = "value" if "value" in df.columns else df.columns[0]
            frames[handle.name or str(iid)] = df[col]

        if not frames:
            return pd.DataFrame()

        return pd.DataFrame(frames)
