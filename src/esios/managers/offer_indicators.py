"""Offer indicator manager — list, get, and historical data retrieval."""

from __future__ import annotations

import logging
from datetime import timedelta

import pandas as pd

from esios.cache import CacheStore
from esios.constants import TIMEZONE
from esios.managers.base import BaseManager
from esios.models.offer_indicator import OfferIndicator
from esios.processing.dataframes import to_dataframe

logger = logging.getLogger("esios")

# Offer indicators use shorter chunks (3 days) per the original code
OFFER_CHUNK_DAYS = 3


class OfferIndicatorHandle:
    """Bound handle to a single offer indicator."""

    def __init__(self, manager: OfferIndicatorsManager, indicator: OfferIndicator):
        self._manager = manager
        self.indicator = indicator
        self.id = indicator.id
        self.name = indicator.name
        self.metadata = indicator.raw

    def __repr__(self) -> str:
        return f"<OfferIndicatorHandle id={self.id} name={self.name!r}>"

    @property
    def _cache(self) -> CacheStore:
        return self._manager._client.cache

    def historical(
        self,
        start: str,
        end: str,
        *,
        locale: str = "es",
    ) -> pd.DataFrame:
        """Fetch historical values as a DataFrame, auto-chunked in 3-day windows.

        Uses local parquet cache when enabled.
        """
        start_date = pd.to_datetime(start)
        end_date = pd.to_datetime(end)
        chunk_delta = timedelta(days=OFFER_CHUNK_DAYS)

        cache = self._cache
        use_cache = cache.config.enabled

        if use_cache:
            cached_df = cache.read("offer_indicators", self.id, start_date, end_date)
            gaps = cache.find_gaps(cached_df, start_date, end_date)

            if not gaps:
                logger.debug("Cache hit for offer indicator %d", self.id)
                return cached_df

            logger.debug("Cache partial — fetching %d gap(s) for offer indicator %d", len(gaps), self.id)
        else:
            from esios.cache import DateRange
            gaps = [DateRange(start_date, end_date)]

        all_values: list[dict] = []
        for gap in gaps:
            current = gap.start
            while current <= gap.end:
                chunk_end = min(current + chunk_delta, gap.end)
                params = {
                    "start_date": current.strftime("%Y-%m-%d"),
                    "end_date": chunk_end.strftime("%Y-%m-%d"),
                    "locale": locale,
                }
                data = self._manager._get(f"offer_indicators/{self.id}", params=params)
                all_values.extend(data.get("indicator", {}).get("values", []))
                current = chunk_end + timedelta(days=1)

        new_df = to_dataframe(all_values, timezone=TIMEZONE) if all_values else pd.DataFrame()

        if use_cache and not new_df.empty:
            cache.write("offer_indicators", self.id, new_df)

        if use_cache and not cached_df.empty and not new_df.empty:
            result = pd.concat([cached_df, new_df])
            result = result[~result.index.duplicated(keep="last")].sort_index()
            return result[start:end]
        elif not new_df.empty:
            return new_df
        elif use_cache:
            return cached_df
        return pd.DataFrame()


class OfferIndicatorsManager(BaseManager):
    """Manager for ``/offer_indicators`` endpoints."""

    def list(self) -> pd.DataFrame:
        """List all available offer indicators as a DataFrame."""
        data = self._get("offer_indicators")
        indicators = data.get("indicators", [])
        for ind in indicators:
            ind["description"] = self._html_to_text(ind.get("description", ""))
        return pd.DataFrame(indicators)

    def get(self, indicator_id: int) -> OfferIndicatorHandle:
        """Get an offer indicator by ID."""
        data = self._get(f"offer_indicators/{indicator_id}")
        raw = data.get("indicator", {})
        # Remove 'values' from metadata to keep it lightweight
        raw_meta = {k: v for k, v in raw.items() if k != "values"}
        indicator = OfferIndicator.from_api(raw_meta)
        return OfferIndicatorHandle(self, indicator)
