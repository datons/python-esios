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

    @property
    def geos(self) -> list[dict[str, Any]]:
        """Available geographies for this indicator.

        Returns a list of ``{"geo_id": int, "geo_name": str}`` dicts.
        """
        return self.metadata.get("geos", [])

    def _build_geo_map(self) -> dict[str, str]:
        """Map geo_id (as string column name) → geo_name."""
        return {str(g["geo_id"]): g["geo_name"] for g in self.geos}

    def _enrich_geo_map(self, values: list[dict]) -> None:
        """Learn geo_id → geo_name mappings from API response values.

        The indicator metadata may not list all geos (e.g. 600 omits
        Países Bajos). This enriches the metadata from actual data.
        """
        known_ids = {g["geo_id"] for g in self.geos}
        for v in values:
            gid = v.get("geo_id")
            gname = v.get("geo_name")
            if gid is not None and gname and gid not in known_ids:
                self.metadata.setdefault("geos", []).append(
                    {"geo_id": gid, "geo_name": gname}
                )
                known_ids.add(gid)

    def geos_dataframe(self) -> pd.DataFrame:
        """Available geographies as a DataFrame with geo_id and geo_name columns."""
        geos = self.geos
        if not geos:
            return pd.DataFrame(columns=["geo_id", "geo_name"])
        return pd.DataFrame(geos)

    def resolve_geo(self, ref: str | int) -> int:
        """Resolve a geo reference (ID or name) to a geo_id.

        Accepts:
        - An integer geo_id (returned as-is)
        - A string that is a valid integer (parsed and returned)
        - A geo_name string (case-insensitive substring match)

        Raises ValueError if the name doesn't match any known geo.
        """
        if isinstance(ref, int):
            return ref
        try:
            return int(ref)
        except (ValueError, TypeError):
            pass

        ref_lower = ref.lower()
        for geo in self.geos:
            if ref_lower in geo.get("geo_name", "").lower():
                return geo["geo_id"]

        available = [g.get("geo_name", str(g["geo_id"])) for g in self.geos]
        raise ValueError(
            f"No geo matching {ref!r} for indicator {self.id}. "
            f"Available: {', '.join(available)}"
        )

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
    ) -> pd.DataFrame:
        """Fetch historical values as a DataFrame with DatetimeIndex.

        Uses local parquet cache when enabled. Only fetches missing date ranges
        from the API. Automatically chunks requests exceeding ~3 weeks.

        When multiple geo_ids are present (e.g. indicator 600 returns data for
        several countries), the result is pivoted so each geo becomes a column
        named by its geo_name. Use *geo_ids* to filter to specific geos.
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

        # Column names in cache are geo_names — determine which to check
        geo_map = self._build_geo_map()  # str(geo_id) → geo_name
        if geo_ids:
            cache_cols = [geo_map.get(str(g), str(g)) for g in geo_ids]
        elif self.geos:
            # No filter: expect ALL known geos to have data
            cache_cols = [g["geo_name"] for g in self.geos]
        else:
            cache_cols = None

        # -- Cache-aware fetch -------------------------------------------------
        cache = self._cache
        use_cache = cache.config.enabled and not time_agg and not geo_agg

        # For read: only filter columns when specific geos requested
        read_cols = cache_cols if geo_ids else None

        if use_cache:
            cached_df = cache.read(
                "indicators", self.id, start_date, end_date, columns=read_cols,
            )
            gaps = cache.find_gaps(
                cached_df, start_date, end_date, columns=cache_cols,
            )

            if not gaps:
                logger.debug("Cache hit for indicator %d [%s → %s]", self.id, start, end)
                return self._finalize(cached_df)

            logger.debug(
                "Cache partial — fetching %d gap(s) for indicator %d",
                len(gaps), self.id,
            )
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

        # Learn any new geo mappings from the response
        self._enrich_geo_map(all_values)

        # Convert API response to wide format for cache
        new_wide = self._to_wide(all_values)

        # -- Persist to cache --------------------------------------------------
        if use_cache and not new_wide.empty:
            cache.write("indicators", self.id, new_wide)

        # Combine cached + new
        if use_cache and not cached_df.empty and not new_wide.empty:
            result = new_wide.combine_first(cached_df).sort_index()
            result = result[start:end]
        elif not new_wide.empty:
            result = new_wide
        elif use_cache:
            result = cached_df
        else:
            result = pd.DataFrame()

        # Select only requested geo columns
        if read_cols and not result.empty:
            existing = [c for c in read_cols if c in result.columns]
            if existing:
                result = result[existing]

        return self._finalize(result)

    def _to_wide(self, values: list[dict]) -> pd.DataFrame:
        """Convert raw API value dicts to wide-format DataFrame.

        Columns are geo_names (e.g. "España", "Portugal"). For single-geo
        or no-geo indicators, the column is "value".
        """
        if not values:
            return pd.DataFrame()

        df = to_dataframe(values, timezone=TIMEZONE, pivot_geo=False)

        if "geo_id" in df.columns and df["geo_id"].nunique() >= 1:
            # Use geo_name as column label, fall back to str(geo_id)
            col_key = "geo_name" if "geo_name" in df.columns else "geo_id"

            if df.index.name == "datetime":
                df = df.reset_index()
            pivot = df.pivot_table(
                index="datetime",
                columns=col_key,
                values="value",
                aggfunc="first",
            )
            pivot.columns = [str(c) for c in pivot.columns]
            pivot.columns.name = None
            pivot.index.name = "datetime"
            return pivot.sort_index()

        # No geo column — drop geo cols if present, keep "value"
        geo_drop = [c for c in df.columns if c in ("geo_id", "geo_name")]
        df = df.drop(columns=geo_drop, errors="ignore")
        return df

    def _finalize(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepare DataFrame for user-facing output.

        Single-column DataFrames get the indicator ID as column name.
        Multi-column DataFrames already have geo_name labels from _to_wide().
        """
        if df.empty:
            return df

        if len(df.columns) == 1:
            col = df.columns[0]
            if col == "value":
                df = df.rename(columns={"value": str(self.id)})
            # For single-geo selection, keep the geo_name as column name
        # Multi-geo: columns are already geo_names from _to_wide()

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
            col = df.columns[0] if len(df.columns) == 1 else None
            if col:
                frames[handle.name or str(iid)] = df[col]
            else:
                # Multi-geo: take first column or all
                frames[handle.name or str(iid)] = df.iloc[:, 0]

        if not frames:
            return pd.DataFrame()

        return pd.DataFrame(frames)
