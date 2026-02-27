"""Local parquet cache for ESIOS time-series data.

Caches indicator data as parquet files, fetching only missing date ranges
on subsequent requests. Historical electricity data is immutable once
published, so caching is safe and enabled by default.

Storage layout::

    {cache_dir}/
    ├── geos.json                      # Global geo_id → geo_name registry
    ├── indicators/
    │   ├── catalog.json               # Indicator list cache
    │   ├── 600/
    │   │   ├── data.parquet           # Wide-format time-series
    │   │   └── meta.json              # Per-indicator metadata
    │   └── 10034/
    │       ├── data.parquet
    │       └── meta.json
    └── archives/
        └── 1/
            └── I90DIA_20250101/

Each indicator is a single parquet file in wide format: the DatetimeIndex
holds timestamps, and columns are geo_names (e.g. "España", "Portugal").
NaN cells indicate that a particular geo has not been fetched for that
time range.

Gap detection is per-column: requesting ``--geo España`` checks whether
that column has data for the requested range, not whether any column does.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

logger = logging.getLogger("esios")

# Default cache location — respects XDG_CACHE_HOME
_DEFAULT_CACHE_DIR = Path(
    os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache")
) / "esios"

# Data older than this (hours) is considered final and won't be re-fetched
_DEFAULT_RECENT_TTL_HOURS = 48

# TTL for metadata caches
_DEFAULT_META_TTL_DAYS = 7
_DEFAULT_CATALOG_TTL_HOURS = 24


@dataclass
class CacheConfig:
    """Cache configuration."""

    enabled: bool = True
    cache_dir: Path = field(default_factory=lambda: _DEFAULT_CACHE_DIR)
    recent_ttl_hours: int = _DEFAULT_RECENT_TTL_HOURS
    meta_ttl_days: int = _DEFAULT_META_TTL_DAYS
    catalog_ttl_hours: int = _DEFAULT_CATALOG_TTL_HOURS

    def __post_init__(self) -> None:
        self.cache_dir = Path(self.cache_dir)


@dataclass(frozen=True)
class DateRange:
    """A contiguous date range [start, end] inclusive."""

    start: pd.Timestamp
    end: pd.Timestamp


class CacheStore:
    """Read, write, and merge parquet files for cached indicator data."""

    def __init__(self, config: CacheConfig):
        self.config = config

    # -- Path resolution -------------------------------------------------------

    def _item_dir(self, endpoint: str, item_id: int) -> Path:
        """Per-item directory: {cache_dir}/{endpoint}/{item_id}/"""
        return self.config.cache_dir / endpoint / str(item_id)

    def _parquet_path(self, endpoint: str, item_id: int) -> Path:
        """Data file: {cache_dir}/{endpoint}/{item_id}/data.parquet"""
        return self._item_dir(endpoint, item_id) / "data.parquet"

    def _meta_path(self, endpoint: str, item_id: int) -> Path:
        """Metadata file: {cache_dir}/{endpoint}/{item_id}/meta.json"""
        return self._item_dir(endpoint, item_id) / "meta.json"

    def _catalog_path(self, endpoint: str) -> Path:
        """Catalog file: {cache_dir}/{endpoint}/catalog.json"""
        return self.config.cache_dir / endpoint / "catalog.json"

    def _geos_path(self) -> Path:
        """Global geos registry: {cache_dir}/geos.json"""
        return self.config.cache_dir / "geos.json"

    def archive_dir(self, archive_id: int, name: str, date_key: str) -> Path:
        """Resolve the cache path for an archive download.

        Returns: ``{cache_dir}/archives/{archive_id}/{name}_{date_key}/``
        """
        return self.config.cache_dir / "archives" / str(archive_id) / f"{name}_{date_key}"

    def archive_exists(self, archive_id: int, name: str, date_key: str) -> bool:
        """Check if an archive is already cached (folder exists with files)."""
        folder = self.archive_dir(archive_id, name, date_key)
        return folder.exists() and any(folder.iterdir())

    # -- Migration -------------------------------------------------------------

    def _maybe_migrate(self, endpoint: str, item_id: int) -> None:
        """Auto-migrate old flat cache files to new directory layout.

        Old layout: ``{cache_dir}/{endpoint}/{item_id}.parquet``
        New layout: ``{cache_dir}/{endpoint}/{item_id}/data.parquet``
        """
        old_path = self.config.cache_dir / endpoint / f"{item_id}.parquet"
        if not old_path.exists():
            return

        new_path = self._parquet_path(endpoint, item_id)
        if new_path.exists():
            # New layout already has data — just remove old file
            old_path.unlink()
            logger.info("Removed old cache file %s (already migrated).", old_path)
            return

        # Move old flat file into new directory layout
        new_path.parent.mkdir(parents=True, exist_ok=True)
        old_path.rename(new_path)
        logger.info("Migrated cache %s → %s", old_path, new_path)

    # -- Data Read / Write -----------------------------------------------------

    def read(
        self,
        endpoint: str,
        indicator_id: int,
        start: pd.Timestamp,
        end: pd.Timestamp,
        *,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        """Read cached data for a date range.

        The file is in wide format (columns = geo_names or "value").
        When *columns* is given, only those columns are returned.
        Returns empty DataFrame on cache miss.
        """
        self._maybe_migrate(endpoint, indicator_id)
        path = self._parquet_path(endpoint, indicator_id)
        if not path.exists():
            return pd.DataFrame()

        try:
            df = pd.read_parquet(path)
        except Exception as exc:
            logger.warning("Corrupted cache file %s: %s — removing.", path, exc)
            path.unlink(missing_ok=True)
            return pd.DataFrame()

        if df.empty or not isinstance(df.index, pd.DatetimeIndex):
            return pd.DataFrame()

        df = self._slice(df, start, end)

        if columns:
            existing = [c for c in columns if c in df.columns]
            if not existing:
                return pd.DataFrame()
            df = df[existing]

        return df

    def _slice(
        self, df: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp
    ) -> pd.DataFrame:
        """Slice a DataFrame by [start, end], handling timezone alignment."""
        if df.index.tz is not None:
            if start.tz is None:
                start = start.tz_localize(df.index.tz)
            if end.tz is None:
                end = end.tz_localize(df.index.tz)

        # When end is a date-level timestamp (midnight), extend to end of day
        if end.hour == 0 and end.minute == 0 and end.second == 0:
            end = end + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)

        return df[start:end]

    def write(
        self,
        endpoint: str,
        indicator_id: int,
        df: pd.DataFrame,
    ) -> None:
        """Merge new wide-format data with existing cache and persist.

        *df* should already be in wide format (columns = geo_names or "value",
        index = DatetimeIndex). New data is merged with existing using
        ``combine_first`` so that new values fill in NaN cells without
        overwriting existing data, then overlapping rows use the new values.
        """
        if df.empty:
            return

        path = self._parquet_path(endpoint, indicator_id)
        path.parent.mkdir(parents=True, exist_ok=True)

        # Read existing and merge
        existing = pd.DataFrame()
        if path.exists():
            try:
                existing = pd.read_parquet(path)
            except Exception:
                logger.warning("Corrupted cache at %s — overwriting.", path)

        if not existing.empty:
            merged = df.combine_first(existing)
            merged = merged.sort_index()
        else:
            merged = df.sort_index()

        _atomic_write_parquet(path, merged)

    # -- Gap detection ---------------------------------------------------------

    def find_gaps(
        self,
        cached_df: pd.DataFrame,
        start: pd.Timestamp,
        end: pd.Timestamp,
        *,
        columns: list[str] | None = None,
        recent_ttl_hours: int | None = None,
    ) -> list[DateRange]:
        """Find date ranges not covered by cached data.

        When *columns* is given, checks coverage for those specific columns.
        A row counts as "covered" only if all requested columns have non-NaN
        values. This enables per-geo gap detection.

        Also marks data within ``recent_ttl_hours`` of now as a gap
        (needs re-fetch since it may have been updated).
        """
        ttl = recent_ttl_hours if recent_ttl_hours is not None else self.config.recent_ttl_hours
        cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=ttl)

        if cached_df.empty:
            return [DateRange(start, end)]

        # If specific columns requested, check only those (per-geo gap detection)
        if columns:
            missing = [c for c in columns if c not in cached_df.columns]
            if missing:
                return [DateRange(start, end)]
            mask = cached_df[columns].notna().all(axis=1)
            effective_df = cached_df[mask]
            if effective_df.empty:
                return [DateRange(start, end)]
        else:
            effective_df = cached_df

        # Normalize to UTC for comparison
        idx = effective_df.index
        if idx.tz is None:
            idx = idx.tz_localize("UTC")
        else:
            idx = idx.tz_convert("UTC")

        start_utc = start.tz_localize("UTC") if start.tz is None else start.tz_convert("UTC")
        end_utc = end.tz_localize("UTC") if end.tz is None else end.tz_convert("UTC")

        cached_start = idx.min()
        cached_end = idx.max()

        gaps: list[DateRange] = []

        if start_utc < cached_start:
            gap_end = min(cached_start - pd.Timedelta(hours=1), end_utc)
            if gap_end >= start_utc:
                gaps.append(DateRange(start, _to_naive_date(gap_end)))

        if end_utc > cached_end:
            gap_start = max(cached_end + pd.Timedelta(hours=1), start_utc)
            if gap_start <= end_utc:
                gaps.append(DateRange(_to_naive_date(gap_start), end))

        if cached_end > cutoff and end_utc > cutoff:
            recent_start = max(cutoff, start_utc)
            if recent_start <= end_utc:
                gaps.append(DateRange(_to_naive_date(recent_start), end))

        return _merge_overlapping(gaps)

    # -- Global geos registry --------------------------------------------------

    def read_geos(self) -> dict[str, str]:
        """Read the global geo_id → geo_name registry.

        Returns a dict like ``{"3": "España", "8828": "Países Bajos"}``.
        """
        path = self._geos_path()
        if not path.exists():
            return {}
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            return data.get("geos", {})
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Corrupted geos registry %s: %s — ignoring.", path, exc)
            return {}

    def merge_geos(self, geos: dict[str, str]) -> None:
        """Merge new geo_id → geo_name mappings into the global registry.

        Existing mappings are preserved; new ones are added.
        """
        if not geos:
            return

        existing = self.read_geos()
        existing.update(geos)

        data = {
            "version": 1,
            "updated_at": datetime.now().isoformat(),
            "geos": existing,
        }
        path = self._geos_path()
        _atomic_write_json(path, data)

    # -- Per-endpoint catalog --------------------------------------------------

    def read_catalog(self, endpoint: str) -> list[dict] | None:
        """Read the cached catalog for an endpoint.

        Returns the list of items, or None if not cached or stale.
        """
        path = self._catalog_path(endpoint)
        if not path.exists():
            return None

        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Corrupted catalog %s: %s — ignoring.", path, exc)
            return None

        # Check freshness
        updated_at = data.get("updated_at")
        if updated_at:
            try:
                cached_time = datetime.fromisoformat(updated_at)
                ttl = timedelta(hours=self.config.catalog_ttl_hours)
                if datetime.now() - cached_time > ttl:
                    logger.debug("Catalog %s is stale (older than %dh).", endpoint, self.config.catalog_ttl_hours)
                    return None
            except (ValueError, TypeError):
                pass

        return data.get("items", [])

    def write_catalog(self, endpoint: str, items: list[dict]) -> None:
        """Write the catalog for an endpoint."""
        data = {
            "updated_at": datetime.now().isoformat(),
            "items": items,
        }
        path = self._catalog_path(endpoint)
        _atomic_write_json(path, data)

    # -- Per-item metadata -----------------------------------------------------

    def read_meta(self, endpoint: str, item_id: int) -> dict | None:
        """Read cached metadata for an item.

        Returns the metadata dict, or None if not cached or stale.
        """
        path = self._meta_path(endpoint, item_id)
        if not path.exists():
            return None

        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Corrupted meta %s: %s — ignoring.", path, exc)
            return None

        # Check freshness
        cached_at = data.get("cached_at")
        if cached_at:
            try:
                cached_time = datetime.fromisoformat(cached_at)
                ttl = timedelta(days=self.config.meta_ttl_days)
                if datetime.now() - cached_time > ttl:
                    logger.debug("Meta for %s/%d is stale (older than %dd).", endpoint, item_id, self.config.meta_ttl_days)
                    return None
            except (ValueError, TypeError):
                pass

        return data

    def write_meta(self, endpoint: str, item_id: int, meta: dict) -> None:
        """Write metadata for an item, adding a cached_at timestamp."""
        meta = {**meta, "cached_at": datetime.now().isoformat()}
        path = self._meta_path(endpoint, item_id)
        _atomic_write_json(path, meta)

    # -- Maintenance -----------------------------------------------------------

    def clear(self, endpoint: str | None = None, indicator_id: int | None = None) -> int:
        """Remove cached files. Returns number of files removed.

        - No args: clear everything
        - endpoint only: clear all data for that endpoint
        - endpoint + indicator_id: clear one indicator/archive
        """
        count = 0

        if endpoint and indicator_id is not None:
            # Single item: remove its directory
            item_dir = self._item_dir(endpoint, indicator_id)
            if item_dir.exists():
                count = sum(1 for f in item_dir.rglob("*") if f.is_file())
                shutil.rmtree(item_dir)
        elif endpoint:
            target = self.config.cache_dir / endpoint
            if target.exists():
                count = sum(1 for f in target.rglob("*") if f.is_file())
                shutil.rmtree(target)
        else:
            target = self.config.cache_dir
            if target.exists():
                count = sum(1 for f in target.rglob("*") if f.is_file())
                shutil.rmtree(target)

        return count

    def status(self) -> dict:
        """Return cache statistics."""
        cache_dir = self.config.cache_dir
        if not cache_dir.exists():
            return {"path": str(cache_dir), "files": 0, "size_mb": 0.0, "endpoints": {}}

        all_files = [f for f in cache_dir.rglob("*") if f.is_file()]
        total_size = sum(f.stat().st_size for f in all_files)

        # Per-endpoint breakdown (skip root-level files like geos.json)
        endpoints: dict[str, int] = {}
        for f in all_files:
            try:
                rel = f.relative_to(cache_dir)
                if len(rel.parts) > 1:
                    ep = rel.parts[0]
                    endpoints[ep] = endpoints.get(ep, 0) + 1
            except ValueError:
                pass

        return {
            "path": str(cache_dir),
            "files": len(all_files),
            "size_mb": round(total_size / (1024 * 1024), 2),
            "endpoints": endpoints,
        }


# -- Helpers -------------------------------------------------------------------


def _to_naive_date(ts: pd.Timestamp) -> pd.Timestamp:
    """Convert a tz-aware timestamp to a naive date-level timestamp."""
    return pd.Timestamp(ts.date())


def _merge_overlapping(gaps: list[DateRange]) -> list[DateRange]:
    """Merge overlapping or adjacent date ranges."""
    if not gaps:
        return []

    sorted_gaps = sorted(gaps, key=lambda g: g.start)
    merged = [sorted_gaps[0]]

    for gap in sorted_gaps[1:]:
        prev = merged[-1]
        if gap.start <= prev.end + pd.Timedelta(days=1):
            merged[-1] = DateRange(prev.start, max(prev.end, gap.end))
        else:
            merged.append(gap)

    return merged


def _atomic_write_json(path: Path, data: dict) -> None:
    """Write JSON atomically via temp file + rename."""
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        fd, tmp_path = tempfile.mkstemp(suffix=".json", dir=path.parent)
        os.close(fd)
        Path(tmp_path).write_text(
            json.dumps(data, indent=2, ensure_ascii=False, default=str),
            encoding="utf-8",
        )
        Path(tmp_path).rename(path)
    except OSError as exc:
        logger.warning("Failed to write %s: %s", path, exc)
        Path(tmp_path).unlink(missing_ok=True)


def _atomic_write_parquet(path: Path, df: pd.DataFrame) -> None:
    """Write parquet atomically via temp file + rename."""
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        fd, tmp_path = tempfile.mkstemp(suffix=".parquet", dir=path.parent)
        os.close(fd)
        df.to_parquet(tmp_path)
        Path(tmp_path).rename(path)
    except OSError as exc:
        logger.warning("Failed to write cache %s: %s — continuing without cache.", path, exc)
        Path(tmp_path).unlink(missing_ok=True)
