"""Local parquet cache for ESIOS time-series data.

Caches indicator data as parquet files, fetching only missing date ranges
on subsequent requests. Historical electricity data is immutable once
published, so caching is safe and enabled by default.

Storage layout::

    {cache_dir}/indicators/{indicator_id}.parquet

Each indicator is a single parquet file in wide format: the DatetimeIndex
holds timestamps, and columns are geo_ids (integers). For single-geo
indicators, there is one column. For multi-geo indicators (e.g. spot
prices across countries), each geo_id gets its own column. NaN cells
indicate that a particular geo has not been fetched for that time range.

Gap detection is per-column: requesting ``--geo 3`` checks whether column
``3`` has data for the requested range, not whether any column does.
"""

from __future__ import annotations

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


@dataclass
class CacheConfig:
    """Cache configuration."""

    enabled: bool = True
    cache_dir: Path = field(default_factory=lambda: _DEFAULT_CACHE_DIR)
    recent_ttl_hours: int = _DEFAULT_RECENT_TTL_HOURS

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

    def _indicator_dir(self, endpoint: str, indicator_id: int) -> Path:
        return self.config.cache_dir / endpoint / str(indicator_id)

    def _parquet_path(self, endpoint: str, indicator_id: int) -> Path:
        return self.config.cache_dir / endpoint / f"{indicator_id}.parquet"

    def archive_dir(self, archive_id: int, name: str, date_key: str) -> Path:
        """Resolve the cache path for an archive download.

        Returns: ``{cache_dir}/archives/{archive_id}/{name}_{date_key}/``
        """
        return self.config.cache_dir / "archives" / str(archive_id) / f"{name}_{date_key}"

    def archive_exists(self, archive_id: int, name: str, date_key: str) -> bool:
        """Check if an archive is already cached (folder exists with files)."""
        folder = self.archive_dir(archive_id, name, date_key)
        return folder.exists() and any(folder.iterdir())

    # -- Read / Write ----------------------------------------------------------

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

        The file is in wide format (columns = geo_ids or "value").
        When *columns* is given, only those columns are returned.
        Returns empty DataFrame on cache miss.
        """
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
            # Only return requested columns (geo_ids) that exist in cache
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

        *df* should already be in wide format (columns = geo_ids or "value",
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
            # New data takes priority for overlapping cells,
            # existing data fills gaps where new has NaN
            merged = df.combine_first(existing)
            merged = merged.sort_index()
        else:
            merged = df.sort_index()

        # Atomic write: temp file + rename
        try:
            fd, tmp_path = tempfile.mkstemp(
                suffix=".parquet", dir=path.parent
            )
            os.close(fd)
            merged.to_parquet(tmp_path)
            Path(tmp_path).rename(path)
        except OSError as exc:
            logger.warning("Failed to write cache %s: %s — continuing without cache.", path, exc)
            Path(tmp_path).unlink(missing_ok=True)

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
            # If any requested column is completely missing, it's a full gap
            missing = [c for c in columns if c not in cached_df.columns]
            if missing:
                return [DateRange(start, end)]
            # Rows where ALL requested columns have data
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

        # Gap before cached data
        if start_utc < cached_start:
            gap_end = min(cached_start - pd.Timedelta(hours=1), end_utc)
            if gap_end >= start_utc:
                gaps.append(DateRange(start, _to_naive_date(gap_end)))

        # Gap after cached data
        if end_utc > cached_end:
            gap_start = max(cached_end + pd.Timedelta(hours=1), start_utc)
            if gap_start <= end_utc:
                gaps.append(DateRange(_to_naive_date(gap_start), end))

        # Recent data gap (within TTL)
        if cached_end > cutoff and end_utc > cutoff:
            recent_start = max(cutoff, start_utc)
            if recent_start <= end_utc:
                gaps.append(DateRange(_to_naive_date(recent_start), end))

        return _merge_overlapping(gaps)

    # -- Maintenance -----------------------------------------------------------

    def clear(self, endpoint: str | None = None, indicator_id: int | None = None) -> int:
        """Remove cached files. Returns number of files/folders removed.

        - No args: clear everything
        - endpoint only: clear all data for that endpoint
        - endpoint + indicator_id: clear one indicator/archive
        """
        count = 0

        if endpoint and indicator_id is not None:
            # Single indicator: could be a file or a directory (archives)
            path_file = self._parquet_path(endpoint, indicator_id)
            path_dir = self._indicator_dir(endpoint, indicator_id)
            if path_file.exists():
                path_file.unlink()
                count = 1
            if path_dir.exists():
                for f in path_dir.rglob("*"):
                    if f.is_file():
                        f.unlink()
                        count += 1
                _remove_empty_dirs(path_dir)
        elif endpoint:
            target = self.config.cache_dir / endpoint
            if target.exists():
                for f in target.rglob("*"):
                    if f.is_file():
                        f.unlink()
                        count += 1
                _remove_empty_dirs(target)
        else:
            target = self.config.cache_dir
            if target.exists():
                for f in target.rglob("*"):
                    if f.is_file():
                        f.unlink()
                        count += 1
                _remove_empty_dirs(target)

        return count

    def status(self) -> dict:
        """Return cache statistics (indicators + archives)."""
        cache_dir = self.config.cache_dir
        if not cache_dir.exists():
            return {"path": str(cache_dir), "files": 0, "size_mb": 0.0, "endpoints": {}}

        all_files = [f for f in cache_dir.rglob("*") if f.is_file()]
        total_size = sum(f.stat().st_size for f in all_files)

        # Per-endpoint breakdown
        endpoints: dict[str, int] = {}
        for f in all_files:
            try:
                rel = f.relative_to(cache_dir)
                ep = rel.parts[0] if rel.parts else "unknown"
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


def _remove_empty_dirs(path: Path) -> None:
    """Remove empty directories recursively (bottom-up)."""
    for dirpath in sorted(path.rglob("*"), reverse=True):
        if dirpath.is_dir() and not any(dirpath.iterdir()):
            dirpath.rmdir()
    if path.is_dir() and not any(path.iterdir()):
        path.rmdir()
