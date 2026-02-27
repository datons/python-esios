"""Local parquet cache for ESIOS time-series data.

Caches indicator data as parquet files, fetching only missing date ranges
on subsequent requests. Historical electricity data is immutable once
published, so caching is safe and enabled by default.
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

    def _parquet_path(
        self, endpoint: str, indicator_id: int, geo_id: int | None = None
    ) -> Path:
        directory = self._indicator_dir(endpoint, indicator_id)
        filename = f"{geo_id}.parquet" if geo_id is not None else "_all.parquet"
        return directory / filename

    # -- Read / Write ----------------------------------------------------------

    def read(
        self,
        endpoint: str,
        indicator_id: int,
        start: pd.Timestamp,
        end: pd.Timestamp,
        geo_id: int | None = None,
    ) -> pd.DataFrame:
        """Read cached data for a date range. Returns empty DataFrame on miss."""
        path = self._parquet_path(endpoint, indicator_id, geo_id)
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

        # Align tz for slicing — ensure start/end match the index timezone
        if df.index.tz is not None:
            if start.tz is None:
                start = start.tz_localize(df.index.tz)
            if end.tz is None:
                end = end.tz_localize(df.index.tz)

        return df[start:end]

    def write(
        self,
        endpoint: str,
        indicator_id: int,
        df: pd.DataFrame,
        geo_id: int | None = None,
    ) -> None:
        """Merge new data with existing cache and persist."""
        if df.empty:
            return

        path = self._parquet_path(endpoint, indicator_id, geo_id)
        path.parent.mkdir(parents=True, exist_ok=True)

        # Read existing and merge
        existing = pd.DataFrame()
        if path.exists():
            try:
                existing = pd.read_parquet(path)
            except Exception:
                logger.warning("Corrupted cache at %s — overwriting.", path)

        if not existing.empty:
            merged = pd.concat([existing, df])
            merged = merged[~merged.index.duplicated(keep="last")].sort_index()
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
        recent_ttl_hours: int | None = None,
    ) -> list[DateRange]:
        """Find date ranges not covered by cached data.

        Also marks data within ``recent_ttl_hours`` of now as a gap
        (needs re-fetch since it may have been updated).
        """
        ttl = recent_ttl_hours if recent_ttl_hours is not None else self.config.recent_ttl_hours
        cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=ttl)

        if cached_df.empty:
            return [DateRange(start, end)]

        # Normalize to UTC for comparison
        idx = cached_df.index
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
        """Remove cached files. Returns number of files removed.

        - No args: clear everything
        - endpoint only: clear all indicators for that endpoint
        - endpoint + indicator_id: clear one indicator
        """
        count = 0

        if endpoint and indicator_id:
            target = self._indicator_dir(endpoint, indicator_id)
        elif endpoint:
            target = self.config.cache_dir / endpoint
        else:
            target = self.config.cache_dir

        if not target.exists():
            return 0

        if target.is_dir():
            for f in target.rglob("*.parquet"):
                f.unlink()
                count += 1
            # Clean up empty dirs
            _remove_empty_dirs(target)
        elif target.is_file():
            target.unlink()
            count = 1

        return count

    def status(self) -> dict:
        """Return cache statistics."""
        cache_dir = self.config.cache_dir
        if not cache_dir.exists():
            return {"path": str(cache_dir), "files": 0, "size_mb": 0.0}

        files = list(cache_dir.rglob("*.parquet"))
        total_size = sum(f.stat().st_size for f in files)

        return {
            "path": str(cache_dir),
            "files": len(files),
            "size_mb": round(total_size / (1024 * 1024), 2),
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
