"""Tests for the local parquet cache."""

from __future__ import annotations

import pandas as pd
import pytest

from esios.cache import CacheConfig, CacheStore, DateRange


@pytest.fixture
def cache_store(tmp_path):
    """CacheStore backed by a tmp directory."""
    config = CacheConfig(enabled=True, cache_dir=tmp_path / "cache", recent_ttl_hours=48)
    return CacheStore(config)


@pytest.fixture
def sample_df():
    """Sample DataFrame resembling indicator data."""
    idx = pd.date_range("2025-01-01", periods=24, freq="h", tz="Europe/Madrid")
    return pd.DataFrame({"value": range(24), "geo_id": 3}, index=idx)


class TestCacheStore:
    def test_read_empty(self, cache_store):
        """Read on empty cache returns empty DataFrame."""
        df = cache_store.read("indicators", 600, pd.Timestamp("2025-01-01"), pd.Timestamp("2025-01-02"))
        assert df.empty

    def test_write_and_read(self, cache_store, sample_df):
        """Written data should be readable."""
        cache_store.write("indicators", 600, sample_df, geo_id=3)
        result = cache_store.read(
            "indicators", 600,
            pd.Timestamp("2025-01-01", tz="Europe/Madrid"),
            pd.Timestamp("2025-01-02", tz="Europe/Madrid"),
            geo_id=3,
        )
        assert not result.empty
        assert len(result) == 24

    def test_write_merges_existing(self, cache_store):
        """Writing new data merges with existing."""
        df1 = pd.DataFrame(
            {"value": [1, 2, 3]},
            index=pd.date_range("2025-01-01", periods=3, freq="h", tz="Europe/Madrid"),
        )
        df2 = pd.DataFrame(
            {"value": [4, 5, 6]},
            index=pd.date_range("2025-01-01T03:00", periods=3, freq="h", tz="Europe/Madrid"),
        )
        cache_store.write("indicators", 600, df1, geo_id=3)
        cache_store.write("indicators", 600, df2, geo_id=3)

        result = cache_store.read(
            "indicators", 600,
            pd.Timestamp("2025-01-01", tz="Europe/Madrid"),
            pd.Timestamp("2025-01-02", tz="Europe/Madrid"),
            geo_id=3,
        )
        assert len(result) == 6

    def test_write_deduplicates(self, cache_store):
        """Overlapping writes should deduplicate by index."""
        df1 = pd.DataFrame(
            {"value": [1, 2, 3]},
            index=pd.date_range("2025-01-01", periods=3, freq="h", tz="Europe/Madrid"),
        )
        df2 = pd.DataFrame(
            {"value": [10, 20, 30]},
            index=pd.date_range("2025-01-01", periods=3, freq="h", tz="Europe/Madrid"),
        )
        cache_store.write("indicators", 600, df1, geo_id=3)
        cache_store.write("indicators", 600, df2, geo_id=3)

        result = cache_store.read(
            "indicators", 600,
            pd.Timestamp("2025-01-01", tz="Europe/Madrid"),
            pd.Timestamp("2025-01-02", tz="Europe/Madrid"),
            geo_id=3,
        )
        # Latest write wins
        assert result["value"].iloc[0] == 10

    def test_geo_id_none_uses_all(self, cache_store, sample_df):
        """No geo_id uses _all.parquet."""
        cache_store.write("indicators", 600, sample_df)
        path = cache_store._parquet_path("indicators", 600, None)
        assert path.name == "_all.parquet"
        assert path.exists()

    def test_corrupted_file_handled(self, cache_store):
        """Corrupted parquet files should be deleted and return empty."""
        path = cache_store._parquet_path("indicators", 600, 3)
        path.parent.mkdir(parents=True)
        path.write_text("not a parquet file")

        result = cache_store.read("indicators", 600, pd.Timestamp("2025-01-01"), pd.Timestamp("2025-01-02"), geo_id=3)
        assert result.empty
        assert not path.exists()  # should be cleaned up


class TestCacheGaps:
    def test_full_gap_on_empty(self, cache_store):
        """Empty cache should return one gap covering the full range."""
        gaps = cache_store.find_gaps(
            pd.DataFrame(),
            pd.Timestamp("2025-01-01"),
            pd.Timestamp("2025-01-31"),
        )
        assert len(gaps) == 1
        assert gaps[0].start == pd.Timestamp("2025-01-01")
        assert gaps[0].end == pd.Timestamp("2025-01-31")

    def test_no_gap_when_fully_cached(self, cache_store):
        """No gaps when cache fully covers requested range with old data."""
        # Use data far in the past (outside TTL window)
        idx = pd.date_range("2024-01-01", periods=48, freq="h", tz="UTC")
        cached = pd.DataFrame({"value": range(48)}, index=idx)

        gaps = cache_store.find_gaps(
            cached,
            pd.Timestamp("2024-01-01"),
            pd.Timestamp("2024-01-02"),
        )
        assert len(gaps) == 0

    def test_gap_before_cached(self, cache_store):
        """Gap before the start of cached data."""
        idx = pd.date_range("2024-06-15", periods=24, freq="h", tz="UTC")
        cached = pd.DataFrame({"value": range(24)}, index=idx)

        gaps = cache_store.find_gaps(
            cached,
            pd.Timestamp("2024-06-01"),
            pd.Timestamp("2024-06-15"),
        )
        assert len(gaps) >= 1
        assert gaps[0].start == pd.Timestamp("2024-06-01")

    def test_gap_after_cached(self, cache_store):
        """Gap after the end of cached data."""
        idx = pd.date_range("2024-06-01", periods=24, freq="h", tz="UTC")
        cached = pd.DataFrame({"value": range(24)}, index=idx)

        gaps = cache_store.find_gaps(
            cached,
            pd.Timestamp("2024-06-01"),
            pd.Timestamp("2024-06-30"),
        )
        assert any(g.end == pd.Timestamp("2024-06-30") for g in gaps)


class TestCacheMaintenance:
    def test_clear_all(self, cache_store, sample_df):
        cache_store.write("indicators", 600, sample_df, geo_id=3)
        cache_store.write("indicators", 10034, sample_df, geo_id=3)

        count = cache_store.clear()
        assert count == 2

    def test_clear_one_indicator(self, cache_store, sample_df):
        cache_store.write("indicators", 600, sample_df, geo_id=3)
        cache_store.write("indicators", 10034, sample_df, geo_id=3)

        count = cache_store.clear(endpoint="indicators", indicator_id=600)
        assert count == 1

        # Other indicator still cached
        result = cache_store.read(
            "indicators", 10034,
            pd.Timestamp("2025-01-01", tz="Europe/Madrid"),
            pd.Timestamp("2025-01-02", tz="Europe/Madrid"),
            geo_id=3,
        )
        assert not result.empty

    def test_status(self, cache_store, sample_df):
        cache_store.write("indicators", 600, sample_df, geo_id=3)
        info = cache_store.status()
        assert info["files"] == 1
        assert info["size_mb"] >= 0  # tiny test files may round to 0.0

    def test_status_empty(self, cache_store):
        info = cache_store.status()
        assert info["files"] == 0


class TestArchiveCache:
    def test_archive_dir_path(self, cache_store):
        path = cache_store.archive_dir(1, "I90DIA", "20250101")
        assert path == cache_store.config.cache_dir / "archives" / "1" / "I90DIA_20250101"

    def test_archive_exists_false_when_empty(self, cache_store):
        assert not cache_store.archive_exists(1, "I90DIA", "20250101")

    def test_archive_exists_true_when_populated(self, cache_store):
        folder = cache_store.archive_dir(1, "I90DIA", "20250101")
        folder.mkdir(parents=True)
        (folder / "data.xls").write_bytes(b"fake xls")
        assert cache_store.archive_exists(1, "I90DIA", "20250101")

    def test_clear_archives(self, cache_store):
        folder = cache_store.archive_dir(1, "I90DIA", "20250101")
        folder.mkdir(parents=True)
        (folder / "data.xls").write_bytes(b"fake")
        (folder / "data2.csv").write_bytes(b"fake")

        count = cache_store.clear(endpoint="archives", indicator_id=1)
        assert count == 2
        assert not folder.exists()

    def test_status_includes_archives(self, cache_store):
        # Write an indicator parquet
        idx = pd.date_range("2025-01-01", periods=3, freq="h", tz="Europe/Madrid")
        cache_store.write("indicators", 600, pd.DataFrame({"value": [1, 2, 3]}, index=idx), geo_id=3)

        # Write an archive file
        folder = cache_store.archive_dir(1, "I90DIA", "20250101")
        folder.mkdir(parents=True)
        (folder / "data.xls").write_bytes(b"fake")

        info = cache_store.status()
        assert info["files"] == 2
        assert "indicators" in info["endpoints"]
        assert "archives" in info["endpoints"]


class TestCacheConfig:
    def test_defaults(self):
        config = CacheConfig()
        assert config.enabled is True
        assert config.recent_ttl_hours == 48

    def test_disabled(self):
        config = CacheConfig(enabled=False)
        assert config.enabled is False
