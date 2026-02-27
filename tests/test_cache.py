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
def sample_wide_df():
    """Sample wide-format DataFrame (single geo as column, named by geo_name)."""
    idx = pd.date_range("2025-01-01", periods=24, freq="h", tz="Europe/Madrid")
    return pd.DataFrame({"España": range(24)}, index=idx)


@pytest.fixture
def sample_multi_geo_df():
    """Sample wide-format DataFrame with multiple geo columns (named by geo_name)."""
    idx = pd.date_range("2025-01-01", periods=24, freq="h", tz="Europe/Madrid")
    return pd.DataFrame(
        {"España": range(24), "Portugal": range(100, 124)},
        index=idx,
    )


class TestCacheStore:
    def test_read_empty(self, cache_store):
        """Read on empty cache returns empty DataFrame."""
        df = cache_store.read("indicators", 600, pd.Timestamp("2025-01-01"), pd.Timestamp("2025-01-02"))
        assert df.empty

    def test_write_and_read(self, cache_store, sample_wide_df):
        """Written data should be readable."""
        cache_store.write("indicators", 600, sample_wide_df)
        result = cache_store.read(
            "indicators", 600,
            pd.Timestamp("2025-01-01", tz="Europe/Madrid"),
            pd.Timestamp("2025-01-02", tz="Europe/Madrid"),
        )
        assert not result.empty
        assert len(result) == 24
        assert "España" in result.columns

    def test_write_and_read_single_column(self, cache_store, sample_wide_df):
        """Read with columns filter returns only requested columns."""
        cache_store.write("indicators", 600, sample_wide_df)
        result = cache_store.read(
            "indicators", 600,
            pd.Timestamp("2025-01-01", tz="Europe/Madrid"),
            pd.Timestamp("2025-01-02", tz="Europe/Madrid"),
            columns=["España"],
        )
        assert not result.empty
        assert list(result.columns) == ["España"]

    def test_write_merges_existing(self, cache_store):
        """Writing new data merges with existing via combine_first."""
        idx1 = pd.date_range("2025-01-01", periods=3, freq="h", tz="Europe/Madrid")
        df1 = pd.DataFrame({"España": [1, 2, 3]}, index=idx1)

        idx2 = pd.date_range("2025-01-01T03:00", periods=3, freq="h", tz="Europe/Madrid")
        df2 = pd.DataFrame({"España": [4, 5, 6]}, index=idx2)

        cache_store.write("indicators", 600, df1)
        cache_store.write("indicators", 600, df2)

        result = cache_store.read(
            "indicators", 600,
            pd.Timestamp("2025-01-01", tz="Europe/Madrid"),
            pd.Timestamp("2025-01-02", tz="Europe/Madrid"),
        )
        assert len(result) == 6

    def test_write_deduplicates(self, cache_store):
        """Overlapping writes: new data takes priority."""
        idx = pd.date_range("2025-01-01", periods=3, freq="h", tz="Europe/Madrid")
        df1 = pd.DataFrame({"España": [1, 2, 3]}, index=idx)
        df2 = pd.DataFrame({"España": [10, 20, 30]}, index=idx)

        cache_store.write("indicators", 600, df1)
        cache_store.write("indicators", 600, df2)

        result = cache_store.read(
            "indicators", 600,
            pd.Timestamp("2025-01-01", tz="Europe/Madrid"),
            pd.Timestamp("2025-01-02", tz="Europe/Madrid"),
        )
        # New data wins on overlap
        assert result["España"].iloc[0] == 10

    def test_multi_geo_wide_format(self, cache_store, sample_multi_geo_df):
        """Multi-geo wide format: each geo is a column named by geo_name."""
        cache_store.write("indicators", 600, sample_multi_geo_df)

        # Read all columns
        result = cache_store.read(
            "indicators", 600,
            pd.Timestamp("2025-01-01", tz="Europe/Madrid"),
            pd.Timestamp("2025-01-02", tz="Europe/Madrid"),
        )
        assert "España" in result.columns
        assert "Portugal" in result.columns
        assert len(result) == 24

        # Read single column
        result_single = cache_store.read(
            "indicators", 600,
            pd.Timestamp("2025-01-01", tz="Europe/Madrid"),
            pd.Timestamp("2025-01-02", tz="Europe/Madrid"),
            columns=["España"],
        )
        assert list(result_single.columns) == ["España"]
        assert len(result_single) == 24

    def test_sparse_geo_merge(self, cache_store):
        """Writing different geos at different times creates sparse columns."""
        idx = pd.date_range("2025-01-01", periods=3, freq="h", tz="Europe/Madrid")

        # First write: only España
        df1 = pd.DataFrame({"España": [1, 2, 3]}, index=idx)
        cache_store.write("indicators", 600, df1)

        # Second write: only Portugal
        df2 = pd.DataFrame({"Portugal": [10, 20, 30]}, index=idx)
        cache_store.write("indicators", 600, df2)

        result = cache_store.read(
            "indicators", 600,
            pd.Timestamp("2025-01-01", tz="Europe/Madrid"),
            pd.Timestamp("2025-01-02", tz="Europe/Madrid"),
        )
        assert "España" in result.columns
        assert "Portugal" in result.columns
        assert result["España"].iloc[0] == 1
        assert result["Portugal"].iloc[0] == 10

    def test_one_file_per_indicator(self, cache_store, sample_wide_df):
        """Each indicator is a single parquet file."""
        cache_store.write("indicators", 600, sample_wide_df)
        path = cache_store._parquet_path("indicators", 600)
        assert path.name == "600.parquet"
        assert path.exists()

    def test_corrupted_file_handled(self, cache_store):
        """Corrupted parquet files should be deleted and return empty."""
        path = cache_store._parquet_path("indicators", 600)
        path.parent.mkdir(parents=True)
        path.write_text("not a parquet file")

        result = cache_store.read("indicators", 600, pd.Timestamp("2025-01-01"), pd.Timestamp("2025-01-02"))
        assert result.empty
        assert not path.exists()


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
        idx = pd.date_range("2024-01-01", periods=48, freq="h", tz="UTC")
        cached = pd.DataFrame({"España": range(48)}, index=idx)

        gaps = cache_store.find_gaps(
            cached,
            pd.Timestamp("2024-01-01"),
            pd.Timestamp("2024-01-02"),
        )
        assert len(gaps) == 0

    def test_gap_before_cached(self, cache_store):
        """Gap before the start of cached data."""
        idx = pd.date_range("2024-06-15", periods=24, freq="h", tz="UTC")
        cached = pd.DataFrame({"España": range(24)}, index=idx)

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
        cached = pd.DataFrame({"España": range(24)}, index=idx)

        gaps = cache_store.find_gaps(
            cached,
            pd.Timestamp("2024-06-01"),
            pd.Timestamp("2024-06-30"),
        )
        assert any(g.end == pd.Timestamp("2024-06-30") for g in gaps)

    def test_per_column_gap_detection(self, cache_store):
        """Gap detection checks specific columns when requested."""
        idx = pd.date_range("2024-01-01", periods=24, freq="h", tz="UTC")
        # Column "España" has data, column "Portugal" is all NaN
        cached = pd.DataFrame({"España": range(24), "Portugal": [float("nan")] * 24}, index=idx)

        # No gap for column "España"
        gaps_es = cache_store.find_gaps(
            cached, pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-01"),
            columns=["España"],
        )
        assert len(gaps_es) == 0

        # Full gap for column "Portugal" (all NaN)
        gaps_pt = cache_store.find_gaps(
            cached, pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-01"),
            columns=["Portugal"],
        )
        assert len(gaps_pt) == 1

    def test_gap_for_missing_column(self, cache_store):
        """Missing column in cache = full gap."""
        idx = pd.date_range("2024-01-01", periods=24, freq="h", tz="UTC")
        cached = pd.DataFrame({"España": range(24)}, index=idx)

        gaps = cache_store.find_gaps(
            cached, pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-01"),
            columns=["Alemania"],  # doesn't exist
        )
        assert len(gaps) == 1


class TestCacheMaintenance:
    def test_clear_all(self, cache_store, sample_wide_df):
        cache_store.write("indicators", 600, sample_wide_df)
        cache_store.write("indicators", 10034, sample_wide_df)

        count = cache_store.clear()
        assert count == 2

    def test_clear_one_indicator(self, cache_store, sample_wide_df):
        cache_store.write("indicators", 600, sample_wide_df)
        cache_store.write("indicators", 10034, sample_wide_df)

        count = cache_store.clear(endpoint="indicators", indicator_id=600)
        assert count == 1

        # Other indicator still cached
        result = cache_store.read(
            "indicators", 10034,
            pd.Timestamp("2025-01-01", tz="Europe/Madrid"),
            pd.Timestamp("2025-01-02", tz="Europe/Madrid"),
        )
        assert not result.empty

    def test_status(self, cache_store, sample_wide_df):
        cache_store.write("indicators", 600, sample_wide_df)
        info = cache_store.status()
        assert info["files"] == 1
        assert info["size_mb"] >= 0

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

    def test_status_includes_archives(self, cache_store, sample_wide_df):
        cache_store.write("indicators", 600, sample_wide_df)

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
