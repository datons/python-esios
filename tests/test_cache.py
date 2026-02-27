"""Tests for the local parquet cache."""

from __future__ import annotations

import json
from datetime import datetime, timedelta

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

    def test_directory_layout(self, cache_store, sample_wide_df):
        """Each indicator gets its own directory with data.parquet."""
        cache_store.write("indicators", 600, sample_wide_df)
        path = cache_store._parquet_path("indicators", 600)
        assert path.name == "data.parquet"
        assert path.parent.name == "600"
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

    def test_ttl_defaults(self):
        config = CacheConfig()
        assert config.meta_ttl_days == 7
        assert config.catalog_ttl_hours == 24


class TestGeosRegistry:
    def test_read_empty(self, cache_store):
        """Reading geos from empty cache returns empty dict."""
        assert cache_store.read_geos() == {}

    def test_merge_and_read(self, cache_store):
        """merge_geos persists and read_geos retrieves."""
        cache_store.merge_geos({"3": "España", "1": "Portugal"})
        result = cache_store.read_geos()
        assert result == {"3": "España", "1": "Portugal"}

    def test_merge_preserves_existing(self, cache_store):
        """New geos are added without removing existing ones."""
        cache_store.merge_geos({"3": "España"})
        cache_store.merge_geos({"8828": "Países Bajos"})
        result = cache_store.read_geos()
        assert result == {"3": "España", "8828": "Países Bajos"}

    def test_merge_updates_existing(self, cache_store):
        """Merging an existing geo_id updates its name."""
        cache_store.merge_geos({"3": "Spain"})
        cache_store.merge_geos({"3": "España"})
        result = cache_store.read_geos()
        assert result["3"] == "España"

    def test_merge_empty_is_noop(self, cache_store):
        """Merging empty dict doesn't create the file."""
        cache_store.merge_geos({})
        assert not cache_store._geos_path().exists()

    def test_geos_path(self, cache_store):
        """geos.json lives at cache root."""
        path = cache_store._geos_path()
        assert path.name == "geos.json"
        assert path.parent == cache_store.config.cache_dir

    def test_corrupted_geos_handled(self, cache_store):
        """Corrupted geos.json returns empty dict."""
        path = cache_store._geos_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("not json")
        assert cache_store.read_geos() == {}


class TestCatalog:
    def test_read_empty(self, cache_store):
        """Reading catalog from empty cache returns None."""
        assert cache_store.read_catalog("indicators") is None

    def test_write_and_read(self, cache_store):
        """write_catalog persists and read_catalog retrieves."""
        items = [
            {"id": 600, "name": "Spot price", "short_name": "Spot"},
            {"id": 10034, "name": "Solar generation", "short_name": "Solar"},
        ]
        cache_store.write_catalog("indicators", items)
        result = cache_store.read_catalog("indicators")
        assert result == items

    def test_catalog_path(self, cache_store):
        """catalog.json lives inside the endpoint directory."""
        path = cache_store._catalog_path("indicators")
        assert path.name == "catalog.json"
        assert path.parent.name == "indicators"

    def test_catalog_ttl_fresh(self, cache_store):
        """Fresh catalog (within TTL) is returned."""
        items = [{"id": 600, "name": "Spot"}]
        cache_store.write_catalog("indicators", items)
        assert cache_store.read_catalog("indicators") == items

    def test_catalog_ttl_stale(self, cache_store):
        """Stale catalog (older than TTL) returns None."""
        items = [{"id": 600, "name": "Spot"}]
        cache_store.write_catalog("indicators", items)

        # Manually backdate the updated_at
        path = cache_store._catalog_path("indicators")
        data = json.loads(path.read_text())
        data["updated_at"] = (datetime.now() - timedelta(hours=25)).isoformat()
        path.write_text(json.dumps(data))

        assert cache_store.read_catalog("indicators") is None

    def test_corrupted_catalog_handled(self, cache_store):
        """Corrupted catalog.json returns None."""
        path = cache_store._catalog_path("indicators")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("broken json {{{")
        assert cache_store.read_catalog("indicators") is None

    def test_clear_endpoint_removes_catalog(self, cache_store, sample_wide_df):
        """Clearing an endpoint also removes its catalog."""
        cache_store.write("indicators", 600, sample_wide_df)
        cache_store.write_catalog("indicators", [{"id": 600}])
        cache_store.clear(endpoint="indicators")
        assert cache_store.read_catalog("indicators") is None


class TestMetaCache:
    def test_read_empty(self, cache_store):
        """Reading meta from empty cache returns None."""
        assert cache_store.read_meta("indicators", 600) is None

    def test_write_and_read(self, cache_store):
        """write_meta persists and read_meta retrieves."""
        meta = {"id": 600, "name": "Spot price", "geos": [{"geo_id": 3, "geo_name": "España"}]}
        cache_store.write_meta("indicators", 600, meta)
        result = cache_store.read_meta("indicators", 600)
        assert result["id"] == 600
        assert result["name"] == "Spot price"
        assert "cached_at" in result

    def test_meta_path(self, cache_store):
        """meta.json lives inside the indicator directory."""
        path = cache_store._meta_path("indicators", 600)
        assert path.name == "meta.json"
        assert path.parent.name == "600"

    def test_meta_ttl_fresh(self, cache_store):
        """Fresh meta (within TTL) is returned."""
        meta = {"id": 600, "name": "Spot"}
        cache_store.write_meta("indicators", 600, meta)
        assert cache_store.read_meta("indicators", 600) is not None

    def test_meta_ttl_stale(self, cache_store):
        """Stale meta (older than TTL) returns None."""
        meta = {"id": 600, "name": "Spot"}
        cache_store.write_meta("indicators", 600, meta)

        # Manually backdate the cached_at
        path = cache_store._meta_path("indicators", 600)
        data = json.loads(path.read_text())
        data["cached_at"] = (datetime.now() - timedelta(days=8)).isoformat()
        path.write_text(json.dumps(data))

        assert cache_store.read_meta("indicators", 600) is None

    def test_meta_coexists_with_data(self, cache_store, sample_wide_df):
        """meta.json and data.parquet live in the same indicator directory."""
        cache_store.write("indicators", 600, sample_wide_df)
        cache_store.write_meta("indicators", 600, {"id": 600})

        item_dir = cache_store._item_dir("indicators", 600)
        files = sorted(f.name for f in item_dir.iterdir())
        assert files == ["data.parquet", "meta.json"]

    def test_clear_indicator_removes_meta(self, cache_store, sample_wide_df):
        """Clearing an indicator removes both data and meta."""
        cache_store.write("indicators", 600, sample_wide_df)
        cache_store.write_meta("indicators", 600, {"id": 600})

        count = cache_store.clear(endpoint="indicators", indicator_id=600)
        assert count == 2
        assert not cache_store._item_dir("indicators", 600).exists()

    def test_corrupted_meta_handled(self, cache_store):
        """Corrupted meta.json returns None."""
        path = cache_store._meta_path("indicators", 600)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("not valid json")
        assert cache_store.read_meta("indicators", 600) is None


class TestDirectoryLayout:
    def test_full_layout(self, cache_store, sample_wide_df):
        """Verify the full directory structure after writing data, meta, catalog, geos."""
        # Write everything
        cache_store.write("indicators", 600, sample_wide_df)
        cache_store.write_meta("indicators", 600, {"id": 600, "name": "Spot"})
        cache_store.write_catalog("indicators", [{"id": 600, "name": "Spot"}])
        cache_store.merge_geos({"3": "España"})

        cache_dir = cache_store.config.cache_dir

        # Check geos.json at root
        assert (cache_dir / "geos.json").exists()

        # Check catalog in endpoint dir
        assert (cache_dir / "indicators" / "catalog.json").exists()

        # Check per-indicator dir
        assert (cache_dir / "indicators" / "600" / "data.parquet").exists()
        assert (cache_dir / "indicators" / "600" / "meta.json").exists()

    def test_status_counts_all_files(self, cache_store, sample_wide_df):
        """status() counts data, meta, catalog, and geos files."""
        cache_store.write("indicators", 600, sample_wide_df)
        cache_store.write_meta("indicators", 600, {"id": 600})
        cache_store.write_catalog("indicators", [{"id": 600}])
        cache_store.merge_geos({"3": "España"})

        info = cache_store.status()
        assert info["files"] == 4  # data.parquet + meta.json + catalog.json + geos.json

    def test_clear_all_removes_everything(self, cache_store, sample_wide_df):
        """clear() with no args removes all cache files."""
        cache_store.write("indicators", 600, sample_wide_df)
        cache_store.write_meta("indicators", 600, {"id": 600})
        cache_store.write_catalog("indicators", [{"id": 600}])
        cache_store.merge_geos({"3": "España"})

        count = cache_store.clear()
        assert count == 4
        assert not cache_store.config.cache_dir.exists()
