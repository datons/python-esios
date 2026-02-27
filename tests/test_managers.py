"""Tests for indicator and archive managers."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from esios.client import ESIOSClient


class TestIndicatorsManager:
    def test_list(self, client, mock_httpx, sample_indicators_list_response):
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = sample_indicators_list_response
        mock_httpx.get.return_value = response

        df = client.indicators.list()
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        assert "PVPC T. Defecto" in df["name"].values

    def test_search(self, client, mock_httpx, sample_indicators_list_response):
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = sample_indicators_list_response
        mock_httpx.get.return_value = response

        df = client.indicators.search("solar")
        assert len(df) == 1
        assert df.iloc[0]["id"] == 10035

    def test_get_returns_handle(self, client, mock_httpx, sample_indicator_response):
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = sample_indicator_response
        mock_httpx.get.return_value = response

        handle = client.indicators.get(600)
        assert handle.id == 600
        assert handle.name == "PVPC T. Defecto"

    def test_historical_returns_dataframe(
        self, client, mock_httpx, sample_indicator_response
    ):
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = sample_indicator_response
        mock_httpx.get.return_value = response

        handle = client.indicators.get(600)
        df = handle.historical("2025-01-01", "2025-01-01")
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert isinstance(df.index, pd.DatetimeIndex)


class TestArchivesManager:
    def test_list(self, client, mock_httpx, sample_archives_list_response):
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = sample_archives_list_response
        mock_httpx.get.return_value = response

        df = client.archives.list()
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2


class TestIndicatorsCaching:
    """Test manager-level cache integration (catalog, meta, geos)."""

    @pytest.fixture
    def cached_client(self, mock_httpx, tmp_path):
        """ESIOSClient with cache enabled, pointed at tmp directory."""
        return ESIOSClient(
            token="test-token-123",
            cache=True,
            cache_dir=str(tmp_path / "cache"),
        )

    def test_list_caches_catalog(
        self, cached_client, mock_httpx, sample_indicators_list_response,
    ):
        """Second call to list() should use cached catalog, not API."""
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = sample_indicators_list_response
        mock_httpx.get.return_value = response

        # First call: hits API
        df1 = cached_client.indicators.list()
        assert mock_httpx.get.call_count == 1
        assert len(df1) == 3

        # Second call: should use cached catalog (no additional API call)
        df2 = cached_client.indicators.list()
        assert mock_httpx.get.call_count == 1  # No new API call
        assert len(df2) == 3

    def test_list_cache_stores_lightweight_catalog(
        self, cached_client, mock_httpx, sample_indicators_list_response,
    ):
        """Cached catalog stores only id/name/short_name, not full response."""
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = sample_indicators_list_response
        mock_httpx.get.return_value = response

        cached_client.indicators.list()

        catalog = cached_client.cache.read_catalog("indicators")
        assert catalog is not None
        # Catalog items should have id, name, short_name
        assert all("id" in item for item in catalog)
        assert all("name" in item for item in catalog)

    def test_get_caches_meta(
        self, cached_client, mock_httpx, sample_indicator_response,
    ):
        """Second call to get() should use cached meta, not API."""
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = sample_indicator_response
        mock_httpx.get.return_value = response

        # First call: hits API
        handle1 = cached_client.indicators.get(600)
        assert mock_httpx.get.call_count == 1
        assert handle1.id == 600

        # Second call: should use cached meta (no additional API call)
        handle2 = cached_client.indicators.get(600)
        assert mock_httpx.get.call_count == 1  # No new API call
        assert handle2.id == 600
        assert handle2.name == "PVPC T. Defecto"

    def test_get_persists_geos_to_registry(
        self, cached_client, mock_httpx,
    ):
        """indicators.get() should persist geos to global registry."""
        indicator_response = {
            "indicator": {
                "id": 600,
                "name": "Spot price",
                "geos": [
                    {"geo_id": 3, "geo_name": "España"},
                    {"geo_id": 1, "geo_name": "Portugal"},
                ],
                "values": [],
            }
        }
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = indicator_response
        mock_httpx.get.return_value = response

        cached_client.indicators.get(600)

        geos = cached_client.cache.read_geos()
        assert "3" in geos
        assert geos["3"] == "España"
        assert "1" in geos
        assert geos["1"] == "Portugal"

    def test_enrich_geo_map_persists(
        self, cached_client, mock_httpx,
    ):
        """_enrich_geo_map should persist new geos to geos.json."""
        # Response with a geo NOT in metadata geos list
        indicator_response = {
            "indicator": {
                "id": 600,
                "name": "Spot price",
                "geos": [{"geo_id": 3, "geo_name": "España"}],
                "values": [
                    {
                        "value": 50.0,
                        "datetime": "2025-01-01T00:00:00.000+01:00",
                        "datetime_utc": "2024-12-31T23:00:00Z",
                        "tz_time": "2025-01-01T00:00:00.000+01:00",
                        "geo_id": 3,
                        "geo_name": "España",
                    },
                    {
                        "value": 45.0,
                        "datetime": "2025-01-01T00:00:00.000+01:00",
                        "datetime_utc": "2024-12-31T23:00:00Z",
                        "tz_time": "2025-01-01T00:00:00.000+01:00",
                        "geo_id": 8828,
                        "geo_name": "Países Bajos",
                    },
                ],
            }
        }
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = indicator_response
        mock_httpx.get.return_value = response

        handle = cached_client.indicators.get(600)
        handle.historical("2025-01-01", "2025-01-01")

        # Países Bajos should be in global geos registry
        geos = cached_client.cache.read_geos()
        assert "8828" in geos
        assert geos["8828"] == "Países Bajos"

    def test_resolve_geo_falls_back_to_global_registry(
        self, cached_client, mock_httpx,
    ):
        """resolve_geo() should find geos from global registry."""
        # Pre-populate global geos registry
        cached_client.cache.merge_geos({"8828": "Países Bajos"})

        # Create a handle with only España in its metadata
        indicator_response = {
            "indicator": {
                "id": 600,
                "name": "Spot price",
                "geos": [{"geo_id": 3, "geo_name": "España"}],
                "values": [],
            }
        }
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = indicator_response
        mock_httpx.get.return_value = response

        handle = cached_client.indicators.get(600)

        # España found in indicator metadata
        assert handle.resolve_geo("España") == 3

        # Países Bajos found via global registry fallback
        assert handle.resolve_geo("Países Bajos") == 8828

    def test_search_uses_cached_catalog(
        self, cached_client, mock_httpx, sample_indicators_list_response,
    ):
        """search() should use cached catalog from prior list() call."""
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = sample_indicators_list_response
        mock_httpx.get.return_value = response

        # First: list() to populate catalog
        cached_client.indicators.list()
        assert mock_httpx.get.call_count == 1

        # search() should use cached catalog
        df = cached_client.indicators.search("solar")
        assert mock_httpx.get.call_count == 1  # No additional API call
        assert len(df) == 1
