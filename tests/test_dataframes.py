"""Tests for DataFrame processing utilities."""

import pandas as pd
import pytest

from esios.processing.dataframes import to_dataframe, convert_timezone


class TestToDataframe:
    def test_empty_data(self):
        df = to_dataframe([])
        assert df.empty

    def test_with_datetime(self):
        data = [
            {"value": 100, "datetime": "2025-01-01T00:00:00Z", "tz_time": "01:00"},
            {"value": 200, "datetime": "2025-01-01T01:00:00Z", "tz_time": "02:00"},
        ]
        df = to_dataframe(data)
        assert isinstance(df.index, pd.DatetimeIndex)
        assert str(df.index.tz) == "Europe/Madrid"
        assert len(df) == 2
        assert "tz_time" not in df.columns  # time cols dropped

    def test_without_datetime(self):
        data = [{"value": 1, "name": "a"}, {"value": 2, "name": "b"}]
        df = to_dataframe(data)
        assert len(df) == 2
        assert "value" in df.columns

    def test_single_geo_drops_geo_columns(self):
        """Single geo_id should drop geo columns and return flat DataFrame."""
        data = [
            {"value": 100, "datetime": "2025-01-01T00:00:00Z", "geo_id": 3, "geo_name": "España"},
            {"value": 200, "datetime": "2025-01-01T01:00:00Z", "geo_id": 3, "geo_name": "España"},
        ]
        df = to_dataframe(data)
        assert "geo_id" not in df.columns
        assert "geo_name" not in df.columns
        assert "value" in df.columns
        assert len(df) == 2

    def test_multi_geo_pivots(self):
        """Multiple geo_ids should pivot so each geo becomes a column."""
        data = [
            {"value": 100, "datetime": "2025-01-01T00:00:00Z", "geo_id": 3, "geo_name": "España"},
            {"value": 110, "datetime": "2025-01-01T00:00:00Z", "geo_id": 1, "geo_name": "Portugal"},
            {"value": 200, "datetime": "2025-01-01T01:00:00Z", "geo_id": 3, "geo_name": "España"},
            {"value": 210, "datetime": "2025-01-01T01:00:00Z", "geo_id": 1, "geo_name": "Portugal"},
        ]
        df = to_dataframe(data)
        assert "España" in df.columns
        assert "Portugal" in df.columns
        assert len(df) == 2
        assert df.loc[df.index[0], "España"] == 100
        assert df.loc[df.index[0], "Portugal"] == 110

    def test_multi_geo_no_pivot_when_disabled(self):
        """pivot_geo=False should keep raw format."""
        data = [
            {"value": 100, "datetime": "2025-01-01T00:00:00Z", "geo_id": 3, "geo_name": "España"},
            {"value": 110, "datetime": "2025-01-01T00:00:00Z", "geo_id": 1, "geo_name": "Portugal"},
        ]
        df = to_dataframe(data, pivot_geo=False)
        assert "geo_id" in df.columns
        assert "geo_name" in df.columns
        assert "value" in df.columns
        assert len(df) == 2

    def test_multi_geo_falls_back_to_geo_id(self):
        """Without geo_name, should use geo_id as column names."""
        data = [
            {"value": 100, "datetime": "2025-01-01T00:00:00Z", "geo_id": 3},
            {"value": 110, "datetime": "2025-01-01T00:00:00Z", "geo_id": 1},
        ]
        df = to_dataframe(data)
        assert 3 in df.columns or "3" in df.columns
        assert 1 in df.columns or "1" in df.columns


class TestConvertTimezone:
    def test_convert(self):
        idx = pd.to_datetime(["2025-01-01T00:00:00"], utc=True)
        df = pd.DataFrame({"val": [1]}, index=idx)
        result = convert_timezone(df, "Europe/Madrid")
        assert str(result.index.tz) == "Europe/Madrid"

    def test_naive_index_localized_first(self):
        idx = pd.to_datetime(["2025-01-01T00:00:00"])
        df = pd.DataFrame({"val": [1]}, index=idx)
        result = convert_timezone(df, "Europe/Madrid")
        assert str(result.index.tz) == "Europe/Madrid"
