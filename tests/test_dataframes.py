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
