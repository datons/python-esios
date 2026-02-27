"""Tests for indicator and archive managers."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


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
