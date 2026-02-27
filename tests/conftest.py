"""Shared fixtures for ESIOS tests."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from esios.client import ESIOSClient


@pytest.fixture
def mock_httpx():
    """Patch httpx.Client so no real HTTP requests are made."""
    with patch("esios.client.httpx.Client") as mock_cls:
        mock_instance = MagicMock()
        mock_cls.return_value = mock_instance
        yield mock_instance


@pytest.fixture
def client(mock_httpx):
    """ESIOSClient with mocked HTTP layer and cache disabled."""
    return ESIOSClient(token="test-token-123", cache=False)


@pytest.fixture
def sample_indicator_response():
    """Sample API response for GET /indicators/600."""
    return {
        "indicator": {
            "id": 600,
            "name": "PVPC T. Defecto",
            "short_name": "PVPC",
            "description": "<p>Precio voluntario para el pequeño consumidor</p>",
            "values": [
                {
                    "value": 120.5,
                    "datetime": "2025-01-01T00:00:00.000+01:00",
                    "datetime_utc": "2024-12-31T23:00:00Z",
                    "tz_time": "2025-01-01T00:00:00.000+01:00",
                    "geo_id": 3,
                    "geo_name": "España",
                },
                {
                    "value": 115.3,
                    "datetime": "2025-01-01T01:00:00.000+01:00",
                    "datetime_utc": "2025-01-01T00:00:00Z",
                    "tz_time": "2025-01-01T01:00:00.000+01:00",
                    "geo_id": 3,
                    "geo_name": "España",
                },
            ],
        }
    }


@pytest.fixture
def sample_indicators_list_response():
    """Sample API response for GET /indicators."""
    return {
        "indicators": [
            {
                "id": 600,
                "name": "PVPC T. Defecto",
                "short_name": "PVPC",
                "description": "<p>Precio voluntario</p>",
            },
            {
                "id": 10034,
                "name": "Generación eólica",
                "short_name": "Eólica",
                "description": "<p>Generación eólica en tiempo real</p>",
            },
            {
                "id": 10035,
                "name": "Generación solar fotovoltaica",
                "short_name": "Solar FV",
                "description": "<p>Generación solar fotovoltaica</p>",
            },
        ]
    }


@pytest.fixture
def sample_archives_list_response():
    """Sample API response for GET /archives."""
    return {
        "archives": [
            {"id": 1, "name": "I90DIA", "horizon": "D", "archive_type": "xls"},
            {"id": 2, "name": "LIQUIDACIONES", "horizon": "M", "archive_type": "zip"},
        ]
    }
