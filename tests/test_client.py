"""Tests for the ESIOSClient."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import httpx
import pytest

from esios.client import ESIOSClient
from esios.exceptions import AuthenticationError, ESIOSError, NetworkError


def test_requires_token():
    """Client raises if no token is provided and env var is unset."""
    with patch.dict("os.environ", {}, clear=True):
        with pytest.raises(ESIOSError, match="API key required"):
            ESIOSClient(token=None)


def test_token_from_param(mock_httpx):
    client = ESIOSClient(token="my-token")
    assert client.token == "my-token"


def test_context_manager(mock_httpx):
    with ESIOSClient(token="tok") as client:
        assert client.token == "tok"
    # close() called
    mock_httpx.close.assert_called_once()


def test_get_auth_error(client, mock_httpx):
    """401/403 should raise AuthenticationError immediately."""
    response = MagicMock()
    response.status_code = 401
    response.text = "Unauthorized"
    mock_httpx.get.return_value = response

    with pytest.raises(AuthenticationError):
        client.get("indicators")


def test_get_success(client, mock_httpx):
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {"indicators": []}
    mock_httpx.get.return_value = response

    result = client.get("indicators")
    assert result == {"indicators": []}


def test_managers_attached(client):
    """Client should have indicators, archives, offer_indicators managers."""
    assert hasattr(client, "indicators")
    assert hasattr(client, "archives")
    assert hasattr(client, "offer_indicators")
