"""Tests for custom exceptions."""

from esios.exceptions import (
    APIResponseError,
    AuthenticationError,
    ESIOSError,
    NetworkError,
)


def test_hierarchy():
    """All custom exceptions inherit from ESIOSError."""
    assert issubclass(AuthenticationError, ESIOSError)
    assert issubclass(APIResponseError, ESIOSError)
    assert issubclass(NetworkError, ESIOSError)


def test_api_response_error_status_code():
    err = APIResponseError(404, "Not found")
    assert err.status_code == 404
    assert "Not found" in str(err)


def test_api_response_error_default_message():
    err = APIResponseError(500)
    assert "500" in str(err)


def test_authentication_error_default():
    err = AuthenticationError()
    assert "API key" in str(err)
