"""python-esios: A Python wrapper for the ESIOS API (Spanish electricity market)."""

from esios.client import ESIOSClient
from esios.async_client import AsyncESIOSClient
from esios.exceptions import (
    ESIOSError,
    AuthenticationError,
    APIResponseError,
    NetworkError,
)

__version__ = "2.0.0"

__all__ = [
    "ESIOSClient",
    "AsyncESIOSClient",
    "ESIOSError",
    "AuthenticationError",
    "APIResponseError",
    "NetworkError",
]
