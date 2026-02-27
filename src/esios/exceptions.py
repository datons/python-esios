"""Custom exceptions for the ESIOS API client."""


class ESIOSError(Exception):
    """Base exception for all ESIOS errors."""


class AuthenticationError(ESIOSError):
    """Raised on 401/403 responses — invalid or missing API key."""

    def __init__(self, message: str = "Authentication failed. Check your ESIOS API key."):
        super().__init__(message)


class APIResponseError(ESIOSError):
    """Raised on non-2xx HTTP responses (excluding auth errors)."""

    def __init__(self, status_code: int, message: str | None = None):
        self.status_code = status_code
        msg = message or f"ESIOS API returned HTTP {status_code}"
        super().__init__(msg)


class NetworkError(ESIOSError):
    """Raised on connection failures, timeouts, and DNS errors."""

    def __init__(self, message: str = "Network error communicating with ESIOS API."):
        super().__init__(message)
