"""Synchronous ESIOS API client using httpx."""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path
from typing import Any

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from esios.constants import (
    DEFAULT_HEADERS,
    DEFAULT_TIMEOUT,
    ESIOS_API_URL,
    MAX_RETRIES,
    RETRY_MAX_WAIT,
    RETRY_MIN_WAIT,
)
from esios.exceptions import (
    APIResponseError,
    AuthenticationError,
    ESIOSError,
    NetworkError,
)
from esios.cache import CacheConfig, CacheStore
from esios.managers.archives import ArchivesManager
from esios.managers.indicators import IndicatorsManager
from esios.managers.offer_indicators import OfferIndicatorsManager

logger = logging.getLogger("esios")


def _should_retry(exc: BaseException) -> bool:
    """Don't retry on auth errors."""
    if isinstance(exc, AuthenticationError):
        return False
    return isinstance(exc, (APIResponseError, NetworkError))


class ESIOSClient:
    """Synchronous client for the ESIOS API.

    Usage::

        with ESIOSClient(token="...") as client:
            df = client.indicators.get(600).historical("2025-01-01", "2025-01-07")

    Or without context manager::

        client = ESIOSClient()
        df = client.indicators.get(600).historical("2025-01-01", "2025-01-07")
        client.close()
    """

    def __init__(
        self,
        token: str | None = None,
        timeout: float = DEFAULT_TIMEOUT,
        base_url: str = ESIOS_API_URL,
        *,
        cache: bool = True,
        cache_dir: str | None = None,
        cache_recent_ttl: int = 48,
    ):
        self.token = token or os.getenv("ESIOS_API_KEY")
        if not self.token:
            raise ESIOSError(
                "API key required. Pass token= or set ESIOS_API_KEY env var."
            )

        self.base_url = base_url
        self.timeout = timeout

        headers = {**DEFAULT_HEADERS, "x-api-key": self.token}
        self._http = httpx.Client(
            base_url=self.base_url,
            headers=headers,
            timeout=self.timeout,
        )

        # Cache
        cache_config = CacheConfig(
            enabled=cache,
            recent_ttl_hours=cache_recent_ttl,
        )
        if cache_dir:
            cache_config.cache_dir = Path(cache_dir)
        self.cache = CacheStore(cache_config)

        # Managers
        self.indicators = IndicatorsManager(self)
        self.archives = ArchivesManager(self)
        self.offer_indicators = OfferIndicatorsManager(self)

    # -- HTTP primitives -------------------------------------------------------

    @retry(
        retry=retry_if_exception_type((APIResponseError, NetworkError)),
        stop=stop_after_attempt(MAX_RETRIES),
        wait=wait_exponential(min=RETRY_MIN_WAIT, max=RETRY_MAX_WAIT),
        reraise=True,
    )
    def get(self, endpoint: str, params: dict[str, Any] | None = None) -> dict:
        """Issue a GET request with retry logic.

        Raises:
            AuthenticationError: On 401/403 (no retry).
            APIResponseError: On other HTTP errors (retried).
            NetworkError: On connection failures (retried).
        """
        url = f"/{endpoint}"
        t0 = time.monotonic()
        try:
            logger.debug("GET %s params=%s", url, params)
            response = self._http.get(url, params=params)
        except httpx.ConnectError as exc:
            raise NetworkError(f"Connection failed: {exc}") from exc
        except httpx.TimeoutException as exc:
            raise NetworkError(f"Request timed out: {exc}") from exc
        except httpx.HTTPError as exc:
            raise NetworkError(str(exc)) from exc

        elapsed = time.monotonic() - t0
        logger.debug("Response %s in %.2fs", response.status_code, elapsed)

        if response.status_code in (401, 403):
            raise AuthenticationError()
        if response.status_code >= 400:
            raise APIResponseError(
                response.status_code,
                f"ESIOS API error: {response.status_code} — {response.text[:200]}",
            )

        return response.json()

    def download(self, url: str) -> bytes:
        """Download raw bytes from an absolute URL (for archive files).

        The ESIOS API may return a 307 redirect to an S3 presigned URL.
        We follow redirects explicitly here (without sending the API key
        header to S3, which would cause a 400).
        """
        t0 = time.monotonic()
        try:
            response = self._http.get(url)
            # Follow redirect (e.g. 307 to S3) with a plain client
            if response.status_code in (301, 302, 307, 308) and "location" in response.headers:
                redirect_url = response.headers["location"]
                logger.debug("Following redirect to %s", redirect_url[:100])
                response = httpx.get(redirect_url, timeout=self.timeout)
        except httpx.HTTPError as exc:
            raise NetworkError(str(exc)) from exc

        elapsed = time.monotonic() - t0
        logger.debug("Download %s in %.2fs (%d bytes)", url, elapsed, len(response.content))

        if response.status_code in (401, 403):
            raise AuthenticationError()
        if response.status_code >= 400:
            raise APIResponseError(response.status_code)

        return response.content

    # -- Lifecycle -------------------------------------------------------------

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> ESIOSClient:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()
