"""Asynchronous ESIOS API client using httpx."""

from __future__ import annotations

import logging
import os
import time
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

logger = logging.getLogger("esios")


class AsyncESIOSClient:
    """Asynchronous client for the ESIOS API.

    Usage::

        async with AsyncESIOSClient(token="...") as client:
            data = await client.get("indicators/600")
    """

    def __init__(
        self,
        token: str | None = None,
        timeout: float = DEFAULT_TIMEOUT,
        base_url: str = ESIOS_API_URL,
    ):
        self.token = token or os.getenv("ESIOS_API_KEY")
        if not self.token:
            raise ESIOSError(
                "API key required. Pass token= or set ESIOS_API_KEY env var."
            )

        self.base_url = base_url
        self.timeout = timeout

        headers = {**DEFAULT_HEADERS, "x-api-key": self.token}
        self._http = httpx.AsyncClient(
            base_url=self.base_url,
            headers=headers,
            timeout=self.timeout,
        )

    @retry(
        retry=retry_if_exception_type((APIResponseError, NetworkError)),
        stop=stop_after_attempt(MAX_RETRIES),
        wait=wait_exponential(min=RETRY_MIN_WAIT, max=RETRY_MAX_WAIT),
        reraise=True,
    )
    async def get(self, endpoint: str, params: dict[str, Any] | None = None) -> dict:
        """Issue an async GET request with retry logic."""
        url = f"/{endpoint}"
        t0 = time.monotonic()
        try:
            logger.debug("GET %s params=%s", url, params)
            response = await self._http.get(url, params=params)
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

    async def download(self, url: str) -> bytes:
        """Download raw bytes from an absolute URL."""
        try:
            response = await self._http.get(url)
        except httpx.HTTPError as exc:
            raise NetworkError(str(exc)) from exc

        if response.status_code in (401, 403):
            raise AuthenticationError()
        if response.status_code >= 400:
            raise APIResponseError(response.status_code)

        return response.content

    async def close(self) -> None:
        await self._http.aclose()

    async def __aenter__(self) -> AsyncESIOSClient:
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()
