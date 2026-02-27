"""Base manager with shared utilities."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from bs4 import BeautifulSoup

if TYPE_CHECKING:
    from esios.client import ESIOSClient

logger = logging.getLogger("esios")


class BaseManager:
    """Shared functionality for all resource managers."""

    def __init__(self, client: ESIOSClient):
        self._client = client

    @staticmethod
    def _html_to_text(html: str) -> str:
        """Strip HTML tags from API description fields."""
        if not html:
            return ""
        soup = BeautifulSoup(html, "html.parser")
        return "\n\n".join(p.get_text() for p in soup.find_all("p"))

    def _get(self, endpoint: str, params: dict[str, Any] | None = None) -> dict:
        return self._client.get(endpoint, params=params)
