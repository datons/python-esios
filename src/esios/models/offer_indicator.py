"""Pydantic models for ESIOS offer indicators."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class OfferIndicator(BaseModel):
    """ESIOS offer indicator metadata."""

    id: int
    name: str = ""
    short_name: str = ""
    description: str = ""
    raw: dict[str, Any] = Field(default_factory=dict, exclude=True)

    model_config = {"extra": "allow"}

    @classmethod
    def from_api(cls, data: dict[str, Any]) -> OfferIndicator:
        """Build from raw API JSON."""
        return cls(
            id=data["id"],
            name=data.get("name", ""),
            short_name=data.get("short_name", ""),
            description=data.get("description", ""),
            raw=data,
        )
