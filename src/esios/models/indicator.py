"""Pydantic models for ESIOS indicators."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class IndicatorValue(BaseModel):
    """A single data point from an indicator time series."""

    value: float
    datetime: datetime
    datetime_utc: datetime | None = None
    tz_time: str | None = None
    geo_id: int | None = None
    geo_name: str | None = None

    model_config = {"extra": "allow"}


class Indicator(BaseModel):
    """ESIOS indicator metadata."""

    id: int
    name: str
    short_name: str = ""
    description: str = ""
    raw: dict[str, Any] = Field(default_factory=dict, exclude=True)

    model_config = {"extra": "allow"}

    @classmethod
    def from_api(cls, data: dict[str, Any]) -> Indicator:
        """Build an Indicator from the raw API JSON (the 'indicator' object)."""
        return cls(
            id=data["id"],
            name=data.get("name", ""),
            short_name=data.get("short_name", ""),
            description=data.get("description", ""),
            raw=data,
        )
