"""Pydantic models for ESIOS archives."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class ArchiveDownload(BaseModel):
    """Download metadata for an archive."""

    name: str = ""
    url: str = ""


class Archive(BaseModel):
    """ESIOS archive metadata."""

    id: int
    name: str = ""
    horizon: str = "D"
    archive_type: str = "zip"
    download: ArchiveDownload | None = None
    raw: dict[str, Any] = Field(default_factory=dict, exclude=True)

    model_config = {"extra": "allow"}

    @classmethod
    def from_api(cls, data: dict[str, Any]) -> Archive:
        """Build an Archive from the raw API JSON."""
        download_data = data.get("download")
        download = ArchiveDownload(**download_data) if download_data else None
        return cls(
            id=data["id"],
            name=data.get("name", ""),
            horizon=data.get("horizon", "D"),
            archive_type=data.get("archive_type", "zip"),
            download=download,
            raw=data,
        )
