"""Archive manager — list, configure, download with date-range iteration."""

from __future__ import annotations

import logging
from datetime import timedelta
from io import BytesIO
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

from esios.constants import ESIOS_API_URL
from esios.exceptions import APIResponseError
from esios.managers.base import BaseManager
from esios.models.archive import Archive
from esios.processing.zip import ZipExtractor

if TYPE_CHECKING:
    from esios.client import ESIOSClient

logger = logging.getLogger("esios")


class ArchiveHandle:
    """Bound handle to a single archive, returned by ``ArchivesManager.get()``."""

    def __init__(self, manager: ArchivesManager, archive: Archive):
        self._manager = manager
        self.archive = archive
        self.id = archive.id
        self.name = archive.name
        self.metadata = archive.raw

    def __repr__(self) -> str:
        return f"<ArchiveHandle id={self.id} name={self.name!r}>"

    def configure(
        self,
        *,
        date: str | None = None,
        start: str | None = None,
        end: str | None = None,
        date_type: str = "datos",
        locale: str = "es",
    ) -> None:
        """Configure archive parameters and resolve the download URL."""
        params: dict[str, str] = {"date_type": date_type, "locale": locale}
        if date:
            params["date"] = date + "T00:00:00"
        elif start and end:
            params["start_date"] = start + "T00:00:00"
            params["end_date"] = end + "T23:59:59"
        else:
            raise ValueError("Provide either 'date', or both 'start' and 'end'.")

        response = self._manager._get(f"archives/{self.id}", params=params)
        self.metadata = response
        download = self.metadata["archive"]["download"]
        self.name = download["name"]
        self._download_url = ESIOS_API_URL + download["url"]

    def download(
        self,
        *,
        start: str | None = None,
        end: str | None = None,
        date: str | None = None,
        output_dir: str | Path = ".",
        date_type: str = "datos",
    ) -> None:
        """Download archive files for a single date or date range.

        Skips dates whose output folder already contains files.
        """
        output_dir = Path(output_dir)

        if date and not (start and end):
            # Single date download
            self.configure(date=date, date_type=date_type)
            self._download_single(output_dir)
            return

        if not (start and end):
            raise ValueError("Provide 'date' or both 'start' and 'end'.")

        # Date-range iteration
        start_date = pd.to_datetime(start)
        end_date = pd.to_datetime(end)
        base_dir = output_dir / self.name
        base_dir.mkdir(parents=True, exist_ok=True)

        horizon = self.metadata.get("archive", {}).get("horizon", "D")
        archive_type = self.metadata.get("archive", {}).get("archive_type", "zip")
        current = start_date

        while current <= end_date:
            if horizon == "M":
                key = current.strftime("%Y%m")
                folder = base_dir / f"{self.name}_{key}"
                next_month = (current.replace(day=1) + timedelta(days=32)).replace(day=1)
                chunk_end = min(next_month - timedelta(days=1), end_date)
            else:
                key = current.strftime("%Y%m%d")
                folder = base_dir / f"{self.name}_{key}"
                chunk_end = current

            # Skip existing
            if folder.exists() and any(folder.glob("*")):
                logger.info("Skipping already downloaded: %s", folder)
                current = chunk_end + timedelta(days=1)
                continue

            s = current.strftime("%Y-%m-%d")
            e = chunk_end.strftime("%Y-%m-%d")

            try:
                self.configure(start=s, end=e, date_type=date_type)
                content = self._manager._client.download(self._download_url)
            except (APIResponseError, Exception) as exc:
                logger.warning("Failed to download %s to %s: %s — skipping.", s, e, exc)
                current = chunk_end + timedelta(days=1)
                continue

            self._write_content(content, folder, key, archive_type)
            current = chunk_end + timedelta(days=1)

    # -- Internal helpers ------------------------------------------------------

    def _download_single(self, output_dir: Path) -> None:
        """Download for a single date (after configure)."""
        archive_data = self.metadata.get("archive", {})
        horizon = archive_data.get("horizon", "D")
        archive_type = archive_data.get("archive_type", "zip")

        if horizon == "M":
            date_folder = pd.to_datetime(archive_data["date_times"][0]).strftime("%Y%m")
        else:
            date_folder = pd.to_datetime(archive_data["date"]["date"]).strftime("%Y%m%d")

        folder = output_dir / self.name / f"{self.name}_{date_folder}"
        folder.mkdir(parents=True, exist_ok=True)

        content = self._manager._client.download(self._download_url)
        self._write_content(content, folder, date_folder, archive_type)

    def _write_content(self, content: bytes, folder: Path, key: str, archive_type: str) -> None:
        """Extract zip or write xls to folder."""
        if archive_type == "zip":
            folder.mkdir(parents=True, exist_ok=True)
            zx = ZipExtractor(content, folder)
            zx.unzip()
        elif archive_type == "xls":
            folder.mkdir(parents=True, exist_ok=True)
            file_path = folder / f"{self.name}_{key}.xls"
            file_path.write_bytes(content)
        else:
            raise ValueError(f"Unsupported archive_type: {archive_type}")


class ArchivesManager(BaseManager):
    """Manager for ``/archives`` endpoints."""

    def list(self) -> pd.DataFrame:
        """List all available archives as a DataFrame."""
        data = self._get("archives")
        return pd.DataFrame(data.get("archives", []))

    def get(self, archive_id: int) -> ArchiveHandle:
        """Get an archive by ID — returns a handle with ``.download()``."""
        data = self._get(f"archives/{archive_id}")
        raw = data.get("archive", {})
        archive = Archive.from_api(raw)
        handle = ArchiveHandle(self, archive)
        handle.metadata = data  # Keep full response for configure/download
        return handle

    def download(
        self,
        archive_id: int,
        *,
        start: str | None = None,
        end: str | None = None,
        date: str | None = None,
        output_dir: str | Path = ".",
        date_type: str = "datos",
    ) -> None:
        """Convenience method: get + download in one call."""
        handle = self.get(archive_id)
        handle.download(start=start, end=end, date=date, output_dir=output_dir, date_type=date_type)
