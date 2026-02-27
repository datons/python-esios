"""Archive manager — list, configure, download with date-range iteration.

Archives are cached in ``{cache_dir}/archives/{archive_id}/{name}_{datekey}/``.
When ``output_dir`` is specified, files are copied there from the cache.
"""

from __future__ import annotations

import logging
import shutil
from datetime import timedelta
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

    @property
    def _cache(self):
        return self._manager._client.cache

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
        output_dir: str | Path | None = None,
        date_type: str = "datos",
    ) -> list[Path]:
        """Download archive files for a single date or date range.

        Files are always stored in the cache directory. If ``output_dir`` is
        provided, a copy is placed there as well.

        Returns a sorted list of downloaded/cached file paths.
        """
        if date and not (start and end):
            self.configure(date=date, date_type=date_type)
            cache_folder = self._download_single()
            if output_dir:
                self._copy_to_output(cache_folder, Path(output_dir))
            return sorted(f for f in cache_folder.iterdir() if f.is_file())

        if not (start and end):
            raise ValueError("Provide 'date' or both 'start' and 'end'.")

        # Date-range iteration
        start_date = pd.to_datetime(start)
        end_date = pd.to_datetime(end)

        horizon = self.metadata.get("archive", {}).get("horizon", "D")
        archive_type = self.metadata.get("archive", {}).get("archive_type", "zip")
        current = start_date
        files: list[Path] = []

        while current <= end_date:
            if horizon == "M":
                key = current.strftime("%Y%m")
                next_month = (current.replace(day=1) + timedelta(days=32)).replace(day=1)
                chunk_end = min(next_month - timedelta(days=1), end_date)
            else:
                key = current.strftime("%Y%m%d")
                chunk_end = current

            # Skip if already cached
            if self._cache.config.enabled and self._cache.archive_exists(self.id, self.name, key):
                cache_folder = self._cache.archive_dir(self.id, self.name, key)
                logger.info("Cache hit: %s", cache_folder)
                if output_dir:
                    self._copy_to_output(cache_folder, Path(output_dir))
                files.extend(f for f in cache_folder.iterdir() if f.is_file())
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

            # Write to cache
            cache_folder = self._cache.archive_dir(self.id, self.name, key)
            self._write_content(content, cache_folder, key, archive_type)
            files.extend(f for f in cache_folder.iterdir() if f.is_file())

            # Copy to output if requested
            if output_dir:
                self._copy_to_output(cache_folder, Path(output_dir))

            current = chunk_end + timedelta(days=1)

        return sorted(files)

    # -- Internal helpers ------------------------------------------------------

    def _download_single(self) -> Path:
        """Download for a single date (after configure). Returns cache folder."""
        archive_data = self.metadata.get("archive", {})
        horizon = archive_data.get("horizon", "D")
        archive_type = archive_data.get("archive_type", "zip")

        if horizon == "M":
            date_key = pd.to_datetime(archive_data["date_times"][0]).strftime("%Y%m")
        else:
            date_key = pd.to_datetime(archive_data["date"]["date"]).strftime("%Y%m%d")

        # Check cache first
        if self._cache.config.enabled and self._cache.archive_exists(self.id, self.name, date_key):
            cache_folder = self._cache.archive_dir(self.id, self.name, date_key)
            logger.info("Cache hit: %s", cache_folder)
            return cache_folder

        cache_folder = self._cache.archive_dir(self.id, self.name, date_key)
        content = self._manager._client.download(self._download_url)
        self._write_content(content, cache_folder, date_key, archive_type)
        return cache_folder

    def _write_content(self, content: bytes, folder: Path, key: str, archive_type: str) -> None:
        """Extract zip or write xls to folder."""
        folder.mkdir(parents=True, exist_ok=True)
        if archive_type == "zip":
            zx = ZipExtractor(content, folder)
            zx.unzip()
        elif archive_type == "xls":
            file_path = folder / f"{self.name}_{key}.xls"
            file_path.write_bytes(content)
        else:
            raise ValueError(f"Unsupported archive_type: {archive_type}")

    @staticmethod
    def _copy_to_output(cache_folder: Path, output_dir: Path) -> None:
        """Copy cached files to a user-specified output directory."""
        dest = output_dir / cache_folder.name
        if dest.exists() and any(dest.iterdir()):
            logger.info("Output already exists: %s", dest)
            return
        dest.mkdir(parents=True, exist_ok=True)
        for src_file in cache_folder.iterdir():
            if src_file.is_file():
                shutil.copy2(src_file, dest / src_file.name)
        logger.info("Copied to %s", dest)


class ArchivesManager(BaseManager):
    """Manager for ``/archives`` endpoints."""

    def list(self, *, source: str = "local") -> pd.DataFrame:
        """List all available archives as a DataFrame.

        Args:
            source: ``"local"`` (default) returns the full static catalog
                (153 archives including I90, settlements, etc.).
                ``"api"`` queries the ESIOS API which only returns ~24 archives.
        """
        if source == "api":
            data = self._get("archives", params={"date_type": "publicacion"})
            df = pd.DataFrame(data.get("archives", []))
            if "id" in df.columns:
                df = df.set_index("id")
            return df

        from esios.data.catalogs.archives import ARCHIVES_CATALOG

        df = pd.DataFrame.from_dict(ARCHIVES_CATALOG, orient="index")
        df.index.name = "id"
        return df

    def get(self, archive_id: int) -> ArchiveHandle:
        """Get an archive by ID — returns a handle with ``.download()``."""
        data = self._get(f"archives/{archive_id}")
        raw = data.get("archive", {})
        archive = Archive.from_api(raw)
        handle = ArchiveHandle(self, archive)
        handle.metadata = data
        return handle

    def download(
        self,
        archive_id: int,
        *,
        start: str | None = None,
        end: str | None = None,
        date: str | None = None,
        output_dir: str | Path | None = None,
        date_type: str = "datos",
    ) -> list[Path]:
        """Convenience method: get + download in one call.

        Returns a sorted list of downloaded/cached file paths.
        """
        handle = self.get(archive_id)
        return handle.download(
            start=start, end=end, date=date, output_dir=output_dir, date_type=date_type,
        )
