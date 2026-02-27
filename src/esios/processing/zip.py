"""Zip extraction utilities for ESIOS archive files."""

from __future__ import annotations

import logging
import warnings
import zipfile
from io import BytesIO
from pathlib import Path

logger = logging.getLogger("esios")


class ZipExtractor:
    """Extract zip files, including nested zips, to a target folder.

    Accepts various input types: zipfile.ZipFile, BytesIO, bytes, or file paths.
    """

    def __init__(
        self,
        zip_file: zipfile.ZipFile | BytesIO | str | Path | bytes,
        folder: str | Path,
        overwrite: bool = True,
    ):
        if isinstance(zip_file, zipfile.ZipFile):
            self.zip_file = zip_file
        elif isinstance(zip_file, (str, Path)):
            self.zip_file = zipfile.ZipFile(zip_file)
        elif isinstance(zip_file, bytes):
            self.zip_file = zipfile.ZipFile(BytesIO(zip_file))
        else:
            self.zip_file = zipfile.ZipFile(zip_file)

        self.folder = Path(folder)
        self.overwrite = overwrite
        self.folder.mkdir(parents=True, exist_ok=True)

    def unzip(self) -> None:
        """Extract all files, recursing into nested zips."""
        for filename, zipinfo in self.zip_file.NameToInfo.items():
            if filename.endswith(".zip"):
                # Recurse into nested zip
                nested_folder = self.folder / filename.split(".")[0]
                nested_zip = zipfile.ZipFile(self.zip_file.open(filename))
                nested_extractor = ZipExtractor(nested_zip, nested_folder, self.overwrite)
                nested_extractor.unzip()
            else:
                path_output = self.folder / filename
                if path_output.exists() and self.overwrite:
                    warnings.warn(f"Overwriting {filename} in {self.folder}")
                if not path_output.exists() or self.overwrite:
                    self.zip_file.extract(zipinfo, self.folder)
