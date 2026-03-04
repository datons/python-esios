"""YAML-backed catalog for ESIOS indicators and archives.

Provides offline browsing of known indicators and archives, with
``refresh()`` to sync against the live API while preserving hand-curated
notes and tags.
"""

from __future__ import annotations

import importlib.resources
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd
import yaml

if TYPE_CHECKING:
    from esios.client import ESIOSClient

logger = logging.getLogger("esios")


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class CatalogEntry:
    """A single catalog entry (indicator or archive)."""

    id: int
    name: str
    extra: dict[str, Any] = field(default_factory=dict)

    def __getattr__(self, key: str) -> Any:
        try:
            return self.extra[key]
        except KeyError:
            raise AttributeError(f"CatalogEntry has no attribute {key!r}")


@dataclass
class RefreshResult:
    """Summary of a catalog refresh operation."""

    added: int = 0
    updated: int = 0
    removed: int = 0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _yaml_path(filename: str) -> Path:
    """Resolve the absolute path to a YAML file inside ``esios.data``."""
    ref = importlib.resources.files("esios.data").joinpath(filename)
    # Traversable may not have a real filesystem path in zip installs,
    # but for editable / sdist installs this works fine.
    return Path(str(ref))


def _load_yaml(filename: str) -> dict:
    path = _yaml_path(filename)
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _save_yaml(filename: str, data: dict) -> None:
    path = _yaml_path(filename)
    with open(path, "w", encoding="utf-8") as f:
        yaml.dump(data, f, allow_unicode=True, default_flow_style=False, sort_keys=False, width=200)


# ---------------------------------------------------------------------------
# Shared reference catalogs
# ---------------------------------------------------------------------------

_REF_FILES = {
    "geos": "geos.yaml",
    "magnitudes": "magnitudes.yaml",
    "time_periods": "time_periods.yaml",
}


def load_reference(kind: str) -> dict[int, str]:
    """Load a shared reference catalog (geos, magnitudes, or time_periods).

    Returns ``{id: name}`` with int keys.
    """
    doc = _load_yaml(_REF_FILES[kind])
    raw = doc.get(kind, {})
    return {int(k): v for k, v in raw.items()}


def _save_reference(kind: str, mapping: dict[int, str]) -> None:
    """Persist a shared reference catalog."""
    doc = {
        "version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        kind: {k: v for k, v in sorted(mapping.items())},
    }
    _save_yaml(_REF_FILES[kind], doc)


# ---------------------------------------------------------------------------
# IndicatorsCatalog
# ---------------------------------------------------------------------------


class IndicatorsCatalog:
    """Catalog of ESIOS indicators backed by ``indicators.yaml``."""

    YAML_FILE = "indicators.yaml"

    def __init__(self, client: ESIOSClient | None = None):
        self._client = client

    def _load_entries(self) -> list[CatalogEntry]:
        doc = _load_yaml(self.YAML_FILE)
        entries: list[CatalogEntry] = []
        for item in doc.get("indicators", []):
            entries.append(CatalogEntry(
                id=item["id"],
                name=item["name"],
                extra={
                    "short_name": item.get("short_name", ""),
                    "notes": item.get("notes", ""),
                    "tags": item.get("tags", []),
                    "magnitude_id": item.get("magnitude_id"),
                    "time_period_id": item.get("time_period_id"),
                    "geo_ids": item.get("geo_ids", []),
                },
            ))
        return entries

    def list(self, query: str | None = None) -> pd.DataFrame:
        """List catalog entries, optionally filtering by substring query."""
        entries = self._load_entries()
        rows = [
            {"id": e.id, "name": e.name, "short_name": e.extra.get("short_name", ""), "notes": e.extra.get("notes", ""), "tags": ",".join(e.extra.get("tags", []))}
            for e in entries
        ]
        df = pd.DataFrame(rows)
        if df.empty:
            return df
        df = df.set_index("id")
        if query:
            mask = df["name"].str.contains(query, case=False, na=False) | df["short_name"].str.contains(query, case=False, na=False)
            df = df[mask]
        return df

    def get(self, indicator_id: int) -> CatalogEntry:
        """Get a single catalog entry by ID."""
        for entry in self._load_entries():
            if entry.id == indicator_id:
                return entry
        raise KeyError(f"Indicator {indicator_id} not found in catalog")

    def _fetch_indicator_detail(self, indicator_id: int) -> dict | None:
        """Fetch per-indicator detail from ``/indicators/{id}``."""
        try:
            data = self._client.get(f"indicators/{indicator_id}")
            return data.get("indicator", {})
        except Exception:
            logger.warning("Failed to fetch detail for indicator %d", indicator_id)
            return None

    def refresh(self, *, dry_run: bool = False) -> RefreshResult:
        """Sync catalog against the live API.

        Fetches the full indicator list from the API and merges with the
        existing YAML, preserving hand-curated ``notes`` and ``tags``.

        Also fetches per-indicator detail to extract metadata IDs
        (magnitude, time_period, geos) and populates the shared
        reference catalogs.
        """
        if self._client is None:
            raise RuntimeError("Cannot refresh without a client — pass client to ESIOSCatalog")

        # Fetch from API
        data = self._client.get("indicators")
        api_indicators = data.get("indicators", [])

        # Build lookup of existing entries
        doc = _load_yaml(self.YAML_FILE)
        existing = {item["id"]: item for item in doc.get("indicators", [])}

        # Only fetch detail for indicators already in the catalog
        cataloged_ids = set(existing.keys())

        # Load current shared reference catalogs
        all_geos = load_reference("geos")
        all_magnitudes = load_reference("magnitudes")
        all_time_periods = load_reference("time_periods")

        # Fetch per-indicator detail in parallel for cataloged indicators
        detail_map: dict[int, dict] = {}
        if cataloged_ids:
            with ThreadPoolExecutor(max_workers=8) as pool:
                futures = {
                    pool.submit(self._fetch_indicator_detail, iid): iid
                    for iid in cataloged_ids
                }
                for future in as_completed(futures):
                    iid = futures[future]
                    detail = future.result()
                    if detail:
                        detail_map[iid] = detail

        result = RefreshResult()
        merged: list[dict] = []

        for ind in api_indicators:
            iid = ind["id"]
            if iid in existing:
                old = existing[iid]

                # Extract metadata IDs from detail response
                detail = detail_map.get(iid, {})
                magnitude_id, time_period_id, geo_ids = self._extract_meta_ids(
                    detail, all_geos, all_magnitudes, all_time_periods,
                )

                new_entry = {
                    "id": iid,
                    "name": ind.get("name", old.get("name", "")),
                    "short_name": ind.get("short_name", old.get("short_name", "")),
                    "notes": old.get("notes", ""),
                    "tags": old.get("tags", []),
                    "magnitude_id": magnitude_id or old.get("magnitude_id"),
                    "time_period_id": time_period_id or old.get("time_period_id"),
                    "geo_ids": geo_ids or old.get("geo_ids", []),
                }

                # Check if anything API-fetched changed
                old_api_fields = (
                    old.get("name"), old.get("short_name"),
                    old.get("magnitude_id"), old.get("time_period_id"),
                    old.get("geo_ids", []),
                )
                new_api_fields = (
                    new_entry["name"], new_entry["short_name"],
                    new_entry["magnitude_id"], new_entry["time_period_id"],
                    new_entry["geo_ids"],
                )
                if old_api_fields != new_api_fields:
                    result.updated += 1
            else:
                new_entry = {
                    "id": iid,
                    "name": ind.get("name", ""),
                    "short_name": ind.get("short_name", ""),
                    "notes": "",
                    "tags": [],
                }
                result.added += 1
            merged.append(new_entry)

        api_ids = {ind["id"] for ind in api_indicators}
        for iid, old in existing.items():
            if iid not in api_ids:
                result.removed += 1

        merged.sort(key=lambda x: x["id"])

        if not dry_run:
            new_doc = {
                "version": doc.get("version", 1),
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "indicators": merged,
            }
            _save_yaml(self.YAML_FILE, new_doc)

            # Persist shared reference catalogs
            _save_reference("geos", all_geos)
            _save_reference("magnitudes", all_magnitudes)
            _save_reference("time_periods", all_time_periods)

        return result

    @staticmethod
    def _extract_meta_ids(
        detail: dict,
        all_geos: dict[int, str],
        all_magnitudes: dict[int, str],
        all_time_periods: dict[int, str],
    ) -> tuple[int | None, int | None, list[int]]:
        """Extract metadata IDs from a detail response, updating shared catalogs.

        Returns ``(magnitude_id, time_period_id, geo_ids)``.
        """
        # Magnitude (units)
        magnitude_id = None
        for mag in detail.get("magnitud", []):
            mid = mag.get("id")
            mname = mag.get("name", "")
            if mid is not None:
                magnitude_id = mid
                all_magnitudes[mid] = mname
                break  # first one

        # Time period (granularity)
        time_period_id = None
        for tp in detail.get("tiempo", []):
            tid = tp.get("id")
            tname = tp.get("name", "")
            if tid is not None:
                time_period_id = tid
                all_time_periods[tid] = tname
                break  # first one

        # Geos
        geo_ids: list[int] = []
        for g in detail.get("geos", []):
            gid = g.get("geo_id")
            gname = g.get("geo_name", "")
            if gid is not None:
                geo_ids.append(gid)
                all_geos[gid] = gname

        return magnitude_id, time_period_id, sorted(geo_ids)


# ---------------------------------------------------------------------------
# ArchivesCatalog
# ---------------------------------------------------------------------------


class ArchivesCatalog:
    """Catalog of ESIOS archives backed by ``archives.yaml``."""

    YAML_FILE = "archives.yaml"

    def __init__(self, client: ESIOSClient | None = None):
        self._client = client

    def _load_entries(self) -> list[CatalogEntry]:
        doc = _load_yaml(self.YAML_FILE)
        entries: list[CatalogEntry] = []
        for item in doc.get("archives", []):
            entries.append(CatalogEntry(
                id=item["id"],
                name=item["name"],
                extra={
                    "description": item.get("description", ""),
                    "horizon": item.get("horizon", ""),
                    "archive_type": item.get("archive_type", ""),
                    "notes": item.get("notes", ""),
                },
            ))
        return entries

    def list(self, query: str | None = None) -> pd.DataFrame:
        """List catalog entries, optionally filtering by substring query."""
        entries = self._load_entries()
        rows = [
            {"id": e.id, "name": e.name, "description": e.extra.get("description", ""), "horizon": e.extra.get("horizon", ""), "archive_type": e.extra.get("archive_type", ""), "notes": e.extra.get("notes", "")}
            for e in entries
        ]
        df = pd.DataFrame(rows)
        if df.empty:
            return df
        df = df.set_index("id")
        if query:
            mask = df["name"].str.contains(query, case=False, na=False) | df["description"].str.contains(query, case=False, na=False)
            df = df[mask]
        return df

    def get(self, archive_id: int) -> CatalogEntry:
        """Get a single catalog entry by ID."""
        for entry in self._load_entries():
            if entry.id == archive_id:
                return entry
        raise KeyError(f"Archive {archive_id} not found in catalog")

    def refresh(self, *, dry_run: bool = False) -> RefreshResult:
        """Sync catalog against the live API.

        Scans archive IDs 1-200 against the API and merges with the
        existing YAML, preserving hand-curated ``notes``.
        """
        if self._client is None:
            raise RuntimeError("Cannot refresh without a client — pass client to ESIOSCatalog")

        # Build lookup of existing entries
        doc = _load_yaml(self.YAML_FILE)
        existing = {item["id"]: item for item in doc.get("archives", [])}

        result = RefreshResult()
        merged: list[dict] = []

        for i in range(1, 201):
            try:
                data = self._client.get(f"archives/{i}")
                a = data.get("archive", {})
            except Exception:
                continue

            if i in existing:
                old = existing[i]
                new_entry = {
                    "id": i,
                    "name": a.get("name", old.get("name", "")),
                    "description": a.get("description", old.get("description", "")),
                    "horizon": a.get("horizon", old.get("horizon", "")),
                    "archive_type": a.get("archive_type", old.get("archive_type", "")),
                    "notes": old.get("notes", ""),
                }
                if (
                    new_entry["name"] != old.get("name")
                    or new_entry["description"] != old.get("description")
                    or new_entry["horizon"] != old.get("horizon")
                    or new_entry["archive_type"] != old.get("archive_type")
                ):
                    result.updated += 1
            else:
                new_entry = {
                    "id": i,
                    "name": a.get("name", ""),
                    "description": a.get("description", ""),
                    "horizon": a.get("horizon", ""),
                    "archive_type": a.get("archive_type", ""),
                    "notes": "",
                }
                result.added += 1

            merged.append(new_entry)

        api_ids = {e["id"] for e in merged}
        for iid in existing:
            if iid not in api_ids:
                result.removed += 1

        merged.sort(key=lambda x: x["id"])

        if not dry_run:
            new_doc = {
                "version": doc.get("version", 1),
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "archives": merged,
            }
            _save_yaml(self.YAML_FILE, new_doc)

        return result


# ---------------------------------------------------------------------------
# ESIOSCatalog (umbrella)
# ---------------------------------------------------------------------------


class ESIOSCatalog:
    """Unified catalog giving access to indicators and archives.

    Usage::

        catalog = ESIOSCatalog(client)
        catalog.indicators.list()
        catalog.archives.get(2)
        catalog.indicators.refresh(dry_run=True)

    Can also be used without a client for read-only access::

        catalog = ESIOSCatalog()
        catalog.indicators.list()
    """

    def __init__(self, client: ESIOSClient | None = None):
        self.indicators = IndicatorsCatalog(client)
        self.archives = ArchivesCatalog(client)
