# Changelog

## [2.0.0] - 2026-02-27

Complete rewrite of the library with a new architecture, CLI, and caching system.

### Breaking Changes

- New package structure: `from esios import ESIOSClient` (was `from esios.client import Esios`)
- Indicators return wide-format DataFrames with geo names as columns (e.g., `"España"`)
- Archives API redesigned with `client.archives.download()`

### Added

- **CLI** (`esios`): full command-line interface with `indicators`, `archives`, `cache`, and `config` commands
- **Multi-geo support**: indicators with multiple geographies return one column per geo (e.g., España, Francia, Portugal)
- **`--geo` filter**: filter by geography ID or name in both CLI and Python API
- **Smart caching**: parquet-based cache with per-column gap detection and sparse column support
  - Per-indicator directories (`{id}/data.parquet` + `{id}/meta.json`)
  - Global geo registry (`geos.json`) learned incrementally from API responses
  - Per-endpoint catalog (`catalog.json`) with 24h TTL
  - Per-indicator metadata (`meta.json`) with 7-day TTL
  - Auto-migration from v1 flat cache files
- **`esios exec`**: ad-hoc pandas expressions on indicator data
- **`esios indicators meta`**: inspect indicator metadata
- **`esios cache status`**: Rich panel showing cache health, geos registry, and catalog info
- **`esios cache geos`**: inspect the global geography registry
- **`esios config set token`**: store API token in `~/.config/esios/config.toml`
- **I90 processing**: `I90Book` class for parsing settlement files with automatic frequency detection
- **Async client**: `AsyncESIOSClient` for concurrent requests
- **Claude Code plugin**: installable skill with full domain knowledge (`/plugin marketplace add datons/python-esios`)

### Improved

- Automatic date-range chunking for large queries (>3 weeks)
- Atomic file writes (tempfile + rename) for cache safety
- TTL-based cache freshness: recent data (48h) re-fetched for electricity market corrections
- Geo enrichment: learns missing geo mappings from API values (e.g., indicator 600 omits Países Bajos/8828)
- Custom exceptions: `ESIOSError`, `AuthenticationError`, `APIResponseError`, `NetworkError`

## [0.1.0] - 2024-05-15

Initial release.

- Basic indicator data download
- Archive file download (I90, Liquicomun, CoefK)
- I90 preprocessing with sheet access
