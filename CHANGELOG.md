# Changelog

## [2.3.0](https://github.com/datons/python-esios/compare/python-esios-v2.2.0...python-esios-v2.3.0) (2026-03-16)


### Features

* restore column_name parameter in historical() ([a967b6f](https://github.com/datons/python-esios/commit/a967b6f54cd4e84d18469d99f0d4e03665b73e81))


### Bug Fixes

* handle DST transitions in I90 datetime parsing ([aa1deef](https://github.com/datons/python-esios/commit/aa1deefd442f2c53a376a4f3773b6fffea562121))

## [2.2.0](https://github.com/datons/python-esios/compare/python-esios-v2.1.0...python-esios-v2.2.0) (2026-03-04)


### Features

* add YAML-backed catalog with enriched indicator metadata ([00cc4b6](https://github.com/datons/python-esios/commit/00cc4b69a66268d6d4d6e63ce7d4d8f91c7e813a))


### Bug Fixes

* update skill with catalog commands and Python-first approach ([6051fb3](https://github.com/datons/python-esios/commit/6051fb31cbea2dce0b5eaeb87a561c33d8d8d4e6))

## [2.1.0](https://github.com/datons/python-esios/compare/python-esios-v2.0.2...python-esios-v2.1.0) (2026-03-04)


### Features

* add archives sheets and exec CLI commands for I90 processing ([b54f94a](https://github.com/datons/python-esios/commit/b54f94adfa56ee2150fc85f92055b002b0d3e9bb))
* add static archives catalog (153 archives) and improve download API ([22e6095](https://github.com/datons/python-esios/commit/22e60953fb3d5314452c03807be3e12595b88fd4))
* replace manual PyPI workflow with release-please ([3c0dcb3](https://github.com/datons/python-esios/commit/3c0dcb35b01a47ea489d5a1ecb891d6805486356))


### Bug Fixes

* correct author email to datons.com ([8e17132](https://github.com/datons/python-esios/commit/8e1713248a56928ba61898b5b84b0b4689798ce3))
* rename release-please config to expected non-dotted filename ([2f53b76](https://github.com/datons/python-esios/commit/2f53b7658c3f3e02e53dcbb3a2fe149b8f16ad7c))
* update archives test to match static catalog default ([9bbd98b](https://github.com/datons/python-esios/commit/9bbd98b56dee2ae1cfbed24537ad989f5214ae76))


### Performance

* lazy sheet loading in I90Book — only read sheets on demand ([2dfe941](https://github.com/datons/python-esios/commit/2dfe941bf71c74bb0bd07c72c90e92b61a0d3059))

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
