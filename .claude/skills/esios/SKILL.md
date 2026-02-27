---
name: esios
description: Query Spanish electricity market data (ESIOS/REE). Use when the user asks about electricity prices, generation, demand, I90 files, or any ESIOS indicator.
version: 1.0.0
---

# ESIOS Data Assistant

You have access to the `python-esios` library for querying the Spanish electricity market.

## Quick Start

```python
from esios import ESIOSClient

client = ESIOSClient()  # reads ESIOS_API_KEY from env

# Get indicator data as DataFrame
handle = client.indicators.get(600)  # PVPC price
df = handle.historical("2025-01-01", "2025-01-31")

# Search indicators
results = client.indicators.search("precio")

# Compare multiple indicators
df = client.indicators.compare([600, 10034, 10035], "2025-01-01", "2025-01-07")

# Download archives
client.archives.download(1, start="2025-01-01", end="2025-01-31", output_dir="./data")
```

## Common Indicator IDs

| ID | Name | Description |
|----|------|-------------|
| 600 | PVPC | Voluntary price for small consumers |
| 10034 | Wind generation | Real-time wind generation |
| 10035 | Solar PV generation | Real-time solar generation |
| 10033 | Demand | Real-time electricity demand |
| 1001 | Day-ahead price | OMIE spot market price |

Use `client.indicators.search("query")` to find more.

## I90 Processing

```python
from esios.processing import I90Book

book = I90Book("path/to/I90DIA_20250101.xls")
sheet = book["3.1"]  # Access specific sheet
df = sheet.df        # Preprocessed DataFrame with datetime index
print(sheet.frequency)  # "hourly" or "hourly-quarterly"
```

## CLI Usage

```bash
esios indicators list
esios indicators search "precio"
esios indicators history 600 --start 2025-01-01 --end 2025-01-31
esios indicators history 600 -s 2025-01-01 -e 2025-01-31 --format csv --output data.csv
esios archives list
esios archives download 1 --start 2025-01-01 --end 2025-01-31 --output ./data
esios config set token <API_KEY>
```

## Key Conventions

- All timestamps are in Europe/Madrid timezone
- Date ranges > 3 weeks are auto-chunked
- Archives support skip-existing (won't re-download)
- I90 sheets detect hourly vs quarter-hourly frequency automatically
- Use context manager for proper cleanup: `with ESIOSClient() as client:`
- Custom exceptions: `ESIOSError`, `AuthenticationError`, `APIResponseError`, `NetworkError`
