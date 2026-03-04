---
name: esios
description: Query Spanish electricity market data (ESIOS/REE). Use when the user asks about electricity prices, generation, demand, I90 files, or any ESIOS indicator.
version: 3.0.0
---

# ESIOS Data Assistant

You have access to the `python-esios` library and CLI for querying the Spanish electricity market (ESIOS/REE).

## When to use what

- **Python scripts** (default): reproducible, composable, saveable. Use for any data work the user will want to keep or iterate on.
- **CLI**: quick one-shot lookups, exploration, sanity checks. Use when the user wants a fast answer they won't need again.
- **If unsure**: ask the user whether they want a script or a quick CLI check.

## Python Library (default)

```python
from esios import ESIOSClient

client = ESIOSClient()  # reads config file, then ESIOS_API_KEY env var

# --- Indicators ---

# Get indicator handle
handle = client.indicators.get(600)

# Historical data as DataFrame
df = handle.historical("2025-01-01", "2025-01-31")

# Filter by geography
df = handle.historical("2025-01-01", "2025-01-31", geo_ids=[3])  # España only

# Inspect geographies
handle.geos              # List of {"geo_id": int, "geo_name": str}
handle.geos_dataframe()  # DataFrame with geo_id and geo_name columns
handle.resolve_geo("España")  # Returns 3

# Search indicators
results = client.indicators.search("precio")

# Compare multiple indicators
df = client.indicators.compare([600, 10034, 10035], "2025-01-01", "2025-01-07")

# --- I90 Settlement Files ---

from esios.processing.i90 import I90Book

archive = client.archives.get(34)
books = I90Book.from_archive(archive, start="2025-05-05", end="2025-06-08")

book = books[0]
sheet = book["I90DIA03"]
df = sheet.df             # Preprocessed DataFrame with datetime index
print(sheet.frequency)    # "hourly" or "hourly-quarterly"
```

### Common Indicator IDs

| ID | Name | Description | Geos |
|----|------|-------------|------|
| 600 | Precio mercado spot | OMIE spot market price | ES, PT, FR, DE, BE, NL |
| 1001 | Precio mercado diario | Day-ahead market price | ES |
| 10033 | Demanda real | Real-time electricity demand | ES |
| 10034 | Generación eólica | Real-time wind generation | ES |
| 10035 | Generación solar FV | Real-time solar PV generation | ES |
| 1293 | Demanda prevista | Forecasted demand | ES |

Use `client.indicators.search("query")` to find more.

### Multi-Geo Indicators

Some indicators (e.g. 600) return data for multiple countries. Output is pivoted so each geography becomes a column:

```
datetime                España  Portugal  Francia  Alemania  Bélgica  Países Bajos
2025-01-01 00:00:00     63.50    63.50     72.10    58.20     58.20     58.20
```

### Geography Reference

| geo_id | geo_name |
|--------|----------|
| 1 | Portugal |
| 2 | Francia |
| 3 | España |
| 8826 | Alemania |
| 8827 | Bélgica |
| 8828 | Países Bajos |

### I90 Key Sheets

| Sheet | Description |
|-------|-------------|
| I90DIA03 | Restricciones en el Mercado Diario (curtailment) |
| I90DIA08 | Restricciones en Tiempo Real |
| I90DIA26 | Programa Base de Funcionamiento (PBF, generation program) |
| I90DIA01 | Programa PVP |
| I90DIA07 | Regulación Terciaria (mFRR) |

### Key conventions

- All timestamps are in Europe/Madrid timezone
- Date ranges > 3 weeks are auto-chunked into smaller API requests
- Data older than 48h is considered final (won't be re-fetched)
- Recent data (last 48h) is re-fetched on each request (electricity market corrections)
- Cache is per-column sparse: fetching `geo_ids=[3]` only caches that column
- Custom exceptions: `ESIOSError`, `AuthenticationError`, `APIResponseError`, `NetworkError`

## CLI Reference (quick lookups)

### Catalog (offline)

```bash
esios catalog list indicators                   # List cataloged indicators
esios catalog list indicators "precio"          # Filter by name
esios catalog list archives                     # List cataloged archives
esios catalog show indicator 600                # Details for indicator
esios catalog show archive 34                   # Details for archive
esios catalog refresh                           # Refresh from live API
esios catalog refresh --dry-run                 # Preview changes
```

### Indicators

```bash
esios indicators list                           # List all indicators
esios indicators search "precio"                # Search by name
esios indicators meta 600                       # Show metadata
esios indicators history 600 -s 2025-01-01 -e 2025-01-31
esios indicators history 600 -s 2025-01-01 -e 2025-01-31 --geo España
esios indicators history 600 -s 2025-01-01 -e 2025-01-31 --format csv --output data.csv
```

### Archives

```bash
esios archives list                             # List available archives
esios archives download 34 -s 2025-05-01 -e 2025-05-31 --output ./data
esios archives sheets 34 --date 2025-06-01      # List sheets in I90 file
```

### Exec (ad-hoc pandas)

```bash
# Indicators
esios indicators exec 600 -s 2025-01-01 -e 2025-01-31 -x "df.describe()"
esios indicators exec 600 -s 2025-01-01 -e 2025-01-31 --geo España -x "df.resample('D').mean()"
esios indicators exec 600 10034 -s 2025-01-01 -e 2025-01-31 -x "df.corr()"

# Archives (I90)
esios archives exec 34 --sheet I90DIA03 --date 2025-06-01 -x "df.groupby('Sentido')['value'].sum()"
esios archives exec 34 --sheet I90DIA03 -s 2025-05-05 -e 2025-06-08 \
  -x "df[df['Sentido']=='Bajar'].groupby('Unidad de Programación')['value'].sum().sort_values()"
```

### Cache management

```bash
esios cache status                              # Path, size, registry info
esios cache geos                                # Global geo_id → geo_name registry
esios cache clear                               # Clear indicator cache
esios cache clear --all                         # Clear everything
esios cache clear --indicator 600               # Clear one indicator
```

### Output options

```
--format table|csv|json|parquet   (default: table)
--output file.csv                 (write to file instead of stdout)
```

## Configuration

```bash
esios config set token <API_KEY>
esios config show
```

Config file: `~/.config/esios/config.toml`. API key resolution: config file > `ESIOS_API_KEY` env var.
