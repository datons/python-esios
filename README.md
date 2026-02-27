# python-esios

A Python library and CLI to query the Spanish electricity market API (ESIOS/REE).

Download indicators (prices, generation, demand), archives (I90 settlement files), and more.

## Install

```shell
pip install python-esios
```

## Configure your token

Request a personal token from [REE](https://www.esios.ree.es/es/pagina/api), then:

```shell
esios config set token YOUR_TOKEN
```

This stores the token in `~/.config/esios/config.toml`. Alternatively, set the `ESIOS_API_KEY` environment variable.

## CLI usage

```shell
# Search indicators
esios indicators search "precio"

# Download historical data
esios indicators history 600 --start 2025-01-01 --end 2025-01-31

# Export to CSV
esios indicators history 600 -s 2025-01-01 -e 2025-01-31 --format csv --output prices.csv

# List archives
esios archives list

# Download I90 settlement files
esios archives download 1 --start 2025-01-01 --end 2025-01-31

# Cache status
esios cache status
```

## Python usage

```python
from esios import ESIOSClient

client = ESIOSClient()

# Get indicator data as DataFrame
handle = client.indicators.get(600)  # PVPC price
df = handle.historical("2025-01-01", "2025-01-31")

# Search indicators
results = client.indicators.search("precio")

# Download archives
client.archives.download(1, start="2025-01-01", end="2025-01-31", output_dir="./data")
```

## Common indicators

| ID | Name | Description |
|----|------|-------------|
| 600 | PVPC | Voluntary price for small consumers |
| 1001 | Day-ahead price | OMIE spot market price |
| 10033 | Demand | Real-time electricity demand |
| 10034 | Wind generation | Real-time wind generation |
| 10035 | Solar PV generation | Real-time solar generation |

## License

GPL-3.0
