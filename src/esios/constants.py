"""Constants for the ESIOS API client."""

ESIOS_API_URL = "https://api.esios.ree.es"

DEFAULT_HEADERS = {
    "Accept": "application/json; application/vnd.esios-api-v1+json",
    "Content-Type": "application/json",
    "Host": "api.esios.ree.es",
}

DEFAULT_TIMEOUT = 30.0  # seconds

MAX_RETRIES = 3
RETRY_MIN_WAIT = 2  # seconds
RETRY_MAX_WAIT = 10  # seconds

# ESIOS API chunk sizes for historical data fetching.
# High-geo indicators (40+ geos) timeout (504) at >21 days.
# Low-geo indicators handle 6+ months per request in <0.1s.
CHUNK_SIZE_DAYS = 21  # Legacy default, kept for backward compat
CHUNK_SIZE_DAYS_LOW_GEO = 180  # 6 months for indicators with few geos
CHUNK_SIZE_DAYS_HIGH_GEO = 21  # Conservative for indicators with many geos
HIGH_GEO_THRESHOLD = 15  # Indicators with >= this many geos use smaller chunks

# Concurrent chunk fetching within a single indicator.
# 4 workers gives ~17-95x speedup over sequential with no errors.
# Diminishing returns past 4 (ESIOS server becomes the bottleneck).
DEFAULT_CHUNK_WORKERS = 4

TIMEZONE = "Europe/Madrid"
