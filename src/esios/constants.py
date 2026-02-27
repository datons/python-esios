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

# ESIOS API limits responses to ~3 weeks of data per request
CHUNK_SIZE_DAYS = 21

TIMEZONE = "Europe/Madrid"
