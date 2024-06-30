import os
import requests
import warnings
from .indicators import Indicators
from .archives import Archives
from .offer_indicators import OfferIndicators

class ESIOSClient:
    def __init__(self, api_key_esios=None, api_key_premium=None):
        self.public_base_url = 'https://api.esios.ree.es'
        self.private_base_url = 'https://private-api-url-for-forecast'  # Replace with your private API URL
        
        self.api_key_esios = api_key_esios if api_key_esios else os.getenv('ESIOS_API_KEY')
        if not self.api_key_esios:
            raise ValueError("API key must be set in the 'ESIOS_API_KEY' environment variable or passed as a parameter")
        
        self.api_key_premium = api_key_premium if api_key_premium else os.getenv('ESIOS_API_KEY_PREMIUM')
        
        self.public_headers = {
            'Accept': "application/json; application/vnd.esios-api-v1+json",
            'Content-Type': "application/json",
            'Host': 'api.esios.ree.es',
            'x-api-key': self.api_key_esios
        }
        
        self.private_headers = {
            'Accept': "application/json",
            'Content-Type': "application/json",
            'Authorization': f'Bearer {self.api_key_premium}'
        }
        
        self.session = requests.Session()
        
    def _get(self, endpoint, headers, params=None):
        url = self._construct_url(endpoint)
        
        try:
            response = self.session.get(url, headers=headers, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as http_err:
            warnings.warn(f"HTTP error occurred: {http_err}")
        except Exception as err:
            warnings.warn(f"An error occurred: {err}")

    def _construct_url(self, endpoint):
        return f"{self.public_base_url}/{endpoint}"
    
    def endpoint(self, name):
        if name == 'indicators':
            return Indicators(self)
        elif name == 'archives':
            return Archives(self)
        elif name == 'offer_indicators':
            return OfferIndicators(self)
        else:
            raise ValueError(f"Unknown endpoint: {name}")
