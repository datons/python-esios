import os
import requests
import warnings

class ESIOSClient:
    def __init__(self, api_key_esios=None, api_key_premium=None):
        self.public_base_url = 'https://api.esios.ree.es'
        self.private_base_url = 'https://private-api-url-for-forecast'  # Replace with your private API URL
        
        self.api_key_esios = api_key_esios if api_key_esios else os.getenv('ESIOS_API_KEY')
        if not self.api_key_esios:
            raise ValueError("API key must be set in the 'ESIOS_API_KEY' environment variable or passed as a parameter")
        
        self.api_key_premium = api_key_premium if api_key_premium else os.getenv('ESIOS_API_KEY_PREMIUM')
        if not self.api_key_premium:
            warnings.warn("API key for premium services is not set")
        
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

    def get_indicator(self, indicator_id: str) -> 'IndicatorData':
        from .indicators import IndicatorData
        return IndicatorData(self, indicator_id)

    def get_archive(self, archive_id: str) -> 'ArchiveData':
        from .archives import ArchiveData
        return ArchiveData(self, archive_id)

class BaseData:
    def __init__(self, client: ESIOSClient):
        self.client = client

    def get(self, url, headers, params):
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response
