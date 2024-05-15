import requests
import os

class APIClient:

    def __init__(self, api_key=None):
        self.base_url = 'https://api.esios.ree.es'
        self.api_key = api_key if api_key else os.getenv('ESIOS_API_KEY')
        if not self.api_key:
            raise ValueError("API key must be provided directly or set in the 'ESIOS_API_KEY' environment variable")
        self.headers = {
            'Accept': "application/json; application/vnd.esios-api-v1+json",
            'Content-Type': "application/json",
            'Host': 'api.esios.ree.es',
            'x-api-key': self.api_key
        }

    def _api_call(self, method, endpoint, params=None, data=None):
        url = self.base_url + endpoint
        response = requests.request(method, url, headers=self.headers, params=params, json=data)
        response.raise_for_status()
        return response