import requests
import os

class APIClient:
    base_url = 'https://api.esios.ree.es'
    headers = {
        'Accept': "application/json; application/vnd.esios-api-v1+json",
        'Content-Type': "application/json",
        'Host': 'api.esios.ree.es',
    }

    def __init__(self, api_key=None):
        if api_key is None:
            api_key = os.getenv('ESIOS_API_KEY')  # Try to get the API key from environment variable
            if api_key is None:
                raise ValueError("API key must be provided either through constructor or as an environment variable 'ESIOS_API_KEY'")
        self.api_key = api_key
        self.headers['x-api-key'] = self.api_key

    def _api_call(self, method, endpoint, params=None, data=None):
        url = self.base_url + endpoint
        response = requests.request(method, url, headers=self.headers, params=params, json=data)
        response.raise_for_status()
        return response