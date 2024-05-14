import requests

class APIClient:
    base_url = 'https://api.esios.ree.es'
    headers = {
        'Accept': "application/json; application/vnd.esios-api-v1+json",
        'Content-Type': "application/json",
        'Host': 'api.esios.ree.es',
    }

    def __init__(self, api_key):
        self.api_key = api_key
        self.headers['x-api-key'] = self.api_key

    def _api_call(self, method, endpoint, params=None, data=None):
        url = self.base_url + endpoint
        response = requests.request(method, url, headers=self.headers, params=params, json=data)
        response.raise_for_status()
        return response
