import os
import pandas as pd
from esios.api_client import APIClient
from esios.utils import preprocessing

class Archives(APIClient):
    def __init__(self, api_key):
        super().__init__(api_key)

    def list(self, params=None):
        endpoint = "/archives"
        response = self._api_call('GET', endpoint)
        return response.json()

    def get(self, archive_id, params=None, write_home=None):
        endpoint = f"/archives/{archive_id}/download"
        response = self._api_call('GET', endpoint, params=params)

        if write_home:
            path = os.path.join(write_home, f'{archive_id}.zip')
            with open(path, 'wb') as f:
                f.write(response.content)
            preprocessing.unzip_files_and_remove(path)

        return ArchiveData(response.content)

class ArchiveData:
    def __init__(self, data):
        self.data = data

    def to_pandas(self):
        return pd.DataFrame(self.data)
