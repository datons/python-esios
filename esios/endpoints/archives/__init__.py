import os
import pandas as pd
from esios.api_client import APIClient
from esios.endpoints.archives import utils

class Archives(APIClient):
    def __init__(self, api_key=None):
        super().__init__(api_key)
        
    def list(self, as_dataframe=False):
        endpoint = "/archives"
        response = self._api_call('GET', endpoint)
        data = response.json()
        
        if as_dataframe:
            return pd.DataFrame(data['archives'])
        else:
            return data

    def get(self, archive_id, start_date=None, end_date=None, date_type='datos'):
        """
        start_date : str, optional
            The start date of the data to retrieve (format: 'YYYY-MM-DD').
        end_date : str, optional
            The end date of the data to retrieve (format: 'YYYY-MM-DD').
        date_type : str {'datos', 'publicacion'}, default 'datos'
            The type of date to filter by. Options are 'datos' (data date) or 'publicacion' (publication date).
        """
        
        params = {'date_type': date_type}
        
        if start_date:
            params['start_date'] = start_date
        if end_date:
            params['end_date'] = end_date
        
        endpoint = f"/archives/{archive_id}"
        response = self._api_call('GET', endpoint, params=params)
    
        return ArchiveData(response.json(), self.base_url)


class ArchiveData:
    def __init__(self, data, base_url):
        self.data = data
        self.base_url = base_url
        
    def download_and_extract(self, output_dir='.'):
        """
        Downloads the archive file and extracts its contents to the specified output directory.
        
        Parameters
        ----------
        output_dir : str, default '.'
            The directory where the archive contents will be extracted.
        """
        data = self.data['archive']['download']
        name = data['name']
        url = self.base_url + data['url']
        
        path = os.path.join(output_dir, name)
        utils.download_file(url, path)
        
        
    def get_metadata(self):
        """
        Returns the metadata of the archive.
        
        Returns
        -------
        dict
            A dictionary containing the metadata of the archive.
        """
        return self.data