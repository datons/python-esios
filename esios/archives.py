from .client import BaseData, ESIOSClient

import pandas as pd
import zipfile
import os
from io import BytesIO
import requests

class ArchiveData(BaseData):
    def __init__(self, client: ESIOSClient, archive_id: str):
        super().__init__(client)
        self.archive_id = archive_id
        self.historical_params = None
        self.historical_data = None
        self.forecast_params = None
        self.forecast_data = None
        
        
    def configure_request(self, date=None, start=None, end=None, data_type='real'):
        
        if data_type == 'real':
            data_type = 'datos'
        elif data_type == 'publication':
            data_type = 'publicacion'
        else:
            raise ValueError("Invalid data type. Must be 'real' or 'publication'")
        
        if date:
            date = date + 'T00:00:00'
            params = {'date_type': data_type, 'date': date}
        elif start and end:
            start = start + 'T00:00:00'
            end = end + 'T23:59:59'
            params = {'date_type': data_type, 'start_date': start, 'end_date': end}
        else:
            raise ValueError("Either 'date' or, 'start' and 'end' dates must be provided")
        
        endpoint = f"{self.client.public_base_url}/archives/{self.archive_id}"
        response = self.get(endpoint, self.client.public_headers, params=params)
        
        self.metadata = response.json()
        
        data = self.metadata['archive']['download']
        
        self.name = data['name']
        self.url_download = 'https://api.esios.ree.es' + data['url']
        
    def _extract_zip(self, file, directory):
        """
        Extracts a ZIP file to the specified directory. If there are nested ZIP files,
        they are extracted recursively.
        """
        with zipfile.ZipFile(file) as z:
            z.extractall(directory)
            for member in z.namelist():
                member_path = os.path.join(directory, member)
                if zipfile.is_zipfile(member_path):
                    nested_dir = os.path.splitext(member_path)[0]
                    os.makedirs(nested_dir, exist_ok=True)
                    with open(member_path, 'rb') as nested_file:
                        self._extract_zip(nested_file, nested_dir)
                    os.remove(member_path)  # Remove the nested ZIP file after extraction
        
        
    def download_and_extract(self, output_dir='.'):
        """
        Downloads the archive file and extracts its contents to the specified output directory.
        
        Parameters
        ----------
        
        output_dir : str, default '.'
            The directory where the archive contents will be extracted. If the directory does not exist, it will be created.
            
        Returns
        -------
        
        str
            The path to the extracted file.
        """
        
        
        response = requests.get(self.url_download)
        response.raise_for_status()
        
        zip_file = BytesIO(response.content)
        
        output_dir = os.path.join(output_dir, self.name)
        os.makedirs(output_dir, exist_ok=True)
        
        # Extract the main ZIP file
        self._extract_zip(zip_file, output_dir)
