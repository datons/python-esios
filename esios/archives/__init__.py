import pandas as pd
import zipfile
import os
from io import BytesIO
import requests
from datetime import timedelta
from pathlib import Path

class Archives:
    def __init__(self, client):
        self.client = client

    def list(self):
        endpoint = "archives"
        data = self.client._get(endpoint, self.client.public_headers)
        return pd.DataFrame(data.get("archives", []))

    def select(self, id):
        return Archive(self.client, id)


class Archive:
    def __init__(self, client, id):
        self.client = client
        self.id = id
        self.metadata = self._get_metadata()

    def _get_metadata(self):
        endpoint = f"archives/{self.id}"
        data = self.client._get(endpoint, self.client.public_headers)
        return data.get("archive", {})

    def configure(self, date=None, start=None, end=None, date_type = "datos", locale="es"):
        """
        Configures the archive to download.

        Parameters
        ----------

        date_type : str, default 'real'
            The type of date to download. Must be 'datos' or 'publicacion'.

        locale : str, default 'es'
            The locale of the data. Must be 'es' or 'en'.
        """

        params = {"date_type": date_type, "locale": locale}
        if date:
            params["date"] = date + "T00:00:00"
        elif start and end:
            params["start_date"] = start + "T00:00:00"
            params["end_date"] = end + "T23:59:59"
        else:
            raise ValueError(
                "Either 'date', or 'start' and 'end' dates must be provided"
            )

        endpoint = f"archives/{self.id}"

        response = self.client._get(endpoint, self.client.public_headers, params=params)

        self.metadata = response

        data = self.metadata["archive"]["download"]

        self.name = data["name"]
        self.url_download = "https://api.esios.ree.es" + data["url"]

    def download_and_extract(self, output_dir="."):
        """
        Downloads the archive file and extracts its contents to the specified output directory.
        
        It takes ~4 minutes to download and extract one year of data.

        Parameters
        ----------

        output_dir : str, default '.'
            The directory where the archive contents will be extracted. If the directory does not exist, it will be created.

        Returns
        -------

        str
            The path to the extracted file.
        """

        params = self.metadata["archive"]["date"]
        
        if 'date' in params:
            response = requests.get(self.url_download)
            response.raise_for_status()
            zip_file = BytesIO(response.content)
            self._extract_zip(zip_file, output_dir)
            return

        start_date = pd.to_datetime(params["start_date"])
        end_date = pd.to_datetime(params["end_date"])

        three_weeks = timedelta(weeks=3)

        output_dir = os.path.join(output_dir, self.name)
        os.makedirs(output_dir, exist_ok=True)

        if end_date - start_date <= three_weeks:
            response = requests.get(self.url_download)
            response.raise_for_status()

            zip_file = BytesIO(response.content)

            # Extract the main ZIP file
            self._extract_zip(zip_file, output_dir)

        else:
            current_start = start_date
            while current_start < end_date:
                current_end = min(current_start + three_weeks, end_date)

                start = current_start.strftime("%Y-%m-%d")
                end = current_end.strftime("%Y-%m-%d")
                self.configure(start=start, end=end, date_type=params["date_type"])

                response = requests.get(self.url_download)
                response.raise_for_status()
                
                zip_file = BytesIO(response.content)

                try:
                    # Extract the main ZIP file
                    self._extract_zip(zip_file, output_dir)
                except Exception as e:
                    print(response.content)

                current_start = current_end + timedelta(days=1)

    def _extract_zip(self, file, directory):
        """
        Extracts a ZIP file to the specified directory. If there are nested ZIP files,
        they are extracted recursively.
        """
        
        root = self.metadata["archive"]["name"]
        with zipfile.ZipFile(file) as z:
            incoming = z.namelist()
            
            # Check if the ZIP contains a single file
            if len(incoming) == 1 and not incoming[0].endswith('/'):
                file = incoming[0]
                folder = file.split('.')[0]
                path = Path(os.path.join(directory, root, folder, file))
                path.parent.mkdir(parents=True, exist_ok=True)
                z.extract(file, path.parent)  # Extract the single file into the folder
                return  # Exit after handling the single file

            # Extract all files
            z.extractall(directory)
            
            for member in incoming:
                member_path = os.path.join(directory, member)
                if member.endswith('.xlsx') or member.endswith('.xls'):
                    # If it's an Excel file, ensure it's in its own folder
                    folder_name = os.path.splitext(member)[0]
                    folder = os.path.join(directory, folder_name)
                    os.makedirs(folder, exist_ok=True)
                    os.rename(member_path, os.path.join(folder, member))
                elif zipfile.is_zipfile(member_path):
                    # If it's a nested ZIP file, extract it recursively
                    nested_dir = os.path.splitext(member_path)[0]
                    os.makedirs(nested_dir, exist_ok=True)
                    with open(member_path, "rb") as nested_file:
                        self._extract_zip(nested_file, nested_dir)
                    os.remove(member_path)  # Remove the nested ZIP file after extraction
