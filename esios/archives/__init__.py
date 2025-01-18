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
        self.response = None

    def _get_metadata(self):
        endpoint = f"archives/{self.id}"
        data = self.client._get(endpoint, self.client.public_headers)
        return data.get("archive", {})

    def configure(
        self, date=None, start=None, end=None, date_type="datos", locale="es"
    ):
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

        if "date" in params:
            response = requests.get(self.url_download)
            response.raise_for_status()
            zip_file = BytesIO(response.content)
            self._extract_zip(zip_file, output_dir)
            return

        start_date = pd.to_datetime(params["start_date"])
        end_date = pd.to_datetime(params["end_date"])

        time_interval_chunk_limit = timedelta(weeks=5)

        output_dir = os.path.join(output_dir, self.name)
        os.makedirs(output_dir, exist_ok=True)

        if end_date - start_date <= time_interval_chunk_limit:
            response = requests.get(self.url_download)
            response.raise_for_status()
            self.response = response
            print("download without chunking")

            zip_file = BytesIO(response.content)
            self._extract_zip(zip_file, output_dir)

        else:
            current_start = start_date
            while current_start < end_date:
                # Calculate the first day of the current month
                first_day_of_month = current_start.replace(day=1)
                # Calculate the last day of the current month
                next_month = (first_day_of_month + timedelta(days=32)).replace(day=1)
                last_day_of_month = next_month - timedelta(days=1)
                current_end = min(last_day_of_month, end_date)

                start = current_start.strftime("%Y-%m-%d")
                end = current_end.strftime("%Y-%m-%d")

                self.configure(start=start, end=end, date_type=params["date_type"])

                response = requests.get(self.url_download)
                response.raise_for_status()
                self.response = response

                if len(self.metadata["archive"]["date_times"]) > 1:
                    date = self.metadata["archive"]["date_times"][0]
                    date = date.replace("-", "")
                elif "start_date" in self.metadata["archive"]["date"]:
                    date = self.metadata["archive"]["date"]["start_date"].split()[0]
                    date = date.replace("-", "")
                else:
                    print(f"No dates found for {self.name}")
                    return
                if response.headers["Content-Type"] == "zip":
                    output_dir_date = os.path.join(output_dir, date)
                    os.makedirs(output_dir_date, exist_ok=True)
                    zip_file = BytesIO(response.content)
                    # return zip_file, response
                    # return zip_file
                    try:
                        # Extract the main ZIP file
                        self._extract_zip(zip_file, output_dir_date)
                    except Exception as e:
                        print(e)
                elif response.headers["Content-Type"] == "xls":
                    path = os.path.join(output_dir, self.name + f"_{date}.xls")
                    with open(path, "wb") as f:
                        f.write(response.content)

                current_start = current_end + timedelta(days=1)

    def _extract_zip(self, file, directory):
        """
        Extracts a ZIP file to the specified directory. If there are nested ZIP files,
        they are extracted recursively.
        """

        with zipfile.ZipFile(file) as z:
            z.extractall(directory)
