import pandas as pd
import zipfile
import os
from io import BytesIO
import requests
from datetime import timedelta, date
from pathlib import Path
import calendar
from .utils import ZipExtractor


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
        print(data)
        return data.get("archive", {})

    def configure(
        self, date=None, start=None, end=None, date_type="datos", locale="es"
    ):
        params = {"date_type": date_type, "locale": locale}
        if date:
            params["date"] = date + "T00:00:00"
        elif start and end:
            params["start_date"] = start + "T00:00:00"
            params["end_date"] = end + "T23:59:59"
        else:
            raise ValueError("Either 'date', or 'start' and 'end' dates must be provided")

        endpoint = f"archives/{self.id}"
        response = self.client._get(endpoint, self.client.public_headers, params=params)
        self.metadata = response
        data = self.metadata["archive"]["download"]
        self.name = data["name"]
        self.url_download = "https://api.esios.ree.es" + data["url"]

    def download_and_extract(self, output_dir="."):
        params = self.metadata["archive"]["date"]
        horizon = self.metadata["archive"].get("horizon", "D")  # Always access horizon from inside 'archive'

        print(horizon)
        print(params)
        if "date" in params:
            response = requests.get(self.url_download)
            response.raise_for_status()
            zip_file = BytesIO(response.content)
            if horizon == "M":
                # Use the first date in date_times as the canonical month start
                date_folder = pd.to_datetime(self.metadata["archive"]["date_times"][0]).strftime("%Y%m%d")
            else:
                date_folder = pd.to_datetime(self.metadata["archive"]["date"]["date"]).strftime("%Y%m%d")
            output_dir = Path(output_dir) / self.name / f'{self.name}_{date_folder}'
            zx = ZipExtractor(zip_file, output_dir)
            zx.unzip()
            return

        start_date = pd.to_datetime(params["start_date"])
        end_date = pd.to_datetime(params["end_date"])
        base_dir = Path(output_dir) / self.name
        base_dir.mkdir(parents=True, exist_ok=True)

        current_start = start_date

        while current_start <= end_date:
            if horizon == "M":
                current_key = current_start.strftime("%Y%m")
                folder = base_dir / f"{self.name}_{current_key}"
                next_month = (current_start.replace(day=1) + timedelta(days=32)).replace(day=1)
                current_end = min(next_month - timedelta(days=1), end_date)
            else:
                current_key = current_start.strftime("%Y%m%d")
                folder = base_dir / f"{self.name}_{current_key}"
                current_end = current_start

            if folder.exists() and any(folder.glob("*")):
                print(f"Skipping already downloaded: {folder}")
                current_start = current_end + timedelta(days=1)
                continue

            start = current_start.strftime("%Y-%m-%d")
            end = current_end.strftime("%Y-%m-%d")

            self.configure(start=start, end=end, date_type=params["date_type"])

            try:
                response = requests.get(self.url_download)
                response.raise_for_status()
            except requests.HTTPError:
                print(f"Failed to download for {start} to {end}, skipping.")
                current_start = current_end + timedelta(days=1)
                continue

            self.response = response
            self._unzip_file(response, folder)

            current_start = current_end + timedelta(days=1)

    def _unzip_file(self, response, output_dir):
        zip_file = zipfile.ZipFile(BytesIO(response.content))
        metadata = zip_file.NameToInfo

        has_nested_zip = any(filename.endswith(".zip") for filename in metadata)
        if not has_nested_zip:
            date_val = self.metadata["archive"]["date_times"][0]
            date_obj = pd.to_datetime(date_val)
            output_dir = Path(output_dir).parent / f'{self.name}_{date_obj.strftime("%Y%m%d")}'

        zx = ZipExtractor(response.content, output_dir)
        zx.unzip()