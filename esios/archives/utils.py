import os
import zipfile
import shutil
from datetime import datetime


class NestedZipExtractor:
    def __init__(self, output_dir):
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def ensure_folder(self, parent_dir=None):
        target_dir = parent_dir or self.output_dir
        os.makedirs(target_dir, exist_ok=True)
        return target_dir

    def extract_zip_object(self, zip_obj, extract_to):
        zip_obj.extractall(extract_to)

    def process_zip_object(self, zip_obj, parent_dir=None):
        extract_to = self.ensure_folder(parent_dir)
        self.extract_zip_object(zip_obj, extract_to)

        for root, dirs, files in os.walk(extract_to):
            for file in files:
                file_path = os.path.join(root, file)
                if zipfile.is_zipfile(file_path):
                    with zipfile.ZipFile(file_path, "r") as nested_zip_obj:
                        self.process_zip_object(nested_zip_obj, root)
                    os.remove(file_path)

    def extract(self, zip_obj, save_zip=True):
        if not self.contains_only_directories(zip_obj):
            print("ZIP file contains files.")
            if save_zip:
                print("Saving ZIP file.")
                self.save_zip_file(zip_obj)
            return

        self.process_zip_object(zip_obj)

    def contains_only_directories(self, zip_obj):
        for info in zip_obj.infolist():
            if (
                not info.is_dir()
                and not info.filename.endswith(".zip")
                and "/" not in info.filename.strip("/")
            ):
                return False
        return True

    def save_zip_file(self, zip_obj):
        """
        Save the ZIP file to the output directory without extracting.
        """
        zip_name = f"archive_{datetime.now().strftime('%Y%m%d')}.zip"
        zip_path = os.path.join(self.output_dir, zip_name)
        with open(zip_path, "wb") as f:
            shutil.copyfileobj(zip_obj.fp, f)
        print(f"ZIP file saved at: {zip_path}")


# Example usage:
# extractor = NestedZipExtractor("output_directory")
# with zipfile.ZipFile('path/to/nested.zip', 'r') as zip_obj:
#     extractor.extract(zip_obj)
