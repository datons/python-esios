import os
import zipfile
import requests
from io import BytesIO

def extract_zip(file, directory):
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
                    extract_zip(nested_file, nested_dir)
                os.remove(member_path)  # Remove the nested ZIP file after extraction
                
                
def download_file(url, output_dir):
    """
    Downloads a ZIP file from the given URL and extracts its contents to the specified output directory.
    Handles nested ZIP files by extracting them recursively.
    """
    response = requests.get(url)
    response.raise_for_status()
    
    zip_file = BytesIO(response.content)
    
    # Create the output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Extract the main ZIP file
    extract_zip(zip_file, output_dir)