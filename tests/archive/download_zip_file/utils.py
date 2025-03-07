
from pathlib import Path
import zipfile
import warnings

class ZipExtractor:
    def __init__(self, zip_file: zipfile.ZipFile, folder: str | Path, overwrite: bool = True):
        self.zip_file = zip_file
        self.folder = Path(folder) if isinstance(folder, str) else folder
        self.overwrite = overwrite
        if not self.folder.exists():
            self.folder.mkdir(parents=True)

    def unzip(self):
        metadata = self.zip_file.NameToInfo

        for filename, zipinfo in metadata.items():
            if filename.endswith('.zip'):
                folder_nested = self.folder / filename.split('.')[0]
                zip_file_nested = zipfile.ZipFile(self.zip_file.open(filename))
                nested_extractor = ZipExtractor(zip_file_nested, folder_nested)
                nested_extractor.unzip()
            else:
                path_output = self.folder / filename
                if path_output.exists():
                    if self.overwrite:
                        warnings.warn(f'File {filename} already exists in {self.folder}')
                        self.zip_file.extract(zipinfo, self.folder)                        
                else:
                    self.zip_file.extract(zipinfo, self.folder)