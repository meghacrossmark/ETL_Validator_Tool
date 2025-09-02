from pathlib import Path
import pandas as pd
import tempfile
import zipfile
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError


class AzureBlobLoader:
    _FILE_HANDLERS = {
        'txt': pd.read_csv,
        'xlsx': pd.read_excel,
        'json': pd.read_json,
        'csv': pd.read_csv
    }

    def __init__(self, account_url: str, sas_token: str, container_name: str, separator: str = '\t'):
        self.account_url = account_url
        self.sas_token = sas_token
        self.container_name = container_name
        self.separator = separator

    def _download_blob_to_tempfile(self, blob_name: str) -> Path:
        # Create clients here, only when downloading
        blob_service_client = BlobServiceClient(account_url=self.account_url, credential=self.sas_token)
        container_client = blob_service_client.get_container_client(self.container_name)
        blob_client = container_client.get_blob_client(blob_name)

        file_ext = Path(blob_name).suffix.lower()
        with tempfile.NamedTemporaryFile(delete=False, suffix=file_ext) as tmp_file:
            tmp_path = Path(tmp_file.name)
            tmp_file.write(blob_client.download_blob().readall())
        return tmp_path

    def _extract_and_combine_zip_files(self, zip_path: Path) -> pd.DataFrame:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            extract_dir = Path(tempfile.mkdtemp())
            zip_ref.extractall(extract_dir)

            dfs = []
            for file in extract_dir.rglob("*"):
                ext = file.suffix[1:].lower()
                if ext in self._FILE_HANDLERS:
                    try:
                        reader = self._FILE_HANDLERS[ext]
                        kwargs = {'sep': self.separator} if ext == 'txt' else {}
                        df = reader(file, **kwargs)
                        df['__source_file__'] = file.name  # optional
                        dfs.append(df)
                    except Exception as e:
                        print(f"Skipping file '{file.name}' due to error: {e}")

            if not dfs:
                raise ValueError("No supported files found in ZIP.")

            return pd.concat(dfs, ignore_index=True)

    def _process_single_file(self, file_path: Path) -> pd.DataFrame:
        ext = file_path.suffix[1:].lower()
        if ext not in self._FILE_HANDLERS:
            raise ValueError(f"Unsupported file type: {ext}")

        reader = self._FILE_HANDLERS[ext]
        kwargs = {'sep': self.separator} if ext == 'txt' else {}
        return reader(file_path, **kwargs)

    def load(self, blob_name: str) -> pd.DataFrame:
        try:
            local_path = self._download_blob_to_tempfile(blob_name)
            ext = local_path.suffix[1:].lower()

            if ext == 'zip':
                return self._extract_and_combine_zip_files(local_path)
            else:
                return self._process_single_file(local_path)

        except ResourceNotFoundError:
            raise FileNotFoundError(f"Blob '{blob_name}' not found in container '{self.container_name}'.")
        except Exception as e:
            raise ValueError(f"Failed to load blob '{blob_name}': {str(e)}")
