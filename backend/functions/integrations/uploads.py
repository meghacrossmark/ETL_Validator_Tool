from pathlib import Path
import pandas as pd

class Uploads:
    _BASE_UPLOAD_PATH = Path(__file__).parent.parent.parent / "resources"

    _FILE_HANDLERS = {
        'txt': pd.read_csv,
        'csv': pd.read_csv,
        'xlsx': pd.read_excel,
        'json': pd.read_json
    }

    def __init__(self, uploaded_file: str | dict, separator: str = ','):
        self.uploaded_file = uploaded_file
        self.separator = separator
        # Initial load: do NOT drop blank rows here
        self.df_file = self._process_file(drop_blank_rows=False)

    def _detect_header_row(self, file_path: Path, file_ext: str, sep: str = ',') -> int:
        max_scan_rows = 50  # Scan first 50 rows for header
        if file_ext in ['csv', 'txt']:
            df_sample = pd.read_csv(file_path, header=None, sep=sep, nrows=max_scan_rows)
        elif file_ext == 'xlsx':
            df_sample = pd.read_excel(file_path, header=None, nrows=max_scan_rows)
        else:
            return 0  # fallback default header row

        for i in range(min(max_scan_rows, len(df_sample))):
            row = df_sample.iloc[i]
            # Check row is non-null, non-empty (trimmed), and values are unique
            if (
                row.notnull().all()
                and row.apply(lambda x: str(x).strip() != '').all()
                and row.nunique() == len(row)
            ):
                return i
        return 0

    def _process_file(self, drop_blank_rows: bool = False) -> Path:
        file_name = (
            self.uploaded_file["selected_fileName"]
            if isinstance(self.uploaded_file, dict)
            else self.uploaded_file
        )
        file_path = self._BASE_UPLOAD_PATH / file_name
        file_ext = file_path.suffix[1:].lower()
        file_stem = file_path.stem
        output_csv = self._BASE_UPLOAD_PATH / f"{file_stem}.csv"

        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        if file_ext == 'csv':
            header_row = self._detect_header_row(file_path, file_ext, sep=self.separator)
            df = pd.read_csv(file_path, header=header_row, sep=self.separator)
        elif file_ext in self._FILE_HANDLERS:
            header_row = self._detect_header_row(file_path, file_ext)
            reader = self._FILE_HANDLERS[file_ext]
            kwargs = {'header': header_row}
            if file_ext == 'txt':
                kwargs['sep'] = self.separator
            df = reader(file_path, **kwargs)
        else:
            raise ValueError(f"Unsupported file type: {file_ext}")

        if drop_blank_rows:
            # Drop rows where all columns are NaN or empty string after stripping
            df = df.dropna(how='all')
            df = df.loc[~(df.applymap(lambda x: str(x).strip() == '').all(axis=1))].reset_index(drop=True)
        else:
            df = df.reset_index(drop=True)

        df.to_csv(output_csv, index=False)
        return output_csv

    def get_uploaded_file_as_DF(self, is_compare: bool) -> pd.DataFrame:
        try:
            if not is_compare:
                return pd.read_csv(self.df_file)
            else:
                from functions.integrations.Enhanced_CSV_Reader import Enhanced_CSV_Reader
                return Enhanced_CSV_Reader(self.df_file).read_csv()
        except Exception as e:
            raise ValueError(f"Error reading CSV file: {str(e)}")