import csv
import os
import re
import datetime
from collections import Counter
import pandas as pd


class Enhanced_CSV_Reader:
    def __init__(self, filepath):
        self.filepath = filepath
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"The file {filepath} does not exist.")

        # Supported data types
        self.supported_dtypes = [
            'int64', 'float64', 'uint64', 'datetime64[ns]', 'timedelta64[ns]',
            'string', 'bool', 'object', 'category', 'complex128'
        ]

    def get_headers(self):
        with open(self.filepath, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            headers = next(reader)
        return [header.strip() for header in headers]

    def _count_rows(self):
        with open(self.filepath, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            next(reader)  # Skip header
            return sum(1 for _ in reader)

    def _get_sample_data(self, sample_percentage=5):
        total_rows = self._count_rows()
        sample_size = max(int(total_rows * sample_percentage / 100), 10)  # At least 10 rows

        with open(self.filepath, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            headers = next(reader)  # Skip header

            if total_rows <= sample_size:
                return list(reader)

            step = max(1, total_rows // sample_size)
            samples = []
            for i, row in enumerate(reader):
                if i % step == 0 and len(samples) < sample_size:
                    samples.append(row)

            return samples

    def _check_data_type(self, value, type_check):
        if not isinstance(value, str):
            return False

        if value.strip() == '':
            return False

        if type_check == 'int':
            try:
                int(value)
                return True
            except (ValueError, TypeError):
                return False

        elif type_check == 'float':
            try:
                float(value)
                return '.' in value or 'e' in value.lower()
            except (ValueError, TypeError):
                return False

        elif type_check == 'bool':
            return value.strip().lower() in ['true', 'false', '0', '1', 'yes', 'no', 't', 'f']

        elif type_check == 'datetime':
            date_patterns = [
                r'\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
                r'\d{2}/\d{2}/\d{4}',  # MM/DD/YYYY
                r'\d{2}-\d{2}-\d{4}',  # DD-MM-YYYY
                r'\d{4}/\d{2}/\d{2}',  # YYYY/MM/DD
                r'\d{2}/\d{2}/\d{2}',  # MM/DD/YY
                r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}',  # YYYY-MM-DD HH:MM:SS
                r'\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}',  # YYYY/MM/DD HH:MM:SS
            ]

            for pattern in date_patterns:
                if re.match(pattern, value.strip()):
                    try:
                        formats = [
                            '%Y-%m-%d', '%m/%d/%Y', '%d-%m-%Y', '%Y/%m/%d', '%m/%d/%y',
                            '%Y-%m-%d %H:%M:%S', '%Y/%m/%d %H:%M:%S'
                        ]
                        for fmt in formats:
                            datetime.datetime.strptime(value.strip(), fmt)
                            return True
                    except ValueError:
                        pass
            return False

        elif type_check == 'timedelta':
            timedelta_patterns = [
                r'\d+:\d{2}:\d{2}',  # HH:MM:SS
                r'\d+ days? \d+:\d{2}:\d{2}',  # D days HH:MM:SS
            ]

            for pattern in timedelta_patterns:
                if re.match(pattern, value.strip()):
                    return True
            return False

        elif type_check == 'complex':
            try:
                complex(value.replace(' ', ''))
                return 'j' in value or 'i' in value
            except (ValueError, TypeError):
                return False

        return False

    def _is_int(self, value):
        return self._check_data_type(value, 'int')

    def _is_float(self, value):
        return self._check_data_type(value, 'float')

    def _is_bool(self, value):
        return self._check_data_type(value, 'bool')

    def _is_datetime(self, value):
        return self._check_data_type(value, 'datetime')

    def _is_timedelta(self, value):
        return self._check_data_type(value, 'timedelta')

    def _is_complex(self, value):
        return self._check_data_type(value, 'complex')

    def _infer_column_type(self, column_values):
        non_empty_values = [val for val in column_values if val.strip()]

        if not non_empty_values:
            return 'object'

        type_counts = Counter()

        for value in non_empty_values:
            if self._is_int(value):
                if int(value) >= 0:
                    type_counts['uint64'] += 1
                else:
                    type_counts['int64'] += 1
            elif self._is_float(value):
                type_counts['float64'] += 1
            elif self._is_bool(value):
                type_counts['bool'] += 1
            elif self._is_datetime(value):
                type_counts['datetime64[ns]'] += 1
            elif self._is_timedelta(value):
                type_counts['timedelta64[ns]'] += 1
            elif self._is_complex(value):
                type_counts['complex128'] += 1
            else:
                type_counts['string'] += 1

        if type_counts:
            most_common_type, count = type_counts.most_common(1)[0]
            if count >= 0.9 * len(non_empty_values):
                return most_common_type
            if ('int64' in type_counts or 'uint64' in type_counts) and 'float64' in type_counts:
                return 'float64'

        return 'string'

    def analyze_data_types(self):
        headers = self.get_headers()
        sample_data = self._get_sample_data()

        column_data = {header: [] for header in headers}

        for row in sample_data:
            if len(row) == len(headers):
                for i, value in enumerate(row):
                    if i < len(headers):
                        column_data[headers[i]].append(value)

        column_dtypes = {}
        for header, values in column_data.items():
            column_dtypes[header] = self._infer_column_type(values)

        return column_dtypes

    def _create_converters(self, column_dtypes):
        converters = {}

        for column, dtype in column_dtypes.items():
            if dtype == 'int64':
                converters[column] = lambda x: int(x) if x.strip() and self._is_int(x) else pd.NA
            elif dtype == 'uint64':
                converters[column] = lambda x: int(x) if x.strip() and self._is_int(x) and int(x) >= 0 else pd.NA
            elif dtype == 'float64':
                converters[column] = lambda x: float(x) if x.strip() and (
                        self._is_float(x) or self._is_int(x)) else pd.NA
            elif dtype == 'bool':
                def bool_converter(x):
                    if not x.strip():
                        return pd.NA
                    x = x.strip().lower()
                    if x in ['true', 'yes', 't', '1']:
                        return True
                    elif x in ['false', 'no', 'f', '0']:
                        return False
                    return pd.NA

                converters[column] = bool_converter
            elif dtype == 'datetime64[ns]':
                converters[column] = lambda x: pd.to_datetime(x, errors='coerce')
            elif dtype == 'timedelta64[ns]':
                converters[column] = lambda x: pd.to_timedelta(x, errors='coerce')
            elif dtype == 'complex128':
                converters[column] = lambda x: complex(x.replace(' ', '')) if x.strip() and self._is_complex(
                    x) else pd.NA
            elif dtype == 'string':
                converters[column] = lambda x: str(x) if x.strip() else pd.NA
            else:
                converters[column] = lambda x: x if x.strip() else pd.NA

        return converters

    # def read_csv(self):
    #     column_dtypes = self.analyze_data_types()
    #     converters = self._create_converters(column_dtypes)
    #
    #     dtypes = {}
    #     for column, dtype in column_dtypes.items():
    #         if dtype not in ['datetime64[ns]', 'timedelta64[ns]']:
    #             dtypes[column] = dtype
    #
    #     print(dtypes)
    #     df = pd.read_csv(
    #         self.filepath,
    #         converters=converters,
    #         dtype=dtypes,
    #         na_values=['', 'NA', 'N/A', 'NULL', 'null', 'NaN'],
    #         keep_default_na=True
    #     )
    #
    #     for column, dtype in column_dtypes.items():
    #         if column in df.columns:
    #             if dtype == 'datetime64[ns]':
    #                 df[column] = pd.to_datetime(df[column], errors='coerce')
    #             elif dtype == 'timedelta64[ns]':
    #                 df[column] = pd.to_timedelta(df[column], errors='coerce')
    #             elif dtype == 'category':
    #                 df[column] = df[column].astype('category')
    #
    #     return df
    def read_csv(self):
        try:
            column_dtypes = self.analyze_data_types()
            print(column_dtypes)
            converters = self._create_converters(column_dtypes)
            print(converters)
            dtypes = {
                column: dtype
                for column, dtype in column_dtypes.items()
                if dtype not in ['datetime64[ns]', 'timedelta64[ns]']
            }

            print("Inferred dtypes:", dtypes)

            df = pd.read_csv(
                self.filepath,
                converters=converters,
                dtype=dtypes,
                na_values=['', 'NA', 'N/A', 'NULL', 'null', 'NaN'],
                keep_default_na=True
            )

            for column, dtype in column_dtypes.items():
                if column in df.columns:
                    if dtype == 'datetime64[ns]':
                        df[column] = pd.to_datetime(df[column], errors='coerce')
                    elif dtype == 'timedelta64[ns]':
                        df[column] = pd.to_timedelta(df[column], errors='coerce')
                    elif dtype == 'category':
                        df[column] = df[column].astype('category')

            return df

        except Exception as e:
            print(f"Error reading CSV: {e}")
            return None

