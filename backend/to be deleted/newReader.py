import pandas as pd
import csv
from pathlib import Path


def get_file_headers(file_path):
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        headers = next(reader)
    return headers


def id_data_type(values):
    """
    Identify the most appropriate data type for a series of values.
    This function determines what type to use for a column as a whole.
    If even one value doesn't fit the pattern, defaults to string.
    """
    # Filter out empty/NA values for type detection
    non_empty_values = [v for v in values if v not in ['', 'N/A', 'unknown', 'null'] and not pd.isna(v)]

    if not non_empty_values:
        return 'string'

    # Check if EVERY value can be converted to the desired type
    # If even one fails, we'll use 'object' type instead to safely handle mixed types

    # First try to see if all values are boolean
    bool_success = True
    for v in non_empty_values:
        if str(v).lower() not in ('true', 'false', 't', 'f', '1', '0'):
            bool_success = False
            break

    if bool_success:
        return 'boolean'

    # Try integer for all values
    int_success = True
    for v in non_empty_values:
        try:
            # Check if it's a clean integer conversion
            int_val = int(v)
            if str(int_val) != str(v):
                int_success = False
                break
        except (ValueError, TypeError):
            int_success = False
            break

    if int_success:
        return 'Int64'

    # Try float for all values
    float_success = True
    for v in non_empty_values:
        try:
            float(v)  # Just check if conversion works
        except (ValueError, TypeError):
            float_success = False
            break

    if float_success:
        return 'Float64'

    # Try datetime for all values
    datetime_success = True
    for v in non_empty_values:
        try:
            pd.to_datetime(v, errors='raise')
        except (ValueError, TypeError):
            datetime_success = False
            break

    if datetime_success:
        return 'datetime64[ns]'

    # Default to object (mixed type) to handle mixed data safely
    return 'object'


def get_data_type_for_columns(file_path, headers, sample_size=1000):
    """
    Determine data types by sampling rows from the file
    """
    # Read a sample of the data for type inference
    sample_data = {header: [] for header in headers}

    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        next(reader)  # Skip header row

        # Collect sample values for each column
        for i, row in enumerate(reader):
            if i >= sample_size:
                break

            for j, value in enumerate(row):
                if j < len(headers):  # Ensure we don't go out of bounds
                    sample_data[headers[j]].append(value)

    # Determine initial data type for each column
    data_type_map = {}
    for header in headers:
        values = sample_data[header]

        # Check if column appears to be all numeric
        numeric_count = 0
        for value in values:
            if value and value not in ['N/A', 'unknown', 'null']:
                try:
                    float(value)  # Test if value can be converted to float
                    numeric_count += 1
                except (ValueError, TypeError):
                    pass

        # If most values (>95%) are numeric, treat as numeric column
        if len(values) > 0 and numeric_count / len(values) > 0.95:
            # Check if all numeric values are integers
            int_count = 0
            for value in values:
                if value and value not in ['N/A', 'unknown', 'null']:
                    try:
                        int_val = int(value)
                        if str(int_val) == value:  # Check if clean int conversion
                            int_count += 1
                    except (ValueError, TypeError):
                        pass

            # If most numeric values are clean integers
            if int_count / max(1, numeric_count) > 0.95:
                data_type_map[header] = 'Int64'
            else:
                data_type_map[header] = 'Float64'
        else:
            # Check for boolean values
            bool_count = 0
            for value in values:
                if value and value.lower() in ('true', 'false', 't', 'f', '1', '0', 'yes', 'no', 'y', 'n'):
                    bool_count += 1

            if len(values) > 0 and bool_count / len(values) > 0.95:
                data_type_map[header] = 'boolean'
            else:
                # Check for dates
                date_count = 0
                for value in values:
                    if value and value not in ['N/A', 'unknown', 'null']:
                        try:
                            pd.to_datetime(value)
                            date_count += 1
                        except (ValueError, TypeError):
                            pass

                if len(values) > 0 and date_count / len(values) > 0.95:
                    data_type_map[header] = 'datetime64[ns]'
                else:
                    # Check for categorical data
                    unique_values = set(values)
                    if len(unique_values) / max(1, len(values)) < 0.1:  # Less than 10% unique values
                        data_type_map[header] = 'category'
                    else:
                        data_type_map[header] = 'string'

    return data_type_map


def read_file_with_data_types(file_path, sample_size=1000):
    """
    Read a CSV file and convert each column to a consistent data type.
    Non-conforming values within a column will be converted to NA.
    """
    headers = get_file_headers(file_path)
    print(f"Headers: {headers}")

    # Get initial data types based on sampling
    data_types = get_data_type_for_columns(file_path, headers, sample_size)
    print(f"Inferred dtypes: {data_types}")

    # Read all data first
    data_frame = pd.read_csv(
        file_path,
        na_values=['', 'N/A', 'unknown', 'null'],
        keep_default_na=True,
        low_memory=False
    )

    # Convert each column to its inferred data type, with non-conforming values becoming NA
    for header, dtype in data_types.items():
        try:
            if dtype == 'Int64':
                # Convert to numeric, with errors becoming NA
                data_frame[header] = pd.to_numeric(data_frame[header], errors='coerce').astype('Int64')

            elif dtype == 'UInt64':
                # First coerce to numeric (NA for non-numeric), then filter out negatives
                numeric_series = pd.to_numeric(data_frame[header], errors='coerce')
                numeric_series = numeric_series.where(numeric_series >= 0, pd.NA)
                data_frame[header] = numeric_series.astype('UInt64')

            elif dtype == 'Float64':
                # Convert to float, with errors becoming NA
                data_frame[header] = pd.to_numeric(data_frame[header], errors='coerce').astype('Float64')

            elif dtype == 'boolean':
                # Create a temporary series to identify boolean values
                bool_map = {
                    'true': True, 't': True, '1': True, 'yes': True, 'y': True,
                    'false': False, 'f': False, '0': False, 'no': False, 'n': False,
                }

                def safe_bool_convert(x):
                    if pd.isna(x):
                        return pd.NA
                    x_str = str(x).lower()
                    return bool_map.get(x_str, pd.NA)

                data_frame[header] = data_frame[header].map(safe_bool_convert).astype('boolean')

            elif dtype == 'datetime64[ns]':
                # Convert to datetime, with errors becoming NA
                data_frame[header] = pd.to_datetime(data_frame[header], errors='coerce')

            elif dtype == 'category':
                # For categorical data, non-conforming values is not applicable
                data_frame[header] = data_frame[header].astype('category')

            else:
                # For string type, convert all to strings
                data_frame[header] = data_frame[header].astype('string')

        except Exception as e:
            print(f"Warning: Error converting column '{header}' to {dtype}: {e}")
            print(f"Keeping column '{header}' as its original type")

    return data_frame


def main():
    # Specify your CSV file path
    file_path = r'C:\Users\shubhamrat\.cache\kagglehub\datasets\sahirmaharajj\motor-vehicle-collisions-crashes\versions\2\Motor_Vehicle_Collisions_-_Crashes.csv'  # Replace with your actual file path

    # Verify file exists
    if not Path(file_path).is_file():
        raise FileNotFoundError(f"CSV file not found at: {file_path}")

    # Read CSV with inferred dtypes and error handling
    df = read_file_with_data_types(file_path)

    # Print dtypes to verify
    print("\nDataFrame dtypes:")
    print(df.dtypes)

    # Optional: Print first few rows
    print("\nFirst few rows:")
    print(df.head(15))

    return df


if __name__ == "__main__":
    df = main()