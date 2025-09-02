import pandas as pd
import numpy as np
import great_expectations as gx
import re

class Data_Type_Check:
    def __init__(self, connected_data, test_params, connection_type, user_id: int = 0):
        self.__dataframe = connected_data.copy()
        self.__test_options = test_params
        self.__user_id = user_id
        self.__connection_type = connection_type
        self.__replace_string_nulls()

    def __replace_string_nulls(self):
        null_equivalents = ['null', 'NULL', 'NaN', 'nan', 'None', 'none', '']

        def clean_nulls(val):
            if isinstance(val, str):
                val_clean = val.strip().lower()
                if val_clean in null_equivalents:
                    return np.nan
            return val

        self.__dataframe = self.__dataframe.applymap(clean_nulls)

    def __normalize_sql_type(self, sql_type: str) -> str:
        sql_type = sql_type.lower().strip()
        if re.match(r"decimal\s*\(\s*\d+\s*,\s*\d+\s*\)", sql_type):
            return "decimal"
        return sql_type

    def __to_native_type(self, value):
        """
        Convert numpy/pandas numeric types to native Python types for JSON serialization.
        """
        if isinstance(value, (np.integer, np.int64, np.int32)):
            return int(value)
        elif isinstance(value, (np.floating, np.float64, np.float32)):
            return float(value)
        elif isinstance(value, (np.bool_, bool)):
            return bool(value)
        return value

    def perform_test(self):
        headers = self.__test_options.get("column_to_test", [])
        data_types = self.__test_options.get("data_types", [])

        if isinstance(headers, str):
            headers = [headers]
        if isinstance(data_types, str):
            data_types = [data_types]

        sql_to_pandas_type = {
            "int": "int64",
            "integer": "int64",
            "bigint": "int64",
            "smallint": "int32",
            "tinyint": "uint8",
            "float": "float64",
            "decimal": "float32",
            "double": "float64",
            "real": "float32",
            "date": "datetime64[ns]",
            "timestamp": "datetime64[ns]",
            "string": "object",
            "text": "object",
            "bool": "bool"
        }

        normalized_types = [self.__normalize_sql_type(dtype) for dtype in data_types]
        mapped_types = [sql_to_pandas_type.get(ntype, ntype) for ntype in normalized_types]

        context = gx.get_context(mode="ephemeral")
        data_source = context.data_sources.add_pandas("pandas")
        data_asset = data_source.add_dataframe_asset(name="pd_dataframe_asset")
        batch_definition = data_asset.add_batch_definition_whole_dataframe("batch_definition")
        batch = batch_definition.get_batch(batch_parameters={"dataframe": self.__dataframe})

        suite_name = f"Data Type Check_{self.__user_id}"
        suite = gx.ExpectationSuite(name=suite_name)
        validator = context.get_validator(batch=batch, expectation_suite=suite)

        def check_type(value, expected_dtype):
            if pd.isna(value):
                return True  # nulls pass the type check
            try:
                if 'int' in expected_dtype:
                    int(value)
                elif 'float' in expected_dtype:
                    float(value)
                elif 'bool' in expected_dtype:
                    if str(value).lower() not in ['true', 'false', '0', '1']:
                        return False
                elif 'datetime' in expected_dtype:
                    pd.to_datetime(value)
                # For 'object' or string types, accept any non-null value
                return True
            except Exception:
                return False

        results = []

        for header, sql_expected_type, pandas_expected_type in zip(headers, normalized_types, mapped_types):
            validator.expect_column_values_to_be_of_type(column=header, type_=pandas_expected_type)

            actual_dtype = str(self.__dataframe[header].dtype)
            total_count = len(self.__dataframe[header])
            null_count = self.__dataframe[header].isna().sum()

            if actual_dtype == pandas_expected_type:
                match_count = total_count - null_count
                not_match_count = 0
                success = True
                non_matching_unique_values_with_counts = []
            else:
                series = self.__dataframe[header]
                non_matching_mask = ~series.apply(lambda x: check_type(x, pandas_expected_type))
                non_matching_series = series[non_matching_mask].dropna()

                non_matching_values = non_matching_series.unique()

                non_matching_unique_values_with_counts = []
                for val in non_matching_values:  # No limit here
                    matching_indexes = non_matching_series[non_matching_series == val].index.tolist()
                    non_matching_unique_values_with_counts.append({
                        "value": self.__to_native_type(val),
                        "count": len(matching_indexes),
                        "indexes": matching_indexes
                    })

                match_count = 0
                not_match_count = total_count - null_count
                success = False

            results.append({
                "column": header,
                "expected_type": sql_expected_type,
                "actual_type": actual_dtype,
                "match_count": self.__to_native_type(match_count),
                "not_match_count": self.__to_native_type(not_match_count),
                "null_count": self.__to_native_type(null_count),
                "total_count": self.__to_native_type(total_count),
                "success": self.__to_native_type(success),
                "non_matching_unique_values_with_counts": non_matching_unique_values_with_counts
            })

        return results
