import great_expectations as gx
from great_expectations.core import ExpectationSuite
from typing import List
import time
import pandas as pd


class Duplicate_Check:
    def __init__(self, connected_data, test_params: dict, user_id: int = 0):
        self._dataframe = connected_data
        self._test_options = test_params
        self._user_id = user_id

        # Initialize ephemeral context and data source
        self._context = gx.get_context(mode="ephemeral")
        self._data_source = self._context.data_sources.add_pandas("pandas")
        self._data_asset = self._data_source.add_dataframe_asset(name="pd_dataframe_asset")

    def get_duplicates_with_indexes(self, column: str) -> dict:
        value_counts = self._dataframe[column].value_counts()
        duplicates = value_counts[value_counts > 1]

        # Return only value and count (no index), exclude count <= 1
        duplicate_details = {
            str(val): {"count": int(duplicates[val])}
            for val in duplicates.index
        }
        return duplicate_details

    def get_row_duplicates_by_columns(self, columns: List[str]) -> dict:
        # Identify duplicated rows (keep=False keeps all duplicates)
        duplicated_rows = self._dataframe[self._dataframe.duplicated(subset=columns, keep=False)]

        duplicate_details = {}
        for _, row in duplicated_rows.iterrows():
            key = tuple("null" if pd.isna(row[col]) else row[col] for col in columns)
            if key not in duplicate_details:
                duplicate_details[key] = {
                    "count": 0
                }
            duplicate_details[key]["count"] += 1

        # Only return duplicates with count > 1
        filtered_details = {
            str(k): v for k, v in duplicate_details.items() if v["count"] > 1
        }

        return filtered_details

    def perform_test(self) -> List[dict]:
        columns_to_test = self._test_options.get("column_to_test")
        if not columns_to_test:
            raise ValueError("No 'column_to_test' specified in test parameters.")

        if isinstance(columns_to_test, str):
            columns_to_test = [columns_to_test]

        # Create batch from whole DataFrame
        batch_definition = self._data_asset.add_batch_definition_whole_dataframe("batch_definition")
        batch = batch_definition.get_batch(batch_parameters={"dataframe": self._dataframe})

        suite_name = f"Duplicate_Check_user_{self._user_id}_{int(time.time())}"
        suite = ExpectationSuite(suite_name)
        validator = self._context.get_validator(batch=batch, expectation_suite=suite)

        output = []

        # Add uniqueness expectations for each column
        for column in columns_to_test:
            print(f"Adding strict uniqueness expectation for column: {column}")
            validator.expect_column_values_to_be_unique(column)

        validator.save_expectation_suite()
        validation_results = validator.validate(result_format={"result_format": "COMPLETE"})

        # Process individual column results (excluding unexpected list/index)
        for result in validation_results.get("results", []):
            exp_config = result.get("expectation_config", {})
            kwargs = exp_config.get("kwargs", exp_config.get("_raw_kwargs", {}))

            if exp_config.get("type") == "expect_column_values_to_be_unique":
                column = kwargs.get("column")
                result_data = result.get("result", {})

                output.append({
                    "column": column,
                    "element_count": int(result_data.get("element_count", 0)),
                    "unexpected_count": int(result_data.get("unexpected_count", 0)),
                    "duplicate_value_details": self.get_duplicates_with_indexes(column)
                })

        # If more than one column, check row-level duplicates
        if len(columns_to_test) > 1:
            print(f"Checking duplicates based on row combinations of columns: {columns_to_test}")
            row_duplicates = self.get_row_duplicates_by_columns(columns_to_test)
            output.append({
                "columns_combined": columns_to_test,
                "row_duplicates_count": sum(detail["count"] for detail in row_duplicates.values()),
                "row_duplicate_details": row_duplicates
            })

        print("Duplicate_Check output:", output)
        return output