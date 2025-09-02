import pandas as pd

class Count_Comparison:
    def __init__(self, source_df: pd.DataFrame, target_df: pd.DataFrame, verbose=False):
        if not isinstance(source_df, pd.DataFrame):
            raise TypeError("source_df must be a pandas DataFrame")
        if not isinstance(target_df, pd.DataFrame):
            raise TypeError("target_df must be a pandas DataFrame")

        self.source_df = source_df
        self.target_df = target_df
        self.verbose = verbose

        if self.verbose:
            print("Source DataFrame:\n", self.source_df)
            print("Target DataFrame:\n", self.target_df)

    def perform_test(self) -> dict:
        if self.source_df.empty:
            raise ValueError("source_df is empty")
        if self.target_df.empty:
            raise ValueError("target_df is empty")

        def clean_str(x):
            if pd.isna(x):
                return ''
            # Remove common invisible whitespace characters
            return str(x).replace('\xa0', '').strip()

        def is_blank_row(row):
            stripped_values = [clean_str(x) for x in row]
            return all(val == '' for val in stripped_values)

        # Create boolean masks identifying blank rows in each DataFrame
        source_blank_rows_mask = self.source_df.apply(is_blank_row, axis=1)
        target_blank_rows_mask = self.target_df.apply(is_blank_row, axis=1)

        source_blank_rows = source_blank_rows_mask.sum()
        target_blank_rows = target_blank_rows_mask.sum()

        if self.verbose:
            print(f"Source blank rows mask:\n{source_blank_rows_mask}")
            print(f"Target blank rows mask:\n{target_blank_rows_mask}")
            print(f"Source blank rows count: {source_blank_rows}")
            print(f"Target blank rows count: {target_blank_rows}")

        # Remove blank rows and reset index
        source_df_clean = self.source_df.loc[~source_blank_rows_mask].reset_index(drop=True)
        target_df_clean = self.target_df.loc[~target_blank_rows_mask].reset_index(drop=True)

        if self.verbose:
            print("Cleaned Source DataFrame:\n", source_df_clean)
            print("Cleaned Target DataFrame:\n", target_df_clean)

        # Count rows and columns after cleaning
        source_row_count = len(source_df_clean)
        target_row_count = len(target_df_clean)

        source_col_count = len(source_df_clean.columns)
        target_col_count = len(target_df_clean.columns)

        # Calculate extra rows based on counts difference
        extra_rows_in_source = max(0, source_row_count - target_row_count)
        extra_rows_in_target = max(0, target_row_count - source_row_count)

        # Calculate extra columns based on column name differences
        extra_cols_in_source = list(set(source_df_clean.columns) - set(target_df_clean.columns))
        extra_cols_in_target = list(set(target_df_clean.columns) - set(source_df_clean.columns))

        # Final status: True only if rows and columns match exactly and no extras
        status = (
            source_row_count == target_row_count and
            source_col_count == target_col_count and
            extra_rows_in_source == 0 and
            extra_rows_in_target == 0 and
            len(extra_cols_in_source) == 0 and
            len(extra_cols_in_target) == 0
        )

        result_metadata = {
            "status": status,
            "source": {
                "rows": source_row_count,
                "columns": source_col_count,
                "extra_rows_count": extra_rows_in_source,
                "extra_columns": extra_cols_in_source,
                "blank_rows_count": int(source_blank_rows)
            },
            "target": {
                "rows": target_row_count,
                "columns": target_col_count,
                "extra_rows_count": extra_rows_in_target,
                "extra_columns": extra_cols_in_target,
                "blank_rows_count": int(target_blank_rows)
            }
        }

        if self.verbose:
            print("Comparison Result Metadata:\n", result_metadata)

        return {"result_metadata": result_metadata}
