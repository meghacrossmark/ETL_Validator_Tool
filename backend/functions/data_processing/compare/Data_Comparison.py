import pandas as pd
import traceback
from collections import Counter


class Data_Comparison:
    def __init__(self, source_df, target_df, test_params=None, user_id: int = 0):
        self.source_df = source_df.copy()
        self.target_df = target_df.copy()
        self.test_params = test_params or {}
        self.user_id = user_id

        # Count blank rows in original data before removing
        self.blank_rows_source = self.source_df.apply(
            lambda row: all(row.isna()) or all(row.astype(str).str.strip() == ''), axis=1
        ).sum()
        self.blank_rows_target = self.target_df.apply(
            lambda row: all(row.isna()) or all(row.astype(str).str.strip() == ''), axis=1
        ).sum()

        # Remove completely blank rows from source
        blank_rows_source = self.source_df.apply(
            lambda row: all(row.isna()) or all(row.astype(str).str.strip() == ''), axis=1
        )
        self.source_df = self.source_df.loc[~blank_rows_source]

        # Remove completely blank rows from target
        blank_rows_target = self.target_df.apply(
            lambda row: all(row.isna()) or all(row.astype(str).str.strip() == ''), axis=1
        )
        self.target_df = self.target_df.loc[~blank_rows_target]

        # Column selection
        if not self.test_params.get("compare_columns_source") or not self.test_params.get("compare_columns_target"):
            common_cols = list(set(self.source_df.columns).intersection(set(self.target_df.columns)))
            self.compare_columns_source = common_cols
            self.compare_columns_target = common_cols
        else:
            self.compare_columns_source = self.test_params.get("compare_columns_source", [])
            self.compare_columns_target = self.test_params.get("compare_columns_target", [])

        # Options
        self.ignore_case = self.test_params.get("ignore_case", False)
        self.ignore_spaces = self.test_params.get("ignore_spaces", False)
        self.to_lower_case = self.test_params.get("to_lower_case", False)
        self.on_index = self.test_params.get("on_index", False)

    def preprocess_column(self, series, for_output=False):
        # Convert to string and strip spaces
        result = series.fillna("").astype(str).str.strip()

        # Remove trailing '.0' from numeric strings like '115.0' -> '115'
        result = result.str.replace(r"\.0$", "", regex=True)

        if self.ignore_spaces:
            result = result.str.replace(" ", "", regex=False)

        if for_output:
            if self.to_lower_case:
                result = result.str.lower()
        else:
            if self.ignore_case or self.to_lower_case:
                result = result.str.lower()

        return result

    def preprocess_df(self, df, columns, for_output=False):
        df_copy = df.copy()
        for col in columns:
            if col in df_copy.columns:
                df_copy[col] = self.preprocess_column(df_copy[col], for_output=for_output)
        return df_copy

    def to_native_python(self, df):
        converted_data = [
            [x.item() if hasattr(x, 'item') else x for x in row]
            for row in df.values.tolist()
        ]
        return pd.DataFrame(converted_data, columns=df.columns)

    def flatten_keys(self, keys_df):
        if keys_df.shape[1] == 1:
            return keys_df.iloc[:, 0].drop_duplicates().tolist()
        else:
            return [list(x) for x in keys_df.drop_duplicates().values]

    def get_excess_rows(self, df, key_columns, excess_keys):
        key_tuples = df[key_columns].apply(tuple, axis=1)
        indexes = []

        excess_counter = Counter(excess_keys)

        for key, count in excess_counter.items():
            matching_indexes = key_tuples[key_tuples == key].index.tolist()
            indexes.extend(matching_indexes[:count])

        return indexes

    def perform_test(self):
        try:
            # Validate
            missing_in_source = [c for c in self.compare_columns_source if c not in self.source_df.columns]
            missing_in_target = [c for c in self.compare_columns_target if c not in self.target_df.columns]

            assert not missing_in_source, f"Missing columns in source dataframe: {missing_in_source}"
            assert not missing_in_target, f"Missing columns in target dataframe: {missing_in_target}"
            assert len(self.compare_columns_source) == len(self.compare_columns_target), \
                "compare_columns_source and compare_columns_target must have the same length"

            # Preprocess for comparison
            source_comp = self.preprocess_df(self.source_df, self.compare_columns_source, for_output=False)
            target_comp = self.preprocess_df(self.target_df, self.compare_columns_target, for_output=False)

            # Preprocess for output display
            source_out = self.preprocess_df(self.source_df, self.compare_columns_source, for_output=True)
            target_out = self.preprocess_df(self.target_df, self.compare_columns_target, for_output=True)

            if self.on_index:
                # Index-based comparison
                source_indexes = set(source_comp.index)
                target_indexes = set(target_comp.index)
                not_in_target_indexes = list(source_indexes - target_indexes)
                not_in_source_indexes = list(target_indexes - source_indexes)
                common_indexes = list(source_indexes & target_indexes)

                mismatch_indexes = []
                for idx in common_indexes:
                    src_row = source_comp.loc[idx, self.compare_columns_source]
                    tgt_row = target_comp.loc[idx, self.compare_columns_target]
                    if isinstance(src_row, pd.Series) and isinstance(tgt_row, pd.Series):
                        if not src_row.equals(tgt_row):
                            mismatch_indexes.append(idx)
                    elif src_row != tgt_row:
                        mismatch_indexes.append(idx)

                matched_rows = len(common_indexes) - len(mismatch_indexes)
                failed_rows = len(mismatch_indexes) + len(not_in_target_indexes) + len(not_in_source_indexes)

                failed_rows_indexes = {
                    "mismatched_rows": mismatch_indexes,
                    "not_in_target": not_in_target_indexes,
                    "not_in_source": not_in_source_indexes
                }

                failed_rows_details = {
                    "mismatched_rows_source": self.to_native_python(
                        source_out.loc[mismatch_indexes, self.compare_columns_source]
                    ).to_dict(orient="records"),
                    "mismatched_rows_target": self.to_native_python(
                        target_out.loc[mismatch_indexes, self.compare_columns_target]
                    ).to_dict(orient="records"),
                    "not_in_target_rows": self.to_native_python(
                        source_out.loc[not_in_target_indexes, self.compare_columns_source]
                    ).to_dict(orient="records"),
                    "not_in_source_rows": self.to_native_python(
                        target_out.loc[not_in_source_indexes, self.compare_columns_target]
                    ).to_dict(orient="records")
                }

            else:
                # Key-based comparison with duplicate counts
                source_tuples = source_comp[self.compare_columns_source].apply(tuple, axis=1)
                target_tuples = target_comp[self.compare_columns_target].apply(tuple, axis=1)

                source_counts = source_tuples.value_counts()
                target_counts = target_tuples.value_counts()

                all_keys = set(source_counts.index).union(set(target_counts.index))

                matched_rows = 0
                not_in_target_keys = []
                not_in_source_keys = []

                for key in all_keys:
                    src_count = source_counts.get(key, 0)
                    tgt_count = target_counts.get(key, 0)

                    matched = min(src_count, tgt_count)
                    matched_rows += matched

                    if src_count > tgt_count:
                        diff = src_count - tgt_count
                        not_in_target_keys.extend([key] * diff)

                    if tgt_count > src_count:
                        diff = tgt_count - src_count
                        not_in_source_keys.extend([key] * diff)

                failed_rows = len(not_in_target_keys) + len(not_in_source_keys)

                not_in_target_indexes = self.get_excess_rows(source_out, self.compare_columns_source, not_in_target_keys)
                not_in_source_indexes = self.get_excess_rows(target_out, self.compare_columns_target, not_in_source_keys)

                failed_rows_indexes = {
                    "mismatched_rows": [],
                    "not_in_target": [list(k) if isinstance(k, tuple) else [k] for k in not_in_target_keys],
                    "not_in_source": [list(k) if isinstance(k, tuple) else [k] for k in not_in_source_keys]
                }

                failed_rows_details = {
                    "mismatched_rows_source": [],
                    "mismatched_rows_target": [],
                    "not_in_target_rows": self.to_native_python(
                        source_out.loc[not_in_target_indexes, self.compare_columns_source]
                    ).to_dict(orient="records"),
                    "not_in_source_rows": self.to_native_python(
                        target_out.loc[not_in_source_indexes, self.compare_columns_target]
                    ).to_dict(orient="records")
                }

            status = (failed_rows == 0)

            return {
                "Connection": {
                    "source": {
                        "conn": "source",
                        "conn_params": self.test_params.get("source_conn_params", {}),
                        "conn_type": self.test_params.get("source_conn_type", "file_upload"),
                        "headers": list(self.source_df.columns)
                    },
                    "target": {
                        "conn": "target",
                        "conn_params": self.test_params.get("target_conn_params", {}),
                        "conn_type": self.test_params.get("target_conn_type", "file_upload"),
                        "headers": list(self.target_df.columns)
                    }
                },
                "Test": {
                    "logged_in_user_id": self.user_id,
                    "operation": "compare_data",
                    "test_params": {
                        "compare_columns_source": self.compare_columns_source,
                        "compare_columns_target": self.compare_columns_target,
                        "ignore_case": self.ignore_case,
                        "ignore_spaces": self.ignore_spaces,
                        "on_index": self.on_index,
                        "to_lower_case": self.to_lower_case
                    },
                    "test_type": "compare"
                },
                "failed_rows_details": failed_rows_details,
                "result_metadata": {
                    "blank_rows_source": int(self.blank_rows_source),
                    "blank_rows_target": int(self.blank_rows_target),
                    "status": status,
                    "matched_rows": matched_rows,
                    "failed_rows": failed_rows,
                    "failed_rows_indexes": failed_rows_indexes,
                    "source": {
                        "rows": self.source_df.shape[0],
                        "columns": len(self.compare_columns_source),
                        "not_in_target": len(not_in_target_keys)
                    },
                    "target": {
                        "rows": self.target_df.shape[0],
                        "columns": len(self.compare_columns_target),
                        "not_in_source": len(not_in_source_keys)
                    }
                },
                "type_of_test": "compare"
            }

        except AssertionError as ae:
            return {"error": f"AssertionError: {ae}"}
        except Exception as e:
            return {"error": str(e), "traceback": traceback.format_exc()}
