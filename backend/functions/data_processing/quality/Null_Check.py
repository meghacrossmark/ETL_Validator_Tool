import great_expectations as gx
import pandas as pd

from functions.data_processing.reporter.GXReporter import GXReporter


class Null_Check:
    __dataframe = None

    def __init__(self, connected_data, test_params, user_id: int = 0):
        self.__dataframe = connected_data
        self.__test_options = test_params
        self.__user_id = user_id

    def clean_null_like_values(self):
        """Replace string representations of nulls with actual nulls."""
        null_like_values = ['null', 'NULL', 'NaN', 'nan', 'None', 'none', '']
        for col in self.__dataframe.columns:
            self.__dataframe[col] = self.__dataframe[col].replace(null_like_values, pd.NA)

    def perform_test(self):
        self.clean_null_like_values()  # <-- Pre-clean the DataFrame here

        print(self.__test_options)

        __headers = self.__test_options.get("column_to_test", None)
        print(__headers)
        threshold = self.__test_options.get("threshold", 100)

        mostly = threshold / 100 if isinstance(threshold, (int, float)) else 1.0
        expectation_kwargs = {"mostly": mostly}

        if __headers is None:
            __headers = self.__dataframe.columns.to_list()

        print(__headers)

        context = gx.get_context(mode="ephemeral")

        data_source = context.data_sources.add_pandas("pandas")
        data_asset = data_source.add_dataframe_asset(name="pd dataframe asset")

        batch_definition = data_asset.add_batch_definition_whole_dataframe("batch definition")
        batch = batch_definition.get_batch(batch_parameters={"dataframe": self.__dataframe})

        suite_name = "Null Check"
        suite = gx.ExpectationSuite(name=suite_name)
        validator = context.get_validator(batch=batch, expectation_suite=suite)

        if isinstance(__headers, str):
            validator.expect_column_values_to_not_be_null(__headers, **expectation_kwargs)
            validator.expect_column_values_to_not_match_regex(__headers, regex=r"^\s*$", **expectation_kwargs)
            validator.expect_column_values_to_not_be_in_set(__headers,
                                                            value_set=[None],
                                                            **expectation_kwargs)
        else:
            for header in __headers:
                validator.expect_column_values_to_not_be_null(header, **expectation_kwargs)
                validator.expect_column_values_to_not_match_regex(header, regex=r"^\s*$", **expectation_kwargs)
                validator.expect_column_values_to_not_be_in_set(header,
                                                                value_set=[None],
                                                                **expectation_kwargs)

        validator.save_expectation_suite()

        validation_results = validator.validate(result_format="COMPLETE")

        return validation_results