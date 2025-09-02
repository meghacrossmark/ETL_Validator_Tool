import pandas as pd
import numpy as np
from functions.data_processing.reporter.GXReporter import GXReporter

def convert_np_types(obj):
    """
    Recursively convert numpy data types to native Python types
    for JSON serialization compatibility.
    """
    if isinstance(obj, dict):
        return {k: convert_np_types(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_np_types(i) for i in obj]
    elif isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    else:
        return obj


class Reporter:
    @staticmethod
    def create_compare_report(report: dict, params: dict) -> dict:
        """
        Create a compare report JSON by merging type_of_test, params, and report.
        """
        report_json = {
            "type_of_test": "compare",
            **params,
            **report
        }
        report_json = convert_np_types(report_json)  # Convert numpy types
        return report_json

    @staticmethod
    def create_quality_report(core_report, params: dict) -> dict:
        """
        Create a quality report JSON.

        Args:
            core_report: dict, list, or object with a `to_json_dict()` method.
            params: Additional parameters as a dict.

        Returns:
            dict: The quality report JSON.

        Raises:
            ValueError: if core_report is not a dict/list and doesn't have `to_json_dict()`.
        """
        if isinstance(core_report, dict):
            report_data = core_report
        elif isinstance(core_report, list):
            report_data = {"results": core_report}
        else:
            try:
                report_data = core_report.to_json_dict()
            except Exception as e:
                raise ValueError(
                    f"core_report must be a dict, list, or have 'to_json_dict' method. Error: {e}"
                )

        if isinstance(report_data, dict) and "statistics" in report_data:
            processed_report = GXReporter.process_report_to_reporter(report_data)
        else:
            processed_report = report_data

        report_json = {
            "type_of_test": "quality",
            **params,
            "result_metadata": processed_report
        }
        report_json = convert_np_types(report_json)  # Convert numpy types
        return report_json