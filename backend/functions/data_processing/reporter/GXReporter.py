import json
from typing import Dict, Any

class GXReporter:

    @staticmethod
    def process_report_to_reporter(report_data: Dict[str, Any]) -> Dict[str, Any]:
        result = {
            "operation": report_data.get("suite_name", ""),
            "status": (report_data.get("statistics", {}).get("success_percent", 0.0) == 100),
            "columns": {}
        }

        expectations = report_data.get("results", [])
        for expectation in expectations:
            config = expectation.get("expectation_config", {})
            kwargs = config.get("kwargs", {})
            column = kwargs.get("column")
            if not column:
                continue

            if column not in result["columns"]:
                result["columns"][column] = {
                    "column": column,
                    "success": True,
                    "element_count": 0,
                    "unexpected_index_list": []
                }

            element_count = expectation.get("result", {}).get("element_count", 0)
            result["columns"][column]["element_count"] = max(
                result["columns"][column]["element_count"], element_count
            )

            if not expectation.get("success", False):
                result["columns"][column]["success"] = False

            if not expectation.get("success", False):
                unexpected_indices = expectation.get("result", {}).get("unexpected_index_list", [])
                result["columns"][column]["unexpected_index_list"].extend(unexpected_indices)

        for column_data in result["columns"].values():
            column_data["unexpected_index_list"] = sorted(
                list(set(column_data["unexpected_index_list"]))
            )

        result["columns"] = list(result["columns"].values())

        return result

    @staticmethod
    def create_report(failed_indices, col_name, patterns, total_rows, failed_values, pattern_counts) -> Dict[str, Any]:
        additional_result = {
            "success": len(failed_indices) == 0,
            "expectation_config": {
                "type": "check_for_blank_inverted",
                "kwargs": {
                    "batch_id": "pandas-pd_dataframe_asset",
                    "column": col_name,
                    "patterns": patterns
                },
                "meta": {}
            },
            "result": {
                "element_count": total_rows,
                "unexpected_count": len(failed_indices),
                "unexpected_percent": (len(failed_indices) / total_rows * 100) if total_rows > 0 else 0,
                "unexpected_list": failed_values,
                "complete_unexpected_counts": pattern_counts,
                "unexpected_index_list": failed_indices,
                "unexpected_index_query": f"df.filter(items={failed_indices}, axis=0)" if failed_indices else "df.filter(items=[], axis=0)"
            },
            "meta": {},
            "exception_info": {
                "raised_exception": False,
                "exception_traceback": None,
                "exception_message": None
            }
        }
        return additional_result

    @staticmethod
    def append_reports(parent_report, to_be_appended_reports):
        parent_report["results"].append(to_be_appended_reports)

        total_expectations = parent_report["statistics"]["evaluated_expectations"] + 1
        parent_report["statistics"]["evaluated_expectations"] = total_expectations

        if to_be_appended_reports["success"] == False:
            unsuccessful = parent_report["statistics"]["unsuccessful_expectations"] + 1
            parent_report["statistics"]["unsuccessful_expectations"] = unsuccessful
        else:
            successful = parent_report["statistics"]["successful_expectations"] + 1
            parent_report["statistics"]["successful_expectations"] = successful

        parent_report["statistics"]["success_percent"] = (
            parent_report["statistics"]["successful_expectations"] / total_expectations
        ) * 100

        return parent_report


def convert_and_process_validation_result(validation_result) -> str:
    report_dict = validation_result.to_json_dict()

    processed_report = GXReporter.process_report_to_reporter(report_dict)

    json_str = json.dumps(processed_report, indent=2)

    return json_str
