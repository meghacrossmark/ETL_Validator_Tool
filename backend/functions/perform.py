# from functions.data_processing.compare.Count_Comparison import Count_Comparison
# from functions.data_processing.compare.Data_Comparison import Data_Comparison
# from functions.data_processing.quality.Data_Type_Check import Data_Type_Check
# from functions.data_processing.quality.Duplicate_Check import Duplicate_Check
# from functions.data_processing.reporter.json_Reporter import Reporter
# from functions.integrations.GetDataFrameFromConnection import GetDataFrameFromConnection
# from functions.data_processing.quality.Null_Check import Null_Check
#
#
# class perform:
#     __source_connection = None
#     __target_connection = None
#     __result = None
#     __user_id = 0
#
#     def __init__(self, params):
#         self.params = params
#         self.__user_id = self.params.get("Test").get("logged_in_user_id", 0)
#         self.__source_connection = GetDataFrameFromConnection(
#             "source",
#             self.params.get("Connection", {}).get("source").get("conn_type"),
#             self.params)
#
#         if "target" in self.params.get("Connection", {}):
#             self.__target_connection = GetDataFrameFromConnection(
#                 "target",
#                 self.params.get("Connection", {}).get("target", {}).get("conn_type"),
#                 self.params)
#
#     def execute(self, params):
#         self.params = params
#         __test_params = self.params.get("Test", {}).get("test_params", {})
#         __opr_type = self.params.get("Test", None).get("operation", {})
#
#         df_target = None
#         df_source = self.__source_connection.get_connection()
#
#         if self.__target_connection is not None:
#             df_target = self.__target_connection.get_connection()
#
#         if __opr_type == "check_null":
#             self.__result = Null_Check(df_source, __test_params, self.__user_id).perform_test()
#             return Reporter.create_quality_report(self.__result, self.params)
#
#         elif __opr_type == "check_duplicate":
#             self.__result = Duplicate_Check(df_source, __test_params, self.__user_id).perform_test()
#             return Reporter.create_quality_report(self.__result, self.params)
#
#         elif __opr_type == "check_data_type":
#             conn_type = self.params.get("Connection", {}).get("source").get("conn_type")
#             self.__result = Data_Type_Check(df_source, __test_params, conn_type, self.__user_id).perform_test()
#             return Reporter.create_quality_report(self.__result, self.params)
#
#         elif __opr_type == "compare_count":
#             self.__result = Count_Comparison(df_source, df_target).perform_test()
#             return Reporter.create_compare_report(self.__result, self.params)
#
#         elif __opr_type == "compare_data":
#             self.__result = Data_Comparison(df_source, df_target, __test_params, self.__user_id).perform_test()
#             return Reporter.create_compare_report(self.__result, self.params)
#
#         else:
#             return {
#                 "report_json": {
#                     "type_of_test": "Test Not Defined"
#                 }
#             }
#
#     def get_headers(self, connection_type):
#         if connection_type == "source":
#             return self.__source_connection.get_header()
#         else:
#             return self.__target_connection.get_header()
#
#     def update_tables(self, connection_type, query):
#         if connection_type == "source":
#             self.__source_connection.update_connection(query)
#         else:
#             self.__target_connection.update_connection(query)

from functions.data_processing.compare.Count_Comparison import Count_Comparison
from functions.data_processing.compare.Data_Comparison import Data_Comparison
from functions.data_processing.quality.Data_Type_Check import Data_Type_Check
from functions.data_processing.quality.Duplicate_Check import Duplicate_Check
from functions.data_processing.reporter.json_Reporter import Reporter
from functions.integrations.GetDataFrameFromConnection import GetDataFrameFromConnection
from functions.data_processing.quality.Null_Check import Null_Check


class perform:
    __source_connection = None
    __target_connection = None
    __result = None
    __user_id = 0

    def __init__(self, params):
        self.params = params
        print("In PERFORM File ", self.params)
        self.__user_id = self.params.get("Test").get("logged_in_user_id", 0)
        self.__source_connection = GetDataFrameFromConnection(
            "source",
            self.params.get("Connection", {}).get("source").get("conn_type"),
            self.params)

        if "target" in self.params.get("Connection", {}):
            self.__target_connection = GetDataFrameFromConnection(
                "target",
                self.params.get("Connection", {}).get("target", {}).get("conn_type"),
                self.params)

    def execute(self, params):
        self.params = params
        print("In Execute")
        __test_params = self.params.get("Test", {}).get("test_params", {})
        __opr_type = self.params.get("Test", None).get("operation", {})

        df_target = None
        df_source = self.__source_connection.get_connection()

        if self.__target_connection is not None:
            df_target = self.__target_connection.get_connection()

        if __opr_type == "check_null":
            self.__result = Null_Check(df_source, __test_params, self.__user_id).perform_test()
            return Reporter.create_quality_report(self.__result, self.params)

        elif __opr_type == "check_duplicate":
            self.__result = Duplicate_Check(df_source, __test_params, self.__user_id).perform_test()
            return Reporter.create_quality_report(self.__result, self.params)

        elif __opr_type == "check_data_type":
            conn_type = self.params.get("Connection", {}).get("source").get("conn_type")
            self.__result = Data_Type_Check(df_source, __test_params, conn_type, self.__user_id).perform_test()
            return Reporter.create_quality_report(self.__result, self.params)

        elif __opr_type == "compare_count":
            self.__result = Count_Comparison(df_source, df_target).perform_test()

            return Reporter.create_compare_report(self.__result, self.params)

        elif __opr_type == "compare_data":
            print("Type of __test_params:", type(__test_params))
            print("__test_params content:", __test_params)
            self.__result = Data_Comparison(df_source, df_target, __test_params, self.__user_id).perform_test()
            print(self.__result)
            return Reporter.create_compare_report(self.__result, self.params)

        else:
            return {
                "report_json": {
                    "type_of_test": "Test Not Defined"
                }
            }

    def get_headers(self, connection_type):
        if connection_type == "source":
            return self.__source_connection.get_header()
        else:
            return self.__target_connection.get_header()

    def update_tables(self, connection_type, query):
        if connection_type == "source":
            self.__source_connection.update_connection(query)
        else:
            self.__target_connection.update_connection(query)

    def get_datatypes(self, connection_type):
        """
        Fetch column datatypes for the given connection_type from Databricks using DESCRIBE TABLE.
        Only works if the connection is of type 'databricks'.
        """
        connection = (
            self.__source_connection if connection_type == "source"
            else self.__target_connection
        )

        if connection is None:
            raise ValueError(f"{connection_type.capitalize()} connection is not initialized.")

        if connection.connection != "databricks":
            # Skip for non-Databricks connections
            print(f"[INFO] Skipping get_datatypes for non-Databricks connection: {connection.connection}")
            return []

        conn_obj = connection.get_connection_obj()
        table_name = connection.get_table_name()

        if not conn_obj or not table_name:
            raise ValueError(f"Connection object or table name missing for {connection_type}")

        cursor = conn_obj.cursor()
        cursor.execute(f"DESCRIBE TABLE {table_name}")
        rows = cursor.fetchall()
        cursor.close()

        # Extract datatypes safely
        datatypes = {
            row[0]: row[1]
            for row in rows
            if row[0] and row[1]
            and not row[0].startswith('#')
            and row[0].lower() not in ('col_name', 'data_type')
        }

        print(datatypes)
        return list(set(datatypes.values()))

    def is_connection_type(self, connection_type, expected_type):
        """
        Returns True if the specified connection_type ('source' or 'target') is of expected_type.
        """
        if connection_type == "source":
            return self.__source_connection.connection == expected_type
        elif connection_type == "target":
            return self.__target_connection and self.__target_connection.connection == expected_type
        return False

    def has_connection(self, connection_type):
        """
        Returns True if the connection_type ('source' or 'target') is initialized.
        """
        if connection_type == "source":
            return self.__source_connection is not None
        elif connection_type == "target":
            return self.__target_connection is not None
        return False
