from functions.data_processing.compare.Count_Comparison import Count_Comparison
from functions.data_processing.compare.Data_Comparison import Data_Comparison
from functions.data_processing.quality.Data_Type_Check import Data_Type_Check
from functions.data_processing.quality.Duplicate_Check import Duplicate_Check
from functions.data_processing.quality.Null_Check import Null_Check
from functions.data_processing.reporter.json_Reporter import Reporter
from functions.integrations.Enhanced_CSV_Reader import Enhanced_CSV_Reader
from functions.integrations.GetDataFrameFromConnection import GetDataFrameFromConnection
import json

raw = {
    "connectionType": "local",
    "operation": "null_check",
    "detailReport": False,
    "targetConnectionType": "local",
    "source_serverName": "",
    "source_dbName": "",
    "source_table": "",
    "source_tableDB": "",
    "source_serverHostName": "",
    "source_httpPath": "",
    "source_accessToken": "",
    "source_workspaceName": "",
    "source_dbSchemaName": "",
    "source_sf_username": "",
    "source_sf_password": "",
    "source_sf_account": "",
    "source_sf_warehouse": "",
    "source_sf_database": "",
    "source_sf_schema": "",
    "source_sf_tableName": "",
    "header": "all",
    "selectColumn": "all",
    "source_selectedFile": "sample_hidden_nulls_extended_v3.csv",
    "source_seperator": ",",
    "target_selectedFile": "Baby_Names_processed.csv",
    "target_seperator": ",",
    "target_serverName": "",
    "target_dbName": "",
    "target_table": "",
    "target_tableDB": "",
    "target_serverHostName": "",
    "target_httpPath": "",
    "target_accessToken": "",
    "targe_workspaceName": "",
    "target_dbSchemaName": "",
    "target_sf_username": "",
    "target_sf_password": "",
    "target_sf_account": "",
    "target_sf_warehouse": "",
    "target_sf_database": "",
    "target_sf_schema": "",
    "target_sf_tableName": "",
    "source_query": "",
    "target_query": ""
}
#instance = perform(raw)
#result = instance.execute()
#print(result)


# dataCompare(GetDataFrameFromConnection("source", raw.get("connectionType", None), raw).get_connection(),
#             GetDataFrameFromConnection("target", raw.get("connectionType", None), raw).get_connection(),
#            "Id")
#matched rows commented out due to size

#validation_results = Null_Check(GetDataFrameFromConnection("source", raw.get("connectionType", None), raw).get_connection()).perform_test()
#validation_results = Duplicate_Check(
#    GetDataFrameFromConnection("source", raw.get("connectionType", None), raw).get_connection(),
#    "matchIt").perform_test()
#print(Reporter.create_quality_report(validation_results, raw))

#Data_Type_Check(GetDataFrameFromConnection("source", raw.get("connectionType", None), raw).get_connection(),"Id","int64").perform_test()

params = {
    "Test": {
        "logged_in_user_id": 0,
        "operation": "data_compare",
        "test_params": {
            "on_index": True,
            "primary_key_source": "EventId",
            "primary_key_target": "EventPrimaryKey",
            "ignore_spaces": False,
            "ignore_case": False,
            "to_lower_case": False
        }
    },
    "Connection": {
        "source": {
            "conn": "source",
                "conn_type": "file_upload",
            "conn_params": {
                "selected_fileName": "testDBTBL2processed.csv"
            },
            "table_name": "",
            "headers": [],
            "tbl_primary_key": "EventId",
            "query": ""
        },
        "target": {
            "conn": "target",
            "conn_type": "file_upload",
            "conn_params": {
                "selected_fileName": "testDBTBL2processedEdited.csv"
            },
            "table_name": "",
            "headers": [],
            "tbl_primary_key": "EventPrimaryKey",
            "query": ""
        }
    }
}

# dc = Data_Comparison(GetDataFrameFromConnection("source", params.get("Connection", None).get("source")
#                                                 .get("conn_type"), params).get_connection(),
#                      GetDataFrameFromConnection("target", params.get("Connection", None).get("target")
#                                              .get("conn_type"), params).get_connection(),
#                      params.get("Test", None).get("test_params"))
# res = dc.perform_test()
# print(json.dumps(res, indent=2))
#
# re = dc.get_raw_dfs_from_indexes(res.get("result_metadata").get("source").get("not_in_target_indexes"),
#                                  res.get("result_metadata").get("target").get("not_in_source_indexes"),
#                                  res.get("result_metadata").get("failed_rows_indexes"),
#                                  params.get("Test").get("test_params").get("primary_key_source"))
# print(json.dumps(re, indent=2))
# count_res = Data_Comparison(GetDataFrameFromConnection("source", params.get("Connection", None).get("source").get("conn_type"),
#                                             params).get_connection(),
#                  GetDataFrameFromConnection("target", params.get("Connection", None).get("target").get("conn_type"),
#                                             params).get_connection(), params.get("Test", None).get("test_params")).perform_test()
#print(json.dumps(count_res, indent=2))

#print(json.dumps(Reporter.create_compare_report(count_res, params), indent=2))
re2 = {
    "result_metadata": {
        "status": False,
        "matched_rows": 17,
        "matched_rows_indexes": 2,
        "failed_rows": 1,
        "failed_rows_indexes": [
            9494
        ],
        "source": {
            "rows": 20,
            "column": 22,
            "not_in_target": 2,
            "not_in_target_indexes": [
                12680,
                12689
            ]
        },
        "target": {
            "rows": 20,
            "columns": 22,
            "not_in_source": 2,
            "not_in_source_indexes": [
                12762,
                12762
            ]
        }
    }
}

reader = Enhanced_CSV_Reader(r"C:\CYB_Validator\resources\Baby_Names.csv")
df = reader.read_csv()
print(df.head(15))
Data_Type_Check(df,["Id","Year", "First Name",  "County", "Sex", "Count"],"int64").perform_test()
print("Executed")
