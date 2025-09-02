import pandasql as ps  # For local SQL on dataframes
from functions.integrations.AzureBlobStorage import AzureBlobLoader
from functions.integrations.DataBricks import DataBricks
from functions.integrations.Snowflake import Snowflake
from functions.integrations.mssqlDB import mssqlDB
from functions.integrations.uploads import Uploads

class GetDataFrameFromConnection:
    __dataFrame = None
    __azure_blob_loader = None
    __connection_obj = None  # For raw DB connection/cursor for databricks
    __table_name = None  # Fully qualified table name for databricks
    __dataType_name = None

    def __init__(self, connection_for, connection_type, params):
        self.connFor = connection_for
        self.connection = connection_type
        self.params = params
        print("In GET DATA FRAME FROM CONNECTIONS File ", self.params)
        self.identify_and_connect()

    def identify_and_connect(self):
        connection_data = self.params.get("Connection", {}).get(self.connFor, {}).get("conn_params", {})

        if self.connection == "file_upload":
            self.__dataFrame = (
                Uploads(
                    connection_data.get("selected_fileName", {}),
                    connection_data.get("separator", None)
                ).get_uploaded_file_as_DF(
                    self.params.get("Test", {}).get("test_type", "quality") == "compare"
                )
            )

        elif self.connection == "azure_blob_storage":
            print("In IDENTIFY AND CONNECT method", connection_data)

            self.__azure_blob_loader = AzureBlobLoader(
                accountUrl=connection_data.get("accountUrl", ""),
                sasToken=connection_data.get("sasToken", ""),
                containerName=connection_data.get("containerName", ""),
                blobName=connection_data.get("blobName", "")
            )

            blobName = connection_data.get("blobName", "")
            self.__dataFrame = self.__azure_blob_loader.load(blobName)
            if self.__dataFrame is not None:
                print(f"[INFO] Azure Blob DataFrame loaded - Shape: {self.__dataFrame.shape}")
            else:
                print("[WARNING] Azure Blob returned no data.")

        elif self.connection == "databricks":
            dbs = DataBricks([
                connection_data.get("serverHostName", None),
                connection_data.get("httpPath", None),
                connection_data.get("accessToken", None),
                connection_data.get("batchId", None),
                connection_data.get("dbSchemaName", None),
                connection_data.get("tableDB", None)
            ])
            # get dataframe as usual
            self.__dataFrame = dbs.create_connection()

            # store raw connection for metadata queries
            self.__connection_obj = dbs.get_raw_connection()

            # store full table name (e.g. schema.table)
            schema = connection_data.get("dbSchemaName", "")
            # print(schema)
            table = connection_data.get("tableDB", "")
            # print(table)
            if schema and table:
                self.__table_name = f"{schema}.{table}"
                self.__dataType_name = dbs.get_datatypes(self.__table_name)
                print("Column & Data Type NAMES:", self.__dataType_name)
                # = self.get_datatypes()
                return self.__dataType_name
            else:
                self.__table_name = table



        elif self.connection == "MSSQL":
            self.__dataFrame = mssqlDB([
                connection_data.get("serverName", None),
                connection_data.get("dbName", None),
                connection_data.get("table", None)
            ]).create_connection()

        elif self.connection == "snowflake":
            sf = Snowflake([
                connection_data.get("sf_username", None),
                connection_data.get("sf_password", None),
                connection_data.get("sf_account", None),
                connection_data.get("sf_warehouse", None),
                connection_data.get("sf_database", None),
                connection_data.get("sf_schema", None),
                connection_data.get("sf_tableName", None)
            ])
            self.__dataFrame = sf.create_connection()

    def get_connection(self):
        return self.__dataFrame

    def get_header(self):
        if self.__dataFrame is None:
            raise ValueError(
                f"No DataFrame loaded. Connection for '{self.connFor}' using type '{self.connection}' failed."
            )
        return self.__dataFrame.columns.tolist()

    def update_connection(self, query):
        local_dataframe = self.__dataFrame
        self.__dataFrame = ps.sqldf(
            query.replace(query.split("FROM ")[1].split()[0], "local_dataframe"),
            locals()
        )

    def get_blob_headers(self):
        if self.connection != "azure_blob_storage":
            raise ValueError("Blob headers can only be fetched for Azure Blob connection type.")

        if not self.__azure_blob_loader:
            raise ValueError("AzureBlobLoader instance is not initialized.")

        connection_data = self.params.get("Connection", {}).get(self.connFor, {}).get("conn_params", {})
        blob_name = connection_data.get("blob_name", "")
        return self.__azure_blob_loader.get_blob_headers(blob_name)

    # --- New Databricks specific methods ---

    def get_connection_obj(self):
        """
        Return raw DB connection or cursor (Databricks only).
        """
        if self.connection == "databricks":
            return self.__connection_obj
        else:
            raise NotImplementedError("get_connection_obj() only implemented for Databricks connection.")

    def get_table_name(self):
        """
        Return fully qualified table name for Databricks.
        """
        if self.connection == "databricks":
             return self.__table_name
        else:
             raise NotImplementedError("get_table_name() only implemented for Databricks connection.")

