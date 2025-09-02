"""
Additional required packages:
azure-mgmt-datafactory==1.0.0
azure-identity==1.8.0
azure-storage-file-datalake==12.6.0
pyodbc==5.2.0 (already in your list)
"""

# ==========================================
# Azure Data Factory Connector
# ==========================================
from azure.identity import ClientSecretCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
import pandas as pd


class AzureDataFactory:
    def __init__(self, params):
        """
        Initialize Azure Data Factory connection parameters.

        Parameters:
        params: List containing [tenant_id, client_id, client_secret, subscription_id, resource_group_name, factory_name, pipeline_name]
        """
        self.parameters = params

    def create_connection(self):
        parameters = self.parameters
        tenant_id = parameters[0]
        client_id = parameters[1]
        client_secret = parameters[2]
        subscription_id = parameters[3]
        resource_group_name = parameters[4]
        factory_name = parameters[5]
        pipeline_name = parameters[6]

        # Create credentials object
        credentials = ClientSecretCredential(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret
        )

        # Create Data Factory client
        adf_client = DataFactoryManagementClient(credentials, subscription_id)

        # Get pipeline runs (can be customized as needed)
        pipeline_runs = adf_client.pipeline_runs.query_by_factory(
            resource_group_name=resource_group_name,
            factory_name=factory_name,
            filter_parameters={
                "pipelineName": pipeline_name,
                "lastUpdatedAfter": "2023-01-01T00:00:00.000Z",
                "lastUpdatedBefore": "2025-12-31T00:00:00.000Z"
            }
        )

        # Convert pipeline runs to DataFrame
        runs_data = []
        for run in pipeline_runs.value:
            runs_data.append({
                "RunId": run.run_id,
                "PipelineName": run.pipeline_name,
                "Status": run.status,
                "StartTime": run.run_start,
                "EndTime": run.run_end,
                "Duration": run.duration_in_ms
            })

        df = pd.DataFrame(runs_data)
        return df

    def run_pipeline(self, parameters=None):
        """Additional method to run a pipeline with optional parameters"""
        params = self.parameters
        tenant_id = params[0]
        client_id = params[1]
        client_secret = params[2]
        subscription_id = params[3]
        resource_group_name = params[4]
        factory_name = params[5]
        pipeline_name = params[6]

        credentials = ClientSecretCredential(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret
        )

        adf_client = DataFactoryManagementClient(credentials, subscription_id)

        # Run the pipeline
        run_response = adf_client.pipelines.create_run(
            resource_group_name=resource_group_name,
            factory_name=factory_name,
            pipeline_name=pipeline_name,
            parameters=parameters
        )

        return run_response.run_id


# ==========================================
# Azure Data Lake Storage Connector
# ==========================================
from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import ClientSecretCredential
import pandas as pd
import io


class AzureDataLakeStorage:
    def __init__(self, params):
        """
        Initialize Azure Data Lake Storage connection parameters.

        Parameters:
        params: List containing [tenant_id, client_id, client_secret, account_name, container_name, file_path]
        """
        self.parameters = params

    def create_connection(self):
        parameters = self.parameters
        tenant_id = parameters[0]
        client_id = parameters[1]
        client_secret = parameters[2]
        account_name = parameters[3]
        container_name = parameters[4]
        file_path = parameters[5]

        # Create credentials
        credential = ClientSecretCredential(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret
        )

        # Create DataLakeServiceClient
        service_client = DataLakeServiceClient(
            account_url=f"https://{account_name}.dfs.core.windows.net",
            credential=credential
        )

        # Get file system client (container)
        file_system_client = service_client.get_file_system_client(file_system=container_name)

        # Get file client
        file_client = file_system_client.get_file_client(file_path)

        # Download file
        download = file_client.download_file()
        downloaded_bytes = download.readall()

        # Convert to DataFrame (assuming CSV format - adjust as needed)
        df = pd.read_csv(io.BytesIO(downloaded_bytes))

        return df

    def list_files(self, directory_path=""):
        """Additional method to list files in a directory"""
        parameters = self.parameters
        tenant_id = parameters[0]
        client_id = parameters[1]
        client_secret = parameters[2]
        account_name = parameters[3]
        container_name = parameters[4]

        credential = ClientSecretCredential(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret
        )

        service_client = DataLakeServiceClient(
            account_url=f"https://{account_name}.dfs.core.windows.net",
            credential=credential
        )

        file_system_client = service_client.get_file_system_client(file_system=container_name)

        paths = file_system_client.get_paths(path=directory_path)

        file_list = []
        for path in paths:
            file_list.append({
                "name": path.name,
                "is_directory": path.is_directory,
                "content_length": path.content_length
            })

        return pd.DataFrame(file_list)


# ==========================================
# Azure SQL Database Connector
# ==========================================
import pyodbc
import pandas as pd
from decimal import Decimal


class AzureSQLDB:
    def __init__(self, params):
        """
        Initialize Azure SQL Database connection parameters.

        Parameters:
        params: List containing [server_name, database_name, username, password, table_name]
        """
        self.parameters = params

    def create_connection(self):
        parameters = self.parameters
        server_name = parameters[0]
        database_name = parameters[1]
        username = parameters[2]
        password = parameters[3]
        table_name = parameters[4]

        # Build connection string
        connection_string = (
            f'DRIVER={{ODBC Driver 17 for SQL Server}};'
            f'SERVER={server_name}.database.windows.net;'  # Azure SQL Server
            f'DATABASE={database_name};'
            f'UID={username};'
            f'PWD={password};'
        )

        try:
            # Create connection
            conn = pyodbc.connect(connection_string)
            cursor = conn.cursor()

            # Execute query
            query = f"SELECT * FROM {table_name}"
            cursor.execute(query)
            data = cursor.fetchall()

            # Get column names
            columns = [column[0] for column in cursor.description]

            # Handle Decimal type
            data = [
                tuple(float(item) if isinstance(item, Decimal) else item for item in row)
                for row in data
            ]

            # Create DataFrame
            df = pd.DataFrame(data, columns=columns)

            # Close resources
            cursor.close()
            conn.close()

            return df

        except Exception as e:
            print(f"Error connecting to Azure SQL Database: {e}")
            return pd.DataFrame()

    def execute_query(self, custom_query):
        """Additional method to execute custom SQL queries"""
        parameters = self.parameters
        server_name = parameters[0]
        database_name = parameters[1]
        username = parameters[2]
        password = parameters[3]

        connection_string = (
            f'DRIVER={{ODBC Driver 17 for SQL Server}};'
            f'SERVER={server_name}.database.windows.net;'
            f'DATABASE={database_name};'
            f'UID={username};'
            f'PWD={password};'
        )

        try:
            conn = pyodbc.connect(connection_string)
            cursor = conn.cursor()

            cursor.execute(custom_query)

            # Check if the query returns data
            if cursor.description:
                data = cursor.fetchall()
                columns = [column[0] for column in cursor.description]

                # Handle Decimal type
                data = [
                    tuple(float(item) if isinstance(item, Decimal) else item for item in row)
                    for row in data
                ]

                df = pd.DataFrame(data, columns=columns)
                result = df
            else:
                result = {"status": "Query executed successfully", "rows_affected": cursor.rowcount}

            cursor.close()
            conn.close()

            return result

        except Exception as e:
            print(f"Error executing query: {e}")
            return {"error": str(e)}


###########
        params = [
            "tenant_id",  # Azure AD tenant ID
            "client_id",  # Service principal client/application ID
            "client_secret",  # Service principal secret
            "subscription_id",  # Azure subscription ID
            "resource_group_name",  # Resource group containing the Data Factory
            "factory_name",  # Name of the Data Factory
            "pipeline_name"  # Name of the pipeline to work with
        ]

        params = [
            "tenant_id",  # Azure AD tenant ID
            "client_id",  # Service principal client/application ID
            "client_secret",  # Service principal secret
            "account_name",  # Storage account name (without ".dfs.core.windows.net")
            "container_name",  # Container/file system name
            "file_path"  # Path to the file within the container you want to read
        ]

        params = [
            "server_name",  # Server name (without ".database.windows.net")
            "database_name",  # Database name
            "username",  # SQL login username
            "password",  # SQL login password
            "table_name"  # Table to query in create_connection method
        ]

        params = [
            "user",  # Snowflake username
            "password",  # Snowflake password
            "account",  # Snowflake account identifier
            "warehouse",  # Warehouse name
            "database",  # Database name
            "schema",  # Schema name
            "table_name"  # Table name to query
        ]

        params = [
            "server_name",  # SQL Server hostname/IP
            "db_name",  # Database name
            "table_name"  # Table name to query
        ]

        params = [
            "server_hostname",  # Databricks SQL endpoint hostname
            "http_path",  # HTTP path for the SQL endpoint
            "access_token",  # Databricks access token
            "catalog_name",  # Catalog name (Unity Catalog)
            "schema_name",  # Schema name
            "table_name"  # Table name
        ]