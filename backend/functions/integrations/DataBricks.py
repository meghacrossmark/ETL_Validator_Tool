from databricks import sql

import pandas as pd


class DataBricks:

    def __init__(self, params: list[str]):

        self.parameters = params

        self.connection = None  # Initialize connection to None

    def create_connection(self) -> pd.DataFrame:

        parameters = self.parameters

        print("In Databricks")

        print(f"Hostname => {parameters[0]}")

        print(f"Path => {parameters[1]}")

        print(f"Access Token => {parameters[2]}")

        print(f"Batch ID => {parameters[3]}")

        print(f"Database => {parameters[4]}")

        print(f"Table => {parameters[5]}")

        # Reuse existing connection or create a new one

        if self.connection is None:
            self.connection = sql.connect(

                server_hostname=parameters[0],

                http_path=parameters[1],

                access_token=parameters[2]

            )

        cursor = self.connection.cursor()

        db_name = parameters[4]

        table_name = parameters[5]

        batch_id = parameters[3].replace("'", "''")  # Basic escaping of single quotes

        if "_dv" in db_name:
            if batch_id == 'NA':
                query = f"SELECT * FROM {db_name}.{table_name}"
            else:
                query = f"SELECT * FROM {db_name}.{table_name} WHERE AUDIT_BATCH_ID='{batch_id}'"

            print("Executing QUERY in Data Vault =>", query)
        else:
            if batch_id == 'NA':
                query = f"SELECT * FROM {db_name}.{table_name}"
            else:
                query = f"SELECT * FROM {db_name}.{table_name} WHERE BATCH_ID='{batch_id}'"

            print("Executing QUERY in RAW =>", query)

        cursor.execute(query)

        data = cursor.fetchall()

        columns = [desc[0] for desc in cursor.description]

        df = pd.DataFrame(data, columns=columns)

        cursor.close()

        # Do NOT close self.connection here because we reuse it

        return df

    def get_raw_connection(self):

        """

        Return a live connection object. Reconnect if needed.

        """

        if self.connection is None:
            parameters = self.parameters

            self.connection = sql.connect(

                server_hostname=parameters[0],

                http_path=parameters[1],

                access_token=parameters[2]

            )

        return self.connection

    def get_datatypes(self, full_table_name: str) -> list[str]:

        conn = self.get_raw_connection()

        cursor = conn.cursor()

        if '.' in full_table_name:

            database_name, table_name = full_table_name.split('.', 1)

        else:

            cursor.close()

            raise ValueError("full_table_name must be in the format 'database.table'")

        print(f"Running DESCRIBE TABLE {database_name}.{table_name}")

        cursor.execute(f"DESCRIBE TABLE {database_name}.{table_name}")

        rows = cursor.fetchall()

        print("Raw DESCRIBE TABLE rows:", rows)

        cursor.close()

        datatypes = {

            row[0]: row[1]

            for row in rows

            if row[0] and row[1]

               and not row[0].startswith('#')

               and row[0].lower() not in ('col_name', 'data_type')

        }

        print("Extracted datatypes:", datatypes)

        return list(set(datatypes.values()))

    def close_connection(self):

        """

        Close the active connection if it exists.

        """

        if self.connection is not None:
            self.connection.close()

            self.connection = None

            print("Connection closed.")

