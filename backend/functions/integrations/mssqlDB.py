import pyodbc
import pandas as pd
from decimal import Decimal


class mssqlDB:

    def __init__(self, params):
        self.parameters = params
        print("Params Are as next ",params)

    def create_connection(self):
        global df
        server_name = self.parameters[0]
        db_name = self.parameters[1]
        table_name = self.parameters[2]
        connection_string = (
                r'DRIVER={ODBC Driver 17 for SQL Server};'
                r'SERVER=' + server_name + ';'  # Replace with your server name or IP address
                                           r'DATABASE=' + db_name + ';'  # Replace with your database name
                                                                    r'Trusted_Connection=yes;'
            # This uses Windows Authentication
        )
        print(connection_string)
        try:
            conn = pyodbc.connect(connection_string)
            cursor = conn.cursor()
            query = f"SELECT * FROM {table_name}"
            cursor.execute(query)
            data = cursor.fetchall()
            columns = [column[0] for column in cursor.description]
            # This step is important if your data contains Decimal objects
            data = [
                tuple(float(item) if isinstance(item, Decimal) else item for item in row)
                for row in data
            ]
            df = pd.DataFrame(data, columns=columns)
            cursor.close()
            conn.close()

        except Exception as e:
            print("Error while connecting to SQL Server:", e)
        finally:
            return df
