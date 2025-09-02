import snowflake.connector
import pandas as pd


class Snowflake:
    def __init__(self, params):
        self.parameters = params

    def create_connection(self):
        parameters = self.parameters
        connection = snowflake.connector.connect(
            user=parameters[0],
            password=parameters[1],
            account=parameters[2],
            warehouse=parameters[3],
            database=parameters[4],
            schema=parameters[5])

        cursor = connection.cursor()

        cursor.execute(f"SELECT * FROM {parameters[6]}")
        data = cursor.fetchall()

        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(data, columns=columns)

        cursor.close()
        connection.close()

        return df


