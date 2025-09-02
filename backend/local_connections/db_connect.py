import pyodbc


def get_connection():
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=localhost;'  # Replace with your SQL Server instance name or IP address
        'DATABASE=Validator_Tool;'  # Replace with the name of the database you want to connect to
        'Trusted_Connection=yes;'
    )
    return conn


def execute_query(query, params=None):
    conn = get_connection()
    cursor = conn.cursor()
    if params:
        cursor.execute(query, params)
    else:
        cursor.execute(query)
    columns = [column[0] for column in cursor.description]
    data = [dict(zip(columns, row)) for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return data
