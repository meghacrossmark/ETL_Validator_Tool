import pandas as pd
import pandasql as ps
import subprocess

# code to read ip of the system to set it as a IP Address in Web

command = 'ipconfig /all'
result = subprocess.run(command, capture_output=True, text=True, shell=True)
ip = result.stdout.split("IPv4 Address")[1].split(": ")[1].split("(")[0]

print(ip)

# code to append data
df1 = pd.read_csv("/resources/testDBTBL1_Null.csv")
#print(df1)
df2 = pd.read_csv("/resources/testDBTBL2processedEdited.csv")
#print(df2)

#print(df1._append(df2, ignore_index=True))


df3 = pd.read_csv("/resources/Baby_Names_processed.csv")
print(df3)
print(df3.dtypes)

query = """
SELECT 
    SUBSTR(County, 1, 3) AS County_Substring, 
    Sex, 
    CAST(Count AS DECIMAL(10, 2)) AS CountDecimal,
     SUBSTR(County, 1, 3) AS County_Substring, 
     COALESCE("County","") as "noNUllCounty"
FROM df3
"""
result = ps.sqldf(query, locals())

print(result)
print(result.dtypes)



# import snowflake.connector
# # Snowflake connection details
# conn = snowflake.connector.connect(
#     user="sunildi",    
#     password="cybagE@1234#",
#     account="tmmnelt-dt29184",  # Your Snowflake AWS-hosted account
#     warehouse="COMPUTE_WH",  # Your warehouse
#     database="SNOWFLAKE_SAMPLE_DATA",  # Your database
#     schema="TPCDS_SF10TCL"  # Your schema
#     #role="ACCOUNTADMIN"  # Optional: specify role if needed)
#
# # Create a cursor object
# cur = conn.cursor()
#
# # Execute a query (fetch all table names)
# cur.execute("SELECT * from SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.INCOME_BAND")
#
# # Fetch results
# for table in cur.fetchall():
#     print(table)
#
# # Close the connection
# cur.close()
# conn.close()

