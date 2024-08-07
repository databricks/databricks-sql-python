import os
import sys
# import logging
#
# logging.basicConfig(level=logging.DEBUG)

#
# # Get the parent directory of the current file
# target_folder_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "databricks-sql-python", "src"))
#
# # Add the parent directory to sys.path
# sys.path.append(target_folder_path)

from databricks import sql

# from dotenv import load_dotenv

#  export DATABRICKS_TOKEN=whatever


# Load environment variables from .env file
# load_dotenv()

host = "e2-dogfood.staging.cloud.databricks.com"
http_path = "/sql/1.0/warehouses/58aa1b363649e722"

access_token = ""
connection = sql.connect(
    server_hostname=host,
    http_path=http_path,
    access_token=access_token)


cursor = connection.cursor()
cursor.execute('SELECT :param `p`, * FROM RANGE(10)', {"param": "foo"})
# cursor.execute('SELECT 1')
result = cursor.fetchall()
for row in result:
    print(row)

cursor.close()
connection.close()