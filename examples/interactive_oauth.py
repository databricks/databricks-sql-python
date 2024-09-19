from databricks import sql
import os

"""databricks-sql-connector supports user to machine OAuth login which means the
end user has to be present to login in a browser which will be popped up by the Python process.

Pre-requisites:
- You have installed a browser (Chrome, Firefox, Safari, Internet Explorer, etc) that will be
  accessible on the machine for performing OAuth login.

This code does not persist the auth token. Hence after the Python process terminates the
end user will have to login again. See examples/persistent_oauth.py to learn about persisting the
token across script executions.
"""

with sql.connect(server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME"),
                 http_path       = os.getenv("DATABRICKS_HTTP_PATH"),
                 auth_type="databricks-oauth") as connection:

    for x in range(1, 100):
        cursor = connection.cursor()
        cursor.execute('SELECT 1+1')
        result = cursor.fetchall()
        for row in result:
            print(row)
        cursor.close()

    connection.close()
