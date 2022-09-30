from databricks import sql
import os

"""Bring Your Own Identity Provider with fined grained OAuth scopes is currently public preview on
Databricks in AWS. databricks-sql-connector supports user to machine OAuth login which means the
end user has to be present to login in a browser which will be popped up by the Python process. You
must enable OAuth in your Databricks account to run this example. More information on how to enable
OAuth in your Databricks Account in AWS can be found here:

https://docs.databricks.com/administration-guide/account-settings-e2/single-sign-on.html

Pre-requisites:
- You have a Databricks account in AWS.
- You have configured OAuth in Databricks account in AWS using the link above.
- You have installed a browser (Chrome, Firefox, Safari, Internet Explorer, etc) that will be
  accessible on the machine for performing OAuth login.

This code does not persist the auth token. Hence after the Python process terminates the
end user will have to login again. See examples/persistent_oauth.py to learn about persisting the
token across script executions.

Bring Your Own Identity Provider is in public preview. The API may change prior to becoming GA. 
You can monitor these two links to find out when it will become generally available:

  1. https://docs.databricks.com/administration-guide/account-settings-e2/single-sign-on.html 
  2. https://docs.databricks.com/dev-tools/python-sql-connector.html
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
