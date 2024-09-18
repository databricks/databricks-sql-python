import os

from databricks.sdk.core import oauth_service_principal, Config
from databricks import sql

"""
This example shows how to use OAuth M2M (machine-to-machine) for service principal

Pre-requisites:
- Create service principal and OAuth secret in Account Console
- Assign the service principal to the workspace

See more https://docs.databricks.com/en/dev-tools/authentication-oauth.html)
"""

server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")


def credential_provider():
    config = Config(
        host=f"https://{server_hostname}",
        # Service Principal UUID
        client_id=os.getenv("DATABRICKS_CLIENT_ID"),
        # Service Principal Secret
        client_secret=os.getenv("DATABRICKS_CLIENT_SECRET"))
    return oauth_service_principal(config)


with sql.connect(
        server_hostname=server_hostname,
        http_path=os.getenv("DATABRICKS_HTTP_PATH"),
        credentials_provider=credential_provider) as connection:
    for x in range(1, 100):
        cursor = connection.cursor()
        cursor.execute('SELECT 1+1')
        result = cursor.fetchall()
        for row in result:
            print(row)
        cursor.close()

    connection.close()
