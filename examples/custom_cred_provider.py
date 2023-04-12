# please pip install databricks-sdk prior to running this example.

from databricks import sql
from databricks.sdk.oauth import OAuthClient
import os

oauth_client = OAuthClient(host=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
                           client_id=os.getenv("DATABRICKS_CLIENT_ID"),
                           client_secret=os.getenv("DATABRICKS_CLIENT_SECRET"),
                           redirect_url=os.getenv("APP_REDIRECT_URL"),
                           scopes=['all-apis', 'offline_access'])

consent = oauth_client.initiate_consent()

creds = consent.launch_external_browser()

with sql.connect(server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME"),
                 http_path       = os.getenv("DATABRICKS_HTTP_PATH"),
                 credentials_provider=creds) as connection:

    for x in range(1, 5):
        cursor = connection.cursor()
        cursor.execute('SELECT 1+1')
        result = cursor.fetchall()
        for row in result:
            print(row)
        cursor.close()

    connection.close()
