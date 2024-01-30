"""databricks-sql-connector supports user to machine OAuth login which means the
end user has to be present to login in a browser which will be popped up by the Python process.

Pre-requisites:
- You have installed a browser (Chrome, Firefox, Safari, Internet Explorer, etc) that will be
  accessible on the machine for performing OAuth login.

For security, databricks-sql-connector does not persist OAuth tokens automatically. Hence, after
the Python process terminates the end user will have to log-in again. We provide APIs to be
implemented by the end user for persisting the OAuth token. The SampleOAuthPersistence reference
shows which methods you may implement.

For this example, the DevOnlyFilePersistence class is provided. Do not use this in production.
"""

import os
from typing import Optional

from databricks import sql
from databricks.sql.experimental.oauth_persistence import OAuthPersistence, OAuthToken, DevOnlyFilePersistence


class SampleOAuthPersistence(OAuthPersistence):
  def persist(self, hostname: str, oauth_token: OAuthToken):
    """To be implemented by the end user to persist in the preferred storage medium.
    
    OAuthToken has two properties:
        1. OAuthToken.access_token
        2. OAuthToken.refresh_token 

    Both should be persisted.
    """
    pass

  def read(self, hostname: str) -> Optional[OAuthToken]:
    """To be implemented by the end user to fetch token from the preferred storage

    Fetch the access_token and refresh_token for the given hostname.
    Return OAuthToken(access_token, refresh_token)
    """
    pass

with sql.connect(server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME"),
                 http_path       = os.getenv("DATABRICKS_HTTP_PATH"),
                 auth_type="databricks-oauth",
                 experimental_oauth_persistence=DevOnlyFilePersistence("./sample.json")) as connection:

    for x in range(1, 100):
        cursor = connection.cursor()
        cursor.execute('SELECT 1+1')
        result = cursor.fetchall()
        for row in result:
            print(row)
        cursor.close()

    connection.close()
