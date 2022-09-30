# `databricks-sql-connector` Example Usage

We provide example scripts so you can see the connector in action for basic usage. You need a Databricks account to run them. The scripts expect to find your Databricks account credentials in these environment variables:

    - DATABRICKS_SERVER_HOSTNAME
    - DATABRICKS_HTTP_PATH
    - DATABRICKS_TOKEN

Follow the quick start in our [README](../README.md) to install `databricks-sql-connector` and see
how to find the hostname, http path, and access token. Note that for the OAuth examples below a 
personal access token is not needed.
# Table of Contents

- **`query_execute.py`** connects to the `samples` database of your default catalog, runs a small query, and prints the result to screen.
- **`insert_data.py`** adds a tables called `squares` to your default catalog and inserts one hundred rows of example data. Then it fetches this data and prints it to the screen.
- **`query_cancel.py`** shows how to cancel a query assuming that you can access the `Cursor` executing that query from a different thread. This is necessary because `databricks-sql-connector` does not yet implement an asynchronous API; calling `.execute()` blocks the current thread until execution completes. Therefore, the connector can't cancel queries from the same thread where they began.
- **`interactive_oauth.py`** shows the simplest example of authenticating by OAuth (no need for a PAT generated in the DBSQL UI). When you run the script it will open a browser window so you can authenticate. Afterward, the script fetches some sample data from Databricks and prints it to the screen. For this script, the OAuth token is not persisted which means you need to authenticate every time you run the script.
- **`persistent_oauth.py`** shows a more advanced example of authenticating by OAuth. In this case, it shows how to use a sublcass of `OAuthPersistence` to reuse an OAuth token across script executions.
- **`set_user_agent.py`** shows how to customize the user agent header used for Thrift commands. In
this example the string `ExamplePartnerTag` will be added to the the user agent on every request.