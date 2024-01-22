# Run queries asynchronously

This driver supports running queries asynchronously using `execute_async()`. This method can return control to the calling code almost immediately instead of blocking until the query completes. `execute_async()` never returns a `ResultSet`. Instead it returns a query handle that you can use to poll for the query status, cancel the query, or fetch the query results.

You can use this method to submit multiple queries in rapid succession and to pick up running query executions that were kicked-off from a separate thread or `Connection`. This can be especially useful for recovering running queries within serverless functions.

**Note:** Asynchronous execution is not the same as _asyncio_ in Python.


# Requirements

- `databricks-sql-connector>=3.1.0`

# Interface

To run a query asynchronously, use `databricks.sql.client.Connection.execute_async()`. This method takes the same arguments as `execute()`. To pick up an existing query run, use `databricks.sql.client.Connection.get_async_execution()`. Both methods return an `AsyncExecution` object, which lets you interact with a query by exposing these properties and methods:

**Properties**
- `AsyncExecution.status` is the last-known status of this query run, expressed as an `AsyncExecutionStatus` enumeration value. For example, `RUNNING`, `FINISHED`, or `CANCELED`. When you first call `execute_async()` the resulting status will usually be `RUNNING`. Calling `sync_status()` will refresh the value of this property. In most usages, you do not need to access `.status` directly and can instead use `.is_running`, `.is_canceled`, and `.is_finished` instead.
- `AsyncExecution.query_id` is the `UUID` for this query run. You can use this to look-up the query in the Databricks SQL query history.
- `AsyncExecution.query_secret` is the `UUID` secret for this query run. Both the `query_id` and `secret` are needed to fetch results for a running query.
- `AsyncExecution.returned_as_direct_result` is a boolean that indicates whether this returned a direct result. See [below](#note-about-direct-result-queries) for more information.

**Methods**
- `AsyncExecution.sync_status()` performs a network round-trip to synchronize `.status`.
- `AsyncExecution.get_results()` returns the `ResultSet` for this query run. This is the same return signature as a synchronous `.execute()` call. Note that if you call `get_results()` on a query that is still running, the code will block until the query finishes running and the result has been fetched.
- `AsyncExecution.cancel()` performs a network round-trip to cancel this query run.
- `AsyncExceution.serialize()` returns a string of the `query_id:query_secret` for this execution.

**Note:** You do not need to directly instantiate an `AsyncExecution` object in your client code. Instead, use the `execute_async` and `get_async_execution` methods to run or pick up a running query.

# Code Examples

### Run a query asynchronously

This snippet mirrors the synchronous example in this repository's README.

```python
import os
import time
from databricks import sql

host = os.getenv("DATABRICKS_HOST")
http_path = os.getenv("DATABRICKS_HTTP_PATH")
access_token = os.getenv("DATABRICKS_TOKEN")

with sql.connect(server_hostname=host, http_path=http_path, access_token=access_token) as connection:
    query = connection.execute_async("SELECT :param `p`, * FROM RANGE(10" {"param": "foo"})
    
    # Poll for the result every 5 seconds
    while query.is_running:
        time.sleep(5)
        query.sync_status()
    
    # this will raise a AsyncExecutionUnrecoverableResultException if the query was canceled
    result = query.get_results().fetchall()

```

### Pick up a running query

Both a `query_id` and `query_secret` are required to pick up a running query. This example runs a query from one `Connection` and fetches its result from a different connection.

```python
import os
import time
from databricks import sql

host = os.getenv("DATABRICKS_HOST")
http_path = os.getenv("DATABRICKS_HTTP_PATH")
access_token = os.getenv("DATABRICKS_TOKEN")

with sql.connect(server_hostname=host, http_path=http_path, access_token=access_token) as connection:
    query = connection.execute_async("SELECT :param `p`, * FROM RANGE(10" {"param": "foo"})
    query_id_and_secret = query.serialize()

# The connection created above has now closed

with sql.connect(server_hostname=host, http_path=http_path, access_token=access_token) as connection:
    query_id, query_secret = query_id_and_secret.split(":")
    query = connection.get_async_execution(query_id, query_secret)
    
    while query.is_running:
        time.sleep(5)
        query.sync_status()

    result = query.get_results().fetchall()
```

# Note about direct result queries

To minimise network roundtrips for small queries, Databricks will eagerly return a query result if the query completes within five seconds and its results can be sent in a single response. This means that `execute_async()` may take up to five seconds to return control back to your calling code. When this happens, `AsyncExecution.returned_as_direct_result` will evaluate `True` and the query result will have already been cached in this `AsyncExecution` object. Calling `get_results()` will not invoke a network round-trip because the query will not be available at the server.

Queries that execute in this fashion cannot be picked up with `get_async_execution()` and their results are not persisted on the server to be fetched by a separate thread. Therefore, before calling `.serialize()` to persist a `query_id:query_secret` pair, you should check if `AsyncExecution.returned_as_direct_result == True` first. 