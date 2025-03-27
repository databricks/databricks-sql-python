from databricks import sql
import os
import time

with sql.connect(
    server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
    http_path=os.getenv("DATABRICKS_HTTP_PATH"),
    access_token=os.getenv("DATABRICKS_TOKEN"),
) as connection:

    with connection.cursor() as cursor:
        long_running_query = """
            SELECT COUNT(*) FROM RANGE(10000 * 16) x 
            JOIN RANGE(10000) y 
            ON FROM_UNIXTIME(x.id * y.id, 'yyyy-MM-dd') LIKE '%not%a%date%'
        """

        # Non-blocking call
        cursor.execute_async(long_running_query)

        # Polling every 5 seconds until the query is no longer pending
        while cursor.is_query_pending():
            print("POLLING")
            time.sleep(5)

        # Blocking call: fetch results when execution completes
        cursor.get_async_execution_result()

        result = cursor.fetchall()

        for res in result:
            print(res)
