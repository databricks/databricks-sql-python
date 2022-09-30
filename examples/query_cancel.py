from databricks import sql
import os, threading, time

"""
The current operation of a cursor may be cancelled by calling its `.cancel()` method as shown in the example below.
"""

with sql.connect(server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME"),
                 http_path       = os.getenv("DATABRICKS_HTTP_PATH"),
                 access_token    = os.getenv("DATABRICKS_TOKEN")) as connection:

  with connection.cursor() as cursor:
    def execute_really_long_query():
        try:
            cursor.execute("SELECT SUM(A.id - B.id) " +
                            "FROM range(1000000000) A CROSS JOIN range(100000000) B " +
                            "GROUP BY (A.id - B.id)")
        except sql.exc.RequestError:
          print("It looks like this query was cancelled.")

    exec_thread = threading.Thread(target=execute_really_long_query)
    
    print("\n Beginning to execute long query")
    exec_thread.start()
    
    # Make sure the query has started before cancelling
    print("\n Waiting 15 seconds before canceling", end="", flush=True)
    
    seconds_waited = 0
    while seconds_waited < 15:
      seconds_waited += 1
      print(".", end="", flush=True)
      time.sleep(1)

    print("\n Cancelling the cursor's operation. This can take a few seconds.")
    cursor.cancel()
    
    print("\n Now checking the cursor status:")
    exec_thread.join(5)

    assert not exec_thread.is_alive()
    print("\n The previous command was successfully canceled")

    print("\n Now reusing the cursor to run a separate query.")
    
    # We can still execute a new command on the cursor
    cursor.execute("SELECT * FROM range(3)")

    print("\n Execution was successful. Results appear below:")

    print(cursor.fetchall())
