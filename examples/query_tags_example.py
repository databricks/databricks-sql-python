import os
import databricks.sql as sql

"""
This example demonstrates how to use Query Tags.

Query Tags are key-value pairs that can be attached to SQL executions and will appear
in the system.query.history table for analytical purposes.

Format: "key1:value1,key2:value2,key3:value3"
"""

print("=== Query Tags Example ===\n")

with sql.connect(
    server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
    http_path=os.getenv("DATABRICKS_HTTP_PATH"),
    access_token=os.getenv("DATABRICKS_TOKEN"),
    session_configuration={
        'QUERY_TAGS': 'team:engineering,test:query-tags',
        'ansi_mode': False
    }
) as connection:
    
    with connection.cursor() as cursor:
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        print(f"  Result: {result[0]}")

print("\n=== Query Tags Example Complete ===")