import os
import databricks.sql as sql

"""
This example demonstrates how to use Query Tags.

Query Tags are key-value pairs that can be attached to SQL executions and will appear
in the system.query.history table for analytical purposes.

There are two ways to set query tags:
1. Session-level: Set in session_configuration (applies to all queries in the session)
2. Per-query level: Pass query_tags parameter to execute() or execute_async() (applies to specific query)

Format: Dictionary with string keys and optional string values
Example: {"team": "engineering", "application": "etl", "priority": "high"}

Special cases:
- If a value is None, only the key is included (no colon or value)
- Special characters (:, ,, \\) in values are automatically escaped
- Keys are not escaped (should be controlled identifiers)
"""

print("=== Query Tags Example ===\n")

# Example 1: Session-level query tags (old approach)
print("Example 1: Session-level query tags")
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

print()

# Example 2: Per-query query tags (new approach)
print("Example 2: Per-query query tags")
with sql.connect(
    server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
    http_path=os.getenv("DATABRICKS_HTTP_PATH"),
    access_token=os.getenv("DATABRICKS_TOKEN"),
) as connection:

    with connection.cursor() as cursor:
        # Query 1: Tags for a critical ETL job
        cursor.execute(
            "SELECT 1",
            query_tags={"team": "data-eng", "application": "etl", "priority": "high"}
        )
        result = cursor.fetchone()
        print(f"  ETL Query Result: {result[0]}")

        # Query 2: Tags with None value (key-only tag)
        cursor.execute(
            "SELECT 2",
            query_tags={"team": "analytics", "experimental": None}
        )
        result = cursor.fetchone()
        print(f"  Experimental Query Result: {result[0]}")

        # Query 3: Tags with special characters (automatically escaped)
        cursor.execute(
            "SELECT 3",
            query_tags={"description": "test:with:colons,and,commas"}
        )
        result = cursor.fetchone()
        print(f"  Special Chars Query Result: {result[0]}")

print()

# Example 3: Async execution with query tags
print("Example 3: Async execution with query tags")
with sql.connect(
    server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
    http_path=os.getenv("DATABRICKS_HTTP_PATH"),
    access_token=os.getenv("DATABRICKS_TOKEN"),
) as connection:

    with connection.cursor() as cursor:
        cursor.execute_async(
            "SELECT 4",
            query_tags={"team": "data-eng", "mode": "async"}
        )
        cursor.get_async_execution_result()
        result = cursor.fetchone()
        print(f"  Async Query Result: {result[0]}")

print("\n=== Query Tags Example Complete ===")