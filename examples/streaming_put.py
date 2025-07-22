#!/usr/bin/env python3
"""
Simple example of streaming PUT operations.

This demonstrates the basic usage of streaming PUT with the __input_stream__ token.
"""

import io
import os
from databricks import sql

def main():
    """Simple streaming PUT example."""
    
    # Connect to Databricks
    connection = sql.connect(
        server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
        http_path=os.getenv("DATABRICKS_HTTP_PATH"),
        access_token=os.getenv("DATABRICKS_TOKEN"),
    )
    
    with connection.cursor() as cursor:
        # Create a simple data stream
        data = b"Hello, streaming world!"
        stream = io.BytesIO(data)
        
        # Upload to Unity Catalog volume
        cursor.execute(
            "PUT '__input_stream__' INTO '/Volumes/my_catalog/my_schema/my_volume/hello.txt'",
            input_stream=stream
        )
        
        print("File uploaded successfully!")

if __name__ == "__main__":
    main() 