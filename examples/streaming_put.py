#!/usr/bin/env python3
"""
Simple example of streaming PUT operations.

This demonstrates the basic usage of streaming PUT with the __input_stream__ token.
"""

import io
import os
from databricks import sql

with sql.connect(
    server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
    http_path=os.getenv("DATABRICKS_HTTP_PATH"),
    access_token=os.getenv("DATABRICKS_TOKEN"),
) as connection:

    with connection.cursor() as cursor:
        # Create a simple data stream
        data = b"Hello, streaming world!"
        stream = io.BytesIO(data)
        
        # Get catalog, schema, and volume from environment variables
        catalog = os.getenv("DATABRICKS_CATALOG")
        schema = os.getenv("DATABRICKS_SCHEMA")
        volume = os.getenv("DATABRICKS_VOLUME")
        
        # Upload to Unity Catalog volume
        cursor.execute(
            f"PUT '__input_stream__' INTO '/Volumes/{catalog}/{schema}/{volume}/hello.txt' OVERWRITE",
            input_stream=stream
        )
        
        print("File uploaded successfully!") 