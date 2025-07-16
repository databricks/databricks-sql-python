from databricks import sql
import os

"""
This example demonstrates how to use the VolumeClient.

"""

host = os.getenv("DATABRICKS_SERVER_HOSTNAME")
http_path = os.getenv("DATABRICKS_HTTP_PATH")
access_token = os.getenv("DATABRICKS_TOKEN")
catalog = os.getenv("DATABRICKS_CATALOG")
schema = os.getenv("DATABRICKS_SCHEMA")

if not all([host, http_path, access_token, catalog, schema]):
    print("Error: Some environment variables are missing")
    exit(1)

# Type assertions for the linter
assert host and http_path and access_token and catalog and schema

# Connect to Databricks
with sql.connect(
    server_hostname=host,
    http_path=http_path,
    access_token=access_token,
) as connection:

    # Get the volume client
    volume_client = connection.get_volume_client()
    
    # Example volume name (you can change this to match your setup)
    volume_name = "sv-volume"
    
    print(f"Using volume: /Volumes/{catalog}/{schema}/{volume_name}/")
    print("-" * 60)
    
    # Basic usage examples
    print("Basic usage examples:")
    print("-" * 40)
    
    # Check if a file exists
    exists = volume_client.object_exists(catalog, schema, volume_name, "sample-1.txt")
    print(f"File 'sample-1.txt' exists: {exists}")
    
    # Check if a file in subdirectory exists
    exists = volume_client.object_exists(catalog, schema, volume_name, "dir-1/sample-1.txt")
    print(f"File 'dir-1/sample-1.txt' exists: {exists}")

    # Check if a directory exists
    exists = volume_client.object_exists(catalog, schema, volume_name, "dir-1")
    print(f"Directory 'dir-1' exists: {exists}")
    
    print("\nCase-insensitive matching:")
    print("-" * 40)
    
    # Case-insensitive check
    exists = volume_client.object_exists(catalog, schema, volume_name, "SAMPLE-1.txt", case_sensitive=False)
    print(f"File 'SAMPLE-1.txt' exists (case-insensitive): {exists}")
    
    print("-" * 60)
    print("Volume operations example completed!")

