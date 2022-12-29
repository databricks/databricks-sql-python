from databricks import sql
import os

"""
Databricks experimentally supports data ingestion of local files via a cloud staging location.
Ingestion commands will work on DBR >12. And you must include a staging_allowed_local_path kwarg when
calling sql.connect().

Use databricks-sql-connector to PUT files into the staging location where Databricks can access them:

    PUT '/path/to/local/data.csv' INTO 'stage://tmp/some.user@databricks.com/salesdata/september.csv' OVERWRITE

Files in a staging location can also be retrieved with a GET command

    GET 'stage://tmp/some.user@databricks.com/salesdata/september.csv' TO 'data.csv'

and deleted with a REMOVE command:

    REMOVE 'stage://tmp/some.user@databricks.com/salesdata/september.csv'

Ingestion queries are passed to cursor.execute() like any other query. For GET and PUT commands, a local file
will be read or written. For security, this local file must be contained within, or descended from, a
staging_allowed_local_path of the connection.

Additionally, the connection can only manipulate files within the cloud storage location of the authenticated user.

To run this script: 

1. Set the INGESTION_USER constant to the account email address of the authenticated user
2. Set the FILEPATH constant to the path of a file that will be uploaded (this example assumes its a CSV file)
3. Run this file

Note: staging_allowed_local_path can be either a Pathlike object or a list of Pathlike objects.
"""

INGESTION_USER = "some.user@example.com"
FILEPATH = "example.csv"

# FILEPATH can be relative to the current directory.
# Resolve it into an absolute path
_complete_path = os.path.realpath(FILEPATH)

if not os.path.exists(_complete_path):

    # It's easiest to save a file in the same directory as this script. But any path to a file will work.
    raise Exception(
        "You need to set FILEPATH in this script to a file that actually exists."
    )

# Set staging_allowed_local_path equal to the directory that contains FILEPATH
staging_allowed_local_path = os.path.split(_complete_path)[0]

with sql.connect(
    server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
    http_path=os.getenv("DATABRICKS_HTTP_PATH"),
    access_token=os.getenv("DATABRICKS_TOKEN"),
    staging_allowed_local_path=staging_allowed_local_path,
) as connection:

    with connection.cursor() as cursor:

        # Ingestion commands are executed like any other SQL.
        # Here's a sample PUT query. You can remove OVERWRITE at the end to avoid silently overwriting data.
        query = f"PUT '{_complete_path}' INTO 'stage://tmp/{INGESTION_USER}/pysql_examples/demo.csv' OVERWRITE"

        print(f"Uploading {FILEPATH} to staging location")
        cursor.execute(query)
        print("Upload was successful")

        temp_fp = os.path.realpath("temp.csv")

        # Here's a sample GET query. Note that `temp_fp` must also be contained within, or descended from,
        # the staging_allowed_local_path.
        query = (
            f"GET 'stage://tmp/{INGESTION_USER}/pysql_examples/demo.csv' TO '{temp_fp}'"
        )

        print(f"Fetching from staging location into new file called temp.csv")
        cursor.execute(query)
        print("Download was successful")

        # Here's a sample REMOVE query. It cleans up the the demo.csv created in our first query
        query = f"REMOVE 'stage://tmp/{INGESTION_USER}/pysql_examples/demo.csv'"

        print("Removing demo.csv from staging location")
        cursor.execute(query)
        print("Remove was successful")
