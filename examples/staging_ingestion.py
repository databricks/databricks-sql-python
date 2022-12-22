from databricks import sql
import os


"""
Databricks experimentally supports data ingestion of local files via a cloud staging location.
Ingestion commands will work on DBR >12. And you must include an uploads_base_path kwarg when
calling sql.connect().

Use databricks-sql-connector to PUT files into the staging location where Databricks can access them:

    PUT '/path/to/local/data.csv' INTO 'stage://tmp/some.user@databricks.com/salesdata/september.csv' OVERWRITE

Files in a staging location can also be retrieved with a GET command

    GET 'stage://tmp/some.user@databricks.com/salesdata/september.csv' TO 'data.csv'

and deleted with a REMOVE command:

    REMOVE 'stage://tmp/some.user@databricks.com/salesdata/september.csv'

Ingestion queries are passed to cursor.execute() like any other query. For GET and PUT commands, a local file
will be read or written. For security, this local file must be contained within, or descended from, the
uploads_base_path of the connection.

Additionally, the connection can only manipulate files within the cloud storage location of the authenticated user.

To run this script: 

1. Set the INGESTION_USER constant to the account email address of the authenticated user
2. Set the FILEPATH constant to the path of a file that will be uploaded
"""

INGESTION_USER = "user.name@example.com"
FILEPATH = "example.csv"

_complete_path = os.path.realpath(FILEPATH)
uploads_base_path = os.path.split(_complete_path)[:-1]


with sql.connect(server_hostname    = os.getenv("DATABRICKS_SERVER_HOSTNAME"),
                 http_path          = os.getenv("DATABRICKS_HTTP_PATH"),
                 access_token       = os.getenv("DATABRICKS_TOKEN"),
                 uploads_base_path  = uploads_base_path) as connection:

  with connection.cursor() as cursor:
    query = f"PUT '{_complete_path}' INTO 'stage://tmp/{INGESTION_USER}/pysql_examples/demo.csv' OVERWRITE"
    cursor.execute(query)