from databricks import sql
import os
import logging


logger = logging.getLogger("databricks.sql")
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler("pysqllogs.log")
fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(process)d %(thread)d %(message)s"))
fh.setLevel(logging.DEBUG)
logger.addHandler(fh)

with sql.connect(
    server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
    http_path=os.getenv("DATABRICKS_HTTP_PATH"),
    access_token=os.getenv("DATABRICKS_TOKEN"),
    use_cloud_fetch=True,
    max_download_threads = 2
) as connection:

    with connection.cursor(arraysize=1000, buffer_size_bytes=54857600) as cursor:
        print(
            "executing query: SELECT * FROM range(0, 20000000) AS t1 LEFT JOIN (SELECT 1) AS t2"
        )
        cursor.execute("SELECT * FROM range(0, 20000000) AS t1 LEFT JOIN (SELECT 1) AS t2")
        try:
            while True:
                row = cursor.fetchone()
                if row is None:
                    break
                print(f"row: {row}")
        except sql.exc.ResultSetDownloadError as e:
            print(f"error: {e}")
