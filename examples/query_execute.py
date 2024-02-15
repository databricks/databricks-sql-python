import threading
from databricks import sql
import os
import logging


logger = logging.getLogger("databricks.sql")
logger.setLevel(logging.INFO)
fh = logging.FileHandler('pysqllogs.log')
fh.setFormatter(logging.Formatter("%(asctime)s %(process)d %(thread)d %(message)s"))
fh.setLevel(logging.DEBUG)
logger.addHandler(fh)

with sql.connect(server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME"),
                 http_path       = os.getenv("DATABRICKS_HTTP_PATH"),
                 access_token    = os.getenv("DATABRICKS_TOKEN"),
                #  max_download_threads = 2
                 ) as connection:

  with connection.cursor(
    # arraysize=100
    ) as cursor:
    cursor.execute("SELECT * FROM range(0, 10000000) AS t1 LEFT JOIN (SELECT 1) AS t2")
    # cursor.execute("SELECT * FROM andre.plotly_iot_dashboard.bronze_sensors limit 1000001")
    try:
      result = cursor.fetchall()
      print(f"result length: {len(result)}")
    except sql.exc.ResultSetDownloadError as e:
      print(f"error: {e}")
    # buffer_size_bytes=4857600