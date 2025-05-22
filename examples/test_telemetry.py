import os
import databricks.sql as sql

# Create connection with telemetry enabled
conn = sql.connect(
    server_hostname=os.environ["DATABRICKS_SERVER_HOSTNAME"],
    http_path=os.environ["DATABRICKS_HTTP_PATH"],
    access_token=os.environ["DATABRICKS_TOKEN"],
    enable_telemetry=True,  # Enable telemetry
    telemetry_batch_size=1  # Set batch size to 1
)

# Execute a simple query to generate telemetry
cursor = conn.cursor()
cursor.execute("SELECT * FROM main.eng_lumberjack.staging_frontend_log_sql_driver_log limit 1")
cursor.fetchall()

# Close the connection
cursor.close()
conn.close() 