from databricks import sql
import os

with sql.connect(
    server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
    http_path=os.getenv("DATABRICKS_HTTP_PATH"),
    access_token=os.getenv("DATABRICKS_TOKEN"),
) as connection:

    with connection.cursor() as cursor:
        cursor.execute("CREATE TABLE IF NOT EXISTS squares (x int, x_squared int)")

        squares = [(i, i * i) for i in range(100)]
        values = ",".join([f"({x}, {y})" for (x, y) in squares])

        cursor.execute(f"INSERT INTO squares VALUES {values}")

        cursor.execute("SELECT * FROM squares LIMIT 10")

        result = cursor.fetchall()

        for row in result:
            print(row)
