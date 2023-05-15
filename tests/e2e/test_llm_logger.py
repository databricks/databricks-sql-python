import os
import logging
from databricks import sql

class CustomFormatter(logging.Formatter):

    grey = "\x1b[38;20m"
    pink = "\x1b[38;5;206m"
    green = "\x1b[32;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s (%(filename)s:%(lineno)d)"

    FORMATS = {
        logging.DEBUG: grey + format + reset,
        11: pink + format + reset,
        logging.INFO: green + format + reset,
        logging.WARNING: yellow + format + reset,
        logging.ERROR: red + format + reset,
        logging.CRITICAL: bold_red + format + reset
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)

logger = logging.getLogger()
logger.setLevel(11)
# logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(CustomFormatter())
logger.addHandler(ch)


def main():
    host = os.getenv("DATABRICKS_HOST")
    http_path = os.getenv("DATABRICKS_HTTP_PATH")
    access_token = os.getenv("DATABRICKS_ACCESS_TOKEN")

    connection = sql.connect(
        server_hostname=host,
        http_path=http_path,
        access_token=access_token
    )

    cursor = connection.cursor()
    cursor.execute('''
        SELECT count(*) as total_trips FROM `samples`.`nyctaxi`.`trips`
        WHERE tpep_pickup_datetime BETWEEN TIMESTAMP '2016-01-01 12:07'
        AND TIMESTAMP '2016-02-30 12:07'
        AND pickup_zip IN (10001)
    ''')
    ### Other examples
    # cursor.execute('SELECT * from default.diamonds WHERE 10 LIMIT')
    # cursor.execute('SELE * FROM RANGE(10)')
    # cursor.execute('SELECT * FROM RANGE("hi")')
    # cursor.execute('SELECT max(karate) from default.diamonds')
    # cursor.execute('SELECT max(carat) from default.diamond')

    result = cursor.fetchall()
    for row in result:
        print(row)

    cursor.close()
    connection.close()

if __name__ == "__main__":
    main()