# Databricks SQL Connector for Python

**Status: Private preview**

Documentation can be found here: [Databricks SQL Connector for Python](https://docs.databricks.com/dev-tools/python-sql-connector.html).

## About

The Databricks SQL Connector is a Python library that allows you to use Python code to run
SQL commands on Databricks clusters and Databricks SQL endpoints.
This library follows [PEP 249 -- Python Database API Specification v2.0](https://www.python.org/dev/peps/pep-0249/).

## Quickstart

Install the library with `pip install databricks-sql-connector`

Example usage:

```
from databricks import sql

connection = sql.connect(
  server_hostname='<server-hostname>',
  http_path='<http-path>',
  access_token='<personal-access-token>')

cursor = connection.cursor()

cursor.execute('SELECT * FROM RANGE(10)')
result = cursor.fetchall()
for row in result:
  print(row)

cursor.close()
connection.close()
```

Where:
- `<server-hostname>` is the Databricks instance host name.
- `<http-path>` is the HTTP Path either to a Databricks SQL endpoint (e.g. /sql/1.0/endpoints/1234567890abcdef),
   or to a Databricks Runtime interactive cluster (e.g. /sql/protocolv1/o/1234567890123456/1234-123456-slid123)
- `<personal-access-token>` is a HTTP Bearer access token, e.g. a Databricks Personal Access Token.

For more information, see [Databricks documentation](https://docs.databricks.com/dev-tools/python-sql-connector.html).
