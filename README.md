# Databricks SQL Connector for Python

The Databricks SQL Connector for Python allows you to develop Python applications that connect to Databricks clusters and SQL warehouses. It is a Thrift-based client with no dependencies on ODBC or JDBC. It conforms to the [Python DB API 2.0 specification](https://www.python.org/dev/peps/pep-0249/).

This connector uses Arrow as the data-exchange format, and supports APIs to directly fetch Arrow tables. Arrow tables are wrapped in the `ArrowQueue` class to provide a natural API to get several rows at a time.

You are welcome to file an issue here for general use cases. You can also contact Databricks Support [here](help.databricks.com).

# Documentation

For the latest documentation, see

- [Databricks](https://docs.databricks.com/dev-tools/python-sql-connector.html)
- [Azure Databricks](https://docs.microsoft.com/en-us/azure/databricks/dev-tools/python-sql-connector)

## Quickstart

Install the library with `pip install databricks-sql-connector`

Example usage:

```python
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

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md)

## License

[Apache License 2.0](LICENSE)