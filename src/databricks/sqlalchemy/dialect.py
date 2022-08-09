from databricks import sql
from typing import AnyStr

from sqlalchemy import util, exc, types
from sqlalchemy.engine import default


class DatabricksDialect(default.DefaultDialect):

    # Possible attributes are defined here: https://docs.sqlalchemy.org/en/14/core/internals.html#sqlalchemy.engine.Dialect
    name: str = "databricks"
    driver: str = "thrift"
    default_schema_name: str = "default"

    @classmethod
    def dbapi(cls):
        return sql

    def create_connect_args(self, url):
        # Expected URI format is: databricks+thrift://token:dapi***@***.cloud.databricks.com?http_path=/sql/***

        kwargs = {
            "server_hostname": url.host,
            "access_token": url.password,
            "http_path": url.query.get("http_path"),
        }

        return [], kwargs

    def get_table_names(self, *args, **kwargs):

        # TODO: Implement with native driver `.tables()` call
        return super().get_table_names(*args, **kwargs)

    def get_columns(self, *args, **kwargs):

        # TODO: Implement with native driver `.columns()` call

        return super().get_columns(*args, **kwargs)

    def do_rollback(self, dbapi_connection):
        # Databricks SQL Does not support transaction
        pass

    def has_table(self, connection, table_name, schema=None, **kwargs) -> bool:
        """Required for `tests.sqlalchemy.integration.test_create_table` to pass.
        """
        try:
            COLUMN_NAME = 3
            with self.get_driver_connection(
                connection
            )._dbapi_connection.dbapi_connection.cursor() as cur:
                data = cur.columns(
                    schema_name=schema or "default", table_name=table_name
                ).fetchmany(1)
            # the table exists as long as there's a non-zero number of columns
            return len(data) > 0
        except exc.NoSuchTableError:
            return False
