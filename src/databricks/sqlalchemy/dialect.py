from databricks import sql
from databricks import sql as dbsql

# cribbing from Hive
from pyhive.sqlalchemy_hive import HiveExecutionContext, HiveIdentifierPreparer, HiveCompiler, HiveTypeCompiler
from pyhive.sqlalchemy_hive import _type_map

import re

from sqlalchemy import types
from sqlalchemy import util

from sqlalchemy.engine import default, interfaces
from sqlalchemy.sql import compiler

from typing import AnyStr


class DatabricksIdentifierPreparer(compiler.IdentifierPreparer):
    # SparkSQL identifier specification:
    # ref: https://spark.apache.org/docs/latest/sql-ref-identifier.html

    legal_characters = re.compile(r'^[A-Z0-9_]+$', re.I)

    def __init__(self, dialect):
        super(DatabricksIdentifierPreparer, self).__init__(
            dialect,
            initial_quote='`',
        )


class DatabricksExecutionContext(default.DefaultExecutionContext):
    # There doesn't seem to be any override of DefaultExecutionContext required
    # but I will nonetheless introduce this class for clarity

    # TODO: revisit server-side cursors
    # ref: https://docs.databricks.com/dev-tools/python-sql-connector.html#manage-cursors-and-connections
    pass


class DatabricksTypeCompiler(compiler.GenericTypeCompiler):
    # ref: https://spark.apache.org/docs/latest/sql-ref-datatypes.html

    def visit_TINYINT(self, type_):
        return 'TINYINT'

    def visit_SMALLINT(self, type_, **kw):
        return "SMALLINT"

    def visit_INTEGER(self, type_, **kw):
        return "INT"

    def visit_BIGINT(self, type_, **kw):
        return "BIGINT"


    def visit_FLOAT(self, type_, **kw):
        return "FLOAT"

    def visit_DOUBLE(self, type_, **kw):
        return "DOUBLE"


    def visit_DECIMAL(self, type_, **kw):
        if type_.precision is None:
            return "DECIMAL"
        elif type_.scale is None:
            return "DECIMAL(%(precision)s)" % {"precision": type_.precision}
        else:
            return "DECIMAL(%(precision)s, %(scale)s)" % {
                "precision": type_.precision,
                "scale": type_.scale,
            }


    def visit_DATE(self, type_, **kw):
        return "DATE"

    def visit_TIMESTAMP(self, type_, **kw):
        return "TIMESTAMP"


    def visit_BOOLEAN(self, type_, **kw):
        return "BOOLEAN"

    def visit_STRING(self, type_, **kw):
        return "STRING"


class DatabricksCompiler(compiler.SQLCompiler):
    # stub
    pass



class DatabricksDialect(default.DefaultDialect):
    # Possible attributes are defined here: https://docs.sqlalchemy.org/en/14/core/internals.html#sqlalchemy.engine.Dialect
    name: str = "databricks"
    driver: str= "thrift"
    default_schema_name: str = "default"

    preparer = DatabricksIdentifierPreparer
    execution_ctx_cls = DatabricksExecutionContext
    statement_compiler = DatabricksCompiler
    type_compiler = DatabricksTypeCompiler

    # the following attributes are cribbed from HiveDialect:
    supports_views = False
    supports_alter = True
    supports_pk_autoincrement = False
    supports_default_values = False
    supports_empty_insert = False
    supports_native_decimal = True
    supports_native_boolean = True
    supports_unicode_statements = True
    supports_unicode_binds = True
    returns_unicode_strings = True
    description_encoding = None
    supports_multivalues_insert = True
    supports_sane_rowcount = False


    @classmethod
    def dbapi(cls):
        return sql

    def create_connect_args(self, url: "URL"):
        # Expected URI format is: databricks+thrift://token:dapi***@***.cloud.databricks.com?http_path=/sql/***

        # TODO: add schema in

        kwargs = {
            "server_hostname": url.host,
            "access_token": url.password,
            "http_path": url.query.get("http_path")
        }

        return [], kwargs

    # def initialize(self, connection) -> None:
    #     super().initialize(connection)

    def get_schema_names(self, connection, **kw):
        connection = dbsql.connect(
            server_hostname=kwargs['server_hostname'],
            http_path=kwargs['http_path'],
            access_token=kwargs['access_token'],
            schema='default'
        )
        TABLE_SCHEM = 2
        with connection.cursor() as cur:
            data = cur.schemas(catalog_name='%').fetchall()
            _schemas = [i[TABLE_SCHEM] for i in data]

        return _schemas

    def get_table_names(self, connection, schema = None, **kw):
        # TODO: can initialize() take care of shared initialization?
        connection = dbsql.connect(
            server_hostname=kwargs['server_hostname'],
            http_path=kwargs['http_path'],
            access_token=kwargs['access_token'],
            schema='default'
        )

        breakpoint()

        TABLE_NAME = 2
        with connection.cursor() as cur:
            data = cur.tables(schema_name='default').fetchall()
            _tables = [i[TABLE_NAME] for i in data]

        return _tables


    def get_columns(self, connection, table_name, schema=None, **kw):
        # TODO: Implement with native driver `.columns()` call
        return self._get_table_columns(connection, table_name, schema)

    
    def get_view_names(self, connection, schema=None, **kw):
        # no views at present
        return []

    # private method to serve get_columns() and has_tables()
    def _get_table_columns(self, connection, table_name, schema):
        with connection.cursor() as cur:
            data = cur.columns(schema_name='default', table_name=table_name).fetchall()
            _tables = [i[COLUMN_NAME] for i in data]
        return _tables

    def has_table(
        self,
        connection,
        table_name,
        schema = None,
        **kw,
    ) -> bool:
        try:
            self._get_table_columns(connection, table_name, schema)
            return True
        except exc.NoSuchTableError:
            return False

    def has_table(self, connection, table_name, schema=None):
        # Spark has no foreign keys
        return []

    def has_table(self, connection, table_name, schema=None):
        # Spark has no primary keys
        return []

    def get_indexes(self, connection, table_name, schema=None, **kw):
        # TODO: treat partitions as indices
        return []

    def do_rollback(self, dbapi_connection) -> None:
        # Spark/Delta transaction only covers single-table updates... to simplify things, just skip this for now.
        pass
