import os

from databricks import sql
from databricks import sql as dbsql

# cribbing from Hive
from pyhive.sqlalchemy_hive import HiveExecutionContext, HiveIdentifierPreparer, HiveCompiler, HiveTypeCompiler
from pyhive.sqlalchemy_hive import HiveDate, HiveTimestamp

import re

from sqlalchemy import types

from sqlalchemy.types import Integer, BigInteger, SmallInteger, Float, DECIMAL, Boolean;
from sqlalchemy.types import String;

from sqlalchemy import util
from sqlalchemy import exc

from sqlalchemy.engine import default, interfaces
from sqlalchemy.sql import compiler

from typing import AnyStr


# provide a way to break in
debugbreakpoint = os.getenv("DATABRICKS_DIALECT_DEBUG") or False


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

    # TODO: we are leveraging MySQL's impl
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

    # TODO: we are leveraging MySQL's impl
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

    def visit_DATETIME(self, type_, **kw):
        # TODO: incomplete?
        return "INTERVAL"

    def visit_BOOLEAN(self, type_, **kw):
        return "BOOLEAN"

    def visit_STRING(self, type_, **kw):
        return "STRING"

    # TODO: why is this needed even though there's no occurence of VARCHAR? 
    def visit_VARCHAR(self, type_, **kw):
        return "STRING"


class DatabricksCompiler(compiler.SQLCompiler):
    # stub
    pass



# This lookup table is by DATA_TYPE and is rather nice since Decimal can be detected directly.
# However, as DATA_TYPE is rather obtuse... and potentially risky going forward, we switched to use COLUMN_TYPE_NAME instead (the table below)
# _type_map = {
#     -6: types.Integer,     # tiny_int
#     5: types.Integer,      # small_int
#     4: types.Integer,      # int
#     -5: types.BigInteger,  # big_int
#     6: types.Float, 
#     3: types.DECIMAL,  
#     16: types.Boolean,
#     12: types.String,
#     91: HiveDate,          # date
#     93: HiveTimestamp,     # timestamp
#     # TODO: interval
# }


# This lookup is by TYPE_NAME which is easier to maintain and likely safer in the long term. 
# NB: Decimal is explicitly excluded here as an occurence's TYPE_NAME includes its precision and scale
#     See/refer to COLUMN_TYPE_DECIMAL below.

_type_map = {
    'TINYINT': types.Integer,    # tiny_int
    'SMALLINT': types.Integer,   # small_int
    'INT': types.Integer,        # int
    'BIGINT': types.BigInteger,  # big_int
    'FLOAT': types.Float, 
    'DOUBLE': types.Float,       # double fits into a Python float
    'BOOLEAN': types.Boolean,
    'STRING': types.String,
    'DATE': HiveDate,            # date
    'TIMESTAMP': HiveTimestamp,  # timestamp
    # TODO: interval
}
# this is used to match decimal's DATA_TYPE; it will map to types.DECIMAL
COLUMN_TYPE_DECIMAL=3


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
    
    # added based on comments here: https://docs.sqlalchemy.org/en/14/errors.html#error-cprf
    supports_statement_cache = False

    @classmethod
    def dbapi(cls):
        return sql

    def create_connect_args(self, url: "URL"):
        # Expected URI format is: databricks+thrift://token:dapi***@***.cloud.databricks.com/***?http_path=/sql/***
        kwargs = {
            "server_hostname": url.host,
            "access_token": url.password,
            "http_path": url.query.get("http_path"),
            "schema": url.database or "default"
        }

        return [], kwargs

    # def initialize(self, connection) -> None:
    #     super().initialize(connection)

    def get_schema_names(self, connection, **kwargs):
        # conn = dbsql.connect(
        #     server_hostname=kwargs['server_hostname'],
        #     http_path=kwargs['http_path'],
        #     access_token=kwargs['access_token'],
        #     schema=kwargs['schema']
        # )
        # TODO: look up correct index for TABLE_SCHEM

        TABLE_SCHEM = 2
        # if debugbreakpoint:
        #     breakpoint()
        with self.get_driver_connection(connection)._dbapi_connection.dbapi_connection.cursor() as cur:
            data = cur.schemas(catalog_name='%').fetchall()
            _schemas = [i[TABLE_SCHEM] for i in data]

        return _schemas

    def get_table_names(self, connection, schema = None, **kwargs):
        # if debugbreakpoint:
        #     breakpoint()

        TABLE_NAME = 2
        with self.get_driver_connection(connection)._dbapi_connection.dbapi_connection.cursor() as cur:
            data = cur.tables(schema_name=schema).fetchall()
            _tables = [i[TABLE_NAME] for i in data]

        return _tables


    def get_columns(self, connection, table_name, schema=None, **kwargs):
        # Example row
        # Row(TABLE_CAT='hive_metastore', TABLE_SCHEM='george_chow_dbtest', TABLE_NAME='all_types', COLUMN_NAME='f_byte', DATA_TYPE=4, 
        #       TYPE_NAME='INT', COLUMN_SIZE=4, BUFFER_LENGTH=None, DECIMAL_DIGITS=0, NUM_PREC_RADIX=10, 
        #       NULLABLE=1, REMARKS='', COLUMN_DEF=None, SQL_DATA_TYPE=None, SQL_DATETIME_SUB=None, 
        #       CHAR_OCTET_LENGTH=None, ORDINAL_POSITION=0, IS_NULLABLE='YES', SCOPE_CATALOG=None, SCOPE_SCHEMA=None, 
        #       SCOPE_TABLE=None, SOURCE_DATA_TYPE=None, IS_AUTO_INCREMENT='NO')
        COLUMN_NAME=3
        COLUMN_TYPE_NAME=5 
        COLUMN_TYPE=4 
        COLUMN_NULLABLE=17
        COLUMN_COMMENT=11
        COLUMN_AUTOINCREMENT=22


        result = []
        with self.get_driver_connection(connection)._dbapi_connection.dbapi_connection.cursor() as cur:
            # if debugbreakpoint:
            #     breakpoint()
            data = cur.columns(schema_name=schema, table_name=table_name).fetchall()
            for i in data:
                # if debugbreakpoint:
                #     breakpoint()

                # filled-in according to interfaces.py's class ReflectedColumn(TypedDict):
                try:
                    if (i[COLUMN_TYPE] != COLUMN_TYPE_DECIMAL):
                        coltype = _type_map[i[COLUMN_TYPE_NAME]]
                    else:
                        # special processing needed as DECIMAL's COLUMN_TYPE includes the precision/scale of this occurrence
                        coltype = types.DECIMAL;

                    # coltype = _type_map[i[COLUMN_TYPE]]
                except KeyError:
                    util.warn(f"Did not recognize type '{i[COLUMN_TYPE_NAME]}'({i[COLUMN_TYPE]}) of column '{i[COLUMN_NAME]}'")
                    coltype = types.NullType

                try:
                    nullable = i[COLUMN_NULLABLE] == 'YES';
                except KeyError:
                    nullable = True;

                try:
                    autoincrement = i[COLUMN_AUTOINCREMENT] == 'YES';
                except KeyError:
                    autoincrement = False;

                result.append({
                    'name': i[COLUMN_NAME],
                    'type': coltype,
                    'nullable': nullable,
                    'comment': i[COLUMN_COMMENT],
                    'autoincrement': autoincrement,
                })

        return result

    
    def get_view_names(self, connection, schema=None, **kwargs):
        # no views at present
        return []

    def has_table(
        self,
        connection,
        table_name,
        schema = None,
        **kwargs,
    ) -> bool:
        # if debugbreakpoint:
        #     breakpoint()
        try:
            COLUMN_NAME=3
            with self.get_driver_connection(connection)._dbapi_connection.dbapi_connection.cursor() as cur:
                data = cur.columns(schema_name=schema or 'default', table_name=table_name).fetchmany(1)
            # the table exists as long as there's a non-zero number of columns
            return len(data) > 0
        except exc.NoSuchTableError:
            return False

    def get_foreign_keys(self, connection, table_name, schema=None, **kwargs):
        # Spark has no foreign keys
        return []

    def get_pk_constraint(self, connection, table_name, schema=None, **kwargs):
        # Spark has no primary keys
        return []

    def get_indexes(self, connection, table_name, schema=None, **kwargs):
        # TODO: treat partitions as indices
        return []

    def do_rollback(self, dbapi_connection) -> None:
        # Spark/Delta transaction only covers single-table updates... to simplify things, just skip this for now.
        pass
