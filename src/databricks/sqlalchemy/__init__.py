import re
from typing import Any, Optional

import sqlalchemy
from sqlalchemy import event
from sqlalchemy.engine import Engine, default, reflection
from sqlalchemy.engine.interfaces import (
    ReflectedForeignKeyConstraint,
    ReflectedPrimaryKeyConstraint,
)
from sqlalchemy.exc import DatabaseError, SQLAlchemyError

import databricks.sqlalchemy._ddl as dialect_ddl_impl

# This import is required to process our @compiles decorators
import databricks.sqlalchemy._types as dialect_type_impl
from databricks import sql
from databricks.sqlalchemy.utils import (
    extract_identifier_groups_from_string,
    extract_identifiers_from_string,
)

try:
    import alembic
except ImportError:
    pass
else:
    from alembic.ddl import DefaultImpl

    class DatabricksImpl(DefaultImpl):
        __dialect__ = "databricks"


class DatabricksDialect(default.DefaultDialect):
    """This dialect implements only those methods required to pass our e2e tests"""

    # Possible attributes are defined here: https://docs.sqlalchemy.org/en/14/core/internals.html#sqlalchemy.engine.Dialect
    name: str = "databricks"
    driver: str = "databricks"
    default_schema_name: str = "default"
    preparer = dialect_ddl_impl.DatabricksIdentifierPreparer  # type: ignore
    ddl_compiler = dialect_ddl_impl.DatabricksDDLCompiler
    statement_compiler = dialect_ddl_impl.DatabricksStatementCompiler
    supports_statement_cache: bool = True
    supports_multivalues_insert: bool = True
    supports_native_decimal: bool = True
    supports_sane_rowcount: bool = False
    non_native_boolean_check_constraint: bool = False
    supports_identity_columns: bool = True
    supports_schemas: bool = True
    paramstyle: str = "named"

    colspecs = {
        sqlalchemy.types.DateTime: dialect_type_impl.DatabricksDateTimeNoTimezoneType,
        sqlalchemy.types.Time: dialect_type_impl.DatabricksTimeType,
        sqlalchemy.types.String: dialect_type_impl.DatabricksStringType,
    }

    @classmethod
    def dbapi(cls):
        return sql

    def create_connect_args(self, url):
        # TODO: can schema be provided after HOST?
        # Expected URI format is: databricks+thrift://token:dapi***@***.cloud.databricks.com?http_path=/sql/***

        kwargs = {
            "server_hostname": url.host,
            "access_token": url.password,
            "http_path": url.query.get("http_path"),
            "catalog": url.query.get("catalog"),
            "schema": url.query.get("schema"),
        }

        self.schema = kwargs["schema"]
        self.catalog = kwargs["catalog"]

        return [], kwargs

    def get_columns(self, connection, table_name, schema=None, **kwargs):
        """Return information about columns in `table_name`.

        Given a :class:`_engine.Connection`, a string
        `table_name`, and an optional string `schema`, return column
        information as a list of dictionaries with these keys:

        name
          the column's name

        type
          [sqlalchemy.types#TypeEngine]

        nullable
          boolean

        default
          the column's default value

        autoincrement
          boolean

        sequence
          a dictionary of the form
              {'name' : str, 'start' :int, 'increment': int, 'minvalue': int,
               'maxvalue': int, 'nominvalue': bool, 'nomaxvalue': bool,
               'cycle': bool, 'cache': int, 'order': bool}

        Additional column attributes may be present.
        """

        _type_map = {
            "boolean": sqlalchemy.types.Boolean,
            "smallint": sqlalchemy.types.SmallInteger,
            "int": sqlalchemy.types.Integer,
            "bigint": sqlalchemy.types.BigInteger,
            "float": sqlalchemy.types.Float,
            "double": sqlalchemy.types.Float,
            "string": sqlalchemy.types.String,
            "varchar": sqlalchemy.types.String,
            "char": sqlalchemy.types.String,
            "binary": sqlalchemy.types.String,
            "array": sqlalchemy.types.String,
            "map": sqlalchemy.types.String,
            "struct": sqlalchemy.types.String,
            "uniontype": sqlalchemy.types.String,
            "decimal": sqlalchemy.types.Numeric,
            "timestamp": sqlalchemy.types.DateTime,
            "date": sqlalchemy.types.Date,
        }

        with self.get_connection_cursor(connection) as cur:
            resp = cur.columns(
                catalog_name=self.catalog,
                schema_name=schema or self.schema,
                table_name=table_name,
            ).fetchall()

        columns = []

        for col in resp:
            # Taken from PyHive. This removes added type info from decimals and maps
            _col_type = re.search(r"^\w+", col.TYPE_NAME).group(0)
            this_column = {
                "name": col.COLUMN_NAME,
                "type": _type_map[_col_type.lower()],
                "nullable": bool(col.NULLABLE),
                "default": col.COLUMN_DEF,
                "autoincrement": False if col.IS_AUTO_INCREMENT == "NO" else True,
            }
            columns.append(this_column)

        return columns

    @reflection.cache
    def get_pk_constraint(
        self,
        connection,
        table_name: str,
        schema: Optional[str] = None,
        **kw: Any,
    ) -> ReflectedPrimaryKeyConstraint:
        """Return information about the primary key constraint on
        table_name`.
        """

        with self.get_connection_cursor(connection) as cursor:
            # DESCRIBE TABLE EXTENDED doesn't support parameterised inputs :(
            result = cursor.execute(f"DESCRIBE TABLE EXTENDED {table_name}").fetchall()

        # DESCRIBE TABLE EXTENDED doesn't give a deterministic name to the field where
        # a primary key constraint will be found in its output. So we cycle through its
        # output looking for a match that includes "PRIMARY KEY". This is brittle. We
        # could optionally make two roundtrips: the first would query information_schema
        # for the name of the primary key constraint on this table, and a second to
        # DESCRIBE TABLE EXTENDED, at which point we would know the name of the constraint.
        # But for now we instead assume that Python list comprehension is faster than a
        # network roundtrip.
        dte_dict = {row["col_name"]: row["data_type"] for row in result}
        target = [(k, v) for k, v in dte_dict.items() if "PRIMARY KEY" in v]
        if target:
            name, _constraint_string = target[0]
            column_list = extract_identifiers_from_string(_constraint_string)
        else:
            name, column_list = None, None

        return {"constrained_columns": column_list, "name": name}

    def get_foreign_keys(
        self, connection, table_name, schema=None, **kw
    ) -> ReflectedForeignKeyConstraint:
        """Return information about foreign_keys in `table_name`.

        Given a :class:`_engine.Connection`, a string
        `table_name`, and an optional string `schema`, return foreign
        key information as a list of dicts with these keys:

        name
          the constraint's name

        constrained_columns
          a list of column names that make up the foreign key

        referred_schema
          the name of the referred schema

        referred_table
          the name of the referred table

        referred_columns
          a list of column names in the referred table that correspond to
          constrained_columns
        """
        """Return information about the primary key constraint on
        table_name`.
        """

        with self.get_connection_cursor(connection) as cursor:
            # DESCRIBE TABLE EXTENDED doesn't support parameterised inputs :(
            result = cursor.execute(
                f"DESCRIBE TABLE EXTENDED {schema + '.' if schema else ''}{table_name}"
            ).fetchall()

        # DESCRIBE TABLE EXTENDED doesn't give a deterministic name to the field where
        # a foreign key constraint will be found in its output. So we cycle through its
        # output looking for a match that includes "FOREIGN KEY". This is brittle. We
        # could optionally make two roundtrips: the first would query information_schema
        # for the name of the foreign key constraint on this table, and a second to
        # DESCRIBE TABLE EXTENDED, at which point we would know the name of the constraint.
        # But for now we instead assume that Python list comprehension is faster than a
        # network roundtrip.
        dte_dict = {row["col_name"]: row["data_type"] for row in result}
        target = [(k, v) for k, v in dte_dict.items() if "FOREIGN KEY" in v]

        def extract_constraint_dict_from_target(target):
            if target:
                name, _constraint_string = target
                _extracted = extract_identifier_groups_from_string(_constraint_string)
                constrained_columns_str, referred_columns_str = (
                    _extracted[0],
                    _extracted[1],
                )

                constrained_columns = extract_identifiers_from_string(
                    constrained_columns_str
                )
                referred_columns = extract_identifiers_from_string(referred_columns_str)
                referred_table = str(table_name)
            else:
                name, constrained_columns, referred_columns, referred_table = (
                    None,
                    None,
                    None,
                    None,
                )

            return {
                "constrained_columns": constrained_columns,
                "name": name,
                "referred_table": referred_table,
                "referred_columns": referred_columns,
            }

        if target:
            return [extract_constraint_dict_from_target(i) for i in target]
        else:
            return []

    def get_indexes(self, connection, table_name, schema=None, **kw):
        """Return information about indexes in `table_name`.

        Given a :class:`_engine.Connection`, a string
        `table_name` and an optional string `schema`, return index
        information as a list of dictionaries with these keys:

        name
          the index's name

        column_names
          list of column names in order

        unique
          boolean
        """
        # TODO: Implement this behaviour
        return []

    def get_table_names(self, connection, schema=None, **kwargs):
        TABLE_NAME = 1
        with self.get_connection_cursor(connection) as cur:
            sql_str = "SHOW TABLES FROM {}".format(
                ".".join([self.catalog, schema or self.schema])
            )
            data = cur.execute(sql_str).fetchall()
            _tables = [i[TABLE_NAME] for i in data]

        return _tables

    def get_view_names(self, connection, schema=None, **kwargs):
        VIEW_NAME = 1
        with self.get_connection_cursor(connection) as cur:
            sql_str = "SHOW VIEWS FROM {}".format(
                ".".join([self.catalog, schema or self.schema])
            )
            data = cur.execute(sql_str).fetchall()
            _tables = [i[VIEW_NAME] for i in data]

        return _tables

    def do_rollback(self, dbapi_connection):
        # Databricks SQL Does not support transactions
        pass

    @reflection.cache
    def has_table(
        self, connection, table_name, schema=None, catalog=None, **kwargs
    ) -> bool:
        """SQLAlchemy docstrings say dialect providers must implement this method"""

        _schema = schema or self.schema
        _catalog = catalog or self.catalog

        # DBR >12.x uses underscores in error messages
        DBR_LTE_12_NOT_FOUND_STRING = "Table or view not found"
        DBR_GT_12_NOT_FOUND_STRING = "TABLE_OR_VIEW_NOT_FOUND"

        try:
            res = connection.execute(
                sqlalchemy.text(
                    f"DESCRIBE TABLE `{_catalog}`.`{_schema}`.`{table_name}`"
                )
            )
            return True
        except DatabaseError as e:
            if DBR_GT_12_NOT_FOUND_STRING in str(
                e
            ) or DBR_LTE_12_NOT_FOUND_STRING in str(e):
                return False
            else:
                raise e

    def get_connection_cursor(self, connection):
        """Added for backwards compatibility with 1.3.x"""
        if hasattr(connection, "_dbapi_connection"):
            return connection._dbapi_connection.dbapi_connection.cursor()
        elif hasattr(connection, "raw_connection"):
            return connection.raw_connection().cursor()
        elif hasattr(connection, "connection"):
            return connection.connection.cursor()

        raise SQLAlchemyError(
            "Databricks dialect can't obtain a cursor context manager from the dbapi"
        )

    @reflection.cache
    def get_schema_names(self, connection, **kw):
        # Equivalent to SHOW DATABASES

        # TODO: replace with call to cursor.schemas() once its performance matches raw SQL
        return [row[0] for row in connection.execute("SHOW SCHEMAS")]


@event.listens_for(Engine, "do_connect")
def receive_do_connect(dialect, conn_rec, cargs, cparams):
    """Helpful for DS on traffic from clients using SQLAlchemy in particular"""

    # Ignore connect invocations that don't use our dialect
    if not dialect.name == "databricks":
        return

    if "_user_agent_entry" in cparams:
        new_user_agent = f"sqlalchemy + {cparams['_user_agent_entry']}"
    else:
        new_user_agent = "sqlalchemy"

    cparams["_user_agent_entry"] = new_user_agent

    if sqlalchemy.__version__.startswith("1.3"):
        # SQLAlchemy 1.3.x fails to parse the http_path, catalog, and schema from our connection string
        # These should be passed in as connect_args when building the Engine

        if "schema" in cparams:
            dialect.schema = cparams["schema"]

        if "catalog" in cparams:
            dialect.catalog = cparams["catalog"]
