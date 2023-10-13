import re
from typing import Any, Optional, List

import sqlalchemy
from sqlalchemy import event, DDL
from sqlalchemy.engine import Engine, default, reflection, Connection, Row, CursorResult
from sqlalchemy.engine.interfaces import (
    ReflectedForeignKeyConstraint,
    ReflectedPrimaryKeyConstraint,
)
from sqlalchemy.exc import DatabaseError, SQLAlchemyError

import databricks.sqlalchemy._ddl as dialect_ddl_impl
from databricks.sql.exc import ServerOperationError

# This import is required to process our @compiles decorators
import databricks.sqlalchemy._types as dialect_type_impl
from databricks import sql
from databricks.sqlalchemy.utils import (
    extract_identifier_groups_from_string,
    extract_identifiers_from_string,
    extract_three_level_identifier_from_constraint_string
)

try:
    import alembic
except ImportError:
    pass
else:
    from alembic.ddl import DefaultImpl

    class DatabricksImpl(DefaultImpl):
        __dialect__ = "databricks"


import logging

logger = logging.getLogger(__name__)

DBR_LTE_12_NOT_FOUND_STRING = "Table or view not found"
DBR_GT_12_NOT_FOUND_STRING = "TABLE_OR_VIEW_NOT_FOUND"


def _match_table_not_found_string(message: str) -> bool:
    """Return True if the message contains a substring indicating that a table was not found"""
    return any(
        [
            DBR_LTE_12_NOT_FOUND_STRING in message,
            DBR_GT_12_NOT_FOUND_STRING in message,
        ]
    )


def _describe_table_extended_result_to_dict(result: CursorResult) -> dict:
    """Transform the output of DESCRIBE TABLE EXTENDED into a dictionary

    The output from DESCRIBE TABLE EXTENDED puts all values in the `data_type` column
    Even CONSTRAINT descriptions are contained in the `data_type` column
    Some rows have an empty string for their col_name. These are present only for spacing
    so we ignore them.
    """

    result_dict = {row.col_name: row.data_type for row in result if row.col_name != ""}

    return result_dict


def _extract_pk_from_dte_result(result: dict) -> ReflectedPrimaryKeyConstraint:
    """Return a dictionary with the keys:

    constrained_columns
      a list of column names that make up the primary key. Results is an empty list
      if no PRIMARY KEY is defined.

    name
      the name of the primary key constraint

    Today, DESCRIBE TABLE EXTENDED doesn't give a deterministic name to the field where
    a primary key constraint will be found in its output. So we cycle through its
    output looking for a match that includes "PRIMARY KEY". This is brittle. We
    could optionally make two roundtrips: the first would query information_schema
    for the name of the primary key constraint on this table, and a second to
    DESCRIBE TABLE EXTENDED, at which point we would know the name of the constraint.
    But for now we instead assume that Python list comprehension is faster than a
    network roundtrip.
    """

    # find any rows that contain "PRIMARY KEY" as the `data_type`
    filtered_rows = [(k, v) for k, v in result.items() if "PRIMARY KEY" in v]

    # bail if no primary key was found
    if not filtered_rows:
        return {"constrained_columns": [], "name": None}

    # there should only ever be one PRIMARY KEY that matches
    if len(filtered_rows) > 1:
        logger.warning(
            "Found more than one primary key constraint in DESCRIBE TABLE EXTENDED output. "
            "This is unexpected. Please report this as a bug. "
            "Only the first primary key constraint will be returned."
        )

    # target is a tuple of (constraint_name, constraint_string)
    target = filtered_rows[0]
    name = target[0]
    _constraint_string = target[1]
    column_list = extract_identifiers_from_string(_constraint_string)

    return {"constrained_columns": column_list, "name": name}


def _extract_single_fk_dict_from_dte_result_row(
    table_name: str, schema_name: Optional[str], fk_name: str, fk_constraint_string: str
) -> dict:
    """
    """
    
    # SQLAlchemy's ComponentReflectionTest::test_get_foreign_keys is strange in that it
    # expects the `referred_schema` member of the outputted dictionary to be None if
    # a `schema` argument was not passed to the dialect's `get_foreign_keys` method
    referred_table_dict = extract_three_level_identifier_from_constraint_string(fk_constraint_string)
    referred_table = referred_table_dict["table"]
    if schema_name:
        referred_schema = referred_table_dict["schema"]
    else:
        referred_schema = None

    _extracted = extract_identifier_groups_from_string(fk_constraint_string)
    constrained_columns_str, referred_columns_str = (
        _extracted[0],
        _extracted[1],
    )

    constrainted_columns = extract_identifiers_from_string(constrained_columns_str)
    referred_columns = extract_identifiers_from_string(referred_columns_str)

    return {
        "constrained_columns": constrainted_columns,
        "name": fk_name,
        "referred_table": referred_table,
        "referred_columns": referred_columns,
        "referred_schema": referred_schema,
    }


def _extract_fk_from_dte_result(
    table_name: str, schema_name: Optional[str], result: dict
) -> ReflectedForeignKeyConstraint:
    """Return a list of dictionaries with the keys:

    constrained_columns
      a list of column names that make up the foreign key

    name
      the name of the foreign key constraint

    referred_table
      the name of the table that the foreign key references

    referred_columns
      a list of column names that are referenced by the foreign key

    Returns an empty list if no foreign key is defined.

    Today, DESCRIBE TABLE EXTENDED doesn't give a deterministic name to the field where
    a foreign key constraint will be found in its output. So we cycle through its
    output looking for a match that includes "FOREIGN KEY". This is brittle. We
    could optionally make two roundtrips: the first would query information_schema
    for the name of the foreign key constraint on this table, and a second to
    DESCRIBE TABLE EXTENDED, at which point we would know the name of the constraint.
    But for now we instead assume that Python list comprehension is faster than a
    network roundtrip.
    """

    # find any rows that contain "FOREIGN_KEY" as the `data_type`
    filtered_rows = [(k, v) for k, v in result.items() if "FOREIGN KEY" in v]

    # bail if no foreign key was found
    if not filtered_rows:
        return []

    constraint_list = []

    # target is a tuple of (constraint_name, constraint_string)
    for target in filtered_rows:
        _constraint_name, _constraint_string = target
        this_constraint_dict = _extract_single_fk_dict_from_dte_result_row(
            table_name, schema_name, _constraint_name, _constraint_string
        )
        constraint_list.append(this_constraint_dict)

    return constraint_list


COLUMN_TYPE_MAP = {
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

        with self.get_connection_cursor(connection) as cur:
            resp = cur.columns(
                catalog_name=self.catalog,
                schema_name=schema or self.schema,
                table_name=table_name,
            ).fetchall()

        if not resp:
            raise sqlalchemy.exc.NoSuchTableError(table_name)
        columns = []

        for col in resp:
            # Taken from PyHive. This removes added type info from decimals and maps
            _col_type = re.search(r"^\w+", col.TYPE_NAME).group(0)
            this_column = {
                "name": col.COLUMN_NAME,
                "type": COLUMN_TYPE_MAP[_col_type.lower()],
                "nullable": bool(col.NULLABLE),
                "default": col.COLUMN_DEF,
                "autoincrement": False if col.IS_AUTO_INCREMENT == "NO" else True,
            }
            columns.append(this_column)

        return columns

    def _describe_table_extended(
        self,
        connection: Connection,
        table_name: str,
        catalog_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        expect_result=True,
    ):
        """Run DESCRIBE TABLE EXTENDED on a table and return a dictionary of the result.

        This method is the fastest way to check for the presence of a table in a schema.

        If expect_result is False, this method returns None as the output dict isn't required.

        Raises NoSuchTableError if the table is not present in the schema.
        """

        _target_catalog = catalog_name or self.catalog
        _target_schema = schema_name or self.schema
        _target = f"`{_target_catalog}`.`{_target_schema}`.`{table_name}`"

        # sql injection risk?
        # DESCRIBE TABLE EXTENDED in DBR doesn't support parameterised inputs :(
        stmt = DDL(f"DESCRIBE TABLE EXTENDED {_target}")

        try:
            result = connection.execute(stmt).all()
        except DatabaseError as e:
            if _match_table_not_found_string(str(e)):
                raise sqlalchemy.exc.NoSuchTableError(
                    f"No such table {table_name}"
                ) from e
            raise e

        if not expect_result:
            return None

        fmt_result = _describe_table_extended_result_to_dict(result)
        return fmt_result

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

        result = self._describe_table_extended(
            connection=connection,
            table_name=table_name,
            schema_name=schema,
        )

        return _extract_pk_from_dte_result(result)

    def get_foreign_keys(
        self, connection, table_name, schema=None, **kw
    ) -> ReflectedForeignKeyConstraint:
        """Return information about foreign_keys in `table_name`."""

        result = self._describe_table_extended(
            connection=connection,
            table_name=table_name,
            schema_name=schema,
        )

        return _extract_fk_from_dte_result(table_name, schema, result)

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
        """For internal dialect use, check the existence of a particular table
        or view in the database.
        """

        try:
            self._describe_table_extended(
                connection=connection,
                table_name=table_name,
                catalog_name=catalog,
                schema_name=schema,
            )
            return True
        except sqlalchemy.exc.NoSuchTableError as e:
            return False

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
        """Return a list of all schema names available in the database."""
        stmt = DDL("SHOW SCHEMAS")
        result = connection.execute(stmt)
        schema_list = [row[0] for row in result]
        return schema_list


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
