from typing import Any, List, Optional, Dict, Union

import databricks.sqlalchemy._ddl as dialect_ddl_impl
import databricks.sqlalchemy._types as dialect_type_impl
from databricks import sql
from databricks.sqlalchemy._parse import (
    _describe_table_extended_result_to_dict_list,
    _match_table_not_found_string,
    build_fk_dict,
    build_pk_dict,
    get_fk_strings_from_dte_output,
    get_pk_strings_from_dte_output,
    get_comment_from_dte_output,
    parse_column_info_from_tgetcolumnsresponse,
)

import sqlalchemy
from sqlalchemy import DDL, event
from sqlalchemy.engine import Connection, Engine, default, reflection
from sqlalchemy.engine.interfaces import (
    ReflectedForeignKeyConstraint,
    ReflectedPrimaryKeyConstraint,
    ReflectedColumn,
    ReflectedTableComment,
)
from sqlalchemy.engine.reflection import ReflectionDefaults
from sqlalchemy.exc import DatabaseError, SQLAlchemyError

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


class DatabricksDialect(default.DefaultDialect):
    """This dialect implements only those methods required to pass our e2e tests"""

    # See sqlalchemy.engine.interfaces for descriptions of each of these properties
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
    default_paramstyle: str = "named"
    div_is_floordiv: bool = False
    supports_default_values: bool = False
    supports_server_side_cursors: bool = False
    supports_sequences: bool = False
    supports_native_boolean: bool = True

    colspecs = {
        sqlalchemy.types.DateTime: dialect_type_impl.TIMESTAMP_NTZ,
        sqlalchemy.types.Time: dialect_type_impl.DatabricksTimeType,
        sqlalchemy.types.String: dialect_type_impl.DatabricksStringType,
    }

    # SQLAlchemy requires that a table with no primary key
    # constraint return a dictionary that looks like this.
    EMPTY_PK: Dict[str, Any] = {"constrained_columns": [], "name": None}

    # SQLAlchemy requires that a table with no foreign keys
    # defined return an empty list. Same for indexes.
    EMPTY_FK: List
    EMPTY_INDEX: List
    EMPTY_FK = EMPTY_INDEX = []

    @classmethod
    def import_dbapi(cls):
        return sql

    def _force_paramstyle_to_native_mode(self):
        """This method can be removed after databricks-sql-connector wholly switches to NATIVE ParamApproach.

        This is a hack to trick SQLAlchemy into using a different paramstyle
        than the one declared by this module in src/databricks/sql/__init__.py

        This method is called _after_ the dialect has been initialised, which is important because otherwise
        our users would need to include a `paramstyle` argument in their SQLAlchemy connection string.

        This dialect is written to support NATIVE queries. Although the INLINE approach can technically work,
        the same behaviour can be achieved within SQLAlchemy itself using its literal_processor methods.
        """

        self.paramstyle = self.default_paramstyle

    def create_connect_args(self, url):
        # TODO: can schema be provided after HOST?
        # Expected URI format is: databricks+thrift://token:dapi***@***.cloud.databricks.com?http_path=/sql/***

        kwargs = {
            "server_hostname": url.host,
            "access_token": url.password,
            "http_path": url.query.get("http_path"),
            "catalog": url.query.get("catalog"),
            "schema": url.query.get("schema"),
            "use_inline_params": False,
        }

        self.schema = kwargs["schema"]
        self.catalog = kwargs["catalog"]

        self._force_paramstyle_to_native_mode()

        return [], kwargs

    def get_columns(
        self, connection, table_name, schema=None, **kwargs
    ) -> List[ReflectedColumn]:
        """Return information about columns in `table_name`."""

        with self.get_connection_cursor(connection) as cur:
            resp = cur.columns(
                catalog_name=self.catalog,
                schema_name=schema or self.schema,
                table_name=table_name,
            ).fetchall()

        if not resp:
            # TGetColumnsRequest will not raise an exception if passed a table that doesn't exist
            # But Databricks supports tables with no columns. So if the result is an empty list,
            # we need to check if the table exists (and raise an exception if not) or simply return
            # an empty list.
            self._describe_table_extended(
                connection,
                table_name,
                self.catalog,
                schema or self.schema,
                expect_result=False,
            )
            return resp
        columns = []
        for col in resp:
            row_dict = parse_column_info_from_tgetcolumnsresponse(col)
            columns.append(row_dict)

        return columns

    def _describe_table_extended(
        self,
        connection: Connection,
        table_name: str,
        catalog_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        expect_result=True,
    ) -> Union[List[Dict[str, str]], None]:
        """Run DESCRIBE TABLE EXTENDED on a table and return a list of dictionaries of the result.

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
            result = connection.execute(stmt)
        except DatabaseError as e:
            if _match_table_not_found_string(str(e)):
                raise sqlalchemy.exc.NoSuchTableError(
                    f"No such table {table_name}"
                ) from e
            raise e

        if not expect_result:
            return None

        fmt_result = _describe_table_extended_result_to_dict_list(result)
        return fmt_result

    @reflection.cache
    def get_pk_constraint(
        self,
        connection,
        table_name: str,
        schema: Optional[str] = None,
        **kw: Any,
    ) -> ReflectedPrimaryKeyConstraint:
        """Fetch information about the primary key constraint on table_name.

        Returns a dictionary with these keys:
            constrained_columns
              a list of column names that make up the primary key. Results is an empty list
              if no PRIMARY KEY is defined.

            name
              the name of the primary key constraint
        """

        result = self._describe_table_extended(
            connection=connection,
            table_name=table_name,
            schema_name=schema,
        )

        # Type ignore is because mypy knows that self._describe_table_extended *can*
        # return None (even though it never will since expect_result defaults to True)
        raw_pk_constraints: List = get_pk_strings_from_dte_output(result)  # type: ignore
        if not any(raw_pk_constraints):
            return self.EMPTY_PK  # type: ignore

        if len(raw_pk_constraints) > 1:
            logger.warning(
                "Found more than one primary key constraint in DESCRIBE TABLE EXTENDED output. "
                "This is unexpected. Please report this as a bug. "
                "Only the first primary key constraint will be returned."
            )

        first_pk_constraint = raw_pk_constraints[0]
        pk_name = first_pk_constraint.get("col_name")
        pk_constraint_string = first_pk_constraint.get("data_type")

        # TODO: figure out how to return sqlalchemy.interfaces in a way that mypy respects
        return build_pk_dict(pk_name, pk_constraint_string)  # type: ignore

    def get_foreign_keys(
        self, connection, table_name, schema=None, **kw
    ) -> List[ReflectedForeignKeyConstraint]:
        """Return information about foreign_keys in `table_name`."""

        result = self._describe_table_extended(
            connection=connection,
            table_name=table_name,
            schema_name=schema,
        )

        # Type ignore is because mypy knows that self._describe_table_extended *can*
        # return None (even though it never will since expect_result defaults to True)
        raw_fk_constraints: List = get_fk_strings_from_dte_output(result)  # type: ignore

        if not any(raw_fk_constraints):
            return self.EMPTY_FK

        fk_constraints = []
        for constraint_dict in raw_fk_constraints:
            fk_name = constraint_dict.get("col_name")
            fk_constraint_string = constraint_dict.get("data_type")
            this_constraint_dict = build_fk_dict(
                fk_name, fk_constraint_string, schema_name=schema
            )
            fk_constraints.append(this_constraint_dict)

        # TODO: figure out how to return sqlalchemy.interfaces in a way that mypy respects
        return fk_constraints  # type: ignore

    def get_indexes(self, connection, table_name, schema=None, **kw):
        """SQLAlchemy requires this method. Databricks doesn't support indexes."""
        return self.EMPTY_INDEX

    @reflection.cache
    def get_table_names(self, connection: Connection, schema=None, **kwargs):
        """Return a list of tables in the current schema."""

        _target_catalog = self.catalog
        _target_schema = schema or self.schema
        _target = f"`{_target_catalog}`.`{_target_schema}`"

        stmt = DDL(f"SHOW TABLES FROM {_target}")

        tables_result = connection.execute(stmt).all()
        views_result = self.get_view_names(connection=connection, schema=schema)

        # In Databricks, SHOW TABLES FROM <schema> returns both tables and views.
        # Potential optimisation: rewrite this to instead query information_schema
        tables_minus_views = [
            row.tableName for row in tables_result if row.tableName not in views_result
        ]

        return tables_minus_views

    @reflection.cache
    def get_view_names(
        self,
        connection,
        schema=None,
        only_materialized=False,
        only_temp=False,
        **kwargs,
    ) -> List[str]:
        """Returns a list of string view names contained in the schema, if any."""

        _target_catalog = self.catalog
        _target_schema = schema or self.schema
        _target = f"`{_target_catalog}`.`{_target_schema}`"

        stmt = DDL(f"SHOW VIEWS FROM {_target}")
        result = connection.execute(stmt).all()

        return [
            row.viewName
            for row in result
            if (not only_materialized or row.isMaterialized)
            and (not only_temp or row.isTemporary)
        ]

    @reflection.cache
    def get_materialized_view_names(
        self, connection: Connection, schema: Optional[str] = None, **kw: Any
    ) -> List[str]:
        """A wrapper around get_view_names that fetches only the names of materialized views"""
        return self.get_view_names(connection, schema, only_materialized=True)

    @reflection.cache
    def get_temp_view_names(
        self, connection: Connection, schema: Optional[str] = None, **kw: Any
    ) -> List[str]:
        """A wrapper around get_view_names that fetches only the names of temporary views"""
        return self.get_view_names(connection, schema, only_temp=True)

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

    @reflection.cache
    def get_table_comment(
        self,
        connection: Connection,
        table_name: str,
        schema: Optional[str] = None,
        **kw: Any,
    ) -> ReflectedTableComment:
        result = self._describe_table_extended(
            connection=connection,
            table_name=table_name,
            schema_name=schema,
        )

        if result is None:
            return ReflectionDefaults.table_comment()

        comment = get_comment_from_dte_output(result)

        if comment:
            return dict(text=comment)
        else:
            return ReflectionDefaults.table_comment()


@event.listens_for(Engine, "do_connect")
def receive_do_connect(dialect, conn_rec, cargs, cparams):
    """Helpful for DS on traffic from clients using SQLAlchemy in particular"""

    # Ignore connect invocations that don't use our dialect
    if not dialect.name == "databricks":
        return

    ua = cparams.get("_user_agent_entry", "")

    def add_sqla_tag_if_not_present(val: str):
        if not val:
            output = "sqlalchemy"

        if val and "sqlalchemy" in val:
            output = val

        else:
            output = f"sqlalchemy + {val}"

        return output

    cparams["_user_agent_entry"] = add_sqla_tag_if_not_present(ua)

    if sqlalchemy.__version__.startswith("1.3"):
        # SQLAlchemy 1.3.x fails to parse the http_path, catalog, and schema from our connection string
        # These should be passed in as connect_args when building the Engine

        if "schema" in cparams:
            dialect.schema = cparams["schema"]

        if "catalog" in cparams:
            dialect.catalog = cparams["catalog"]
