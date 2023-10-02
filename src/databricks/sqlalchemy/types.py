import sqlalchemy
from sqlalchemy.ext.compiler import compiles


@compiles(sqlalchemy.types.Enum, "databricks")
@compiles(sqlalchemy.types.String, "databricks")
@compiles(sqlalchemy.types.Text, "databricks")
@compiles(sqlalchemy.types.Time, "databricks")
@compiles(sqlalchemy.types.Unicode, "databricks")
@compiles(sqlalchemy.types.UnicodeText, "databricks")
@compiles(sqlalchemy.types.Uuid, "databricks")
def compile_string_databricks(type_, compiler, **kw):
    """
    We override the default compilation for Enum(), String(), Text(), and Time() because SQLAlchemy
    defaults to incompatible / abnormal compiled names

      Enum -> VARCHAR
      String -> VARCHAR[LENGTH]
      Text -> VARCHAR[LENGTH]
      Time -> TIME
      Unicode -> VARCHAR[LENGTH]
      UnicodeText -> TEXT
      Uuid -> CHAR[32]

    But all of these types will be compiled to STRING in Databricks SQL
    """
    return "STRING"


@compiles(sqlalchemy.types.Integer, "databricks")
def compile_integer_databricks(type_, compiler, **kw):
    """
    We need to override the default Integer compilation rendering because Databricks uses "INT" instead of "INTEGER"
    """
    return "INT"


@compiles(sqlalchemy.types.LargeBinary, "databricks")
def compile_binary_databricks(type_, compiler, **kw):
    """
    We need to override the default LargeBinary compilation rendering because Databricks uses "BINARY" instead of "BLOB"
    """
    return "BINARY"


@compiles(sqlalchemy.types.Numeric, "databricks")
def compile_numeric_databricks(type_, compiler, **kw):
    """
    We need to override the default Numeric compilation rendering because Databricks uses "DECIMAL" instead of "NUMERIC"

    The built-in visit_DECIMAL behaviour captures the precision and scale. Here we're just mapping calls to compile Numeric
    to the SQLAlchemy Decimal() implementation
    """
    return compiler.visit_DECIMAL(type_, **kw)


@compiles(sqlalchemy.types.DateTime, "databricks")
def compile_datetime_databricks(type_, compiler, **kw):
    """
    We need to override the default DateTime compilation rendering because Databricks uses "TIMESTAMP" instead of "DATETIME"
    """
    return "TIMESTAMP"


@compiles(sqlalchemy.types.ARRAY, "databricks")
def compile_array_databricks(type_, compiler, **kw):
    """
    SQLAlchemy's default ARRAY can't compile as it's only implemented for Postgresql.
    The Postgres implementation works for Databricks SQL, so we duplicate that here.

    :type_:
        This is an instance of sqlalchemy.types.ARRAY which always includes an item_type attribute
        which is itself an instance of TypeEngine

    https://docs.sqlalchemy.org/en/20/core/type_basics.html#sqlalchemy.types.ARRAY
    """

    inner = compiler.process(type_.item_type, **kw)

    return f"ARRAY<{inner}>"
