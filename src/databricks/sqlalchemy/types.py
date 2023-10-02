from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.compiler import GenericTypeCompiler
from sqlalchemy.types import (
    DateTime,
    Enum,
    Integer,
    LargeBinary,
    Numeric,
    String,
    Text,
    Time,
    Unicode,
    UnicodeText,
    Uuid,
)


@compiles(Enum, "databricks")
@compiles(String, "databricks")
@compiles(Text, "databricks")
@compiles(Time, "databricks")
@compiles(Unicode, "databricks")
@compiles(UnicodeText, "databricks")
@compiles(Uuid, "databricks")
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


@compiles(Integer, "databricks")
def compile_integer_databricks(type_, compiler, **kw):
    """
    We need to override the default Integer compilation rendering because Databricks uses "INT" instead of "INTEGER"
    """
    return "INT"


@compiles(LargeBinary, "databricks")
def compile_binary_databricks(type_, compiler, **kw):
    """
    We need to override the default LargeBinary compilation rendering because Databricks uses "BINARY" instead of "BLOB"
    """
    return "BINARY"


@compiles(Numeric, "databricks")
def compile_numeric_databricks(type_, compiler, **kw):
    """
    We need to override the default Numeric compilation rendering because Databricks uses "DECIMAL" instead of "NUMERIC"

    The built-in visit_DECIMAL behaviour captures the precision and scale. Here we're just mapping calls to compile Numeric
    to the SQLAlchemy Decimal() implementation
    """
    return compiler.visit_DECIMAL(type_, **kw)


@compiles(DateTime, "databricks")
def compile_datetime_databricks(type_, compiler, **kw):
    """
    We need to override the default DateTime compilation rendering because Databricks uses "TIMESTAMP" instead of "DATETIME"
    """
    return "TIMESTAMP"
