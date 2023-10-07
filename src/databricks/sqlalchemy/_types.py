import sqlalchemy
from sqlalchemy.ext.compiler import compiles

from typing import Union

from datetime import datetime

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
    return "TIMESTAMP_NTZ"


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


class DatabricksDateTimeNoTimezoneType(sqlalchemy.types.TypeDecorator):
    """The decimal that pysql creates when it receives the contents of a TIMESTAMP_NTZ
     includes a timezone of 'Etc/UTC'.  But since SQLAlchemy's test suite assumes that
     the sqlalchemy.types.DateTime type will return a datetime.datetime _without_ any
     timezone set, we need to strip the timezone off the value received from pysql.

     It's not clear if DBR sends a timezone to pysql or if pysql is adding it. This could be a bug.
    """

    impl = sqlalchemy.types.DateTime

    cache_ok = True

    def process_result_value(self, value: Union[None, datetime], dialect):
        if value is None:
            return None
        return value.replace(tzinfo=None)
    
class DatabricksTimeType(sqlalchemy.types.TypeDecorator):
    """Databricks has no native TIME type. So we store it as a string.
    """

    impl = sqlalchemy.types.Time
    cache_ok = True


    def process_bind_param(self, value: Union[datetime.time, None], dialect) -> str:
        """Values sent to the database are converted to %:H:%M:%S strings.
        """
        if value is None:
            return None
        return value.strftime("%H:%M:%S")
    
    def process_literal_param(self, value, dialect) -> datetime.time:
        """It's not clear to me why this is necessary. Without it, SQLAlchemy's Timetest:test_literal fails
        because the string literal renderer receives a str() object and calls .isoformat() on it.

        Whereas this method receives a datetime.time() object which is subsequently passed to that
        same renderer. And that works.
        """
        return value

    
    def process_result_value(self, value: Union[None, str], dialect) -> Union[datetime.time, None]:
        """Values received from the database are parsed into datetime.time() objects
        """
        if value is None:
            return None
        return datetime.strptime(value, "%H:%M:%S").time()
