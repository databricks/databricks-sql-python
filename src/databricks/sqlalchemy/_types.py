import sqlalchemy
from sqlalchemy.ext.compiler import compiles

from typing import Union

from datetime import datetime, time


from databricks.sql.utils import ParamEscaper


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
    """Databricks has no native TIME type. So we store it as a string."""

    impl = sqlalchemy.types.Time
    cache_ok = True

    TIME_WITH_MICROSECONDS_FMT = "%H:%M:%S.%f"
    TIME_NO_MICROSECONDS_FMT = "%H:%M:%S"

    def process_bind_param(self, value: Union[time, None], dialect) -> Union[None, str]:
        """Values sent to the database are converted to %:H:%M:%S strings."""
        if value is None:
            return None
        return value.strftime(self.TIME_WITH_MICROSECONDS_FMT)

    # mypy doesn't like this workaround because TypeEngine wants process_literal_param to return a string
    def process_literal_param(self, value, dialect) -> time:  # type: ignore
        """It's not clear to me why this is necessary. Without it, SQLAlchemy's Timetest:test_literal fails
        because the string literal renderer receives a str() object and calls .isoformat() on it.

        Whereas this method receives a datetime.time() object which is subsequently passed to that
        same renderer. And that works.

        UPDATE: After coping with the literal_processor override in DatabricksStringType, I suspect a similar
        mechanism is at play. Two different processors are are called in sequence. This is likely a byproduct
        of Databricks not having a true TIME type. I think the string representation of Time() types is
        somehow affecting the literal rendering process. But as long as this passes the tests, I'm not
        worried about it.
        """
        return value

    def process_result_value(
        self, value: Union[None, str], dialect
    ) -> Union[time, None]:
        """Values received from the database are parsed into datetime.time() objects"""
        if value is None:
            return None

        try:
            _parsed = datetime.strptime(value, self.TIME_WITH_MICROSECONDS_FMT)
        except ValueError:
            # If the string doesn't have microseconds, try parsing it without them
            _parsed = datetime.strptime(value, self.TIME_NO_MICROSECONDS_FMT)

        return _parsed.time()


class DatabricksStringType(sqlalchemy.types.TypeDecorator):
    """We have to implement our own String() type because SQLAlchemy's default implementation
    wants to escape single-quotes with a doubled single-quote. Databricks uses a backslash for
    escaping of literal strings. And SQLAlchemy's default escaping breaks Databricks SQL.
    """

    impl = sqlalchemy.types.String
    cache_ok = True
    pe = ParamEscaper()

    def process_literal_param(self, value, dialect) -> str:
        """SQLAlchemy's default string escaping for backslashes doesn't work for databricks. The logic here
        implements the same logic as our legacy inline escaping logic.
        """

        return self.pe.escape_string(value)

    def literal_processor(self, dialect):
        """We manually override this method to prevent further processing of the string literal beyond
        what happens in the process_literal_param() method.

        The SQLAlchemy docs _specifically_ say to not override this method.

        It appears that any processing that happens from TypeEngine.process_literal_param happens _before_
        and _in addition to_ whatever the class's impl.literal_processor() method does. The String.literal_processor()
        method performs a string replacement that doubles any single-quote in the contained string. This raises a syntax
        error in Databricks. And it's not necessary because ParamEscaper() already implements all the escaping we need.

        We should consider opening an issue on the SQLAlchemy project to see if I'm using it wrong.

        See type_api.py::TypeEngine.literal_processor:

        ```python
            def process(value: Any) -> str:
                return fixed_impl_processor(
                    fixed_process_literal_param(value, dialect)
                )
        ```

        That call to fixed_impl_processor wraps the result of fixed_process_literal_param (which is the
        process_literal_param defined in our Databricks dialect)

        https://docs.sqlalchemy.org/en/20/core/custom_types.html#sqlalchemy.types.TypeDecorator.literal_processor
        """

        def process(value):
            """This is a copy of the default String.literal_processor() method but stripping away
            its double-escaping behaviour for single-quotes.
            """

            _step1 = self.process_literal_param(value, dialect="databricks")
            if dialect.identifier_preparer._double_percents:
                _step2 = _step1.replace("%", "%%")
            else:
                _step2 = _step1

            return "%s" % _step2

        return process
