from datetime import datetime, time, timezone
from itertools import product
from typing import Any, Union, Optional

import sqlalchemy
from sqlalchemy.engine.interfaces import Dialect
from sqlalchemy.ext.compiler import compiles

from databricks.sql.utils import ParamEscaper


def process_literal_param_hack(value: Any):
    """This method is supposed to accept a Python type and return a string representation of that type.
    But due to some weirdness in the way SQLAlchemy's literal rendering works, we have to return
    the value itself because, by the time it reaches our custom type code, it's already been converted
    into a string.

    TimeTest
    DateTimeTest
    DateTimeTZTest

    This dynamic only seems to affect the literal rendering of datetime and time objects.

    All fail without this hack in-place. I'm not sure why. But it works.
    """
    return value


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
    We need to override the default DateTime compilation rendering because Databricks uses "TIMESTAMP_NTZ" instead of "DATETIME"
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


class TIMESTAMP_NTZ(sqlalchemy.types.TypeDecorator):
    """Represents values comprising values of fields year, month, day, hour, minute, and second.
    All operations are performed without taking any time zone into account.

    Our dialect maps sqlalchemy.types.DateTime() to this type, which means that all DateTime()
    objects are stored without tzinfo. To read and write timezone-aware datetimes use
    databricks.sql.TIMESTAMP instead.

    https://docs.databricks.com/en/sql/language-manual/data-types/timestamp-ntz-type.html
    """

    impl = sqlalchemy.types.DateTime

    cache_ok = True

    def process_result_value(self, value: Union[None, datetime], dialect):
        if value is None:
            return None
        return value.replace(tzinfo=None)


class TIMESTAMP(sqlalchemy.types.TypeDecorator):
    """Represents values comprising values of fields year, month, day, hour, minute, and second,
    with the session local time-zone.

    Our dialect maps sqlalchemy.types.DateTime() to TIMESTAMP_NTZ, which means that all DateTime()
    objects are stored without tzinfo. To read and write timezone-aware datetimes use
    this type instead.

    ```python
    # This won't work
    `Column(sqlalchemy.DateTime(timezone=True))`

    # But this does
    `Column(TIMESTAMP)`
    ````

    https://docs.databricks.com/en/sql/language-manual/data-types/timestamp-type.html
    """

    impl = sqlalchemy.types.DateTime

    cache_ok = True

    def process_result_value(self, value: Union[None, datetime], dialect):
        if value is None:
            return None

        if not value.tzinfo:
            return value.replace(tzinfo=timezone.utc)
        return value

    def process_bind_param(
        self, value: Union[datetime, None], dialect
    ) -> Optional[datetime]:
        """pysql can pass datetime.datetime() objects directly to DBR"""
        return value

    def process_literal_param(
        self, value: Union[datetime, None], dialect: Dialect
    ) -> str:
        """ """
        return process_literal_param_hack(value)


@compiles(TIMESTAMP, "databricks")
def compile_timestamp_databricks(type_, compiler, **kw):
    """
    We need to override the default DateTime compilation rendering because Databricks uses "TIMESTAMP_NTZ" instead of "DATETIME"
    """
    return "TIMESTAMP"


class DatabricksTimeType(sqlalchemy.types.TypeDecorator):
    """Databricks has no native TIME type. So we store it as a string."""

    impl = sqlalchemy.types.Time
    cache_ok = True

    BASE_FMT = "%H:%M:%S"
    MICROSEC_PART = ".%f"
    TIMEZONE_PART = "%z"

    def _generate_fmt_string(self, ms: bool, tz: bool) -> str:
        """Return a format string for datetime.strptime() that includes or excludes microseconds and timezone."""
        _ = lambda x, y: x if y else ""
        return f"{self.BASE_FMT}{_(self.MICROSEC_PART,ms)}{_(self.TIMEZONE_PART,tz)}"

    @property
    def allowed_fmt_strings(self):
        """Time strings can be read with or without microseconds and with or without a timezone."""

        if not hasattr(self, "_allowed_fmt_strings"):
            ms_switch = tz_switch = [True, False]
            self._allowed_fmt_strings = [
                self._generate_fmt_string(x, y)
                for x, y in product(ms_switch, tz_switch)
            ]

        return self._allowed_fmt_strings

    def _parse_result_string(self, value: str) -> time:
        """Parse a string into a time object. Try all allowed formats until one works."""
        for fmt in self.allowed_fmt_strings:
            try:
                # We use timetz() here because we want to preserve the timezone information
                # Calling .time() will strip the timezone information
                return datetime.strptime(value, fmt).timetz()
            except ValueError:
                pass

        raise ValueError(f"Could not parse time string {value}")

    def _determine_fmt_string(self, value: time) -> str:
        """Determine which format string to use to render a time object as a string."""
        ms_bool = value.microsecond > 0
        tz_bool = value.tzinfo is not None
        return self._generate_fmt_string(ms_bool, tz_bool)

    def process_bind_param(self, value: Union[time, None], dialect) -> Union[None, str]:
        """Values sent to the database are converted to %:H:%M:%S strings."""
        if value is None:
            return None
        fmt_string = self._determine_fmt_string(value)
        return value.strftime(fmt_string)

    # mypy doesn't like this workaround because TypeEngine wants process_literal_param to return a string
    def process_literal_param(self, value, dialect) -> time:  # type: ignore
        """ """
        return process_literal_param_hack(value)

    def process_result_value(
        self, value: Union[None, str], dialect
    ) -> Union[time, None]:
        """Values received from the database are parsed into datetime.time() objects"""
        if value is None:
            return None

        return self._parse_result_string(value)


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


class TINYINT(sqlalchemy.types.TypeDecorator):
    """Represents 1-byte signed integers

    Acts like a sqlalchemy SmallInteger() in Python but writes to a TINYINT field in Databricks

    https://docs.databricks.com/en/sql/language-manual/data-types/tinyint-type.html
    """

    impl = sqlalchemy.types.SmallInteger
    cache_ok = True


@compiles(TINYINT, "databricks")
def compile_tinyint(type_, compiler, **kw):
    return "TINYINT"
