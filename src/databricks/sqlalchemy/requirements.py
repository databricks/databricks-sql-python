"""
This module is supposedly used by the compliance tests to control which tests are run based on database capabilities.
However, based on some experimentation that does not appear to be consistently the case. Until we better understand
when these requirements are and are not implemented, we prefer to manually capture the exact nature of the failures
and errors.

Once we better understand how to use requirements.py, an example exclusion will look like this:

    import sqlalchemy.testing.requirements
    import sqlalchemy.testing.exclusions

    class Requirements(sqlalchemy.testing.requirements.SuiteRequirements):
        @property
        def __some_example_requirement(self):
            return sqlalchemy.testing.exclusions.closed


The complete list of requirements is provided by SQLAlchemy here:

https://github.com/sqlalchemy/sqlalchemy/blob/main/lib/sqlalchemy/testing/requirements.py
"""

import sqlalchemy.testing.requirements
import sqlalchemy.testing.exclusions

import logging

logger = logging.getLogger(__name__)

logger.warning("requirements.py is not currently employed by Databricks dialect")


class Requirements(sqlalchemy.testing.requirements.SuiteRequirements):
    
    @property
    def date_historic(self):
        """target dialect supports representation of Python
        datetime.datetime() objects with historic (pre 1970) values."""

        return sqlalchemy.testing.exclusions.open()
    
    @property
    def datetime_historic(self):
        """target dialect supports representation of Python
        datetime.datetime() objects with historic (pre 1970) values."""

        return sqlalchemy.testing.exclusions.open()
    
    @property
    def datetime_literals(self):
        """target dialect supports rendering of a date, time, or datetime as a
        literal string, e.g. via the TypeEngine.literal_processor() method.

        """

        return sqlalchemy.testing.exclusions.open()
    
    @property
    def timestamp_microseconds(self):
        """target dialect supports representation of Python
        datetime.datetime() with microsecond objects but only
        if TIMESTAMP is used."""

        return sqlalchemy.testing.exclusions.open()
    
    @property
    def time_microseconds(self):
        """target dialect supports representation of Python
        datetime.time() with microsecond objects.
        
        This requirement declaration isn't needed but I've included it here for completeness.
        Since Databricks doesn't have a TIME type, SQLAlchemy will compile Time() columns
        as STRING Databricks data types. And we use a custom time type to render those strings
        between str() and time.time() representations. Therefore we can store _any_ precision
        that SQLAlchemy needs. The time_microseconds requirement defaults to ON for all dialects
        except mssql, mysql, mariadb, and oracle. 
        """

        return sqlalchemy.testing.exclusions.open()
    
    @property
    def precision_generic_float_type(self):
        """target backend will return native floating point numbers with at
        least seven decimal places when using the generic Float type.

        Databricks sometimes only returns six digits of precision for the generic Float type
        """
        return sqlalchemy.testing.exclusions.closed()
    
    @property
    def literal_float_coercion(self):
        """target backend will return the exact float value 15.7563
        with only four significant digits from this statement:

        SELECT :param

        where :param is the Python float 15.7563

        i.e. it does not return 15.75629997253418

        Without additional work, Databricks returns 15.75629997253418
        This is a potential area where we could override the Float literal processor.
        Will leave to a PM to decide if we should do so.
        """
        return sqlalchemy.testing.exclusions.closed()
    
    @property
    def precision_numerics_enotation_large(self):
        """target backend supports Decimal() objects using E notation
        to represent very large values.
        
        Databricks supports E notation for FLOAT data types but not for DECIMAL types,
        which is the underlying data type SQLAlchemy uses for Numeric() types.

        """
        return sqlalchemy.testing.exclusions.closed()
    
    @property
    def infinity_floats(self):
        """The Float type can persist and load float('inf'), float('-inf')."""

        return sqlalchemy.testing.exclusions.open()
    
    @property
    def precision_numerics_retains_significant_digits(self):
        """A precision numeric type will return empty significant digits,
        i.e. a value such as 10.000 will come back in Decimal form with
        the .000 maintained."""

        return sqlalchemy.testing.exclusions.open()


