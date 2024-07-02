"""
The complete list of requirements is provided by SQLAlchemy here:

https://github.com/sqlalchemy/sqlalchemy/blob/main/lib/sqlalchemy/testing/requirements.py

When SQLAlchemy skips a test because a requirement is closed() it gives a generic skip message.
To make these failures more actionable, we only define requirements in this file that we wish to
force to be open(). If a test should be skipped on Databricks, it will be specifically marked skip
in test_suite.py with a Databricks-specific reason.

See the special note about the array_type exclusion below.
See special note about has_temp_table exclusion below.
"""

import sqlalchemy.testing.requirements
import sqlalchemy.testing.exclusions


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
    def infinity_floats(self):
        """The Float type can persist and load float('inf'), float('-inf')."""

        return sqlalchemy.testing.exclusions.open()

    @property
    def precision_numerics_retains_significant_digits(self):
        """A precision numeric type will return empty significant digits,
        i.e. a value such as 10.000 will come back in Decimal form with
        the .000 maintained."""

        return sqlalchemy.testing.exclusions.open()

    @property
    def precision_numerics_many_significant_digits(self):
        """target backend supports values with many digits on both sides,
        such as 319438950232418390.273596, 87673.594069654243

        """
        return sqlalchemy.testing.exclusions.open()

    @property
    def array_type(self):
        """While Databricks does support ARRAY types, pysql cannot bind them. So
        we cannot use them with SQLAlchemy

        Due to a bug in SQLAlchemy, we _must_ define this exclusion as closed() here or else the
        test runner will crash the pytest process due to an AttributeError
        """

        # TODO: Implement array type using inline?
        return sqlalchemy.testing.exclusions.closed()

    @property
    def table_ddl_if_exists(self):
        """target platform supports IF NOT EXISTS / IF EXISTS for tables."""

        return sqlalchemy.testing.exclusions.open()

    @property
    def identity_columns(self):
        """If a backend supports GENERATED { ALWAYS | BY DEFAULT }
        AS IDENTITY"""
        return sqlalchemy.testing.exclusions.open()

    @property
    def identity_columns_standard(self):
        """If a backend supports GENERATED { ALWAYS | BY DEFAULT }
        AS IDENTITY with a standard syntax.
        This is mainly to exclude MSSql.
        """
        return sqlalchemy.testing.exclusions.open()

    @property
    def has_temp_table(self):
        """target dialect supports checking a single temp table name

        unfortunately this is not the same as temp_table_names

        SQLAlchemy's HasTableTest is not normalised in such a way that temp table tests
        are separate from temp view and normal table tests. If those tests were split out,
        we would just add detailed skip markers in test_suite.py. But since we'd like to
        run the HasTableTest group for the features we support, we must set this exclusinon
        to closed().

        It would be ideal if there were a separate requirement for has_temp_view. Without it,
        we're in a bind.
        """
        return sqlalchemy.testing.exclusions.closed()

    @property
    def temporary_views(self):
        """target database supports temporary views"""
        return sqlalchemy.testing.exclusions.open()

    @property
    def views(self):
        """Target database must support VIEWs."""

        return sqlalchemy.testing.exclusions.open()

    @property
    def temporary_tables(self):
        """target database supports temporary tables

        ComponentReflection test is intricate and simply cannot function without this exclusion being defined here.
        This happens because we cannot skip individual combinations used in ComponentReflection test.
        """
        return sqlalchemy.testing.exclusions.closed()

    @property
    def table_reflection(self):
        """target database has general support for table reflection"""
        return sqlalchemy.testing.exclusions.open()

    @property
    def comment_reflection(self):
        """Indicates if the database support table comment reflection"""
        return sqlalchemy.testing.exclusions.open()

    @property
    def comment_reflection_full_unicode(self):
        """Indicates if the database support table comment reflection in the
        full unicode range, including emoji etc.
        """
        return sqlalchemy.testing.exclusions.open()

    @property
    def temp_table_reflection(self):
        """ComponentReflection test is intricate and simply cannot function without this exclusion being defined here.
        This happens because we cannot skip individual combinations used in ComponentReflection test.
        """
        return sqlalchemy.testing.exclusions.closed()

    @property
    def index_reflection(self):
        """ComponentReflection test is intricate and simply cannot function without this exclusion being defined here.
        This happens because we cannot skip individual combinations used in ComponentReflection test.
        """
        return sqlalchemy.testing.exclusions.closed()

    @property
    def unique_constraint_reflection(self):
        """ComponentReflection test is intricate and simply cannot function without this exclusion being defined here.
        This happens because we cannot skip individual combinations used in ComponentReflection test.

        Databricks doesn't support UNIQUE constraints.
        """
        return sqlalchemy.testing.exclusions.closed()

    @property
    def reflects_pk_names(self):
        """Target driver reflects the name of primary key constraints."""

        return sqlalchemy.testing.exclusions.open()

    @property
    def datetime_implicit_bound(self):
        """target dialect when given a datetime object will bind it such
        that the database server knows the object is a date, and not
        a plain string.
        """

        return sqlalchemy.testing.exclusions.open()

    @property
    def tuple_in(self):
        return sqlalchemy.testing.exclusions.open()

    @property
    def ctes(self):
        return sqlalchemy.testing.exclusions.open()

    @property
    def ctes_with_update_delete(self):
        return sqlalchemy.testing.exclusions.open()

    @property
    def delete_from(self):
        """Target must support DELETE FROM..FROM or DELETE..USING syntax"""
        return sqlalchemy.testing.exclusions.open()

    @property
    def table_value_constructor(self):
        return sqlalchemy.testing.exclusions.open()

    @property
    def reflect_tables_no_columns(self):
        return sqlalchemy.testing.exclusions.open()

    @property
    def denormalized_names(self):
        """Target database must have 'denormalized', i.e.
        UPPERCASE as case insensitive names."""

        return sqlalchemy.testing.exclusions.open()

    @property
    def time_timezone(self):
        """target dialect supports representation of Python
        datetime.time() with tzinfo with Time(timezone=True)."""

        return sqlalchemy.testing.exclusions.open()
