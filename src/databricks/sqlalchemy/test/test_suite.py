# type: ignore
from sqlalchemy.testing.suite import *
import pytest

# Test definitions are found here:
# https://github.com/sqlalchemy/sqlalchemy/tree/main/lib/sqlalchemy/testing/suite


# Per the instructions for dialect authors, tests should be skippable based on
# dialect limitations defined in requirements.py. However, we found that these
# definitions are not universally honoured by the SQLAlchemy test runner so we
# opt to manually delete them from the test suite for the time-being. This makes
# it obvious what areas of dialect compliance we have not evaluated. The next
# step towards dialect compliance is to review each of these and document exactly
# which methods should or should not work. This can be done by removing the corr-
# esponding skip marker and then running the test.

# If we find tests that are skippable for a documented reason, we can call these
# out directly in the way suggested by SQLAlchemy's document for dialect authors:
#
#   > In the case that the decorators are not covering a particular test, a test
#   > can also be directly modified or bypassed.
#
# See further: https://github.com/sqlalchemy/sqlalchemy/blob/rel_1_4_48/README.dialects.rst


@pytest.mark.skip(reason="pysql doesn't support binding of BINARY type parameters")
class BinaryTest(BinaryTest):
    pass


@pytest.mark.reviewed
class BooleanTest(BooleanTest):
    pass


@pytest.mark.reviewed
class NumericTest(NumericTest):
    @pytest.mark.skip(reason="Databricks doesn't support E notation for DECIMAL types")
    def test_enotation_decimal(self):
        """This test automatically runs if requirements.precision_numerics_enotation_large is open()"""
        pass

    @pytest.mark.skip(reason="Databricks doesn't support E notation for DECIMAL types")
    def test_enotation_decimal_large(self):
        """This test automatically runs if requirements.precision_numerics_enotation_large is open()"""
        pass

    @pytest.mark.skip(
        reason="Without a specific CAST, Databricks doesn't return floats with same precision that was selected."
    )
    def test_float_coerce_round_trip(self):
        """
        This automatically runs if requirements.literal_float_coercion is open()

        Without additional work, Databricks returns 15.75629997253418 when you SELECT 15.7563.
        This is a potential area where we could override the Float literal processor to add a CAST.
        Will leave to a PM to decide if we should do so.
        """
        pass

    @pytest.mark.skip(
        reason="Databricks sometimes only returns six digits of precision for the generic Float type"
    )
    def test_float_custom_scale(self):
        """This test automatically runs if requirements.precision_generic_float_type is open()"""
        pass


@pytest.mark.reviewed
class TimeMicrosecondsTest(TimeMicrosecondsTest):
    pass


@pytest.mark.reviewed
class TextTest(TextTest):
    pass


@pytest.mark.reviewed
class StringTest(StringTest):
    pass


@pytest.mark.reviewed
class DateTimeMicrosecondsTest(DateTimeMicrosecondsTest):
    pass


@pytest.mark.reviewed
class TimestampMicrosecondsTest(TimestampMicrosecondsTest):
    pass


@pytest.mark.reviewed
class DateTimeCoercedToDateTimeTest(DateTimeCoercedToDateTimeTest):
    pass


@pytest.mark.reviewed
class TimeTest(TimeTest):
    pass


@pytest.mark.reviewed
class DateTimeTest(DateTimeTest):
    pass


@pytest.mark.reviewed
class DateTimeHistoricTest(DateTimeHistoricTest):
    pass


@pytest.mark.reviewed
class DateTest(DateTest):
    pass


@pytest.mark.reviewed
class DateHistoricTest(DateHistoricTest):
    pass


@pytest.mark.reviewed
class RowFetchTest(RowFetchTest):
    pass


@pytest.mark.reviewed
class FetchLimitOffsetTest(FetchLimitOffsetTest):
    @pytest.mark.flaky
    @pytest.mark.skip(
        reason="Insertion order on Databricks is not deterministic. See comment in test_suite.py."
    )
    def test_limit_render_multiple_times(self):
        """This test depends on the order that records are inserted into the table. It's passing criteria requires that
        a record inserted with id=1 is the first record returned when no ORDER BY clause is specified. But Databricks occasionally
        INSERTS in a different order, which makes this test seem to fail. The test is flaky, but the underlying functionality
        (can multiple LIMIT clauses be rendered) is not broken.

        Unclear if this is a bug in Databricks, Delta, or some race-condition in the test itself.
        """
        pass

    @pytest.mark.skip(reason="Databricks doesn't support FETCH clauses")
    def test_bound_fetch_offset(self):
        pass

    @pytest.mark.skip(reason="Databricks doesn't support FETCH clauses")
    def test_fetch_offset_no_order(self):
        pass

    @pytest.mark.skip(reason="Databricks doesn't support FETCH clauses")
    def test_fetch_offset_nobinds(self):
        pass

    @pytest.mark.skip(reason="Databricks doesn't support FETCH clauses")
    def test_simple_fetch(self):
        pass

    @pytest.mark.skip(reason="Databricks doesn't support FETCH clauses")
    def test_simple_fetch_offset(self):
        pass

    @pytest.mark.skip(reason="Databricks doesn't support FETCH clauses")
    def test_simple_fetch_percent(self):
        pass

    @pytest.mark.skip(reason="Databricks doesn't support FETCH clauses")
    def test_simple_fetch_percent_ties(self):
        pass

    @pytest.mark.skip(reason="Databricks doesn't support FETCH clauses")
    def test_simple_fetch_ties(self):
        pass

    @pytest.mark.skip(reason="Databricks doesn't support FETCH clauses")
    def test_expr_fetch_offset(self):
        pass

    @pytest.mark.skip(reason="Databricks doesn't support FETCH clauses")
    def test_fetch_offset_percent(self):
        pass

    @pytest.mark.skip(reason="Databricks doesn't support FETCH clauses")
    def test_fetch_offset_percent_ties(self):
        pass

    @pytest.mark.skip(reason="Databricks doesn't support FETCH clauses")
    def test_fetch_offset_ties(self):
        pass

    @pytest.mark.skip(reason="Databricks doesn't support FETCH clauses")
    def test_fetch_offset_ties_exact_number(self):
        pass


@pytest.mark.reviewed
class FutureTableDDLTest(FutureTableDDLTest):
    @pytest.mark.skip(
        reason="Comment reflection is possible but not implemented in this dialect."
    )
    def test_add_table_comment(self):
        """We could use requirements.comment_reflection here to disable this but prefer a more meaningful skip message"""
        pass

    @pytest.mark.skip(
        reason="Comment reflection is possible but not implemented in this dialect."
    )
    def test_drop_table_comment(self):
        """We could use requirements.comment_reflection here to disable this but prefer a more meaningful skip message"""
        pass

    @pytest.mark.skip(reason="Databricks does not support indexes.")
    def test_create_index_if_not_exists(self):
        """We could use requirements.index_reflection and requirements.index_ddl_if_exists
        here to disable this but prefer a more meaningful skip message
        """
        pass

    @pytest.mark.skip(reason="Databricks does not support indexes.")
    def test_drop_index_if_exists(self):
        """We could use requirements.index_reflection and requirements.index_ddl_if_exists
        here to disable this but prefer a more meaningful skip message
        """
        pass


@pytest.mark.reviewed
@pytest.mark.skip(
    reason="Identity works. Test needs rewrite for Databricks. See comments in test_suite.py"
)
class IdentityColumnTest(IdentityColumnTest):
    """The setup for these tests tries to create a table with a DELTA IDENTITY column but has two problems:
    1. It uses an Integer() type for the column. Whereas DELTA IDENTITY columns must be BIGINT.
    2. It tries to set the start == 42, which Databricks doesn't support

    I can get the tests to _run_ by patching the table fixture to use BigInteger(). But it asserts that the
    identity of two rows are 42 and 43, which is not possible since they will be rows 1 and 2 instead.

    I'm satisified through manual testing that our implementation of visit_identity_column works but a better test is needed.
    """

    pass


@pytest.mark.reviewed
class IdentityAutoincrementTest(IdentityAutoincrementTest):
    @pytest.mark.skip(
        reason="Identity works. Test needs rewrite for Databricks. See comments in test_suite.py"
    )
    def test_autoincrement_with_identity(self):
        """This test has the same issue as IdentityColumnTest.test_select_all in that it creates a table with identity
        using an Integer() rather than a BigInteger(). If I override this behaviour to use a BigInteger() instead, the
        test passes.
        """


@pytest.mark.reviewed
class LongNameBlowoutTest(LongNameBlowoutTest):
    """These tests all include assertions that the tested name > 255 characters"""

    @pytest.mark.skip(
        reason="Databricks constraint names are limited to 255 characters"
    )
    def test_long_convention_name(self):
        pass


@pytest.mark.reviewed
class ExceptionTest(ExceptionTest):
    @pytest.mark.skip(reason="Databricks doesn't enforce primary key constraints.")
    def test_integrity_error(self):
        """Per Databricks documentation, primary and foreign key constraints are informational only
        and are not enforced.

        https://docs.databricks.com/api/workspace/tableconstraints
        """
        pass


@pytest.mark.reviewed
class HasTableTest(HasTableTest):
    """Databricks does not support temporary tables."""

    @pytest.mark.skip(reason="Databricks does not support temporary tables.")
    def test_has_table_temp_table(self):
        pass

    @pytest.mark.skip(reason="Strange test design. See comments in test_suite.py")
    def test_has_table_temp_view(self):
        """Databricks supports temporary views but this test depends on requirements.has_temp_table, which we
        explicitly close so that we can run other tests in this group. See the comment under has_temp_table in
        requirements.py for details.

        From what I can see, there is no way to run this test since it will fail during setup if we mark has_temp_table
        open(). It _might_ be possible to hijack this behaviour by implementing temp_table_keyword_args in our own
        provision.py. Doing so would mean creating a real table during this class setup instead of a temp table. Then
        we could just skip the temp table tests but run the temp view tests. But this test fixture doesn't cleanup its
        temp tables and has no hook to do so.

        It would be ideal for SQLAlchemy to define a separate requirements.has_temp_views.
        """
        pass


@pytest.mark.reviewed
@pytest.mark.skip(
    reason="This dialect does not support implicit autoincrement. See comments in test_suite.py"
)
class LastrowidTest(LastrowidTest):
    """SQLAlchemy docs describe that a column without an explicit Identity() may implicitly create one if autoincrement=True.
    That is what this method tests. Databricks supports auto-incrementing IDENTITY columns but they must be explicitly
    declared. This limitation is present in our dialect as well. Which means that SQLAlchemy's autoincrement setting of a column
    is ignored. We emit a logging.WARN message if you try it.

    In the future we could handle this autoincrement by implicitly calling the visit_identity_column() method of our DDLCompiler
    when autoincrement=True. There is an example of this in the Microsoft SQL Server dialect: MSSDDLCompiler.get_column_specification

    For now, if you need to create a SQLAlchemy column with an auto-incrementing identity, you must set this explicitly in your column
    definition by passing an Identity() to the column constructor.
    """

    pass


@pytest.mark.reviewed
class CompositeKeyReflectionTest(CompositeKeyReflectionTest):
    pass


@pytest.mark.reviewed
class ComponentReflectionTestExtra(ComponentReflectionTestExtra):
    @pytest.mark.skip(reason="This dialect does not support check constraints")
    def test_get_check_constraints(self):
        pass

    @pytest.mark.skip(reason="Databricks does not support indexes.")
    def test_reflect_covering_index(self):
        pass

    @pytest.mark.skip(reason="Databricks does not support indexes.")
    def test_reflect_expression_based_indexes(self):
        pass

    @pytest.mark.skip(
        reason="Databricks doesn't enforce String or VARCHAR length limitations."
    )
    def test_varchar_reflection(self):
        """Even if a user specifies String(52), Databricks won't enforce that limit."""
        pass

    @pytest.mark.skip(
        reason="This dialect doesn't implement foreign key options checks."
    )
    def test_get_foreign_key_options(self):
        """It's not clear from the test code what the expected output is here. Further research required."""
        pass


class DifficultParametersTest(DifficultParametersTest):
    @pytest.mark.skip(reason="Error during execution. Requires investigation.")
    def test_round_trip_same_named_column(self):
        """
        Exception:
        - sqlalchemy.exc.DatabaseError: (databricks.sql.exc.ServerOperationError) Found invalid character(s) among ' ,;{}()\n\t=' in the column names of your schema.
        """


class InsertBehaviorTest(InsertBehaviorTest):
    @pytest.mark.skip(reason="Error during execution. Requires investigation.")
    def test_autoclose_on_insert(self):
        """
        Exception:
        - sqlalchemy.exc.DatabaseError: (databricks.sql.exc.ServerOperationError) Column id is not specified in INSERT
        """

    @pytest.mark.skip(reason="Error during execution. Requires investigation.")
    def test_empty_insert(self):
        """
        Exception:
        - sqlalchemy.exc.DatabaseError: (databricks.sql.exc.ServerOperationError)
        """

    @pytest.mark.skip(reason="Error during execution. Requires investigation.")
    def test_insert_from_select_autoinc(self):
        """
        Exception:
        - sqlalchemy.exc.DatabaseError: (databricks.sql.exc.ServerOperationError) Column id is not specified in INSERT
        """

    @pytest.mark.skip(reason="Error during execution. Requires investigation.")
    def test_insert_from_select_autoinc_no_rows(self):
        """
        Exception:
        - sqlalchemy.exc.DatabaseError: (databricks.sql.exc.ServerOperationError) Column id is not specified in INSERT
        """

    @pytest.mark.skip(reason="Databricks doesn't support empty INSERT.")
    def test_empty_insert_multiple(self):
        """
        Exception:
            sqlalchemy.exc.DatabaseError: (databricks.sql.exc.ServerOperationError)

            E               sqlalchemy.exc.DatabaseError: (databricks.sql.exc.ServerOperationError)
            E               [PARSE_SYNTAX_ERROR] Syntax error at or near ')'.(line 1, pos 24)
            E
            E               == SQL ==
            E               INSERT INTO autoinc_pk () VALUES ()
            E               ------------------------^^^
            E
            E               [SQL: INSERT INTO autoinc_pk () VALUES ()]
            E               [parameters: ({}, {}, {})]
            E               (Background on this error at: https://sqlalche.me/e/14/4xp6)
        """


@pytest.mark.reviewed
class ComponentReflectionTest(ComponentReflectionTest):
    """This test requires two schemas be present in the target Databricks workspace:
    - The schema set in --dburi
    - A second schema named "test_schema"

    Note that test_get_multi_foreign keys is flaky because DBR does not guarantee the order of data returned in DESCRIBE TABLE EXTENDED
    """

    @pytest.mark.skip(
        reason="Comment reflection is possible but not enabled in this dialect"
    )
    def test_get_multi_table_comment(self):
        """There are 84 permutations of this test that are skipped."""
        pass

    @pytest.mark.skip(reason="Databricks doesn't support UNIQUE constraints")
    def test_get_multi_unique_constraints(self):
        pass

    @pytest.mark.skip(
        reason="This dialect doesn't support get_table_options. See comment in test_suite.py"
    )
    def test_multi_get_table_options_tables(self):
        """It's not clear what the expected ouput from this method would even _be_. Requires research."""
        pass

    @pytest.mark.skip("This dialect doesn't implement get_view_definition")
    def test_get_view_definition(self):
        pass

    @pytest.mark.skip(reason="This dialect doesn't implement get_view_definition")
    def test_get_view_definition_does_not_exist(self):
        pass

    @pytest.mark.skip(reason="Strange test design. See test_suite.py")
    def test_get_temp_view_names(self):
        """While Databricks supports temporary views, this test creates a temp view aimed at a temp table.
        Databricks doesn't support temp tables. So the test can never pass.
        """
        pass

    @pytest.mark.skip("This dialect doesn't implement get_multi_pk_constraint")
    def test_get_multi_pk_constraint(self):
        pass

    @pytest.mark.skip(reason="Databricks doesn't support temp tables.")
    def test_get_temp_table_columns(self):
        pass

    @pytest.mark.skip(reason="Databricks doesn't support temp tables.")
    def test_get_temp_table_indexes(self):
        pass

    @pytest.mark.skip(reason="Databricks doesn't support temp tables.")
    def test_get_temp_table_names(self):
        pass

    @pytest.mark.skip(reason="Databricks doesn't support temp tables.")
    def test_get_temp_table_unique_constraints(self):
        pass

    @pytest.mark.skip(reason="Databricks doesn't support temp tables.")
    def test_reflect_table_temp_table(self):
        pass


@pytest.mark.reviewed
class TableDDLTest(TableDDLTest):
    @pytest.mark.skip(reason="Databricks does not support indexes.")
    def test_create_index_if_not_exists(self, connection):
        """We could use requirements.index_reflection and requirements.index_ddl_if_exists
        here to disable this but prefer a more meaningful skip message
        """
        pass

    @pytest.mark.skip(reason="Databricks does not support indexes.")
    def test_drop_index_if_exists(self, connection):
        """We could use requirements.index_reflection and requirements.index_ddl_if_exists
        here to disable this but prefer a more meaningful skip message
        """
        pass

    @pytest.mark.skip(
        reason="Comment reflection is possible but not implemented in this dialect."
    )
    def test_add_table_comment(self, connection):
        """We could use requirements.comment_reflection here to disable this but prefer a more meaningful skip message"""
        pass

    @pytest.mark.skip(
        reason="Comment reflection is possible but not implemented in this dialect."
    )
    def test_drop_table_comment(self, connection):
        """We could use requirements.comment_reflection here to disable this but prefer a more meaningful skip message"""
        pass


@pytest.mark.reviewed
@pytest.mark.skip(reason="Databricks does not support indexes.")
class HasIndexTest(HasIndexTest):
    pass


@pytest.mark.reviewed
@pytest.mark.skip(
    reason="Databricks does not support spaces in table names. See comment in test_suite.py"
)
class QuotedNameArgumentTest(QuotedNameArgumentTest):
    """These tests are challenging. The whole test setup depends on a table with a name like `quote ' one`
    which will never work on Databricks because table names can't contains spaces. But QuotedNamedArgumentTest
    also checks the behaviour of DDL identifier preparation process. We need to override some of IdentifierPreparer
    methods because these are the ultimate control for whether or not CHECK and UNIQUE constraints are emitted.
    """


@pytest.mark.reviewed
@pytest.mark.skip(reason="Implementation deferred. See test_suite.py")
class BizarroCharacterFKResolutionTest:
    """Some of the combinations in this test pass. Others fail. Given the esoteric nature of these failures,
    we have opted to defer implementing fixes to a later time, guided by customer feedback. Passage of
    these tests is not an acceptance criteria for our dialect.
    """


@pytest.mark.reviewed
@pytest.mark.skip(reason="Implementation deferred. See test_suite.py")
class DifficultParametersTest:
    """Some of the combinations in this test pass. Others fail. Given the esoteric nature of these failures,
    we have opted to defer implementing fixes to a later time, guided by customer feedback. Passage of
    these tests is not an acceptance criteria for our dialect.
    """
