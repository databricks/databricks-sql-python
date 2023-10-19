import pytest
from typing import Union

# These are test suites that are fully skipped with a SkipReason
from sqlalchemy.testing.suite import (
    SequenceTest,
    SequenceCompilerTest,
    ComputedColumnTest,
    ComputedReflectionTest,
    ReturningTest,
    IsolationLevelTest,
    AutocommitIsolationTest,
    PercentSchemaNamesTest,
    UnicodeSchemaTest,
    ServerSideCursorsTest,
    HasIndexTest,
    HasSequenceTest,
    HasSequenceTestEmpty,
    LongNameBlowoutTest,
    ExceptionTest,
    QuotedNameArgumentTest,
    LastrowidTest
)

from databricks.sqlalchemy.test._regression import (
    FetchLimitOffsetTest,
    UuidTest,
    FutureTableDDLTest,
    TableDDLTest,
    ComponentReflectionTest,
    NumericTest,
    HasTableTest,
    ComponentReflectionTestExtra,
    InsertBehaviorTest
)

from enum import Enum


class SkipReason(Enum):
    ENFORCE_KEYS = "enforcing primary or foreign key restraints"
    IDENTIFIER_LENGTH = "identifiers > 255 characters"
    SEQUENCES = "SQL SEQUENCES"
    INDEXES = "SQL INDEXes"
    SYMBOL_CHARSET = "symbols expected by test"
    CURSORS = "server-side cursors"
    TRANSACTIONS = "transactions"
    RETURNING = "INSERT ... RETURNING syntax"
    GENERATED_COLUMNS = "computed / generated columns"
    FETCH = "fetch clauses"
    IMPLICIT_ORDER = "deterministic return order if ORDER BY is not present"
    UNIQUE = "UNIQUE constraints"
    TEMP_TBL = "temporary tables"
    DECIMAL_FEAT = "required decimal features"
    IMPL_FLOAT_PREC = "required implicit float precision"
    STRING_FEAT = "required STRING type features"
    AUTO_INC = "implicit AUTO_INCREMENT"

def render_skip_reason(rsn: SkipReason, setup_error=False, extra=False) -> str:
    prefix = "[BADSETUP]" if setup_error else ""
    postfix = " More detail in _unsupported.py" if extra else ""
    return f"[UNSUPPORTED]{prefix}[{rsn.name}]: Databricks does not support {rsn.value}.{postfix}"


@pytest.mark.reviewed
@pytest.mark.skip(reason=render_skip_reason(SkipReason.SYMBOL_CHARSET, True, True))
class QuotedNameArgumentTest(QuotedNameArgumentTest):
    """These tests are challenging. The whole test setup depends on a table with a name like `quote ' one`
    which will never work on Databricks because table names can't contains spaces. But QuotedNamedArgumentTest
    also checks the behaviour of DDL identifier preparation process. We need to override some of IdentifierPreparer
    methods because these are the ultimate control for whether or not CHECK and UNIQUE constraints are emitted.
    """




@pytest.mark.reviewed
@pytest.mark.skip(reason=render_skip_reason(SkipReason.ENFORCE_KEYS))
class ExceptionTest(ExceptionTest):
    """Per Databricks documentation, primary and foreign key constraints are informational only
    and are not enforced.

    https://docs.databricks.com/api/workspace/tableconstraints
    """

    pass


@pytest.mark.reviewed
@pytest.mark.skip(reason=render_skip_reason(SkipReason.IDENTIFIER_LENGTH))
class LongNameBlowoutTest(LongNameBlowoutTest):
    """These tests all include assertions that the tested name > 255 characters"""
    pass


@pytest.mark.reviewed
@pytest.mark.skip(reason=render_skip_reason(SkipReason.SEQUENCES))
class HasSequenceTest(HasSequenceTest):
    pass


@pytest.mark.reviewed
@pytest.mark.skip(reason=render_skip_reason(SkipReason.SEQUENCES))
class HasSequenceTestEmpty(HasSequenceTestEmpty):
    pass


@pytest.mark.reviewed
@pytest.mark.skip(reason=render_skip_reason(SkipReason.INDEXES))
class HasIndexTest(HasIndexTest):
    pass


@pytest.mark.reviewed
@pytest.mark.skip(reason=render_skip_reason(SkipReason.SYMBOL_CHARSET))
class UnicodeSchemaTest(UnicodeSchemaTest):
    pass


@pytest.mark.reviewed
@pytest.mark.skip(reason=render_skip_reason(SkipReason.CURSORS))
class ServerSideCursorsTest(ServerSideCursorsTest):
    pass


@pytest.mark.reviewed
@pytest.mark.skip(reason=render_skip_reason(SkipReason.SYMBOL_CHARSET))
class PercentSchemaNamesTest(PercentSchemaNamesTest):
    pass


@pytest.mark.reviewed
@pytest.mark.skip(reason=render_skip_reason(SkipReason.TRANSACTIONS))
class IsolationLevelTest(IsolationLevelTest):
    pass


@pytest.mark.reviewed
@pytest.mark.skip(reason=render_skip_reason(SkipReason.TRANSACTIONS))
class AutocommitIsolationTest(AutocommitIsolationTest):
    pass


@pytest.mark.reviewed
@pytest.mark.skip(reason=render_skip_reason(SkipReason.RETURNING))
class ReturningTest(ReturningTest):
    pass


@pytest.mark.reviewed
@pytest.mark.skip(reason=render_skip_reason(SkipReason.SEQUENCES))
class SequenceTest(SequenceTest):
    pass


@pytest.mark.reviewed
@pytest.mark.skip(reason=render_skip_reason(SkipReason.SEQUENCES))
class SequenceCompilerTest(SequenceCompilerTest):
    pass


@pytest.mark.reviewed
@pytest.mark.skip(reason=render_skip_reason(SkipReason.GENERATED_COLUMNS))
class ComputedColumnTest(ComputedColumnTest):
    pass


@pytest.mark.reviewed
@pytest.mark.skip(reason=render_skip_reason(SkipReason.GENERATED_COLUMNS))
class ComputedReflectionTest(ComputedReflectionTest):
    pass


class FetchLimitOffsetTest(FetchLimitOffsetTest):
    @pytest.mark.flaky
    @pytest.mark.skip(reason=render_skip_reason(SkipReason.IMPLICIT_ORDER, extra=True))
    def test_limit_render_multiple_times(self):
        """This test depends on the order that records are inserted into the table. It's passing criteria requires that
        a record inserted with id=1 is the first record returned when no ORDER BY clause is specified. But Databricks occasionally
        INSERTS in a different order, which makes this test seem to fail. The test is flaky, but the underlying functionality
        (can multiple LIMIT clauses be rendered) is not broken.

        Unclear if this is a bug in Databricks, Delta, or some race-condition in the test itself.
        """
        pass

    @pytest.mark.skip(reason=render_skip_reason(SkipReason.FETCH))
    def test_bound_fetch_offset(self):
        pass

    @pytest.mark.skip(reason=render_skip_reason(SkipReason.FETCH))
    def test_fetch_offset_no_order(self):
        pass

    @pytest.mark.skip(reason=render_skip_reason(SkipReason.FETCH))
    def test_fetch_offset_nobinds(self):
        pass

    @pytest.mark.skip(reason=render_skip_reason(SkipReason.FETCH))
    def test_simple_fetch(self):
        pass

    @pytest.mark.skip(reason=render_skip_reason(SkipReason.FETCH))
    def test_simple_fetch_offset(self):
        pass

    @pytest.mark.skip(reason=render_skip_reason(SkipReason.FETCH))
    def test_simple_fetch_percent(self):
        pass

    @pytest.mark.skip(reason=render_skip_reason(SkipReason.FETCH))
    def test_simple_fetch_percent_ties(self):
        pass

    @pytest.mark.skip(reason=render_skip_reason(SkipReason.FETCH))
    def test_simple_fetch_ties(self):
        pass

    @pytest.mark.skip(reason=render_skip_reason(SkipReason.FETCH))
    def test_expr_fetch_offset(self):
        pass

    @pytest.mark.skip(reason=render_skip_reason(SkipReason.FETCH))
    def test_fetch_offset_percent(self):
        pass

    @pytest.mark.skip(reason=render_skip_reason(SkipReason.FETCH))
    def test_fetch_offset_percent_ties(self):
        pass

    @pytest.mark.skip(reason=render_skip_reason(SkipReason.FETCH))
    def test_fetch_offset_ties(self):
        pass

    @pytest.mark.skip(reason=render_skip_reason(SkipReason.FETCH))
    def test_fetch_offset_ties_exact_number(self):
        pass


class UuidTest(UuidTest):
    @pytest.mark.skip(reason=render_skip_reason(SkipReason.RETURNING))
    def test_uuid_returning(self):
        pass


@pytest.mark.reviewed
class FutureTableDDLTest(FutureTableDDLTest):

    @pytest.mark.skip(render_skip_reason(SkipReason.INDEXES))
    def test_create_index_if_not_exists(self):
        """We could use requirements.index_reflection and requirements.index_ddl_if_exists
        here to disable this but prefer a more meaningful skip message
        """
        pass

    @pytest.mark.skip(render_skip_reason(SkipReason.INDEXES))
    def test_drop_index_if_exists(self):
        """We could use requirements.index_reflection and requirements.index_ddl_if_exists
        here to disable this but prefer a more meaningful skip message
        """
        pass


@pytest.mark.reviewed
class TableDDLTest(TableDDLTest):
    @pytest.mark.skip(reason=render_skip_reason(SkipReason.INDEXES))
    def test_create_index_if_not_exists(self, connection):
        """We could use requirements.index_reflection and requirements.index_ddl_if_exists
        here to disable this but prefer a more meaningful skip message
        """
        pass

    @pytest.mark.skip(reason=render_skip_reason(SkipReason.INDEXES))
    def test_drop_index_if_exists(self, connection):
        """We could use requirements.index_reflection and requirements.index_ddl_if_exists
        here to disable this but prefer a more meaningful skip message
        """
        pass


@pytest.mark.reviewed
class ComponentReflectionTest(ComponentReflectionTest):
    """This test requires two schemas be present in the target Databricks workspace:
    - The schema set in --dburi
    - A second schema named "test_schema"

    Note that test_get_multi_foreign keys is flaky because DBR does not guarantee the order of data returned in DESCRIBE TABLE EXTENDED
    """

    @pytest.mark.skip(reason=render_skip_reason(SkipReason.UNIQUE))
    def test_get_multi_unique_constraints(self):
        pass

    @pytest.mark.skip(reason=render_skip_reason(SkipReason.TEMP_TBL, True, True))
    def test_get_temp_view_names(self):
        """While Databricks supports temporary views, this test creates a temp view aimed at a temp table.
        Databricks doesn't support temp tables. So the test can never pass.
        """
        pass

    @pytest.mark.skip(reason=render_skip_reason(SkipReason.TEMP_TBL))
    def test_get_temp_table_columns(self):
        pass

    @pytest.mark.skip(reason=render_skip_reason(SkipReason.TEMP_TBL))
    def test_get_temp_table_indexes(self):
        pass

    @pytest.mark.skip(reason=render_skip_reason(SkipReason.TEMP_TBL))
    def test_get_temp_table_names(self):
        pass

    @pytest.mark.skip(reason=render_skip_reason(SkipReason.TEMP_TBL))
    def test_get_temp_table_unique_constraints(self):
        pass

    @pytest.mark.skip(reason=render_skip_reason(SkipReason.TEMP_TBL))
    def test_reflect_table_temp_table(self):
        pass


@pytest.mark.reviewed
class NumericTest(NumericTest):
    @pytest.mark.skip(render_skip_reason(SkipReason.DECIMAL_FEAT))
    def test_enotation_decimal(self):
        """This test automatically runs if requirements.precision_numerics_enotation_large is open()"""
        pass

    @pytest.mark.skip(render_skip_reason(SkipReason.DECIMAL_FEAT))
    def test_enotation_decimal_large(self):
        """This test automatically runs if requirements.precision_numerics_enotation_large is open()"""
        pass

    @pytest.mark.skip(render_skip_reason(SkipReason.IMPL_FLOAT_PREC, extra=True))
    def test_float_coerce_round_trip(self):
        """
        This automatically runs if requirements.literal_float_coercion is open()

        Without additional work, Databricks returns 15.75629997253418 when you SELECT 15.7563.
        This is a potential area where we could override the Float literal processor to add a CAST.
        Will leave to a PM to decide if we should do so.
        """
        pass

    @pytest.mark.skip(render_skip_reason(SkipReason.IMPL_FLOAT_PREC, extra=True))
    def test_float_custom_scale(self):
        """This test automatically runs if requirements.precision_generic_float_type is open()"""
        pass

@pytest.mark.reviewed
class HasTableTest(HasTableTest):
    """Databricks does not support temporary tables."""

    @pytest.mark.skip(render_skip_reason(SkipReason.TEMP_TBL))
    def test_has_table_temp_table(self):
        pass

    @pytest.mark.skip(render_skip_reason(SkipReason.TEMP_TBL,True,True))
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
class ComponentReflectionTestExtra(ComponentReflectionTestExtra):

    @pytest.mark.skip(render_skip_reason(SkipReason.INDEXES))
    def test_reflect_covering_index(self):
        pass

    @pytest.mark.skip(render_skip_reason(SkipReason.INDEXES))
    def test_reflect_expression_based_indexes(self):
        pass

    @pytest.mark.skip(render_skip_reason(SkipReason.STRING_FEAT, extra=True))
    def test_varchar_reflection(self):
        """Databricks doesn't enforce string length limitations like STRING(255). """
        pass


@pytest.mark.reviewed
class InsertBehaviorTest(InsertBehaviorTest):

    @pytest.mark.skip(render_skip_reason(SkipReason.AUTO_INC, True, True))
    def test_autoclose_on_insert(self):
        """The setup for this test creates a column with implicit autoincrement enabled.
        This dialect does not implement implicit autoincrement - users must declare Identity() explicitly.
        """
        pass

    @pytest.mark.skip(render_skip_reason(SkipReason.AUTO_INC, True, True))
    def test_insert_from_select_autoinc(self):
        """Implicit autoincrement is not implemented in this dialect."""
        pass

    @pytest.mark.skip(render_skip_reason(SkipReason.AUTO_INC, True, True))
    def test_insert_from_select_autoinc_no_rows(self):
        pass

    @pytest.mark.skip(render_skip_reason(SkipReason.RETURNING))
    def test_autoclose_on_insert_implicit_returning(self):
        pass

@pytest.mark.reviewed
@pytest.mark.skip(render_skip_reason(SkipReason.AUTO_INC, extra=True))
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