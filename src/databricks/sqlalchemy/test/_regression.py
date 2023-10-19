import pytest
from sqlalchemy.testing.suite import (
    TimeMicrosecondsTest,
    TextTest,
    StringTest,
    DateTimeMicrosecondsTest,
    TimestampMicrosecondsTest,
    DateTimeCoercedToDateTimeTest,
    TimeTest,
    DateTimeTest,
    DateTimeHistoricTest,
    DateTest,
    DateHistoricTest,
    RowFetchTest,
    CompositeKeyReflectionTest,
    TrueDivTest,
    ArgSignatureTest,
    CompoundSelectTest,
    DeprecatedCompoundSelectTest,
    CastTypeDecoratorTest,
    DistinctOnTest,
    EscapingTest,
    ExistsTest,
    IntegerTest,
    IsOrIsNotDistinctFromTest,
    JoinTest,
    OrderByLabelTest,
    PingTest,
    ReturningGuardsTest,
    SameNamedSchemaTableTest,
    UnicodeTextTest,
    UnicodeVarcharTest,
    TableNoColumnsTest,
    PostCompileParamsTest,
    BooleanTest,
    ValuesExpressionTest,
    UuidTest,
    FetchLimitOffsetTest,
    FutureTableDDLTest,
    TableDDLTest,
    ComponentReflectionTest,
    InsertBehaviorTest,
    ComponentReflectionTestExtra,
    HasTableTest,
    NumericTest
)

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


@pytest.mark.reviewed
class InsertBehaviorTest(InsertBehaviorTest):
    @pytest.mark.skip(
        reason="Databricks dialect doesn't implement empty inserts. See test_suite.py"
    )
    def test_empty_insert(self):
        """Empty inserts are possible using DEFAULT VALUES on Databricks. To implement it, we need
        to hook into the SQLCompiler to render a no-op column list. With SQLAlchemy's default implementation
        the request fails with a syntax error
        """
        pass

    @pytest.mark.skip(
        reason="Databricks dialect doesn't implement empty inserts. See test_suite.py"
    )
    def test_empty_insert_multiple(self):
        """Empty inserts are possible using DEFAULT VALUES on Databricks. To implement it, we need
        to hook into the SQLCompiler to render a no-op column list. With SQLAlchemy's default implementation
        the request fails with a syntax error
        """
        pass

    @pytest.mark.skip(
        reason="Test setup relies on implicit autoincrement. See test_suite.py"
    )
    def test_autoclose_on_insert(self):
        """The setup for this test creates a column with implicit autoincrement enabled.
        This dialect does not implement implicit autoincrement - users must declare Identity() explicitly.
        """
        pass

    @pytest.mark.skip(
        reason="Test setup relies on implicit autoincrement. See test_suite.py"
    )
    def test_insert_from_select_autoinc(self):
        """Implicit autoincrement is not implemented in this dialect."""
        pass

    @pytest.mark.skip(
        reason="Test setup relies on implicit autoincrement. See test_suite.py"
    )
    def test_insert_from_select_autoinc_no_rows(self):
        pass

    @pytest.mark.skip(reason="Databricks doesn't support INSERT ... RETURNING syntax")
    def test_autoclose_on_insert_implicit_returning(self):
        pass



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
class UuidTest(UuidTest):
    @pytest.mark.skip(reason="Databricks doesn't support INSERT ... RETURNING syntax")
    def test_uuid_returning(self):
        pass


@pytest.mark.reviewed
class ValuesExpressionTest(ValuesExpressionTest):
    pass


@pytest.mark.reviewed
class BooleanTest(BooleanTest):
    pass


@pytest.mark.reviewed
class PostCompileParamsTest(PostCompileParamsTest):
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
class CompositeKeyReflectionTest(CompositeKeyReflectionTest):
    pass


@pytest.mark.reviewed
class TrueDivTest(TrueDivTest):
    pass


@pytest.mark.reviewed
class ArgSignatureTest(ArgSignatureTest):
    pass


@pytest.mark.reviewed
class CompoundSelectTest(CompoundSelectTest):
    pass


@pytest.mark.reviewed
class DeprecatedCompoundSelectTest(DeprecatedCompoundSelectTest):
    pass


@pytest.mark.reviewed
class CastTypeDecoratorTest(CastTypeDecoratorTest):
    pass


@pytest.mark.reviewed
class DistinctOnTest(DistinctOnTest):
    pass


@pytest.mark.reviewed
class EscapingTest(EscapingTest):
    pass


@pytest.mark.reviewed
class ExistsTest(ExistsTest):
    pass


@pytest.mark.reviewed
class IntegerTest(IntegerTest):
    pass


@pytest.mark.reviewed
class IsOrIsNotDistinctFromTest(IsOrIsNotDistinctFromTest):
    pass


@pytest.mark.reviewed
class JoinTest(JoinTest):
    pass


@pytest.mark.reviewed
class OrderByLabelTest(OrderByLabelTest):
    pass


@pytest.mark.reviewed
class PingTest(PingTest):
    pass


@pytest.mark.reviewed
class ReturningGuardsTest(ReturningGuardsTest):
    pass


@pytest.mark.reviewed
class SameNamedSchemaTableTest(SameNamedSchemaTableTest):
    pass


@pytest.mark.reviewed
class UnicodeTextTest(UnicodeTextTest):
    pass


@pytest.mark.reviewed
class UnicodeVarcharTest(UnicodeVarcharTest):
    pass

@pytest.mark.reviewed
class TableNoColumnsTest(TableNoColumnsTest):
    pass
