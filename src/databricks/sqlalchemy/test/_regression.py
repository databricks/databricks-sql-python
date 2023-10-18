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
    FutureTableDDLTest
)

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
