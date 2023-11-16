# type: ignore

import pytest
from sqlalchemy.testing.suite import (
    ArgSignatureTest,
    BooleanTest,
    CastTypeDecoratorTest,
    ComponentReflectionTestExtra,
    CompositeKeyReflectionTest,
    CompoundSelectTest,
    DateHistoricTest,
    DateTest,
    DateTimeCoercedToDateTimeTest,
    DateTimeHistoricTest,
    DateTimeMicrosecondsTest,
    DateTimeTest,
    DeprecatedCompoundSelectTest,
    DistinctOnTest,
    EscapingTest,
    ExistsTest,
    ExpandingBoundInTest,
    FetchLimitOffsetTest,
    FutureTableDDLTest,
    HasTableTest,
    IdentityAutoincrementTest,
    InsertBehaviorTest,
    IntegerTest,
    IsOrIsNotDistinctFromTest,
    JoinTest,
    LikeFunctionsTest,
    NormalizedNameTest,
    NumericTest,
    OrderByLabelTest,
    PingTest,
    PostCompileParamsTest,
    ReturningGuardsTest,
    RowFetchTest,
    SameNamedSchemaTableTest,
    StringTest,
    TableDDLTest,
    TableNoColumnsTest,
    TextTest,
    TimeMicrosecondsTest,
    TimestampMicrosecondsTest,
    TimeTest,
    TimeTZTest,
    TrueDivTest,
    UnicodeTextTest,
    UnicodeVarcharTest,
    UuidTest,
    ValuesExpressionTest,
)

from databricks.sqlalchemy.test.overrides._ctetest import CTETest
from databricks.sqlalchemy.test.overrides._componentreflectiontest import (
    ComponentReflectionTest,
)


@pytest.mark.reviewed
class NumericTest(NumericTest):
    pass


@pytest.mark.reviewed
class HasTableTest(HasTableTest):
    pass


@pytest.mark.reviewed
class ComponentReflectionTestExtra(ComponentReflectionTestExtra):
    pass


@pytest.mark.reviewed
class InsertBehaviorTest(InsertBehaviorTest):
    pass


@pytest.mark.reviewed
class ComponentReflectionTest(ComponentReflectionTest):
    """This test requires two schemas be present in the target Databricks workspace:
    - The schema set in --dburi
    - A second schema named "test_schema"

    Note that test_get_multi_foreign keys is flaky because DBR does not guarantee the order of data returned in DESCRIBE TABLE EXTENDED

    _Most_ of these tests pass if we manually override the bad test setup.
    """

    pass


@pytest.mark.reviewed
class TableDDLTest(TableDDLTest):
    pass


@pytest.mark.reviewed
class FutureTableDDLTest(FutureTableDDLTest):
    pass


@pytest.mark.reviewed
class FetchLimitOffsetTest(FetchLimitOffsetTest):
    pass


@pytest.mark.reviewed
class UuidTest(UuidTest):
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


@pytest.mark.reviewed
class ExpandingBoundInTest(ExpandingBoundInTest):
    pass


@pytest.mark.reviewed
class CTETest(CTETest):
    pass


@pytest.mark.reviewed
class NormalizedNameTest(NormalizedNameTest):
    pass


@pytest.mark.reviewed
class IdentityAutoincrementTest(IdentityAutoincrementTest):
    pass


@pytest.mark.reviewed
class LikeFunctionsTest(LikeFunctionsTest):
    pass


@pytest.mark.reviewed
class TimeTZTest(TimeTZTest):
    pass
