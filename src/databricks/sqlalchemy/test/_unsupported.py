import pytest
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
    ArrayTest,
    QuotedNameArgumentTest
)

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
@pytest.mark.skip(
    reason="pysql doesn't support binding of array parameters. See test_suite.py"
)
class ArrayTest(ArrayTest):
    """While Databricks supports ARRAY types, DBR cannot handle bound parameters of this type.
    This makes them unusable to SQLAlchemy without some workaround. Potentially we could inline
    the values of these parameters (which risks sql injection).
    """



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
class LongNameBlowoutTest(LongNameBlowoutTest):
    """These tests all include assertions that the tested name > 255 characters"""

    @pytest.mark.skip(
        reason="Databricks constraint names are limited to 255 characters"
    )
    def test_long_convention_name(self):
        pass


@pytest.mark.reviewed
@pytest.mark.skip(reason="Databricks doesn't support SEQUENCE server defaults")
class HasSequenceTest(HasSequenceTest):
    pass


@pytest.mark.reviewed
@pytest.mark.skip(reason="Databricks doesn't support SEQUENCE server defaults")
class HasSequenceTestEmpty(HasSequenceTestEmpty):
    pass


@pytest.mark.reviewed
@pytest.mark.skip(reason="Databricks does not support indexes.")
class HasIndexTest(HasIndexTest):
    pass


@pytest.mark.reviewed
@pytest.mark.skipped(reason="Databricks doesn't support unicode in symbol names")
class UnicodeSchemaTest(UnicodeSchemaTest):
    pass




@pytest.mark.reviewed
@pytest.mark.skip(reason="Databricks doesn't support server-side cursors.")
class ServerSideCursorsTest(ServerSideCursorsTest):
    pass



@pytest.mark.reviewed
@pytest.mark.skip(reason="Databricks doesn't allow percent signs in identifiers")
class PercentSchemaNamesTest(PercentSchemaNamesTest):
    pass


@pytest.mark.reviewed
@pytest.mark.skip(reason="Databricks does not support transactions")
class IsolationLevelTest(IsolationLevelTest):
    pass


@pytest.mark.reviewed
@pytest.mark.skip(reason="Databricks does not support transactions")
class AutocommitIsolationTest(AutocommitIsolationTest):
    pass


@pytest.mark.reviewed
@pytest.mark.skip(reason="Databricks doesn't support INSERT ... RETURNING syntax")
class ReturningTest(ReturningTest):
    pass


@pytest.mark.reviewed
@pytest.mark.skip(reason="Databricks does not support sequences.")
class SequenceTest(SequenceTest):
    pass


@pytest.mark.reviewed
@pytest.mark.skip(reason="Databricks does not support sequences.")
class SequenceCompilerTest(SequenceCompilerTest):
    pass

@pytest.mark.reviewed
@pytest.mark.skip(reason="Databricks does not support computed / generated columns")
class ComputedColumnTest(ComputedColumnTest):
    pass


@pytest.mark.reviewed
@pytest.mark.skip(reason="Databricks does not support computed / generated columns")
class ComputedReflectionTest(ComputedReflectionTest):
    pass
