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
    ExceptionTest
)

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
