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

from enum import Enum
class SkipReason(Enum):

    TABLE_NAMES_NO_SPACE = "spaces in table names"
    PK_FK_NO_ENFORCE = "enforcing primary or foreign key restraints"
    IDENTIFIER_LENGTH = "identifiers > 255 characters"
    SEQUENCES = "SQL SEQUENCES"
    INDEXES = "SQL INDEXes"
    SYMBOL_CHARSET = "symbols expected by test"
    CURSORS = "server-side cursors"
    TRANSACTIONS = "transactions"
    RETURNING = "INSERT ... RETURNING syntax"
    GENERATED_COLUMNS = "computed / generated columns" 


def render_skip_reason(rsn: SkipReason, setup_error=False) -> str:
    prefix = "[BADSETUP]" if setup_error else ""
    return f"{prefix}{rsn.name}: Databricks does not support {rsn.value}."

@pytest.mark.reviewed
@pytest.mark.skip(reason=render_skip_reason(SkipReason.TABLE_NAMES_NO_SPACE, True))
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
@pytest.mark.skip(reason=render_skip_reason(SkipReason.PK_FK_NO_ENFORCE))
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
