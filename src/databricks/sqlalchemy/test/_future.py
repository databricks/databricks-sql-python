import pytest
from sqlalchemy.testing.suite import (
    WeCanSetDefaultSchemaWEventsTest,
    FutureWeCanSetDefaultSchemaWEventsTest,
    SimpleUpdateDeleteTest,
    RowCountTest,
    NativeUUIDTest,
    CollateTest,
    TimeTZTest,
    DateTimeTZTest,
    LikeFunctionsTest,
    JSONTest,
    JSONLegacyStringCastIndexTest,
    BizarroCharacterFKResolutionTest,
    DifficultParametersTest,
    IdentityReflectionTest,
    IdentityColumnTest,
    IdentityAutoincrementTest,
    CTETest,
    NormalizedNameTest,
    ExpandingBoundInTest,
    LastrowidTest,
    BinaryTest
)

@pytest.mark.reviewed
@pytest.mark.skip(reason="pysql doesn't support binding of BINARY type parameters")
class BinaryTest(BinaryTest):
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



TUPLES_READ_AS_STRUCT_MSG = (
    "Databricks interprets tuple-like IN markers as though they are structs."
)


@pytest.mark.reviewed
class ExpandingBoundInTest(ExpandingBoundInTest):
    @pytest.mark.skip(reason=TUPLES_READ_AS_STRUCT_MSG)
    def test_empty_heterogeneous_tuples_bindparam(self):
        pass

    @pytest.mark.skip(reason=TUPLES_READ_AS_STRUCT_MSG)
    def test_empty_heterogeneous_tuples_direct(self):
        pass

    @pytest.mark.skip(reason=TUPLES_READ_AS_STRUCT_MSG)
    def test_empty_homogeneous_tuples_bindparam(self):
        pass

    @pytest.mark.skip(reason=TUPLES_READ_AS_STRUCT_MSG)
    def test_empty_homogeneous_tuples_direct(self):
        pass


@pytest.mark.reviewed
class NormalizedNameTest(NormalizedNameTest):
    @pytest.mark.skip(reason="Poor test design? See test_suite.py")
    def test_get_table_names(self):
        """I'm not clear how this test can ever pass given that it's assertion looks like this:

        ```python
                eq_(tablenames[0].upper(), tablenames[0].lower())
                eq_(tablenames[1].upper(), tablenames[1].lower())
        ```

        It's forcibly calling .upper() and .lower() on the same string and expecting them to be equal.
        """
        pass


@pytest.mark.reviewed
class CTETest(CTETest):
    """During the teardown for this test block, it tries to drop a constraint that it never named which raises
    a compilation error. This could point to poor constraint reflection but our other constraint reflection
    tests pass. Requires investigation.
    """

    @pytest.mark.skip(
        reason="Databricks dialect doesn't implement multiple-table criteria within DELETE"
    )
    def test_delete_from_round_trip(self):
        """This may be supported by Databricks but has not been implemented here."""
        pass

    @pytest.mark.skip(reason="Databricks doesn't support recursive CTE")
    def test_select_recursive_round_trip(self):
        pass

    @pytest.mark.skip(reason="Unsupported by Databricks. See test_suite.py")
    def test_delete_scalar_subq_round_trip(self):
        """Error received is [UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.MUST_AGGREGATE_CORRELATED_SCALAR_SUBQUERY]

        This suggests a limitation of the platform. But a workaround may be possible if customers require it.
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
@pytest.mark.skip(reason="Implementation deferred. See test_suite.py")
class BizarroCharacterFKResolutionTest(BizarroCharacterFKResolutionTest):
    """Some of the combinations in this test pass. Others fail. Given the esoteric nature of these failures,
    we have opted to defer implementing fixes to a later time, guided by customer feedback. Passage of
    these tests is not an acceptance criteria for our dialect.
    """


@pytest.mark.reviewed
@pytest.mark.skip(reason="Implementation deferred. See test_suite.py")
class DifficultParametersTest(DifficultParametersTest):
    """Some of the combinations in this test pass. Others fail. Given the esoteric nature of these failures,
    we have opted to defer implementing fixes to a later time, guided by customer feedback. Passage of
    these tests is not an acceptance criteria for our dialect.
    """


@pytest.mark.reviewed
@pytest.mark.skip(
    reason="Identity reflection is not implemented in this dialect. See test_suite.py"
)
class IdentityReflectionTest(IdentityReflectionTest):
    """It's not clear _how_ to implement this for SQLAlchemy. Columns created with GENERATED ALWAYS AS IDENTITY
    are not specially demarked in the output of TGetColumnsResponse or DESCRIBE TABLE EXTENDED.

    We could theoretically parse this from the contents of `SHOW CREATE TABLE` but that feels like a hack.
    """

@pytest.mark.reviewed
@pytest.mark.skip(
    reason="Databricks dialect doesn't implement JSON column types. See test_suite.py"
)
class JSONTest(JSONTest):
    """Databricks supports JSON path expressions in queries it's just not implemented in this dialect."""

    pass


@pytest.mark.reviewed
@pytest.mark.skip(
    reason="Databricks dialect doesn't implement JSON column types. See test_suite.py"
)
class JSONLegacyStringCastIndexTest(JSONLegacyStringCastIndexTest):
    """Same comment applies as JSONTest"""

    pass

@pytest.mark.reviewed
class LikeFunctionsTest(LikeFunctionsTest):
    @pytest.mark.skip(
        reason="Databricks dialect doesn't implement regexp features. See test_suite.py"
    )
    def test_not_regexp_match(self):
        """The defaul dialect doesn't implement _visit_regexp methods so we don't get them automatically."""
        pass

    @pytest.mark.skip(
        reason="Databricks dialect doesn't implement regexp features. See test_suite.py"
    )
    def test_regexp_match(self):
        """The defaul dialect doesn't implement _visit_regexp methods so we don't get them automatically."""
        pass



@pytest.mark.reviewed
@pytest.mark.skip(
    reason="Datetime handling doesn't handle timezones well. Priority to fix."
)
class DateTimeTZTest(DateTimeTZTest):
    """When I initially implemented DateTime type handling, I started using TIMESTAMP_NTZ because
    that's the default behaviour of the DateTime() type and the other tests passed. I simply missed
    this group of tests. Will need to modify the compilation and result_processor for our type override
    so that we can pass both DateTimeTZTest and DateTimeTest. Currently, only DateTimeTest passes.
    """

    pass

@pytest.mark.reviewed
@pytest.mark.skip(
    reason="Databricks dialect does not implement timezone support for Timestamp() types. See test_suite.py"
)
class TimeTZTest(TimeTZTest):
    """Similar to DateTimeTZTest, this should be possible for the dialect since we can override type compilation
    and processing in _types.py. Implementation has been deferred.
    """


@pytest.mark.reviewed
@pytest.mark.skip(reason="Databricks dialect does not implement COLLATE support")
class CollateTest(CollateTest):
    """This is supported in Databricks. Not implemented here."""


@pytest.mark.reviewed
@pytest.mark.skip(
    reason="Databricks dialect doesn't implement UUID type. See test_suite.py"
)
class NativeUUIDTest(NativeUUIDTest):
    """Type implementation will be straightforward. Since Databricks doesn't have a native UUID type we can use
    a STRING field, create a custom TypeDecorator for sqlalchemy.types.Uuid and add it to the dialect's colspecs.

    Then mark requirements.uuid_data_type as open() so this test can run.
    """


@pytest.mark.reviewed
@pytest.mark.skip(reason="Databricks dialect does not implement sane rowcount.")
class RowCountTest(RowCountTest):
    pass


@pytest.mark.reviewed
@pytest.mark.skip(reason="Databricks dialect does not implement sane rowcount.")
class SimpleUpdateDeleteTest(SimpleUpdateDeleteTest):
    pass



@pytest.mark.reviewed
@pytest.mark.skip(reason="Dialect doesn't implement provision.py See test_suite.py")
class WeCanSetDefaultSchemaWEventsTest(WeCanSetDefaultSchemaWEventsTest):
    """provision.py allows us to define event listeners that emit DDL for things like setting up a test schema
    or, in this case, changing the default schema for the connection after it's been built. This would override
    the schema defined in the sqlalchemy connection string. This support is possible but is not implemented
    in the dialect. Deferred for now.
    """

    pass


@pytest.mark.reviewed
@pytest.mark.skip(reason="Dialect doesn't implement provision.py See test_suite.py")
class FutureWeCanSetDefaultSchemaWEventsTest(FutureWeCanSetDefaultSchemaWEventsTest):
    """provision.py allows us to define event listeners that emit DDL for things like setting up a test schema
    or, in this case, changing the default schema for the connection after it's been built. This would override
    the schema defined in the sqlalchemy connection string. This support is possible but is not implemented
    in the dialect. Deferred for now.
    """

    pass
