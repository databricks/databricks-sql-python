# type: ignore

from enum import Enum

import pytest
from databricks.sqlalchemy.test._regression import (
    ExpandingBoundInTest,
    IdentityAutoincrementTest,
    LikeFunctionsTest,
    NormalizedNameTest,
)
from databricks.sqlalchemy.test._unsupported import (
    ComponentReflectionTest,
    ComponentReflectionTestExtra,
    CTETest,
    InsertBehaviorTest,
)
from sqlalchemy.testing.suite import (
    ArrayTest,
    BinaryTest,
    BizarroCharacterFKResolutionTest,
    CollateTest,
    ComputedColumnTest,
    ComputedReflectionTest,
    DifficultParametersTest,
    FutureWeCanSetDefaultSchemaWEventsTest,
    IdentityColumnTest,
    IdentityReflectionTest,
    JSONLegacyStringCastIndexTest,
    JSONTest,
    NativeUUIDTest,
    QuotedNameArgumentTest,
    RowCountTest,
    SimpleUpdateDeleteTest,
    WeCanSetDefaultSchemaWEventsTest,
)


class FutureFeature(Enum):
    ARRAY = "ARRAY column type handling"
    BINARY = "BINARY column type handling"
    CHECK = "CHECK constraint handling"
    COLLATE = "COLLATE DDL generation"
    CTE_FEAT = "required CTE features"
    EMPTY_INSERT = "empty INSERT support"
    FK_OPTS = "foreign key option checking"
    GENERATED_COLUMNS = "Delta computed / generated columns support"
    IDENTITY = "identity reflection"
    JSON = "JSON column type handling"
    MULTI_PK = "get_multi_pk_constraint method"
    PROVISION = "event-driven engine configuration"
    REGEXP = "_visit_regexp"
    SANE_ROWCOUNT = "sane_rowcount support"
    TBL_OPTS = "get_table_options method"
    TEST_DESIGN = "required test-fixture overrides"
    TUPLE_LITERAL = "tuple-like IN markers completely"
    UUID = "native Uuid() type"
    VIEW_DEF = "get_view_definition method"


def render_future_feature(rsn: FutureFeature, extra=False) -> str:
    postfix = " More detail in _future.py" if extra else ""
    return f"[FUTURE][{rsn.name}]: This dialect doesn't implement {rsn.value}.{postfix}"


@pytest.mark.reviewed
@pytest.mark.skip(render_future_feature(FutureFeature.BINARY))
class BinaryTest(BinaryTest):
    """Databricks doesn't support binding of BINARY type values. When DBR supports this, we can implement
    in this dialect.
    """

    pass


class ExpandingBoundInTest(ExpandingBoundInTest):
    @pytest.mark.skip(render_future_feature(FutureFeature.TUPLE_LITERAL))
    def test_empty_heterogeneous_tuples_bindparam(self):
        pass

    @pytest.mark.skip(render_future_feature(FutureFeature.TUPLE_LITERAL))
    def test_empty_heterogeneous_tuples_direct(self):
        pass

    @pytest.mark.skip(render_future_feature(FutureFeature.TUPLE_LITERAL))
    def test_empty_homogeneous_tuples_bindparam(self):
        pass

    @pytest.mark.skip(render_future_feature(FutureFeature.TUPLE_LITERAL))
    def test_empty_homogeneous_tuples_direct(self):
        pass


class NormalizedNameTest(NormalizedNameTest):
    @pytest.mark.skip(render_future_feature(FutureFeature.TEST_DESIGN, True))
    def test_get_table_names(self):
        """I'm not clear how this test can ever pass given that it's assertion looks like this:

        ```python
                eq_(tablenames[0].upper(), tablenames[0].lower())
                eq_(tablenames[1].upper(), tablenames[1].lower())
        ```

        It's forcibly calling .upper() and .lower() on the same string and expecting them to be equal.
        """
        pass


class CTETest(CTETest):
    @pytest.mark.skip(render_future_feature(FutureFeature.CTE_FEAT, True))
    def test_delete_from_round_trip(self):
        """Databricks dialect doesn't implement multiple-table criteria within DELETE"""
        pass


@pytest.mark.reviewed
@pytest.mark.skip(render_future_feature(FutureFeature.TEST_DESIGN, True))
class IdentityColumnTest(IdentityColumnTest):
    """Identity works. Test needs rewrite for Databricks. See comments in test_suite.py

    The setup for these tests tries to create a table with a DELTA IDENTITY column but has two problems:
    1. It uses an Integer() type for the column. Whereas DELTA IDENTITY columns must be BIGINT.
    2. It tries to set the start == 42, which Databricks doesn't support

    I can get the tests to _run_ by patching the table fixture to use BigInteger(). But it asserts that the
    identity of two rows are 42 and 43, which is not possible since they will be rows 1 and 2 instead.

    I'm satisified through manual testing that our implementation of visit_identity_column works but a better test is needed.
    """

    pass


class IdentityAutoincrementTest(IdentityAutoincrementTest):
    @pytest.mark.skip(render_future_feature(FutureFeature.TEST_DESIGN, True))
    def test_autoincrement_with_identity(self):
        """This test has the same issue as IdentityColumnTest.test_select_all in that it creates a table with identity
        using an Integer() rather than a BigInteger(). If I override this behaviour to use a BigInteger() instead, the
        test passes.
        """


@pytest.mark.reviewed
@pytest.mark.skip(render_future_feature(FutureFeature.TEST_DESIGN))
class BizarroCharacterFKResolutionTest(BizarroCharacterFKResolutionTest):
    """Some of the combinations in this test pass. Others fail. Given the esoteric nature of these failures,
    we have opted to defer implementing fixes to a later time, guided by customer feedback. Passage of
    these tests is not an acceptance criteria for our dialect.
    """


@pytest.mark.reviewed
@pytest.mark.skip(render_future_feature(FutureFeature.TEST_DESIGN))
class DifficultParametersTest(DifficultParametersTest):
    """Some of the combinations in this test pass. Others fail. Given the esoteric nature of these failures,
    we have opted to defer implementing fixes to a later time, guided by customer feedback. Passage of
    these tests is not an acceptance criteria for our dialect.
    """


@pytest.mark.reviewed
@pytest.mark.skip(render_future_feature(FutureFeature.IDENTITY, True))
class IdentityReflectionTest(IdentityReflectionTest):
    """It's not clear _how_ to implement this for SQLAlchemy. Columns created with GENERATED ALWAYS AS IDENTITY
    are not specially demarked in the output of TGetColumnsResponse or DESCRIBE TABLE EXTENDED.

    We could theoretically parse this from the contents of `SHOW CREATE TABLE` but that feels like a hack.
    """


@pytest.mark.reviewed
@pytest.mark.skip(render_future_feature(FutureFeature.JSON))
class JSONTest(JSONTest):
    """Databricks supports JSON path expressions in queries it's just not implemented in this dialect."""

    pass


@pytest.mark.reviewed
@pytest.mark.skip(render_future_feature(FutureFeature.JSON))
class JSONLegacyStringCastIndexTest(JSONLegacyStringCastIndexTest):
    """Same comment applies as JSONTest"""

    pass


class LikeFunctionsTest(LikeFunctionsTest):
    @pytest.mark.skip(render_future_feature(FutureFeature.REGEXP))
    def test_not_regexp_match(self):
        """The defaul dialect doesn't implement _visit_regexp methods so we don't get them automatically."""
        pass

    @pytest.mark.skip(render_future_feature(FutureFeature.REGEXP))
    def test_regexp_match(self):
        """The defaul dialect doesn't implement _visit_regexp methods so we don't get them automatically."""
        pass


@pytest.mark.reviewed
@pytest.mark.skip(render_future_feature(FutureFeature.COLLATE))
class CollateTest(CollateTest):
    """This is supported in Databricks. Not implemented here."""


@pytest.mark.reviewed
@pytest.mark.skip(render_future_feature(FutureFeature.UUID, True))
class NativeUUIDTest(NativeUUIDTest):
    """Type implementation will be straightforward. Since Databricks doesn't have a native UUID type we can use
    a STRING field, create a custom TypeDecorator for sqlalchemy.types.Uuid and add it to the dialect's colspecs.

    Then mark requirements.uuid_data_type as open() so this test can run.
    """


@pytest.mark.reviewed
@pytest.mark.skip(render_future_feature(FutureFeature.SANE_ROWCOUNT))
class RowCountTest(RowCountTest):
    pass


@pytest.mark.reviewed
@pytest.mark.skip(render_future_feature(FutureFeature.SANE_ROWCOUNT))
class SimpleUpdateDeleteTest(SimpleUpdateDeleteTest):
    pass


@pytest.mark.reviewed
@pytest.mark.skip(render_future_feature(FutureFeature.PROVISION, True))
class WeCanSetDefaultSchemaWEventsTest(WeCanSetDefaultSchemaWEventsTest):
    """provision.py allows us to define event listeners that emit DDL for things like setting up a test schema
    or, in this case, changing the default schema for the connection after it's been built. This would override
    the schema defined in the sqlalchemy connection string. This support is possible but is not implemented
    in the dialect. Deferred for now.
    """

    pass


@pytest.mark.reviewed
@pytest.mark.skip(render_future_feature(FutureFeature.PROVISION, True))
class FutureWeCanSetDefaultSchemaWEventsTest(FutureWeCanSetDefaultSchemaWEventsTest):
    """provision.py allows us to define event listeners that emit DDL for things like setting up a test schema
    or, in this case, changing the default schema for the connection after it's been built. This would override
    the schema defined in the sqlalchemy connection string. This support is possible but is not implemented
    in the dialect. Deferred for now.
    """

    pass


class ComponentReflectionTest(ComponentReflectionTest):
    @pytest.mark.skip(reason=render_future_feature(FutureFeature.TBL_OPTS, True))
    def test_multi_get_table_options_tables(self):
        """It's not clear what the expected ouput from this method would even _be_. Requires research."""
        pass

    @pytest.mark.skip(render_future_feature(FutureFeature.VIEW_DEF))
    def test_get_view_definition(self):
        pass

    @pytest.mark.skip(render_future_feature(FutureFeature.VIEW_DEF))
    def test_get_view_definition_does_not_exist(self):
        pass

    @pytest.mark.skip(render_future_feature(FutureFeature.MULTI_PK))
    def test_get_multi_pk_constraint(self):
        pass

    @pytest.mark.skip(render_future_feature(FutureFeature.CHECK))
    def test_get_multi_check_constraints(self):
        pass


class ComponentReflectionTestExtra(ComponentReflectionTestExtra):
    @pytest.mark.skip(render_future_feature(FutureFeature.CHECK))
    def test_get_check_constraints(self):
        pass

    @pytest.mark.skip(render_future_feature(FutureFeature.FK_OPTS))
    def test_get_foreign_key_options(self):
        """It's not clear from the test code what the expected output is here. Further research required."""
        pass


class InsertBehaviorTest(InsertBehaviorTest):
    @pytest.mark.skip(render_future_feature(FutureFeature.EMPTY_INSERT, True))
    def test_empty_insert(self):
        """Empty inserts are possible using DEFAULT VALUES on Databricks. To implement it, we need
        to hook into the SQLCompiler to render a no-op column list. With SQLAlchemy's default implementation
        the request fails with a syntax error
        """
        pass

    @pytest.mark.skip(render_future_feature(FutureFeature.EMPTY_INSERT, True))
    def test_empty_insert_multiple(self):
        """Empty inserts are possible using DEFAULT VALUES on Databricks. To implement it, we need
        to hook into the SQLCompiler to render a no-op column list. With SQLAlchemy's default implementation
        the request fails with a syntax error
        """
        pass


@pytest.mark.reviewed
@pytest.mark.skip(render_future_feature(FutureFeature.ARRAY))
class ArrayTest(ArrayTest):
    """While Databricks supports ARRAY types, DBR cannot handle bound parameters of this type.
    This makes them unusable to SQLAlchemy without some workaround. Potentially we could inline
    the values of these parameters (which risks sql injection).
    """


@pytest.mark.reviewed
@pytest.mark.skip(render_future_feature(FutureFeature.TEST_DESIGN, True))
class QuotedNameArgumentTest(QuotedNameArgumentTest):
    """These tests are challenging. The whole test setup depends on a table with a name like `quote ' one`
    which will never work on Databricks because table names can't contains spaces. But QuotedNamedArgumentTest
    also checks the behaviour of DDL identifier preparation process. We need to override some of IdentifierPreparer
    methods because these are the ultimate control for whether or not CHECK and UNIQUE constraints are emitted.
    """


@pytest.mark.reviewed
@pytest.mark.skip(reason=render_future_feature(FutureFeature.GENERATED_COLUMNS))
class ComputedColumnTest(ComputedColumnTest):
    pass


@pytest.mark.reviewed
@pytest.mark.skip(reason=render_future_feature(FutureFeature.GENERATED_COLUMNS))
class ComputedReflectionTest(ComputedReflectionTest):
    pass
