import datetime
from contextlib import contextmanager
from decimal import Decimal
from enum import Enum
from typing import Dict, List, Type, Union
from unittest.mock import patch

import pytest
import pytz

from databricks.sql.parameters.native import (
    BigIntegerParameter,
    BooleanParameter,
    DateParameter,
    DbsqlParameterBase,
    DecimalParameter,
    DoubleParameter,
    FloatParameter,
    IntegerParameter,
    ParameterApproach,
    ParameterStructure,
    SmallIntParameter,
    StringParameter,
    TDbsqlParameter,
    TimestampNTZParameter,
    TimestampParameter,
    TinyIntParameter,
    VoidParameter,
)
from tests.e2e.test_driver import PySQLPytestTestCase


class ParamStyle(Enum):
    NAMED = 1
    PYFORMAT = 2
    NONE = 3


class Primitive(Enum):
    """These are the inferrable types. This Enum is used for parametrized tests."""

    NONE = None
    BOOL = True
    INT = 50
    BIGINT = 2147483648
    STRING = "Hello"
    DECIMAL = Decimal("1234.56")
    DATE = datetime.date(2023, 9, 6)
    TIMESTAMP = datetime.datetime(2023, 9, 6, 3, 14, 27, 843, tzinfo=pytz.UTC)
    DOUBLE = 3.14
    FLOAT = 3.15
    SMALLINT = 51


class PrimitiveExtra(Enum):
    """These are not inferrable types. This Enum is used for parametrized tests."""

    TIMESTAMP_NTZ = datetime.datetime(2023, 9, 6, 3, 14, 27, 843)
    TINYINT = 20


# We don't test inline approach with named paramstyle because it's never supported
# We don't test inline approach with positional parameters because it's never supported
# Paramstyle doesn't apply when ParameterStructure.POSITIONAL because question marks are used.
approach_paramstyle_combinations = [
    (ParameterApproach.INLINE, ParamStyle.PYFORMAT, ParameterStructure.NAMED),
    (ParameterApproach.NATIVE, ParamStyle.NONE, ParameterStructure.POSITIONAL),
    (ParameterApproach.NATIVE, ParamStyle.PYFORMAT, ParameterStructure.NAMED),
    (ParameterApproach.NATIVE, ParamStyle.NONE, ParameterStructure.POSITIONAL),
    (ParameterApproach.NATIVE, ParamStyle.NAMED, ParameterStructure.NAMED),
]


class TestParameterizedQueries(PySQLPytestTestCase):
    """Namespace for tests of this connector's parameterisation behaviour.

    databricks-sql-connector can approach parameterisation in two ways:

        NATIVE: the connector will use server-side bound parameters implemented by DBR 14.1 and above.
        INLINE: the connector will render parameter values as strings and interpolate them into the query.

    Prior to connector version 3.0.0, the connector would always use the INLINE approach. This approach
    is still the default but this will be changed in a subsequent release.

    The INLINE and NATIVE approaches use different query syntax, which these tests verify.

    There is not 1-to-1 feature parity between these approaches. Where possible, we run the same test
    for @both_approaches.
    """

    NAMED_PARAMSTYLE_QUERY = "SELECT :p AS col"
    PYFORMAT_PARAMSTYLE_QUERY = "SELECT %(p)s AS col"
    POSITIONAL_PARAMSTYLE_QUERY = "SELECT ? AS col"

    inline_type_map = {
        Primitive.INT: "int_col",
        Primitive.BIGINT: "bigint_col",
        Primitive.SMALLINT: "small_int_col",
        Primitive.FLOAT: "float_col",
        Primitive.DOUBLE: "double_col",
        Primitive.DECIMAL: "decimal_col",
        Primitive.STRING: "string_col",
        Primitive.BOOL: "boolean_col",
        Primitive.DATE: "date_col",
        Primitive.TIMESTAMP: "timestamp_col",
        Primitive.NONE: "null_col",
    }

    def _get_inline_table_column(self, value):
        return self.inline_type_map[Primitive(value)]

    @pytest.fixture(scope="class")
    def inline_table(self, connection_details):
        self.arguments = connection_details.copy()
        """This table is necessary to verify that a parameter sent with INLINE
        approach can actually write to its analogous data type.

        For example, a Python Decimal(), when rendered inline, should be able
        to read/write into a DECIMAL column in Databricks

        Note that this fixture doesn't clean itself up. So the table will remain
        in the schema for use by subsequent test runs.
        """

        query = """
            CREATE TABLE IF NOT EXISTS pysql_e2e_inline_param_test_table (
            null_col INT,
            int_col INT,
            bigint_col BIGINT,
            small_int_col SMALLINT,
            float_col FLOAT,
            double_col DOUBLE,
            decimal_col DECIMAL(10, 2),
            string_col STRING,
            boolean_col BOOLEAN,
            date_col DATE,
            timestamp_col TIMESTAMP
            ) USING DELTA
        """

        with self.connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)

    @contextmanager
    def patch_server_supports_native_params(self, supports_native_params: bool = True):
        """Applies a patch so we can test the connector's behaviour under different SPARK_CLI_SERVICE_PROTOCOL_VERSION conditions."""

        with patch(
            "databricks.sql.client.Connection.server_parameterized_queries_enabled",
            return_value=supports_native_params,
        ) as mock_parameterized_queries_enabled:
            try:
                yield mock_parameterized_queries_enabled
            finally:
                pass

    def _inline_roundtrip(self, params: dict, paramstyle: ParamStyle):
        """This INSERT, SELECT, DELETE dance is necessary because simply selecting
        ```
        "SELECT %(param)s"
        ```
        in INLINE mode would always return a str and the nature of the test is to
        confirm that types are maintained.

        :paramstyle:
            This is a no-op but is included to make the test-code easier to read.
        """
        target_column = self._get_inline_table_column(params.get("p"))
        INSERT_QUERY = (
            f"INSERT INTO pysql_e2e_inline_param_test_table (`{target_column}`) VALUES (%(p)s)"
        )
        SELECT_QUERY = (
            f"SELECT {target_column} `col` FROM pysql_e2e_inline_param_test_table LIMIT 1"
        )
        DELETE_QUERY = "DELETE FROM pysql_e2e_inline_param_test_table"

        with self.connection(extra_params={"use_inline_params": True}) as conn:
            with conn.cursor() as cursor:
                cursor.execute(INSERT_QUERY, parameters=params)
            with conn.cursor() as cursor:
                to_return = cursor.execute(SELECT_QUERY).fetchone()
            with conn.cursor() as cursor:
                cursor.execute(DELETE_QUERY)

        return to_return

    def _native_roundtrip(
        self,
        parameters: Union[Dict, List[Dict]],
        paramstyle: ParamStyle,
        parameter_structure: ParameterStructure,
    ):
        if parameter_structure == ParameterStructure.POSITIONAL:
            _query = self.POSITIONAL_PARAMSTYLE_QUERY
        elif paramstyle == ParamStyle.NAMED:
            _query = self.NAMED_PARAMSTYLE_QUERY
        elif paramstyle == ParamStyle.PYFORMAT:
            _query = self.PYFORMAT_PARAMSTYLE_QUERY
        with self.connection(extra_params={"use_inline_params": False}) as conn:
            with conn.cursor() as cursor:
                cursor.execute(_query, parameters=parameters)
                return cursor.fetchone()

    def _get_one_result(
        self,
        params,
        approach: ParameterApproach = ParameterApproach.NONE,
        paramstyle: ParamStyle = ParamStyle.NONE,
        parameter_structure: ParameterStructure = ParameterStructure.NONE,
    ):
        """When approach is INLINE then we use %(param)s paramstyle and a connection with use_inline_params=True
        When approach is NATIVE then we use :param paramstyle and a connection with use_inline_params=False
        """

        if approach == ParameterApproach.INLINE:
            # inline mode always uses ParamStyle.PYFORMAT
            # inline mode doesn't support positional parameters
            return self._inline_roundtrip(params, paramstyle=ParamStyle.PYFORMAT)
        elif approach == ParameterApproach.NATIVE:
            # native mode can use either ParamStyle.NAMED or ParamStyle.PYFORMAT
            # native mode can use either ParameterStructure.NAMED or ParameterStructure.POSITIONAL
            return self._native_roundtrip(
                params, paramstyle=paramstyle, parameter_structure=parameter_structure
            )

    def _quantize(self, input: Union[float, int], place_value=2) -> Decimal:
        return Decimal(str(input)).quantize(Decimal("0." + "0" * place_value))

    def _eq(self, actual, expected: Primitive):
        """This is a helper function to make the test code more readable.

        If primitive is Primitive.DOUBLE than an extra quantize step is performed before
        making the assertion.
        """
        if expected in (Primitive.DOUBLE, Primitive.FLOAT):
            return self._quantize(actual) == self._quantize(expected.value)

        return actual == expected.value

    @pytest.mark.parametrize("primitive", Primitive)
    @pytest.mark.parametrize(
        "approach,paramstyle,parameter_structure", approach_paramstyle_combinations
    )
    def test_primitive_single(
        self,
        approach,
        paramstyle,
        parameter_structure,
        primitive: Primitive,
        inline_table,
    ):
        """When ParameterApproach.INLINE is passed, inferrence will not be used.
        When ParameterApproach.NATIVE is passed, primitive inputs will be inferred.
        """

        if parameter_structure == ParameterStructure.NAMED:
            params = {"p": primitive.value}
        elif parameter_structure == ParameterStructure.POSITIONAL:
            params = [primitive.value]

        result = self._get_one_result(params, approach, paramstyle, parameter_structure)

        assert self._eq(result.col, primitive)

    @pytest.mark.parametrize(
        "parameter_structure", (ParameterStructure.NAMED, ParameterStructure.POSITIONAL)
    )
    @pytest.mark.parametrize(
        "primitive,dbsql_parameter_cls",
        [
            (Primitive.NONE, VoidParameter),
            (Primitive.BOOL, BooleanParameter),
            (Primitive.INT, IntegerParameter),
            (Primitive.BIGINT, BigIntegerParameter),
            (Primitive.STRING, StringParameter),
            (Primitive.DECIMAL, DecimalParameter),
            (Primitive.DATE, DateParameter),
            (Primitive.TIMESTAMP, TimestampParameter),
            (Primitive.DOUBLE, DoubleParameter),
            (Primitive.FLOAT, FloatParameter),
            (Primitive.SMALLINT, SmallIntParameter),
            (PrimitiveExtra.TIMESTAMP_NTZ, TimestampNTZParameter),
            (PrimitiveExtra.TINYINT, TinyIntParameter),
        ],
    )
    def test_dbsqlparameter_single(
        self,
        primitive: Primitive,
        dbsql_parameter_cls: Type[TDbsqlParameter],
        parameter_structure: ParameterStructure,
    ):
        dbsql_param = dbsql_parameter_cls(
            value=primitive.value,  # type: ignore
            name="p" if parameter_structure == ParameterStructure.NAMED else None,
        )

        params = [dbsql_param]
        result = self._get_one_result(
            params, ParameterApproach.NATIVE, ParamStyle.NAMED, parameter_structure
        )
        assert self._eq(result.col, primitive)

    @pytest.mark.parametrize("use_inline_params", (True, False, "silent"))
    def test_use_inline_off_by_default_with_warning(self, use_inline_params, caplog):
        """
        use_inline_params should be False by default.
        If a user explicitly sets use_inline_params, don't warn them about it.
        """

        extra_args = {"use_inline_params": use_inline_params} if use_inline_params else {}

        with self.connection(extra_params=extra_args) as conn:
            with conn.cursor() as cursor:
                with self.patch_server_supports_native_params(supports_native_params=True):
                    cursor.execute("SELECT %(p)s", parameters={"p": 1})
                    if use_inline_params is True:
                        assert (
                            "Consider using native parameters." in caplog.text
                        ), "Log message should be suppressed"
                    elif use_inline_params == "silent":
                        assert (
                            "Consider using native parameters." not in caplog.text
                        ), "Log message should not be supressed"

    def test_positional_native_params_with_defaults(self):
        query = "SELECT ? col"
        with self.cursor() as cursor:
            result = cursor.execute(query, parameters=[1]).fetchone()

        assert result.col == 1

    @pytest.mark.parametrize(
        "params",
        (
            [
                StringParameter(value="foo"),
                StringParameter(value="bar"),
                StringParameter(value="baz"),
            ],
            ["foo", "bar", "baz"],
        ),
    )
    def test_positional_native_multiple(self, params):
        query = "SELECT ? `foo`, ? `bar`, ? `baz`"

        with self.cursor(extra_params={"use_inline_params": False}) as cursor:
            result = cursor.execute(query, params).fetchone()

        expected = [i.value if isinstance(i, DbsqlParameterBase) else i for i in params]
        outcome = [result.foo, result.bar, result.baz]

        assert set(outcome) == set(expected)

    def test_readme_example(self):
        with self.cursor() as cursor:
            result = cursor.execute(
                "SELECT :param `p`, * FROM RANGE(10)", {"param": "foo"}
            ).fetchall()

        assert len(result) == 10
        assert result[0].p == "foo"


class TestInlineParameterSyntax(PySQLPytestTestCase):
    """The inline parameter approach uses pyformat markers"""

    def test_params_as_dict(self):
        query = "SELECT %(foo)s foo, %(bar)s bar, %(baz)s baz"
        params = {"foo": 1, "bar": 2, "baz": 3}

        with self.connection(extra_params={"use_inline_params": True}) as conn:
            with conn.cursor() as cursor:
                result = cursor.execute(query, parameters=params).fetchone()

        assert result.foo == 1
        assert result.bar == 2
        assert result.baz == 3

    def test_params_as_sequence(self):
        """One side-effect of ParamEscaper using Python string interpolation to inline the values
        is that it can work with "ordinal" parameters, but only if a user writes parameter markers
        that are not defined with PEP-249. This test exists to prove that it works in the ideal case.
        """

        # `%s` is not a valid paramstyle per PEP-249
        query = "SELECT %s foo, %s bar, %s baz"
        params = (1, 2, 3)

        with self.connection(extra_params={"use_inline_params": True}) as conn:
            with conn.cursor() as cursor:
                result = cursor.execute(query, parameters=params).fetchone()
                assert result.foo == 1
                assert result.bar == 2
                assert result.baz == 3

    def test_inline_ordinals_can_break_sql(self):
        """With inline mode, ordinal parameters can break the SQL syntax
        because `%` symbols are used to wildcard match within LIKE statements. This test
        just proves that's the case.
        """
        query = "SELECT 'samsonite', %s WHERE 'samsonite' LIKE '%sonite'"
        params = ["luggage"]
        with self.cursor(extra_params={"use_inline_params": True}) as cursor:
            with pytest.raises(TypeError, match="not enough arguments for format string"):
                cursor.execute(query, parameters=params)

    def test_inline_named_dont_break_sql(self):
        """With inline mode, ordinal parameters can break the SQL syntax
        because `%` symbols are used to wildcard match within LIKE statements. This test
        just proves that's the case.
        """
        query = """
        with base as (SELECT 'x(one)sonite' as `col_1`)
        SELECT col_1 FROM base WHERE col_1 LIKE CONCAT(%(one)s, 'onite')
        """
        params = {"one": "%(one)s"}
        with self.cursor(extra_params={"use_inline_params": True}) as cursor:
            result = cursor.execute(query, parameters=params).fetchone()
            print("hello")

    def test_native_ordinals_dont_break_sql(self):
        """This test accompanies test_inline_ordinals_can_break_sql to prove that ordinal
        parameters work in native mode for the exact same query, if we use the right marker `?`
        """
        query = "SELECT 'samsonite', ? WHERE 'samsonite' LIKE '%sonite'"
        params = ["luggage"]
        with self.cursor(extra_params={"use_inline_params": False}) as cursor:
            result = cursor.execute(query, parameters=params).fetchone()

        assert result.samsonite == "samsonite"
        assert result.luggage == "luggage"

    def test_inline_like_wildcard_breaks(self):
        """One flaw with the ParameterEscaper is that it fails if a query contains
        a SQL LIKE wildcard %. This test proves that's the case.
        """
        query = "SELECT 1 `col` WHERE 'foo' LIKE '%'"
        params = {"param": "bar"}
        with self.cursor(extra_params={"use_inline_params": True}) as cursor:
            with pytest.raises(ValueError, match="unsupported format character"):
                result = cursor.execute(query, parameters=params).fetchone()

    def test_native_like_wildcard_works(self):
        """This is a mirror of test_inline_like_wildcard_breaks that proves that LIKE
        wildcards work under the native approach.
        """
        query = "SELECT 1 `col` WHERE 'foo' LIKE '%'"
        params = {"param": "bar"}
        with self.cursor(extra_params={"use_inline_params": False}) as cursor:
            result = cursor.execute(query, parameters=params).fetchone()

        assert result.col == 1
