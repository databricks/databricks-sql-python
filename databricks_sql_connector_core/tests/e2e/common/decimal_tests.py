from decimal import Decimal

import pytest

try:
    import pyarrow
except ImportError:
    pyarrow = None

from tests.e2e.common.predicates import pysql_supports_arrow

def decimal_and_expected_results():

    if pyarrow is None:
        return []

    return [
        ("100.001 AS DECIMAL(6, 3)", Decimal("100.001"), pyarrow.decimal128(6, 3)),
        ("1000000.0000 AS DECIMAL(11, 4)", Decimal("1000000.0000"), pyarrow.decimal128(11, 4)),
        ("-10.2343 AS DECIMAL(10, 6)", Decimal("-10.234300"), pyarrow.decimal128(10, 6)),
        # TODO(SC-90767): Re-enable this test after we have a way of passing `ansi_mode` = False
        # ("-13872347.2343 AS DECIMAL(10, 10)", None, pyarrow.decimal128(10, 10)),
        ("NULL AS DECIMAL(1, 1)", None, pyarrow.decimal128(1, 1)),
        ("1 AS DECIMAL(1, 0)", Decimal("1"), pyarrow.decimal128(1, 0)),
        ("0.00000 AS DECIMAL(5, 3)", Decimal("0.000"), pyarrow.decimal128(5, 3)),
        ("1e-3 AS DECIMAL(38, 3)", Decimal("0.001"), pyarrow.decimal128(38, 3)),
    ]

def multi_decimals_and_expected_results():

    if pyarrow is None:
        return []

    return [
        (
            ["1 AS DECIMAL(6, 3)", "100.001 AS DECIMAL(6, 3)", "NULL AS DECIMAL(6, 3)"],
            [Decimal("1.00"), Decimal("100.001"), None],
            pyarrow.decimal128(6, 3),
        ),
        (
            ["1 AS DECIMAL(6, 3)", "2 AS DECIMAL(5, 2)"],
            [Decimal("1.000"), Decimal("2.000")],
            pyarrow.decimal128(6, 3),
        ),
    ]

@pytest.mark.skipif(not pysql_supports_arrow(), reason="Skipping because pyarrow is not installed")
class DecimalTestsMixin:
    @pytest.mark.parametrize("decimal, expected_value, expected_type", decimal_and_expected_results())
    def test_decimals(self, decimal, expected_value, expected_type):
        with self.cursor({}) as cursor:
            query = "SELECT CAST ({})".format(decimal)
            cursor.execute(query)
            table = cursor.fetchmany_arrow(1)
            assert table.field(0).type == expected_type
            assert table.to_pydict().popitem()[1][0] == expected_value

    @pytest.mark.parametrize("decimals, expected_values, expected_type", multi_decimals_and_expected_results())
    def test_multi_decimals(self, decimals, expected_values, expected_type):
        with self.cursor({}) as cursor:
            union_str = " UNION ".join(["(SELECT CAST ({}))".format(dec) for dec in decimals])
            query = "SELECT * FROM ({}) ORDER BY 1 NULLS LAST".format(union_str)

            cursor.execute(query)
            table = cursor.fetchall_arrow()
            assert table.field(0).type == expected_type
            assert table.to_pydict().popitem()[1] == expected_values
