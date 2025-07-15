import decimal
import datetime
from collections import namedtuple

TypeFailure = namedtuple(
    "TypeFailure",
    "query,columnType,resultType,resultValue," "actualValue,actualType,description,conf",
)
ResultFailure = namedtuple(
    "ResultFailure",
    "query,columnType,resultType,resultValue," "actualValue,actualType,description,conf",
)
ExecFailure = namedtuple(
    "ExecFailure",
    "query,columnType,resultType,resultValue," "actualValue,actualType,description,conf,error",
)


class SmokeTestMixin:
    def test_smoke_test(self):
        with self.cursor() as cursor:
            cursor.execute("select 0")
            rows = cursor.fetchall()
            assert len(rows) == 1
            assert rows[0][0] == 0


class CoreTestMixin:
    """
    This mixin expects to be mixed with a CursorTest-like class with the following extra attributes:
    validate_row_value_type: bool
    validate_result: bool
    """

    # A list of (subquery, column_type, python_type, expected_result)
    # To be executed as "SELECT {} FROM RANGE(...)" and "SELECT {}"
    range_queries = [
        ("TRUE", "boolean", bool, True),
        ("cast(1 AS TINYINT)", "byte", int, 1),
        ("cast(1000 AS SMALLINT)", "short", int, 1000),
        ("cast(100000 AS INTEGER)", "integer", int, 100000),
        ("cast(10000000000000 AS BIGINT)", "long", int, 10000000000000),
        ("cast(100.001 AS DECIMAL(6, 3))", "decimal", decimal.Decimal, 100.001),
        ("date '2020-02-20'", "date", datetime.date, datetime.date(2020, 2, 20)),
        ("unhex('f000')", "binary", bytes, b"\xf0\x00"),  #  pyodbc internal mismatch
        ("'foo'", "string", str, "foo"),
        # SPARK-32130: 6.x: "4 weeks 2 days" vs 7.x: "30 days"
        # ("interval 30 days", str, str, "interval 4 weeks 2 days"),
        # ("interval 3 days", str, str, "interval 3 days"),
        ("CAST(NULL AS DOUBLE)", "double", type(None), None),
    ]

    # Full queries, only the first column of the first row is checked
    queries = [("NULL UNION (SELECT 1) order by 1", "integer", type(None), None)]

    def run_tests_on_queries(self, default_conf):
        failures = []
        for query, columnType, rowValueType, answer in self.range_queries:
            with self.cursor(default_conf) as cursor:
                failures.extend(
                    self.run_query(cursor, query, columnType, rowValueType, answer, default_conf)
                )
                failures.extend(
                    self.run_range_query(
                        cursor, query, columnType, rowValueType, answer, default_conf
                    )
                )

        for query, columnType, rowValueType, answer in self.queries:
            with self.cursor(default_conf) as cursor:
                failures.extend(
                    self.run_query(cursor, query, columnType, rowValueType, answer, default_conf)
                )

        if failures:
            self.fail(
                "Failed testing result set with Arrow. "
                "Failed queries: {}".format("\n\n".join([str(f) for f in failures]))
            )

    def run_query(self, cursor, query, columnType, rowValueType, answer, conf):
        full_query = "SELECT {}".format(query)
        expected_column_types = self.expected_column_types(columnType)
        try:
            cursor.execute(full_query)
            (result,) = cursor.fetchone()
            if not all(cursor.description[0][1] == type for type in expected_column_types):
                return [
                    TypeFailure(
                        full_query,
                        expected_column_types,
                        rowValueType,
                        answer,
                        result,
                        type(result),
                        cursor.description,
                        conf,
                    )
                ]
            if self.validate_row_value_type and type(result) is not rowValueType:
                return [
                    TypeFailure(
                        full_query,
                        expected_column_types,
                        rowValueType,
                        answer,
                        result,
                        type(result),
                        cursor.description,
                        conf,
                    )
                ]
            if self.validate_result and str(answer) != str(result):
                return [
                    ResultFailure(
                        full_query,
                        query,
                        expected_column_types,
                        rowValueType,
                        answer,
                        result,
                        type(result),
                        cursor.description,
                        conf,
                    )
                ]
            return []
        except Exception as e:
            return [
                ExecFailure(
                    full_query,
                    columnType,
                    rowValueType,
                    None,
                    None,
                    None,
                    cursor.description,
                    conf,
                    e,
                )
            ]

    def run_range_query(self, cursor, query, columnType, rowValueType, expected, conf):
        full_query = "SELECT {}, id FROM RANGE({})".format(query, 5000)
        expected_column_types = self.expected_column_types(columnType)
        try:
            cursor.execute(full_query)
            while True:
                rows = cursor.fetchmany(1000)
                if len(rows) <= 0:
                    break
                for index, (result, id) in enumerate(rows):
                    if not all(cursor.description[0][1] == type for type in expected_column_types):
                        return [
                            TypeFailure(
                                full_query,
                                expected_column_types,
                                rowValueType,
                                expected,
                                result,
                                type(result),
                                cursor.description,
                                conf,
                            )
                        ]
                    if self.validate_row_value_type and type(result) is not rowValueType:
                        return [
                            TypeFailure(
                                full_query,
                                expected_column_types,
                                rowValueType,
                                expected,
                                result,
                                type(result),
                                cursor.description,
                                conf,
                            )
                        ]
                    if self.validate_result and str(expected) != str(result):
                        return [
                            ResultFailure(
                                full_query,
                                expected_column_types,
                                rowValueType,
                                expected,
                                result,
                                type(result),
                                cursor.description,
                                conf,
                            )
                        ]
            return []
        except Exception as e:
            return [
                ExecFailure(
                    full_query,
                    columnType,
                    rowValueType,
                    None,
                    None,
                    None,
                    cursor.description,
                    conf,
                    e,
                )
            ]
