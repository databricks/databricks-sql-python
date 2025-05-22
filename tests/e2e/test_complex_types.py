import pytest
from numpy import ndarray
from typing import Sequence

from tests.e2e.test_driver import PySQLPytestTestCase


class TestComplexTypes(PySQLPytestTestCase):
    @pytest.fixture(scope="class")
    def table_fixture(self, connection_details):
        self.arguments = connection_details.copy()
        """A pytest fixture that creates a table with a complex type, inserts a record, yields, and then drops the table"""

        with self.cursor() as cursor:
            # Create the table
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS pysql_test_complex_types_table (
                    array_col ARRAY<STRING>,
                    map_col MAP<STRING, INTEGER>,
                    struct_col STRUCT<field1: STRING, field2: INTEGER>,
                    array_array_col ARRAY<ARRAY<STRING>>,
                    array_map_col ARRAY<MAP<STRING, INTEGER>>,
                    map_array_col MAP<STRING, ARRAY<STRING>>
                )
                """
            )
            # Insert a record
            cursor.execute(
                """
                INSERT INTO pysql_test_complex_types_table
                VALUES (
                    ARRAY('a', 'b', 'c'),
                    MAP('a', 1, 'b', 2, 'c', 3),
                    NAMED_STRUCT('field1', 'a', 'field2', 1),
                    ARRAY(ARRAY('a','b','c')),
                    ARRAY(MAP('a', 1, 'b', 2, 'c', 3)),
                    MAP('a', ARRAY('a', 'b', 'c'), 'b', ARRAY('d', 'e'))
                )
                """
            )
            yield
            # Clean up the table after the test
            cursor.execute("DROP TABLE IF EXISTS pysql_test_complex_types_table")

    @pytest.mark.parametrize(
        "field,expected_type",
        [
            ("array_col", ndarray),
            ("map_col", list),
            ("struct_col", dict),
            ("array_array_col", ndarray),
            ("array_map_col", ndarray),
            ("map_array_col", list),
        ],
    )
    def test_read_complex_types_as_arrow(self, field, expected_type, table_fixture):
        """Confirms the return types of a complex type field when reading as arrow"""

        with self.cursor() as cursor:
            result = cursor.execute(
                "SELECT * FROM pysql_test_complex_types_table LIMIT 1"
            ).fetchone()

        assert isinstance(result[field], expected_type)

    @pytest.mark.parametrize(
        "field",
        [
            ("array_col"),
            ("map_col"),
            ("struct_col"),
            ("array_array_col"),
            ("array_map_col"),
            ("map_array_col"),
        ],
    )
    def test_read_complex_types_as_string(self, field, table_fixture):
        """Confirms the return type of a complex type that is returned as a string"""
        with self.cursor(
            extra_params={"_use_arrow_native_complex_types": False}
        ) as cursor:
            result = cursor.execute(
                "SELECT * FROM pysql_test_complex_types_table LIMIT 1"
            ).fetchone()

        assert isinstance(result[field], str)
