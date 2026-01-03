import pytest
from numpy import ndarray
from typing import Sequence
from uuid import uuid4

from tests.e2e.test_driver import PySQLPytestTestCase


class TestComplexTypes(PySQLPytestTestCase):
    @pytest.fixture(scope="class")
    def table_fixture(self, connection_details):
        self.arguments = connection_details.copy()
        """A pytest fixture that creates a table with a complex type, inserts a record, yields, and then drops the table"""
         
        table_name = f"pysql_test_complex_types_table_{str(uuid4()).replace('-', '_')}"
        self.table_name = table_name

        with self.cursor() as cursor:
            # Create the table
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    array_col ARRAY<STRING>,
                    map_col MAP<STRING, INTEGER>,
                    struct_col STRUCT<field1: STRING, field2: INTEGER>,
                    array_array_col ARRAY<ARRAY<STRING>>,
                    array_map_col ARRAY<MAP<STRING, INTEGER>>,
                    map_array_col MAP<STRING, ARRAY<STRING>>
                ) USING DELTA
                """
            )
            # Insert a record
            cursor.execute(
                f"""
                INSERT INTO {table_name}
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
            try:
                yield table_name
            finally:
                # Clean up the table after the test
                cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

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
    @pytest.mark.parametrize(
        "backend_params",
        [
            {},
            {"use_sea": True},
        ],
    )
    def test_read_complex_types_as_arrow(
        self, field, expected_type, table_fixture, backend_params
    ):
        """Confirms the return types of a complex type field when reading as arrow"""

        with self.cursor(extra_params=backend_params) as cursor:
            result = cursor.execute(
                f"SELECT * FROM {table_fixture} LIMIT 1"
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
    @pytest.mark.parametrize(
        "backend_params",
        [
            {},
            {"use_sea": True},
        ],
    )
    def test_read_complex_types_as_string(self, field, table_fixture, backend_params):
        """Confirms the return type of a complex type that is returned as a string"""
        extra_params = {**backend_params, "_use_arrow_native_complex_types": False}
        with self.cursor(extra_params=extra_params) as cursor:
            result = cursor.execute(
                f"SELECT * FROM {table_fixture} LIMIT 1"
            ).fetchone()

        assert isinstance(result[field], str)
