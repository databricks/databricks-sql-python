import pytest
from datetime import datetime
import json
from uuid import uuid4

try:
    import pyarrow
except ImportError:
    pyarrow = None

from tests.e2e.test_driver import PySQLPytestTestCase
from tests.e2e.common.predicates import pysql_supports_arrow


@pytest.mark.skipif(not pysql_supports_arrow(), reason="Requires arrow support")
class TestVariantTypes(PySQLPytestTestCase):
    """Tests for the proper detection and handling of VARIANT type columns"""

    @pytest.fixture(scope="class")
    def variant_table(self, connection_details):
        """A pytest fixture that creates a test table and cleans up after tests"""
        self.arguments = connection_details.copy()
        table_name = f"pysql_test_variant_types_table_{str(uuid4()).replace('-', '_')}"

        with self.cursor() as cursor:
            try:
                # Create the table with variant columns
                cursor.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        id INTEGER,
                        variant_col VARIANT,
                        regular_string_col STRING
                    )
                    """
                )

                # Insert test records with different variant values
                cursor.execute(
                    f"""
                    INSERT INTO {table_name}
                    VALUES 
                    (1, PARSE_JSON('{"name": "John", "age": 30}'), 'regular string'),
                    (2, PARSE_JSON('[1, 2, 3, 4]'), 'another string')
                    """
                )
                yield table_name
            finally:
                cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

    def test_variant_type_detection(self, variant_table):
        """Test that VARIANT type columns are properly detected in schema"""
        with self.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {variant_table} LIMIT 0")

            # Verify column types in description
            assert (
                cursor.description[0][1] == "int"
            ), "Integer column type not correctly identified"
            assert (
                cursor.description[1][1] == "variant"
            ), "VARIANT column type not correctly identified"
            assert (
                cursor.description[2][1] == "string"
            ), "String column type not correctly identified"

    def test_variant_data_retrieval(self, variant_table):
        """Test that VARIANT data is properly retrieved and can be accessed as JSON"""
        with self.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {variant_table} ORDER BY id")
            rows = cursor.fetchall()

            # First row should have a JSON object
            json_obj = rows[0][1]
            assert isinstance(
                json_obj, str
            ), "VARIANT column should be returned as string"

            parsed = json.loads(json_obj)
            assert parsed.get("name") == "John"
            assert parsed.get("age") == 30

            # Second row should have a JSON array
            json_array = rows[1][1]
            assert isinstance(
                json_array, str
            ), "VARIANT array should be returned as string"

            # Parsing to verify it's valid JSON array
            parsed_array = json.loads(json_array)
            assert isinstance(parsed_array, list)
            assert parsed_array == [1, 2, 3, 4]
