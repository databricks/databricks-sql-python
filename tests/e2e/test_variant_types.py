import pytest
from datetime import datetime
import json
try:
    import pyarrow
except ImportError:
    pyarrow = None

from tests.e2e.test_driver import PySQLPytestTestCase

class TestVariantTypes(PySQLPytestTestCase):
    """Tests for the proper detection and handling of VARIANT type columns"""

    @pytest.fixture(scope="class")
    def variant_table_fixture(self, connection_details):
        self.arguments = connection_details.copy()
        """A pytest fixture that creates a table with variant columns, inserts records, yields, and then drops the table"""

        with self.cursor() as cursor:
            # Check if VARIANT type is supported
            try:
                # delete the table if it exists
                cursor.execute("DROP TABLE IF EXISTS pysql_test_variant_types_table")

                # Create the table with variant columns
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS pysql_test_variant_types_table (
                        id INTEGER,
                        variant_col VARIANT,
                        regular_string_col STRING
                    )
                    """
                )
                
                # Insert test records with different variant values
                cursor.execute(
                    """
                    INSERT INTO pysql_test_variant_types_table
                    VALUES 
                    (1, PARSE_JSON('{"name": "John", "age": 30}'), 'regular string'),
                    (2, PARSE_JSON('[1, 2, 3, 4]'), 'another string')
                    """
                )
                
                variant_supported = True
            except Exception as e:
                # VARIANT type not supported in this environment
                print(f"VARIANT type not supported: {e}")
                variant_supported = False
            
            yield variant_supported
            
            # Clean up if table was created
            if variant_supported:
                cursor.execute("DROP TABLE IF EXISTS pysql_test_variant_types_table")

    def test_variant_type_detection(self, variant_table_fixture):
        """Test that VARIANT type columns are properly detected"""
        if not variant_table_fixture:
            pytest.skip("VARIANT type not supported in this environment")
            
        with self.cursor() as cursor:
            cursor.execute("SELECT * FROM pysql_test_variant_types_table LIMIT 1")
            
            # Check that the column type is properly detected as 'variant'
            assert cursor.description[1][1] == 'variant', "VARIANT column type not correctly identified"
            
            # Regular string column should still be reported as string
            assert cursor.description[2][1] == 'string', "Regular string column type not correctly identified"

    def test_variant_data_retrieval(self, variant_table_fixture):
        """Test that VARIANT data is properly retrieved and can be accessed as JSON"""
        if not variant_table_fixture:
            pytest.skip("VARIANT type not supported in this environment")
            
        with self.cursor() as cursor:
            cursor.execute("SELECT * FROM pysql_test_variant_types_table ORDER BY id")
            rows = cursor.fetchall()
            
            # First row should have a JSON object
            json_obj = rows[0][1]
            assert isinstance(json_obj, str), "VARIANT column should be returned as string"
            
            # Parsing to verify it's valid JSON
            parsed = json.loads(json_obj)
            assert parsed.get('name') == 'John'
            assert parsed.get('age') == 30
            
            # Second row should have a JSON array
            json_array = rows[1][1]
            assert isinstance(json_array, str), "VARIANT array should be returned as string"
            
            # Parsing to verify it's valid JSON array
            parsed_array = json.loads(json_array)
            assert isinstance(parsed_array, list)
            assert parsed_array == [1, 2, 3, 4]