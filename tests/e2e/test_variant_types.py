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

    @pytest.mark.parametrize(
        "test_id, json_value, expected_result, description",
        [
            # Primitive types
            (1, '"string value"', "string value", "String value"),
            (2, '42', 42, "Integer value"),
            (3, '3.14159', 3.14159, "Float value"),
            (4, 'true', True, "Boolean true"),
            (5, 'false', False, "Boolean false"),
            (6, 'null', None, "Null value"),
            
            # Complex types
            (7, '["a", "b", "c"]', ["a", "b", "c"], "String array"),
            (8, '[1, 2, 3]', [1, 2, 3], "Integer array"),
            (9, '{"key1": "value1", "key2": "value2"}', {"key1": "value1", "key2": "value2"}, "Simple object"),
            
            # Nested structures
            (10, '{"nested": {"a": 1, "b": 2}}', {"nested": {"a": 1, "b": 2}}, "Nested object"),
            (11, '[["nested"], ["arrays"]]', [["nested"], ["arrays"]], "Nested arrays"),
            (12, '{"array": [1, 2, 3], "object": {"a": "b"}}', {"array": [1, 2, 3], "object": {"a": "b"}}, "Mixed nested structures"),
            
            # Mixed types
            (13, '[1, "string", true, null, {"key": "value"}]', [1, "string", True, None, {"key": "value"}], "Array with mixed types"),
            
            # Special cases
            (14, '{}', {}, "Empty object"),
            (15, '[]', [], "Empty array"),
            (16, '{"unicode": "✓ öäü 😀"}', {"unicode": "✓ öäü 😀"}, "Unicode characters"),
            (17, '{"large_number": 9223372036854775807}', {"large_number": 9223372036854775807}, "Large integer"),
            
            # Deeply nested structure
            (18, '{"level1": {"level2": {"level3": {"level4": {"level5": "deep value"}}}}}',
             {"level1": {"level2": {"level3": {"level4": {"level5": "deep value"}}}}}, "Deeply nested structure"),
             
            # Date and time types
            (19, '"2023-01-01"', "2023-01-01", "Date as string (ISO format)"),
            (20, '"12:34:56"', "12:34:56", "Time as string (ISO format)"),
            (21, '"2023-01-01T12:34:56"', "2023-01-01T12:34:56", "Datetime as string (ISO format)"),
            (22, '"2023-01-01T12:34:56Z"', "2023-01-01T12:34:56Z", "Datetime with Z timezone (UTC)"),
            (23, '"2023-01-01T12:34:56+02:00"', "2023-01-01T12:34:56+02:00", "Datetime with timezone offset"),
            (24, '{"date": "2023-01-01", "time": "12:34:56"}', {"date": "2023-01-01", "time": "12:34:56"}, "Object with date and time fields"),
            (25, '["2023-01-01", "2023-02-02", "2023-03-03"]', ["2023-01-01", "2023-02-02", "2023-03-03"], "Array of dates"),
            (26, '{"events": [{"timestamp": "2023-01-01T12:34:56Z", "name": "event1"}, {"timestamp": "2023-02-02T12:34:56Z", "name": "event2"}]}',
                 {"events": [{"timestamp": "2023-01-01T12:34:56Z", "name": "event1"}, {"timestamp": "2023-02-02T12:34:56Z", "name": "event2"}]},
                 "Complex object with timestamps"),
        ]
    )
    def test_variant_data_types(self, test_id, json_value, expected_result, description):
        """Test that different data types can be stored and retrieved from VARIANT columns"""
        # Use a unique table name for each test case to avoid conflicts in parallel execution
        table_name = f"pysql_test_variant_type_{test_id}"
        
        with self.cursor() as cursor:
            try:
                # Drop the table if it exists
                cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                
                # Create a new table with a variant column
                cursor.execute(f"CREATE TABLE {table_name} (id INTEGER, variant_col VARIANT)")
                
                # Insert the test value
                cursor.execute(f"INSERT INTO {table_name} VALUES (1, PARSE_JSON('{json_value}'))")
                
                # Query the data
                cursor.execute(f"SELECT variant_col FROM {table_name}")
                result = cursor.fetchone()
                
                # Parse the JSON result
                parsed_json = json.loads(result[0])
                
                # Verify the result matches the expected value
                assert parsed_json == expected_result, f"Failed for test case {description}"
                
            finally:
                # Clean up
                cursor.execute(f"DROP TABLE IF EXISTS {table_name}")