#!/usr/bin/env python3
"""
Python Connector Comparator

This script compares the results between the Thrift backend and the SEA backend
of the Databricks SQL Python connector. It executes the same queries against both
backends and compares the results to ensure they match.

Environment variables required:
- DATABRICKS_SERVER_HOSTNAME: The hostname of the Databricks server
- DATABRICKS_HTTP_PATH: The HTTP path of the Databricks server
- DATABRICKS_TOKEN: The token to use for authentication
- DATABRICKS_CATALOG: (Optional) The catalog to use
"""

import os
import sys
import logging
import time
from typing import Any, Dict, List, Optional, Tuple, Union, Callable
import json
import pandas as pd
import pyarrow as pa
from databricks.sql.client import Connection, Cursor
from databricks.sql.types import Row

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Constants
DEFAULT_CATALOG = "main"
DEFAULT_SCHEMA = "default"
DEFAULT_ARRAY_SIZE = 1000
DEFAULT_BUFFER_SIZE = 10485760  # 10MB


class ComparisonResult:
    """Class to store and report comparison results."""

    def __init__(self, test_name: str, query: str, args: List[Any] = None):
        self.test_name = test_name
        self.query = query
        self.args = args or []
        self.differences = []
        self.success = True
        self.thrift_time = 0.0
        self.sea_time = 0.0
        self.thrift_error = None
        self.sea_error = None

    def add_difference(
        self, message: str, thrift_value: Any = None, sea_value: Any = None
    ):
        """Add a difference to the result."""
        self.differences.append(
            {"message": message, "thrift_value": thrift_value, "sea_value": sea_value}
        )
        self.success = False

    def __str__(self) -> str:
        """String representation of the comparison result."""
        result = f"Test: {self.test_name}\n"
        result += f"Query: {self.query}\n"

        if self.args:
            result += f"Args: {self.args}\n"

        result += f"Success: {self.success}\n"
        result += (
            f"Thrift time: {self.thrift_time:.4f}s, SEA time: {self.sea_time:.4f}s\n"
        )

        if self.thrift_error:
            result += f"Thrift error: {self.thrift_error}\n"

        if self.sea_error:
            result += f"SEA error: {self.sea_error}\n"

        if not self.success:
            result += "Differences:\n"
            for diff in self.differences:
                result += f"  - {diff['message']}\n"
                if diff.get("thrift_value") is not None:
                    result += f"    Thrift: {diff['thrift_value']}\n"
                if diff.get("sea_value") is not None:
                    result += f"    SEA: {diff['sea_value']}\n"

        return result


class PythonConnectorComparator:
    """
    Compares the Thrift and SEA backends of the Databricks SQL Python connector.
    """

    def __init__(
        self,
        server_hostname: str,
        http_path: str,
        access_token: str,
        catalog: str = DEFAULT_CATALOG,
        schema: str = DEFAULT_SCHEMA,
        array_size: int = DEFAULT_ARRAY_SIZE,
        buffer_size_bytes: int = DEFAULT_BUFFER_SIZE,
        report_file: str = "python-connector-comparison-report.md",
    ):
        """
        Initialize the comparator with connection parameters.

        Args:
            server_hostname: Databricks server hostname
            http_path: HTTP path for the SQL warehouse
            access_token: Access token for authentication
            catalog: Catalog name to use
            schema: Schema name to use
            array_size: Array size for result fetching
            buffer_size_bytes: Buffer size for result fetching
            report_file: Path to the report file
        """
        self.server_hostname = server_hostname
        self.http_path = http_path
        self.access_token = access_token
        self.catalog = catalog
        self.schema = schema
        self.array_size = array_size
        self.buffer_size_bytes = buffer_size_bytes
        self.report_file = report_file

        self.thrift_connection = None
        self.sea_connection = None
        self.results = []

    def setup_connections(self):
        """Set up connections to both backends."""
        logger.info("Setting up connections to Thrift and SEA backends")

        # Create Thrift connection
        self.thrift_connection = Connection(
            server_hostname=self.server_hostname,
            http_path=self.http_path,
            access_token=self.access_token,
            catalog=self.catalog,
            schema=self.schema,
            use_sea=False,  # Explicitly use Thrift backend
            user_agent_entry="Python-Connector-Comparator",
        )

        # Create SEA connection
        self.sea_connection = Connection(
            server_hostname=self.server_hostname,
            http_path=self.http_path,
            access_token=self.access_token,
            catalog=self.catalog,
            schema=self.schema,
            use_sea=True,  # Explicitly use SEA backend
            user_agent_entry="Python-Connector-Comparator",
        )

        logger.info("Connections established successfully")

    def close_connections(self):
        """Close connections to both backends."""
        logger.info("Closing connections")

        if self.thrift_connection:
            self.thrift_connection.close()

        if self.sea_connection:
            self.sea_connection.close()

        logger.info("Connections closed successfully")

    def compare_cursor_description(
        self, thrift_cursor: Cursor, sea_cursor: Cursor, result: ComparisonResult
    ):
        """
        Compare cursor descriptions between Thrift and SEA backends.

        Args:
            thrift_cursor: Cursor from Thrift backend
            sea_cursor: Cursor from SEA backend
            result: ComparisonResult to update with findings
        """
        thrift_desc = thrift_cursor.description
        sea_desc = sea_cursor.description

        if thrift_desc is None and sea_desc is None:
            return

        if thrift_desc is None:
            result.add_difference(
                "Thrift description is None but SEA description is not"
            )
            return

        if sea_desc is None:
            result.add_difference(
                "SEA description is None but Thrift description is not"
            )
            return

        if len(thrift_desc) != len(sea_desc):
            result.add_difference(
                f"Description length mismatch: Thrift has {len(thrift_desc)} columns, SEA has {len(sea_desc)}",
                thrift_desc,
                sea_desc,
            )
            return

        for i, (thrift_col, sea_col) in enumerate(zip(thrift_desc, sea_desc)):
            # Compare each element of the description tuple
            for j, (thrift_val, sea_val) in enumerate(zip(thrift_col, sea_col)):
                if thrift_val != sea_val:
                    element_names = [
                        "name",
                        "type_code",
                        "display_size",
                        "internal_size",
                        "precision",
                        "scale",
                        "null_ok",
                    ]
                    element_name = (
                        element_names[j] if j < len(element_names) else f"element_{j}"
                    )

                    result.add_difference(
                        f"Column {i} ({thrift_col[0]}) {element_name} mismatch",
                        thrift_val,
                        sea_val,
                    )

    def _safe_compare(self, val1, val2):
        """
        Safely compare two values, handling lists, dicts, and complex types.
        
        Returns True if values are equal, False otherwise.
        """
        try:
            # Handle None values
            if val1 is None and val2 is None:
                return True
            if val1 is None or val2 is None:
                return False
            
            # For lists, tuples, and other sequences (but not strings)
            if isinstance(val1, (list, tuple)) and isinstance(val2, (list, tuple)):
                if len(val1) != len(val2):
                    return False
                return all(self._safe_compare(v1, v2) for v1, v2 in zip(val1, val2))
            
            # For dictionaries
            if isinstance(val1, dict) and isinstance(val2, dict):
                if set(val1.keys()) != set(val2.keys()):
                    return False
                return all(self._safe_compare(val1[k], val2[k]) for k in val1.keys())
            
            # For Row objects (which are tuples with special properties)
            if hasattr(val1, 'asDict') and hasattr(val2, 'asDict'):
                return self._safe_compare(val1.asDict(recursive=True), val2.asDict(recursive=True))
            
            # Default comparison
            return val1 == val2
        except (ValueError, TypeError) as e:
            # If comparison fails (e.g., numpy arrays), convert to string
            return str(val1) == str(val2)

    def compare_rows(
        self, thrift_rows: List[Row], sea_rows: List[Row], result: ComparisonResult
    ):
        """
        Compare rows returned by both backends using the asDict method.

        Args:
            thrift_rows: Rows from Thrift backend
            sea_rows: Rows from SEA backend
            result: ComparisonResult to update with findings
        """
        if len(thrift_rows) != len(sea_rows):
            result.add_difference(
                f"Row count mismatch: Thrift returned {len(thrift_rows)}, SEA returned {len(sea_rows)}"
            )
            # Continue comparison with the smaller set
            min_rows = min(len(thrift_rows), len(sea_rows))
            thrift_rows = thrift_rows[:min_rows]
            sea_rows = sea_rows[:min_rows]

        # Track fields that are consistently missing across all rows
        fields_missing_in_thrift = set()
        fields_missing_in_sea = set()
        field_value_mismatches = {}  # Track per-row mismatches

        for i, (thrift_row, sea_row) in enumerate(zip(thrift_rows, sea_rows)):
            # Convert rows to dictionaries for comparison
            try:
                thrift_dict = thrift_row.asDict(recursive=True)
                sea_dict = sea_row.asDict(recursive=True)

                # Check if dictionaries are different by comparing all fields
                all_fields = set(thrift_dict.keys()) | set(sea_dict.keys())
                dicts_differ = False
                
                for field in all_fields:
                    if field not in thrift_dict or field not in sea_dict:
                        dicts_differ = True
                        break
                    elif not self._safe_compare(thrift_dict.get(field), sea_dict.get(field)):
                        dicts_differ = True
                        break
                
                if dicts_differ:

                    for field in all_fields:
                        thrift_value = thrift_dict.get(field)
                        sea_value = sea_dict.get(field)

                        if field not in thrift_dict:
                            fields_missing_in_thrift.add(field)
                        elif field not in sea_dict:
                            fields_missing_in_sea.add(field)
                        elif not self._safe_compare(thrift_value, sea_value):
                            if field not in field_value_mismatches:
                                field_value_mismatches[field] = []
                            field_value_mismatches[field].append(
                                (i, thrift_value, sea_value)
                            )
            except (AttributeError, TypeError) as e:
                # If asDict fails, fall back to direct comparison
                if thrift_row != sea_row:
                    result.add_difference(
                        f"Row {i} mismatch (asDict failed: {str(e)})",
                        thrift_row,
                        sea_row,
                    )

        # Report consistently missing fields once
        if fields_missing_in_thrift:
            for field in fields_missing_in_thrift:
                result.add_difference(f"Field '{field}' missing in all Thrift rows")

        if fields_missing_in_sea:
            for field in fields_missing_in_sea:
                result.add_difference(f"Field '{field}' missing in all SEA rows")

        # Report value mismatches
        for field, mismatches in field_value_mismatches.items():
            # If all rows have the same mismatch pattern, report it once
            if len(mismatches) == len(thrift_rows):
                # Check if all values are the same
                thrift_values = [m[1] for m in mismatches]
                sea_values = [m[2] for m in mismatches]

                if all(self._safe_compare(v, thrift_values[0]) for v in thrift_values) and all(
                    self._safe_compare(v, sea_values[0]) for v in sea_values
                ):
                    result.add_difference(
                        f"Field '{field}' value mismatch in all rows",
                        thrift_values[0],
                        sea_values[0],
                    )
                else:
                    # Values differ across rows, report first few examples
                    examples = mismatches[:3]  # Limit to first 3 examples
                    for row_idx, thrift_val, sea_val in examples:
                        result.add_difference(
                            f"Row {row_idx}, field '{field}' value mismatch",
                            thrift_val,
                            sea_val,
                        )
                    if len(mismatches) > 3:
                        result.add_difference(
                            f"... and {len(mismatches) - 3} more rows with '{field}' mismatches"
                        )
            else:
                # Not all rows have this mismatch, report individually (up to 3)
                examples = mismatches[:3]
                for row_idx, thrift_val, sea_val in examples:
                    result.add_difference(
                        f"Row {row_idx}, field '{field}' value mismatch",
                        thrift_val,
                        sea_val,
                    )
                if len(mismatches) > 3:
                    result.add_difference(
                        f"... and {len(mismatches) - 3} more rows with '{field}' mismatches"
                    )

    def compare_arrow_tables(
        self, thrift_table: pa.Table, sea_table: pa.Table, result: ComparisonResult
    ):
        """
        Compare Arrow tables returned by both backends.

        Args:
            thrift_table: Arrow table from Thrift backend
            sea_table: Arrow table from SEA backend
            result: ComparisonResult to update with findings
        """
        # Compare schema
        thrift_schema = thrift_table.schema
        sea_schema = sea_table.schema

        if len(thrift_schema) != len(sea_schema):
            result.add_difference(
                f"Arrow schema field count mismatch: Thrift has {len(thrift_schema)}, SEA has {len(sea_schema)}",
                thrift_schema,
                sea_schema,
            )
        else:
            for i, (thrift_field, sea_field) in enumerate(
                zip(thrift_schema, sea_schema)
            ):
                if thrift_field.name != sea_field.name:
                    result.add_difference(
                        f"Arrow schema field {i} name mismatch",
                        thrift_field.name,
                        sea_field.name,
                    )

                if str(thrift_field.type) != str(sea_field.type):
                    result.add_difference(
                        f"Arrow schema field {i} ({thrift_field.name}) type mismatch",
                        str(thrift_field.type),
                        str(sea_field.type),
                    )

        # Compare row count
        if thrift_table.num_rows != sea_table.num_rows:
            result.add_difference(
                f"Arrow table row count mismatch: Thrift has {thrift_table.num_rows}, SEA has {sea_table.num_rows}"
            )

        # Convert to pandas for easier comparison
        try:
            thrift_df = thrift_table.to_pandas()
            sea_df = sea_table.to_pandas()

            # Compare dataframes
            if not thrift_df.equals(sea_df):
                # Find differing rows
                if (
                    thrift_df.shape[0] == sea_df.shape[0]
                    and thrift_df.shape[1] == sea_df.shape[1]
                ):
                    # Same dimensions, compare cell by cell
                    for col in thrift_df.columns:
                        if col in sea_df.columns:
                            mask = thrift_df[col] != sea_df[col]
                            if mask.any():
                                diff_indices = mask[mask].index.tolist()
                                if (
                                    len(diff_indices) > 3
                                ):  # Limit to first 3 differences
                                    diff_indices = diff_indices[:3]

                                for idx in diff_indices:
                                    result.add_difference(
                                        f"Arrow data mismatch at row {idx}, column '{col}'",
                                        thrift_df.loc[idx, col],
                                        sea_df.loc[idx, col],
                                    )
        except Exception as e:
            result.add_difference(f"Error comparing Arrow tables: {str(e)}")

    def execute_and_compare(
        self,
        query: str,
        test_name: str,
        fetch_method: str = "fetchall",
        args: List[Any] = None,
        fetch_size: int = None,
    ) -> ComparisonResult:
        """
        Execute a query on both backends and compare the results.

        Args:
            query: SQL query to execute
            test_name: Name of the test for reporting
            fetch_method: Method to use for fetching results (fetchall, fetchmany, fetchone,
                          fetchall_arrow, fetchmany_arrow)
            args: Arguments to pass to the query
            fetch_size: Size to use for fetchmany/fetchmany_arrow

        Returns:
            ComparisonResult with the comparison details
        """
        result = ComparisonResult(test_name, query, args)

        # Create cursors
        thrift_cursor = self.thrift_connection.cursor(
            arraysize=self.array_size, buffer_size_bytes=self.buffer_size_bytes
        )

        sea_cursor = self.sea_connection.cursor(
            arraysize=self.array_size, buffer_size_bytes=self.buffer_size_bytes
        )

        try:
            # Execute query on Thrift backend
            start_time = time.time()
            thrift_cursor.execute(query, args)
            result.thrift_time = time.time() - start_time

            # Execute query on SEA backend
            start_time = time.time()
            sea_cursor.execute(query, args)
            result.sea_time = time.time() - start_time

            # Compare cursor descriptions
            self.compare_cursor_description(thrift_cursor, sea_cursor, result)

            # Fetch and compare results based on fetch_method
            if fetch_method == "fetchall":
                thrift_rows = thrift_cursor.fetchall()
                sea_rows = sea_cursor.fetchall()
                self.compare_rows(thrift_rows, sea_rows, result)

            elif fetch_method == "fetchmany":
                size = fetch_size or self.array_size
                thrift_rows = thrift_cursor.fetchmany(size)
                sea_rows = sea_cursor.fetchmany(size)
                self.compare_rows(thrift_rows, sea_rows, result)

            elif fetch_method == "fetchone":
                thrift_row = thrift_cursor.fetchone()
                sea_row = sea_cursor.fetchone()
                if thrift_row is None and sea_row is None:
                    pass  # Both returned None, which is fine
                elif thrift_row is None:
                    result.add_difference(
                        "Thrift returned None but SEA returned a row", None, sea_row
                    )
                elif sea_row is None:
                    result.add_difference(
                        "SEA returned None but Thrift returned a row", thrift_row, None
                    )
                else:
                    self.compare_rows([thrift_row], [sea_row], result)

            elif fetch_method == "fetchall_arrow":
                thrift_table = thrift_cursor.fetchall_arrow()
                sea_table = sea_cursor.fetchall_arrow()
                self.compare_arrow_tables(thrift_table, sea_table, result)

            elif fetch_method == "fetchmany_arrow":
                size = fetch_size or self.array_size
                thrift_table = thrift_cursor.fetchmany_arrow(size)
                sea_table = sea_cursor.fetchmany_arrow(size)
                self.compare_arrow_tables(thrift_table, sea_table, result)

            else:
                result.add_difference(f"Unknown fetch method: {fetch_method}")

        except Exception as e:
            logger.exception(f"Error in test {test_name}")
            result.success = False
            result.add_difference(f"Exception: {str(e)}")

        finally:
            # Close cursors
            thrift_cursor.close()
            sea_cursor.close()

        return result

    def compare_metadata_results(
        self, thrift_rows: List[Row], sea_rows: List[Row], result: ComparisonResult
    ):
        """
        Compare metadata results (like catalogs, schemas, tables) between backends.
        These have specific columns that we want to validate.
        """
        # First do regular row comparison
        self.compare_rows(thrift_rows, sea_rows, result)

        # Additional validation could be added here for specific metadata formats

    def test_metadata_methods(self):
        """Test catalog, schema, table, and column metadata methods."""
        logger.info("Testing metadata methods")

        # Test catalogs()
        try:
            thrift_cursor = self.thrift_connection.cursor()
            sea_cursor = self.sea_connection.cursor()

            result = ComparisonResult("catalogs()", "catalogs()")

            start_time = time.time()
            thrift_cursor.catalogs()
            result.thrift_time = time.time() - start_time
            thrift_catalogs = thrift_cursor.fetchall()

            start_time = time.time()
            sea_cursor.catalogs()
            result.sea_time = time.time() - start_time
            sea_catalogs = sea_cursor.fetchall()

            self.compare_cursor_description(thrift_cursor, sea_cursor, result)
            self.compare_metadata_results(thrift_catalogs, sea_catalogs, result)

            self.results.append(result)

            thrift_cursor.close()
            sea_cursor.close()
        except Exception as e:
            logger.exception("Error testing catalogs()")
            result = ComparisonResult("catalogs()", "catalogs()")
            result.success = False
            result.add_difference(f"Exception: {str(e)}")
            self.results.append(result)

        # Test schemas() with various parameters
        test_cases = [
            ("schemas() - no params", None, None),
            ("schemas() - with catalog", self.catalog, None),
            ("schemas() - with pattern", self.catalog, "def%"),
        ]

        for test_name, catalog_arg, schema_arg in test_cases:
            try:
                thrift_cursor = self.thrift_connection.cursor()
                sea_cursor = self.sea_connection.cursor()

                result = ComparisonResult(
                    test_name, f"schemas({catalog_arg}, {schema_arg})"
                )

                start_time = time.time()
                thrift_cursor.schemas(catalog_arg, schema_arg)
                result.thrift_time = time.time() - start_time
                thrift_schemas = thrift_cursor.fetchall()

                start_time = time.time()
                sea_cursor.schemas(catalog_arg, schema_arg)
                result.sea_time = time.time() - start_time
                sea_schemas = sea_cursor.fetchall()

                self.compare_cursor_description(thrift_cursor, sea_cursor, result)
                self.compare_metadata_results(thrift_schemas, sea_schemas, result)

                self.results.append(result)

                thrift_cursor.close()
                sea_cursor.close()
            except Exception as e:
                logger.exception(f"Error testing {test_name}")
                result = ComparisonResult(
                    test_name, f"schemas({catalog_arg}, {schema_arg})"
                )
                result.success = False
                result.add_difference(f"Exception: {str(e)}")
                self.results.append(result)

        # Test tables() with various parameters
        table_test_cases = [
            ("tables() - no params", None, None, None, None),
            ("tables() - with catalog", self.catalog, None, None, None),
            ("tables() - with schema", self.catalog, self.schema, None, None),
            (
                "tables() - with table pattern",
                self.catalog,
                self.schema,
                "%sales",
                None,
            ),
            (
                "tables() - with table types",
                self.catalog,
                self.schema,
                None,
                ["TABLE", "VIEW"],
            ),
        ]

        for test_name, cat, sch, tab, types in table_test_cases:
            try:
                thrift_cursor = self.thrift_connection.cursor()
                sea_cursor = self.sea_connection.cursor()

                result = ComparisonResult(
                    test_name, f"tables({cat}, {sch}, {tab}, {types})"
                )

                start_time = time.time()
                thrift_cursor.tables(cat, sch, tab, types)
                result.thrift_time = time.time() - start_time
                thrift_tables = thrift_cursor.fetchall()

                start_time = time.time()
                sea_cursor.tables(cat, sch, tab, types)
                result.sea_time = time.time() - start_time
                sea_tables = sea_cursor.fetchall()

                self.compare_cursor_description(thrift_cursor, sea_cursor, result)
                self.compare_metadata_results(thrift_tables, sea_tables, result)

                self.results.append(result)

                thrift_cursor.close()
                sea_cursor.close()
            except Exception as e:
                logger.exception(f"Error testing {test_name}")
                result = ComparisonResult(
                    test_name, f"tables({cat}, {sch}, {tab}, {types})"
                )
                result.success = False
                result.add_difference(f"Exception: {str(e)}")
                self.results.append(result)

        # Test columns() - let's use a known table
        column_test_cases = [
            (
                "columns() - specific table",
                self.catalog,
                "tpcds_sf100_delta",
                "catalog_sales",
                None,
            ),
            (
                "columns() - with column pattern",
                self.catalog,
                "tpcds_sf100_delta",
                "catalog_sales",
                "cs_%",
            ),
        ]

        for test_name, cat, sch, tab, col in column_test_cases:
            try:
                thrift_cursor = self.thrift_connection.cursor()
                sea_cursor = self.sea_connection.cursor()

                result = ComparisonResult(
                    test_name, f"columns({cat}, {sch}, {tab}, {col})"
                )

                start_time = time.time()
                thrift_cursor.columns(cat, sch, tab, col)
                result.thrift_time = time.time() - start_time
                thrift_columns = thrift_cursor.fetchall()

                start_time = time.time()
                sea_cursor.columns(cat, sch, tab, col)
                result.sea_time = time.time() - start_time
                sea_columns = sea_cursor.fetchall()

                self.compare_cursor_description(thrift_cursor, sea_cursor, result)
                self.compare_metadata_results(thrift_columns, sea_columns, result)

                self.results.append(result)

                thrift_cursor.close()
                sea_cursor.close()
            except Exception as e:
                logger.exception(f"Error testing {test_name}")
                result = ComparisonResult(
                    test_name, f"columns({cat}, {sch}, {tab}, {col})"
                )
                result.success = False
                result.add_difference(f"Exception: {str(e)}")
                self.results.append(result)

    def test_fetch_variations(self):
        """Test various sequences of fetch operations."""
        logger.info("Testing fetch operation variations")

        # Test: fetchone x3, then fetchmany, then fetchone
        query = "SELECT * FROM main.tpcds_sf100_delta.catalog_sales LIMIT 10"

        try:
            thrift_cursor = self.thrift_connection.cursor()
            sea_cursor = self.sea_connection.cursor()

            result = ComparisonResult("Fetch variation - mixed operations", query)

            # Execute on both backends
            start_time = time.time()
            thrift_cursor.execute(query)
            thrift_exec_time = time.time() - start_time

            start_time = time.time()
            sea_cursor.execute(query)
            sea_exec_time = time.time() - start_time

            # Compare descriptions first
            self.compare_cursor_description(thrift_cursor, sea_cursor, result)

            # Fetch sequence: 3x fetchone
            thrift_rows = []
            sea_rows = []

            for i in range(3):
                thrift_row = thrift_cursor.fetchone()
                sea_row = sea_cursor.fetchone()

                if thrift_row is None and sea_row is None:
                    break
                elif thrift_row is None:
                    result.add_difference(
                        f"fetchone {i+1}: Thrift returned None but SEA returned a row"
                    )
                    break
                elif sea_row is None:
                    result.add_difference(
                        f"fetchone {i+1}: SEA returned None but Thrift returned a row"
                    )
                    break
                else:
                    thrift_rows.append(thrift_row)
                    sea_rows.append(sea_row)

            # Then fetchmany(3)
            thrift_many = thrift_cursor.fetchmany(3)
            sea_many = sea_cursor.fetchmany(3)
            thrift_rows.extend(thrift_many)
            sea_rows.extend(sea_many)

            # Then another fetchone
            thrift_last = thrift_cursor.fetchone()
            sea_last = sea_cursor.fetchone()

            if thrift_last is not None:
                thrift_rows.append(thrift_last)
            if sea_last is not None:
                sea_rows.append(sea_last)

            # Compare all fetched rows
            self.compare_rows(thrift_rows, sea_rows, result)

            result.thrift_time = thrift_exec_time
            result.sea_time = sea_exec_time

            self.results.append(result)

            thrift_cursor.close()
            sea_cursor.close()
        except Exception as e:
            logger.exception("Error in fetch variation test")
            result.success = False
            result.add_difference(f"Exception: {str(e)}")
            self.results.append(result)

        # Test: fetchall after partial fetch
        try:
            thrift_cursor = self.thrift_connection.cursor()
            sea_cursor = self.sea_connection.cursor()

            result = ComparisonResult(
                "Fetch variation - fetchmany then fetchall", query
            )

            # Execute on both backends
            thrift_cursor.execute(query)
            sea_cursor.execute(query)

            # Fetch first 2 rows
            thrift_first = thrift_cursor.fetchmany(2)
            sea_first = sea_cursor.fetchmany(2)

            # Then fetch remaining
            thrift_rest = thrift_cursor.fetchall()
            sea_rest = sea_cursor.fetchall()

            # Combine results
            thrift_all = thrift_first + thrift_rest
            sea_all = sea_first + sea_rest

            self.compare_rows(thrift_all, sea_all, result)

            self.results.append(result)

            thrift_cursor.close()
            sea_cursor.close()
        except Exception as e:
            logger.exception("Error in fetchmany/fetchall variation test")
            result = ComparisonResult(
                "Fetch variation - fetchmany then fetchall", query
            )
            result.success = False
            result.add_difference(f"Exception: {str(e)}")
            self.results.append(result)

    def test_edge_cases(self):
        """Test edge cases like empty results, NULL values, etc."""
        logger.info("Testing edge cases")

        # Test empty result set
        empty_query = "SELECT * FROM main.tpcds_sf100_delta.catalog_sales WHERE 1=0"
        self.results.append(
            self.execute_and_compare(
                empty_query, "Edge case - empty result set", "fetchall"
            )
        )

        # Test NULL values
        null_query = "SELECT NULL as null_col, 'test' as string_col, 123 as int_col"
        self.results.append(
            self.execute_and_compare(null_query, "Edge case - NULL values", "fetchall")
        )

        # Test various data types
        types_query = """
        SELECT 
            CAST(123 AS TINYINT) as tiny_col,
            CAST(456 AS SMALLINT) as small_col,
            CAST(789 AS INT) as int_col,
            CAST(123456789 AS BIGINT) as big_col,
            CAST(123.45 AS FLOAT) as float_col,
            CAST(678.90 AS DOUBLE) as double_col,
            CAST(123.456 AS DECIMAL(10,3)) as decimal_col,
            'test_string' as string_col,
            TRUE as bool_col,
            CAST('2023-01-01' AS DATE) as date_col,
            CAST('2023-01-01 12:34:56' AS TIMESTAMP) as timestamp_col,
            ARRAY(1,2,3) as array_col,
            STRUCT(1 as a, 'b' as b) as struct_col,
            MAP('key1', 'value1', 'key2', 'value2') as map_col
        """
        self.results.append(
            self.execute_and_compare(
                types_query, "Edge case - various data types", "fetchall"
            )
        )

        # Test large result set (but limited)
        large_query = "SELECT * FROM main.tpcds_sf100_delta.catalog_sales LIMIT 1000"
        self.results.append(
            self.execute_and_compare(
                large_query, "Edge case - larger result set (1000 rows)", "fetchall"
            )
        )

    def test_parameterized_queries(self):
        """Test parameterized queries with both native and inline parameters."""
        logger.info("Testing parameterized queries")

        # Native parameter test (if supported)
        if not self.thrift_connection.use_inline_params:
            # Test with named parameters
            named_query = "SELECT * FROM main.tpcds_sf100_delta.catalog_sales WHERE cs_sold_date_sk = :date_sk LIMIT 5"
            params = {"date_sk": 2451088}

            result = self.execute_and_compare(
                named_query,
                "Parameterized query - named parameters",
                "fetchall",
                args=params,
            )
            self.results.append(result)

            # Test with positional parameters
            pos_query = "SELECT * FROM main.tpcds_sf100_delta.catalog_sales WHERE cs_sold_date_sk = ? AND cs_sold_time_sk = ? LIMIT 5"
            params = [2451088, 48000]

            result = self.execute_and_compare(
                pos_query,
                "Parameterized query - positional parameters",
                "fetchall",
                args=params,
            )
            self.results.append(result)

    def test_executemany(self):
        """Test executemany method."""
        logger.info("Testing executemany")

        # Create a temporary table for testing
        create_table_query = """
        CREATE TABLE IF NOT EXISTS main.default.comparator_test_table (
            id INT,
            value STRING
        ) USING DELTA
        """

        try:
            # Create table on both backends
            thrift_cursor = self.thrift_connection.cursor()
            sea_cursor = self.sea_connection.cursor()

            thrift_cursor.execute(create_table_query)
            sea_cursor.execute(create_table_query)

            # Test executemany with INSERT
            insert_query = (
                "INSERT INTO main.default.comparator_test_table VALUES (?, ?)"
            )
            data = [(1, "one"), (2, "two"), (3, "three")]

            result = ComparisonResult("executemany - INSERT", insert_query, data)

            try:
                start_time = time.time()
                thrift_cursor.executemany(insert_query, data)
                result.thrift_time = time.time() - start_time
            except Exception as e:
                result.thrift_error = str(e)

            try:
                start_time = time.time()
                sea_cursor.executemany(insert_query, data)
                result.sea_time = time.time() - start_time
            except Exception as e:
                result.sea_error = str(e)

            # If both succeeded, compare the inserted data
            if not result.thrift_error and not result.sea_error:
                # Verify data
                verify_query = (
                    "SELECT * FROM main.default.comparator_test_table ORDER BY id"
                )

                thrift_cursor.execute(verify_query)
                thrift_data = thrift_cursor.fetchall()

                sea_cursor.execute(verify_query)
                sea_data = sea_cursor.fetchall()

                self.compare_rows(thrift_data, sea_data, result)
            else:
                result.success = False
                if result.thrift_error and result.sea_error:
                    result.add_difference("Both backends failed with errors")
                elif result.thrift_error:
                    result.add_difference("Only Thrift backend failed")
                else:
                    result.add_difference("Only SEA backend failed")

            self.results.append(result)

            # Cleanup
            cleanup_query = "DROP TABLE IF EXISTS main.default.comparator_test_table"
            try:
                thrift_cursor.execute(cleanup_query)
            except:
                pass
            try:
                sea_cursor.execute(cleanup_query)
            except:
                pass

            thrift_cursor.close()
            sea_cursor.close()
        except Exception as e:
            logger.exception("Error in executemany test")
            result = ComparisonResult("executemany - INSERT", insert_query, data)
            result.success = False
            result.add_difference(f"Exception during test setup: {str(e)}")
            self.results.append(result)

    def test_cursor_description(self):
        """Test cursor.description property in detail."""
        logger.info("Testing cursor.description property")

        # Test description before execute
        try:
            thrift_cursor = self.thrift_connection.cursor()
            sea_cursor = self.sea_connection.cursor()

            result = ComparisonResult(
                "description - before execute", "No query executed"
            )

            if thrift_cursor.description is None and sea_cursor.description is None:
                result.success = True
            elif thrift_cursor.description is None:
                result.add_difference("Thrift description is None but SEA is not")
            elif sea_cursor.description is None:
                result.add_difference("SEA description is None but Thrift is not")
            else:
                result.add_difference("Both should be None before execute")

            self.results.append(result)

            thrift_cursor.close()
            sea_cursor.close()
        except Exception as e:
            logger.exception("Error testing description before execute")
            result = ComparisonResult(
                "description - before execute", "No query executed"
            )
            result.success = False
            result.add_difference(f"Exception: {str(e)}")
            self.results.append(result)

        # Test description after non-SELECT statement
        try:
            thrift_cursor = self.thrift_connection.cursor()
            sea_cursor = self.sea_connection.cursor()

            show_query = "SHOW TABLES IN main.default"
            result = ComparisonResult("description - after SHOW statement", show_query)

            thrift_cursor.execute(show_query)
            sea_cursor.execute(show_query)

            self.compare_cursor_description(thrift_cursor, sea_cursor, result)

            self.results.append(result)

            thrift_cursor.close()
            sea_cursor.close()
        except Exception as e:
            logger.exception("Error testing description after SHOW")
            result = ComparisonResult("description - after SHOW statement", show_query)
            result.success = False
            result.add_difference(f"Exception: {str(e)}")
            self.results.append(result)

    def run_comparison_tests(self):
        """Run a set of comparison tests between Thrift and SEA backends."""
        logger.info("Starting comparison tests")

        # Basic fetch method tests with TPC-DS query
        tpc_query = "SELECT * FROM main.tpcds_sf100_delta.catalog_sales LIMIT 5"

        # Test with fetchall
        self.results.append(
            self.execute_and_compare(tpc_query, "TPC-DS query - fetchall", "fetchall")
        )

        # Test with fetchmany
        self.results.append(
            self.execute_and_compare(
                tpc_query, "TPC-DS query - fetchmany", "fetchmany", fetch_size=2
            )
        )

        # Test with fetchone
        self.results.append(
            self.execute_and_compare(tpc_query, "TPC-DS query - fetchone", "fetchone")
        )

        # Test with fetchall_arrow
        self.results.append(
            self.execute_and_compare(
                tpc_query, "TPC-DS query - fetchall_arrow", "fetchall_arrow"
            )
        )

        # Test with fetchmany_arrow
        self.results.append(
            self.execute_and_compare(
                tpc_query,
                "TPC-DS query - fetchmany_arrow",
                "fetchmany_arrow",
                fetch_size=2,
            )
        )

        # Run additional test suites
        self.test_cursor_description()
        self.test_metadata_methods()
        self.test_fetch_variations()
        self.test_edge_cases()
        self.test_parameterized_queries()
        self.test_executemany()

        logger.info(f"Completed {len(self.results)} comparison tests")

    def generate_report(self):
        """Generate a report of the comparison results in JDBC comparator format."""
        logger.info(f"Generating report to {self.report_file}")

        with open(self.report_file, "w") as f:
            # Header
            f.write("# Python Connector Comparison Report\n\n")
            f.write(f"**Date:** {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"**Server:** {self.server_hostname}\n")
            f.write(f"**HTTP Path:** {self.http_path}\n\n")

            # Summary
            total_tests = len(self.results)
            passed_tests = sum(1 for r in self.results if r.success)
            failed_tests = total_tests - passed_tests

            f.write("## Summary\n\n")
            f.write(f"- **Total Tests:** {total_tests}\n")
            f.write(f"- **Passed:** {passed_tests}\n")
            f.write(f"- **Failed:** {failed_tests}\n\n")

            # Performance summary
            total_thrift_time = sum(r.thrift_time for r in self.results)
            total_sea_time = sum(r.sea_time for r in self.results)
            f.write("## Performance Summary\n\n")
            f.write(f"- **Total Thrift Execution Time:** {total_thrift_time:.4f}s\n")
            f.write(f"- **Total SEA Execution Time:** {total_sea_time:.4f}s\n")

            if total_thrift_time > 0:
                percentage = (total_sea_time / total_thrift_time - 1) * 100
                f.write(
                    f"- **SEA Performance:** {percentage:+.2f}% {'slower' if percentage > 0 else 'faster'} than Thrift\n\n"
                )

            # Test results in JDBC comparator format
            f.write("## Test Results\n\n")

            for result in self.results:
                # Determine query type
                if "metadata" in result.test_name.lower() or any(
                    x in result.query.lower()
                    for x in ["catalogs()", "schemas(", "tables(", "columns("]
                ):
                    query_type = "Cursor Metadata Methods"
                elif "description" in result.test_name.lower():
                    query_type = "Cursor Properties"
                elif "executemany" in result.test_name.lower():
                    query_type = "Batch Operations"
                elif "parameterized" in result.test_name.lower():
                    query_type = "Parameterized Queries"
                else:
                    query_type = "SQL Query"

                f.write(f"**Query Type:** {query_type}\n")
                f.write(f"**Query/Method:** {result.query}\n")

                if result.args and result.args != []:
                    f.write(
                        f"**Method Arguments:** {', '.join(str(arg) for arg in result.args)}\n"
                    )

                f.write("============================\n\n")

                if not result.success:
                    # Group differences by type
                    metadata_diffs = []
                    data_diffs = []
                    other_diffs = []

                    for diff in result.differences:
                        msg = diff["message"]
                        if any(
                            x in msg.lower()
                            for x in [
                                "description",
                                "column",
                                "type",
                                "schema",
                                "catalog",
                            ]
                        ):
                            metadata_diffs.append(diff)
                        elif any(
                            x in msg.lower() for x in ["row", "value", "data", "count"]
                        ):
                            data_diffs.append(diff)
                        else:
                            other_diffs.append(diff)

                    # Write metadata differences
                    if metadata_diffs:
                        f.write("**Metadata Differences:**\n")
                        f.write("---------------------\n")
                        f.write("Column Metadata:\n")
                        for diff in metadata_diffs:
                            f.write(f"  - {diff['message']}")
                            if (
                                diff.get("thrift_value") is not None
                                and diff.get("sea_value") is not None
                            ):
                                f.write(
                                    f": {diff['thrift_value']} vs {diff['sea_value']}"
                                )
                            f.write("\n")
                        f.write("\n")

                    # Write data differences
                    if data_diffs:
                        f.write("**Data Differences:**\n")
                        f.write("-----------------\n")
                        f.write("Row Data:\n")
                        for diff in data_diffs:
                            f.write(f"  - {diff['message']}")
                            if (
                                diff.get("thrift_value") is not None
                                and diff.get("sea_value") is not None
                            ):
                                f.write(
                                    f": {diff['thrift_value']} vs {diff['sea_value']}"
                                )
                            f.write("\n")
                        f.write("\n")

                    # Write other differences
                    if other_diffs:
                        f.write("**Other Differences:**\n")
                        f.write("-----------------\n")
                        for diff in other_diffs:
                            f.write(f"  - {diff['message']}")
                            if (
                                diff.get("thrift_value") is not None
                                and diff.get("sea_value") is not None
                            ):
                                f.write(
                                    f": {diff['thrift_value']} vs {diff['sea_value']}"
                                )
                            f.write("\n")
                        f.write("\n")

                    # Write error information if present
                    if result.thrift_error or result.sea_error:
                        f.write("**Errors:**\n")
                        f.write("--------\n")
                        if result.thrift_error:
                            f.write(f"  - Thrift Error: {result.thrift_error}\n")
                        if result.sea_error:
                            f.write(f"  - SEA Error: {result.sea_error}\n")
                        f.write("\n")
                else:
                    f.write("**Result:** PASSED\n")
                    f.write(
                        f"**Execution Time:** Thrift: {result.thrift_time:.4f}s, SEA: {result.sea_time:.4f}s\n\n"
                    )

                f.write("============================\n\n")

        logger.info(f"Report generated: {self.report_file}")

        # Print summary to console
        print(f"\nSummary: {passed_tests}/{total_tests} tests passed")
        print(
            f"Total execution time - Thrift: {total_thrift_time:.4f}s, SEA: {total_sea_time:.4f}s"
        )
        if total_thrift_time > 0:
            percentage = (total_sea_time / total_thrift_time - 1) * 100
            print(
                f"SEA is {percentage:+.2f}% {'slower' if percentage > 0 else 'faster'} than Thrift"
            )
        print(f"Detailed report saved to: {self.report_file}")

    def run(self):
        """Run the full comparison workflow."""
        try:
            self.setup_connections()
            self.run_comparison_tests()
            self.generate_report()
        finally:
            self.close_connections()


def main():
    """Main entry point."""
    # Check required environment variables
    required_vars = [
        "DATABRICKS_SERVER_HOSTNAME",
        "DATABRICKS_HTTP_PATH",
        "DATABRICKS_TOKEN",
    ]
    missing_vars = [var for var in required_vars if not os.environ.get(var)]

    if missing_vars:
        logger.error(
            f"Missing required environment variables: {', '.join(missing_vars)}"
        )
        logger.error("Please set these variables before running the comparator.")
        sys.exit(1)

    # Get connection parameters from environment
    server_hostname = os.environ["DATABRICKS_SERVER_HOSTNAME"]
    http_path = os.environ["DATABRICKS_HTTP_PATH"]
    access_token = os.environ["DATABRICKS_TOKEN"]
    catalog = os.environ.get("DATABRICKS_CATALOG", DEFAULT_CATALOG)

    # Create and run comparator
    comparator = PythonConnectorComparator(
        server_hostname=server_hostname,
        http_path=http_path,
        access_token=access_token,
        catalog=catalog,
    )

    comparator.run()


if __name__ == "__main__":
    main()
