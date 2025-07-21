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
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
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

    def add_difference(self, message: str, thrift_value: Any = None, sea_value: Any = None):
        """Add a difference to the result."""
        self.differences.append({
            "message": message,
            "thrift_value": thrift_value,
            "sea_value": sea_value
        })
        self.success = False

    def __str__(self) -> str:
        """String representation of the comparison result."""
        result = f"Test: {self.test_name}\n"
        result += f"Query: {self.query}\n"
        
        if self.args:
            result += f"Args: {self.args}\n"
        
        result += f"Success: {self.success}\n"
        result += f"Thrift time: {self.thrift_time:.4f}s, SEA time: {self.sea_time:.4f}s\n"
        
        if self.thrift_error:
            result += f"Thrift error: {self.thrift_error}\n"
        
        if self.sea_error:
            result += f"SEA error: {self.sea_error}\n"
        
        if not self.success:
            result += "Differences:\n"
            for diff in self.differences:
                result += f"  - {diff['message']}\n"
                if diff.get('thrift_value') is not None:
                    result += f"    Thrift: {diff['thrift_value']}\n"
                if diff.get('sea_value') is not None:
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
        report_file: str = "python-connector-comparison-report.txt"
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
            user_agent_entry="Python-Connector-Comparator"
        )
        
        # Create SEA connection
        self.sea_connection = Connection(
            server_hostname=self.server_hostname,
            http_path=self.http_path,
            access_token=self.access_token,
            catalog=self.catalog,
            schema=self.schema,
            use_sea=True,  # Explicitly use SEA backend
            user_agent_entry="Python-Connector-Comparator"
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
            result.add_difference("Thrift description is None but SEA description is not")
            return
        
        if sea_desc is None:
            result.add_difference("SEA description is None but Thrift description is not")
            return
        
        if len(thrift_desc) != len(sea_desc):
            result.add_difference(
                f"Description length mismatch: Thrift has {len(thrift_desc)} columns, SEA has {len(sea_desc)}",
                thrift_desc,
                sea_desc
            )
            return
        
        for i, (thrift_col, sea_col) in enumerate(zip(thrift_desc, sea_desc)):
            # Compare each element of the description tuple
            for j, (thrift_val, sea_val) in enumerate(zip(thrift_col, sea_col)):
                if thrift_val != sea_val:
                    element_names = ["name", "type_code", "display_size", "internal_size", 
                                    "precision", "scale", "null_ok"]
                    element_name = element_names[j] if j < len(element_names) else f"element_{j}"
                    
                    result.add_difference(
                        f"Column {i} ({thrift_col[0]}) {element_name} mismatch",
                        thrift_val,
                        sea_val
                    )

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
        
        for i, (thrift_row, sea_row) in enumerate(zip(thrift_rows, sea_rows)):
            # Convert rows to dictionaries for comparison
            try:
                thrift_dict = thrift_row.asDict(recursive=True)
                sea_dict = sea_row.asDict(recursive=True)
                
                if thrift_dict != sea_dict:
                    # Find which fields differ
                    all_fields = set(thrift_dict.keys()) | set(sea_dict.keys())
                    
                    for field in all_fields:
                        thrift_value = thrift_dict.get(field)
                        sea_value = sea_dict.get(field)
                        
                        if field not in thrift_dict:
                            result.add_difference(
                                f"Row {i}: Field '{field}' missing in Thrift row",
                                None,
                                sea_value
                            )
                        elif field not in sea_dict:
                            result.add_difference(
                                f"Row {i}: Field '{field}' missing in SEA row",
                                thrift_value,
                                None
                            )
                        elif thrift_value != sea_value:
                            result.add_difference(
                                f"Row {i}, field '{field}' value mismatch",
                                thrift_value,
                                sea_value
                            )
            except (AttributeError, TypeError) as e:
                # If asDict fails, fall back to direct comparison
                if thrift_row != sea_row:
                    result.add_difference(
                        f"Row {i} mismatch (asDict failed: {str(e)})",
                        thrift_row,
                        sea_row
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
                sea_schema
            )
        else:
            for i, (thrift_field, sea_field) in enumerate(zip(thrift_schema, sea_schema)):
                if thrift_field.name != sea_field.name:
                    result.add_difference(
                        f"Arrow schema field {i} name mismatch",
                        thrift_field.name,
                        sea_field.name
                    )
                
                if str(thrift_field.type) != str(sea_field.type):
                    result.add_difference(
                        f"Arrow schema field {i} ({thrift_field.name}) type mismatch",
                        str(thrift_field.type),
                        str(sea_field.type)
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
                if thrift_df.shape[0] == sea_df.shape[0] and thrift_df.shape[1] == sea_df.shape[1]:
                    # Same dimensions, compare cell by cell
                    for col in thrift_df.columns:
                        if col in sea_df.columns:
                            mask = thrift_df[col] != sea_df[col]
                            if mask.any():
                                diff_indices = mask[mask].index.tolist()
                                if len(diff_indices) > 3:  # Limit to first 3 differences
                                    diff_indices = diff_indices[:3]
                                
                                for idx in diff_indices:
                                    result.add_difference(
                                        f"Arrow data mismatch at row {idx}, column '{col}'",
                                        thrift_df.loc[idx, col],
                                        sea_df.loc[idx, col]
                                    )
        except Exception as e:
            result.add_difference(f"Error comparing Arrow tables: {str(e)}")

    def execute_and_compare(
        self,
        query: str,
        test_name: str,
        fetch_method: str = "fetchall",
        args: List[Any] = None,
        fetch_size: int = None
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
            arraysize=self.array_size,
            buffer_size_bytes=self.buffer_size_bytes
        )
        
        sea_cursor = self.sea_connection.cursor(
            arraysize=self.array_size,
            buffer_size_bytes=self.buffer_size_bytes
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
                    result.add_difference("Thrift returned None but SEA returned a row", None, sea_row)
                elif sea_row is None:
                    result.add_difference("SEA returned None but Thrift returned a row", thrift_row, None)
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

    def run_comparison_tests(self):
        """Run a set of comparison tests between Thrift and SEA backends."""
        logger.info("Starting comparison tests")
        
        # Following the JDBC comparator approach with a single TPC-DS query
        # Adjust the table path if needed based on your environment
        tpc_query = "SELECT * FROM main.tpcds_sf100_delta.catalog_sales LIMIT 5"
        
        # Test with fetchall
        self.results.append(
            self.execute_and_compare(
                tpc_query,
                "TPC-DS query - fetchall",
                "fetchall"
            )
        )
        
        # Test with fetchmany
        self.results.append(
            self.execute_and_compare(
                tpc_query,
                "TPC-DS query - fetchmany",
                "fetchmany",
                fetch_size=2
            )
        )
        
        # Test with fetchone
        self.results.append(
            self.execute_and_compare(
                tpc_query,
                "TPC-DS query - fetchone",
                "fetchone"
            )
        )
        
        # Test with fetchall_arrow
        self.results.append(
            self.execute_and_compare(
                tpc_query,
                "TPC-DS query - fetchall_arrow",
                "fetchall_arrow"
            )
        )
        
        # Test with fetchmany_arrow
        self.results.append(
            self.execute_and_compare(
                tpc_query,
                "TPC-DS query - fetchmany_arrow",
                "fetchmany_arrow",
                fetch_size=2
            )
        )
        
        logger.info(f"Completed {len(self.results)} comparison tests")

    def generate_report(self):
        """Generate a report of the comparison results."""
        logger.info(f"Generating report to {self.report_file}")
        
        with open(self.report_file, "w") as f:
            f.write("Python Connector Comparison Report\n")
            f.write("=================================\n\n")
            f.write(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Server: {self.server_hostname}\n")
            f.write(f"HTTP Path: {self.http_path}\n\n")
            
            # Summary
            total_tests = len(self.results)
            passed_tests = sum(1 for r in self.results if r.success)
            f.write(f"Summary: {passed_tests}/{total_tests} tests passed\n\n")
            
            # Performance summary
            total_thrift_time = sum(r.thrift_time for r in self.results)
            total_sea_time = sum(r.sea_time for r in self.results)
            f.write(f"Total execution time - Thrift: {total_thrift_time:.4f}s, SEA: {total_sea_time:.4f}s\n")
            
            if total_thrift_time > 0:
                percentage = (total_sea_time / total_thrift_time - 1) * 100
                f.write(f"SEA is {percentage:.2f}% {'slower' if percentage > 0 else 'faster'} than Thrift\n\n")
            
            # Test details
            f.write("Test Details\n")
            f.write("------------\n\n")
            
            for i, result in enumerate(self.results):
                f.write(f"Test {i+1}: {result.test_name}\n")
                f.write(f"{'PASSED' if result.success else 'FAILED'}\n")
                f.write(f"Query: {result.query}\n")
                
                if result.args:
                    f.write(f"Args: {result.args}\n")
                
                f.write(f"Thrift time: {result.thrift_time:.4f}s, SEA time: {result.sea_time:.4f}s\n")
                
                if not result.success:
                    f.write("Differences:\n")
                    for diff in result.differences:
                        f.write(f"  - {diff['message']}\n")
                        if diff.get('thrift_value') is not None:
                            f.write(f"    Thrift: {diff['thrift_value']}\n")
                        if diff.get('sea_value') is not None:
                            f.write(f"    SEA: {diff['sea_value']}\n")
                
                f.write("\n")
        
        logger.info(f"Report generated: {self.report_file}")
        
        # Print summary to console
        print(f"\nSummary: {passed_tests}/{total_tests} tests passed")
        print(f"Total execution time - Thrift: {total_thrift_time:.4f}s, SEA: {total_sea_time:.4f}s")
        if total_thrift_time > 0:
            percentage = (total_sea_time / total_thrift_time - 1) * 100
            print(f"SEA is {percentage:.2f}% {'slower' if percentage > 0 else 'faster'} than Thrift")
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
    required_vars = ["DATABRICKS_SERVER_HOSTNAME", "DATABRICKS_HTTP_PATH", "DATABRICKS_TOKEN"]
    missing_vars = [var for var in required_vars if not os.environ.get(var)]
    
    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
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
        catalog=catalog
    )
    
    comparator.run()


if __name__ == "__main__":
    main()