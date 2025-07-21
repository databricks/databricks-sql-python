#!/usr/bin/env python3
"""
E2E tests for streaming PUT operations.
"""

import io
import pytest
from datetime import datetime


class PySQLStreamingPutTestSuiteMixin:
    """Test suite for streaming PUT operations."""

    def test_streaming_put_basic(self, catalog, schema):
        """Test basic streaming PUT functionality."""
        
        # Create test data
        test_data = b"Hello, streaming world! This is test data."
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"stream_test_{timestamp}.txt"
        
        with self.connection() as conn:
            with conn.cursor() as cursor:
                with io.BytesIO(test_data) as stream:
                    cursor.execute(
                        f"PUT '__input_stream__' INTO '/Volumes/{catalog}/{schema}/e2etests/{filename}'",
                        input_stream=stream
                    )
                
                # Verify file exists
                cursor.execute(f"LIST '/Volumes/{catalog}/{schema}/e2etests/'")
                files = cursor.fetchall()
                
                # Check if our file is in the list
                file_paths = [row[0] for row in files]
                expected_path = f"/Volumes/{catalog}/{schema}/e2etests/{filename}"
                
                assert expected_path in file_paths, f"File {expected_path} not found in {file_paths}"
    
    
    def test_streaming_put_missing_stream(self, catalog, schema):
        """Test that missing stream raises appropriate error."""
        
        with self.connection() as conn:
            with conn.cursor() as cursor:
                # Test without providing stream
                with pytest.raises(Exception):  # Should fail
                    cursor.execute(
                        f"PUT '__input_stream__' INTO '/Volumes/{catalog}/{schema}/e2etests/test.txt'"
                        # Note: No input_stream parameter
                    ) 