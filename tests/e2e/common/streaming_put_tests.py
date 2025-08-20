#!/usr/bin/env python3
"""
E2E tests for streaming PUT operations.
"""

import io
import logging
import pytest

logger = logging.getLogger(__name__)


class PySQLStreamingPutTestSuiteMixin:
    """Test suite for streaming PUT operations."""

    def test_streaming_put_basic(self, catalog, schema):
        """Test basic streaming PUT functionality."""
        
        # Create test data
        test_data = b"Hello, streaming world! This is test data."
        filename = "streaming_put_test.txt"
        file_path = f"/Volumes/{catalog}/{schema}/e2etests/{filename}"
        
        try:
            with self.connection() as conn:
                with conn.cursor() as cursor:
                    self._cleanup_test_file(file_path)
                    
                    with io.BytesIO(test_data) as stream:
                        cursor.execute(
                            f"PUT '__input_stream__' INTO '{file_path}'",
                            input_stream=stream
                        )
                    
                    # Verify file exists
                    cursor.execute(f"LIST '/Volumes/{catalog}/{schema}/e2etests/'")
                    files = cursor.fetchall()
                    
                    # Check if our file is in the list
                    file_paths = [row[0] for row in files]
                    assert file_path in file_paths, f"File {file_path} not found in {file_paths}"
        finally:
            self._cleanup_test_file(file_path)
    
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

    def _cleanup_test_file(self, file_path):
        """Clean up a test file if it exists."""
        try:
            with self.connection(extra_params={"staging_allowed_local_path": "/"}) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"REMOVE '{file_path}'")
                    logger.info("Successfully cleaned up test file: %s", file_path)
        except Exception as e:
            logger.error("Cleanup failed for %s: %s", file_path, e)