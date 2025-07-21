
import io
import unittest
from unittest.mock import patch, Mock, MagicMock
import databricks.sql.client as client
from databricks.sql import ProgrammingError
import requests


class TestStreamingPutUnit(unittest.TestCase):
    """Unit tests for streaming PUT functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_connection = Mock()
        self.mock_backend = Mock()
        self.cursor = client.Cursor(
            connection=self.mock_connection,
            backend=self.mock_backend
        )
    
    def _setup_mock_staging_put_stream_response(self):
        """Helper method to set up mock staging PUT stream response."""
        mock_result_set = Mock()
        mock_result_set.is_staging_operation = True
        self.mock_backend.execute_command.return_value = mock_result_set
        
        mock_row = Mock()
        mock_row.operation = "PUT"
        mock_row.localFile = "__input_stream__"
        mock_row.presignedUrl = "https://example.com/upload"
        mock_row.headers = "{}"
        mock_result_set.fetchone.return_value = mock_row
        
        return mock_result_set


    def test_execute_with_valid_stream(self):
        """Test execute method with valid input stream."""
        
        # Mock the backend response
        self._setup_mock_staging_put_stream_response()
        
        # Test with valid stream
        test_stream = io.BytesIO(b"test data")
        
        with patch.object(self.cursor, '_handle_staging_put_stream') as mock_handler:
            self.cursor.execute(
                "PUT '__input_stream__' INTO '/Volumes/test/cat/schema/vol/file.txt'",
                input_stream=test_stream
            )
            
            # Verify staging handler was called
            mock_handler.assert_called_once()
            
            # Verify the finally block cleanup
            self.assertIsNone(self.cursor._input_stream_data)
    

    def test_execute_with_invalid_stream_types(self):
        """Test execute method rejects all invalid stream types."""
        
        # Test all invalid input types in one place
        invalid_inputs = [
            "not a stream",
            [1, 2, 3],
            {"key": "value"},
            42,
            True
        ]
        
        for invalid_input in invalid_inputs:
            with self.subTest(invalid_input=invalid_input):
                with self.assertRaises(TypeError) as context:
                    self.cursor.execute(
                        "PUT '__input_stream__' INTO '/Volumes/test/cat/schema/vol/file.txt'",
                        input_stream=invalid_input
                    )
                error_msg = str(context.exception)
                self.assertIn("input_stream must be a binary stream", error_msg)
    

    def test_execute_with_none_stream_for_staging_put(self):
        """Test execute method rejects None stream for streaming PUT operations."""
        
        # Mock staging operation response for None case
        self._setup_mock_staging_put_stream_response()
        
        # None with __input_stream__ raises ProgrammingError
        with self.assertRaises(client.ProgrammingError) as context:
            self.cursor.execute(
                "PUT '__input_stream__' INTO '/Volumes/test/cat/schema/vol/file.txt'",
                input_stream=None
            )
        error_msg = str(context.exception)
        self.assertIn("No input stream provided for streaming operation", error_msg)
    
    
    def test_handle_staging_put_stream_success(self):
        """Test successful streaming PUT operation."""
        
        test_stream = io.BytesIO(b"test data")
        presigned_url = "https://example.com/upload"
        headers = {"Content-Type": "text/plain"}
        
        with patch('requests.put') as mock_put:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_put.return_value = mock_response
            
            self.cursor._handle_staging_put_stream(
                presigned_url=presigned_url,
                stream=test_stream,
                headers=headers
            )
            
            # Verify HTTP PUT was called correctly
            mock_put.assert_called_once_with(
                url=presigned_url,
                data=test_stream,
                headers=headers,
                timeout=300
            )
    

    def test_handle_staging_put_stream_http_error(self):
        """Test streaming PUT operation with HTTP error."""
        
        test_stream = io.BytesIO(b"test data")
        presigned_url = "https://example.com/upload"
        
        with patch('requests.put') as mock_put:
            mock_response = Mock()
            mock_response.status_code = 500
            mock_response.text = "Internal Server Error"
            mock_put.return_value = mock_response
            
            with self.assertRaises(client.OperationalError) as context:
                self.cursor._handle_staging_put_stream(
                    presigned_url=presigned_url,
                    stream=test_stream
                )
            
            # Check for the actual error message format
            self.assertIn("500", str(context.exception))
    

    def test_handle_staging_put_stream_network_error(self):
        """Test streaming PUT operation with network error."""
        
        test_stream = io.BytesIO(b"test data")
        presigned_url = "https://example.com/upload"
        
        with patch('requests.put') as mock_put:
            # Use requests.exceptions.RequestException instead of generic Exception
            mock_put.side_effect = requests.exceptions.RequestException("Network error")
            
            with self.assertRaises(client.OperationalError) as context:
                self.cursor._handle_staging_put_stream(
                    presigned_url=presigned_url,
                    stream=test_stream
                )
            
            self.assertIn("HTTP request failed", str(context.exception))
    
    
    def test_stream_cleanup_after_execute(self):
        """Test that stream data is cleaned up after execute."""
        
        # Mock the backend response
        mock_result_set = Mock()
        mock_result_set.is_staging_operation = False
        self.mock_backend.execute_command.return_value = mock_result_set
        
        test_stream = io.BytesIO(b"test data")
        
        # Execute with stream
        self.cursor.execute("SELECT 1", input_stream=test_stream)
        
        # Verify stream data is cleaned up
        self.assertIsNone(self.cursor._input_stream_data)
    

    def test_stream_cleanup_after_exception(self):
        """Test that stream data is cleaned up even after exception."""
        
        # Mock the backend to raise exception
        self.mock_backend.execute_command.side_effect = Exception("Backend error")
        
        test_stream = io.BytesIO(b"test data")
        
        # Execute should raise exception but cleanup should still happen
        with self.assertRaises(Exception):
            self.cursor.execute("SELECT 1", input_stream=test_stream)
        
        # Verify stream data is still cleaned up
        self.assertIsNone(self.cursor._input_stream_data)


if __name__ == '__main__':
    unittest.main() 