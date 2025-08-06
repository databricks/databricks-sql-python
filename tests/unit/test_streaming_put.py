
import io
import pytest
from unittest.mock import patch, Mock, MagicMock
import databricks.sql.client as client
from databricks.sql import ProgrammingError
import requests


class TestStreamingPut:
    """Unit tests for streaming PUT functionality."""

    @pytest.fixture
    def mock_connection(self):
        return Mock()

    @pytest.fixture
    def mock_backend(self):
        return Mock()

    @pytest.fixture
    def cursor(self, mock_connection, mock_backend):
        return client.Cursor(
            connection=mock_connection,
            backend=mock_backend
        )

    def _setup_mock_staging_put_stream_response(self, mock_backend):
        """Helper method to set up mock staging PUT stream response."""
        mock_result_set = Mock()
        mock_result_set.is_staging_operation = True
        mock_backend.execute_command.return_value = mock_result_set
        
        mock_row = Mock()
        mock_row.operation = "PUT"
        mock_row.localFile = "__input_stream__"
        mock_row.presignedUrl = "https://example.com/upload"
        mock_row.headers = "{}"
        mock_result_set.fetchone.return_value = mock_row
        
        return mock_result_set

    def test_execute_with_valid_stream(self, cursor, mock_backend):
        """Test execute method with valid input stream."""
        
        # Mock the backend response
        self._setup_mock_staging_put_stream_response(mock_backend)
        
        # Test with valid stream
        test_stream = io.BytesIO(b"test data")
        
        with patch.object(cursor, '_handle_staging_put_stream') as mock_handler:
            cursor.execute(
                "PUT '__input_stream__' INTO '/Volumes/test/cat/schema/vol/file.txt'",
                input_stream=test_stream
            )
            
            # Verify staging handler was called
            mock_handler.assert_called_once()

    def test_execute_with_invalid_stream_types(self, cursor, mock_backend):

        # Mock the backend response
        self._setup_mock_staging_put_stream_response(mock_backend)
        
        # Test with None input stream
        with pytest.raises(client.ProgrammingError) as excinfo:
            cursor.execute(
                "PUT '__input_stream__' INTO '/Volumes/test/cat/schema/vol/file.txt'",
                input_stream=None
            )
        assert "No input stream provided for streaming operation" in str(excinfo.value)

    def test_execute_with_none_stream_for_staging_put(self, cursor, mock_backend):
        """Test execute method rejects None stream for streaming PUT operations."""
        
        # Mock staging operation response for None case
        self._setup_mock_staging_put_stream_response(mock_backend)
        
        # None with __input_stream__ raises ProgrammingError
        with pytest.raises(client.ProgrammingError) as excinfo:
            cursor.execute(
                "PUT '__input_stream__' INTO '/Volumes/test/cat/schema/vol/file.txt'",
                input_stream=None
            )
        error_msg = str(excinfo.value)
        assert "No input stream provided for streaming operation" in error_msg

    def test_handle_staging_put_stream_success(self, cursor):
        """Test successful streaming PUT operation."""
        
        test_stream = io.BytesIO(b"test data")
        presigned_url = "https://example.com/upload"
        headers = {"Content-Type": "text/plain"}
        
        with patch('databricks.sql.client.DatabricksHttpClient') as mock_client_class:
            mock_client = Mock()
            mock_client_class.get_instance.return_value = mock_client
            
            # Mock the context manager properly using MagicMock
            mock_context = MagicMock()
            mock_response = Mock()
            mock_response.status_code = 200
            mock_context.__enter__.return_value = mock_response
            mock_context.__exit__.return_value = None
            mock_client.execute.return_value = mock_context
            
            cursor._handle_staging_put_stream(
                presigned_url=presigned_url,
                stream=test_stream,
                headers=headers
            )
            
            # Verify the HTTP client was called correctly
            mock_client.execute.assert_called_once()
            call_args = mock_client.execute.call_args
            assert call_args[1]['method'].value == 'PUT'
            assert call_args[1]['url'] == presigned_url
            assert call_args[1]['data'] == test_stream
            assert call_args[1]['headers'] == headers

    def test_handle_staging_put_stream_http_error(self, cursor):
        """Test streaming PUT operation with HTTP error."""
        
        test_stream = io.BytesIO(b"test data")
        presigned_url = "https://example.com/upload"
        
        with patch('databricks.sql.client.DatabricksHttpClient') as mock_client_class:
            mock_client = Mock()
            mock_client_class.get_instance.return_value = mock_client
            
            # Mock the context manager with error response
            mock_context = MagicMock()
            mock_response = Mock()
            mock_response.status_code = 500
            mock_response.text = "Internal Server Error"
            mock_context.__enter__.return_value = mock_response
            mock_context.__exit__.return_value = None
            mock_client.execute.return_value = mock_context
            
            with pytest.raises(client.OperationalError) as excinfo:
                cursor._handle_staging_put_stream(
                    presigned_url=presigned_url,
                    stream=test_stream
                )
            
            # Check for the actual error message format
            assert "500" in str(excinfo.value)

    def test_handle_staging_put_stream_network_error(self, cursor):
        """Test streaming PUT operation with network error."""
        
        test_stream = io.BytesIO(b"test data")
        presigned_url = "https://example.com/upload"
        
        with patch('databricks.sql.client.DatabricksHttpClient') as mock_client_class:
            mock_client = Mock()
            mock_client_class.get_instance.return_value = mock_client
            
            # Mock the context manager to raise an exception
            mock_context = MagicMock()
            mock_context.__enter__.side_effect = requests.exceptions.RequestException("Network error")
            mock_client.execute.return_value = mock_context
            
            with pytest.raises(requests.exceptions.RequestException) as excinfo:
                cursor._handle_staging_put_stream(
                    presigned_url=presigned_url,
                    stream=test_stream
                )
            
            assert "Network error" in str(excinfo.value)
