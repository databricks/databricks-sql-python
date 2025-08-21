import io
from unittest.mock import patch, Mock, MagicMock

import pytest

import databricks.sql.client as client


class TestStreamingPut:
    """Unit tests for streaming PUT functionality."""

    @pytest.fixture
    def cursor(self):
        return client.Cursor(connection=Mock(), backend=Mock())

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

    def test_execute_with_valid_stream(self, cursor):
        """Test execute method with valid input stream."""

        # Mock the backend response
        self._setup_mock_staging_put_stream_response(cursor.backend)

        # Test with valid stream
        test_stream = io.BytesIO(b"test data")

        with patch.object(cursor, "_handle_staging_put_stream") as mock_handler:
            cursor.execute(
                "PUT '__input_stream__' INTO '/Volumes/test/cat/schema/vol/file.txt'",
                input_stream=test_stream,
            )

            # Verify staging handler was called
            mock_handler.assert_called_once()

    def test_execute_with_none_stream_for_staging_put(self, cursor):
        """Test execute method rejects None stream for streaming PUT operations."""

        # Mock staging operation response for None case
        self._setup_mock_staging_put_stream_response(cursor.backend)

        # None with __input_stream__ raises ProgrammingError
        with pytest.raises(client.ProgrammingError) as excinfo:
            cursor.execute(
                "PUT '__input_stream__' INTO '/Volumes/test/cat/schema/vol/file.txt'",
                input_stream=None,
            )
        error_msg = str(excinfo.value)
        assert "No input stream provided for streaming operation" in error_msg

    def test_handle_staging_put_stream_success(self, cursor):
        """Test successful streaming PUT operation."""

        presigned_url = "https://example.com/upload"
        headers = {"Content-Type": "text/plain"}

        with patch.object(
            cursor.connection.http_client, "request"
        ) as mock_http_request:
            mock_response = MagicMock()
            mock_response.status = 200
            mock_response.data = b"success"
            mock_http_request.return_value = mock_response

            test_stream = io.BytesIO(b"test data")
            cursor._handle_staging_put_stream(
                presigned_url=presigned_url, stream=test_stream, headers=headers
            )

            # Verify the HTTP client was called correctly
            mock_http_request.assert_called_once()
            call_args = mock_http_request.call_args
            # Check positional arguments: (method, url, body=..., headers=...)
            assert call_args[0][0].value == "PUT"  # First positional arg is method
            assert call_args[0][1] == presigned_url  # Second positional arg is url
            # Check keyword arguments
            assert call_args[1]["body"] == b"test data"
            assert call_args[1]["headers"] == headers

    def test_handle_staging_put_stream_http_error(self, cursor):
        """Test streaming PUT operation with HTTP error."""

        presigned_url = "https://example.com/upload"

        with patch.object(
            cursor.connection.http_client, "request"
        ) as mock_http_request:
            mock_response = MagicMock()
            mock_response.status = 500
            mock_response.data = b"Internal Server Error"
            mock_http_request.return_value = mock_response

            test_stream = io.BytesIO(b"test data")
            with pytest.raises(client.OperationalError) as excinfo:
                cursor._handle_staging_put_stream(
                    presigned_url=presigned_url, stream=test_stream
                )

            # Check for the actual error message format
            assert "500" in str(excinfo.value)
