from unittest.mock import patch, Mock, MagicMock

import pytest

import databricks.sql.client as client


class TestStagingRemove:
    """Unit tests for staging REMOVE operations.

    REMOVE deletes a remote resource and never touches the local filesystem,
    so it must not require ``staging_allowed_local_path`` to be configured
    (see GitHub issue #726).
    """

    @pytest.fixture
    def cursor(self):
        return client.Cursor(connection=Mock(), backend=Mock())

    def _setup_mock_remove_response(self, cursor):
        """Set up a mock staging REMOVE response with no localFile."""
        mock_result_set = Mock()
        mock_result_set.is_staging_operation = True
        cursor.backend.execute_command.return_value = mock_result_set

        mock_row = Mock()
        mock_row.operation = "REMOVE"
        # The server does not include a localFile for REMOVE.
        mock_row.localFile = None
        mock_row.presignedUrl = "https://example.com/delete"
        mock_row.headers = "{}"
        mock_result_set.fetchone.return_value = mock_row

        cursor.active_result_set = mock_result_set
        return mock_result_set

    def test_remove_without_staging_allowed_local_path(self, cursor):
        """REMOVE must succeed when staging_allowed_local_path is None."""

        self._setup_mock_remove_response(cursor)

        with patch.object(cursor, "_handle_staging_remove") as mock_remove:
            cursor._handle_staging_operation(staging_allowed_local_path=None)

            mock_remove.assert_called_once()
            # local_file must not be forwarded to the REMOVE handler.
            assert "local_file" not in mock_remove.call_args.kwargs
            assert (
                mock_remove.call_args.kwargs["presigned_url"]
                == "https://example.com/delete"
            )

    def test_remove_with_staging_allowed_local_path(self, cursor):
        """REMOVE must still work when staging_allowed_local_path is provided."""

        self._setup_mock_remove_response(cursor)

        with patch.object(cursor, "_handle_staging_remove") as mock_remove:
            cursor._handle_staging_operation(staging_allowed_local_path="/tmp")

            mock_remove.assert_called_once()

    def test_get_still_requires_staging_allowed_local_path(self, cursor):
        """GET must still fail when staging_allowed_local_path is None."""

        mock_result_set = Mock()
        mock_result_set.is_staging_operation = True
        mock_row = Mock()
        mock_row.operation = "GET"
        mock_row.localFile = "/tmp/file.csv"
        mock_row.presignedUrl = "https://example.com/download"
        mock_row.headers = "{}"
        mock_result_set.fetchone.return_value = mock_row
        cursor.active_result_set = mock_result_set

        with pytest.raises(client.ProgrammingError) as excinfo:
            cursor._handle_staging_operation(staging_allowed_local_path=None)
        assert "You must provide at least one staging_allowed_local_path" in str(
            excinfo.value
        )

    def test_unsupported_operation_still_errors(self, cursor):
        """An unknown operation must still raise, even with a path configured."""

        mock_result_set = Mock()
        mock_result_set.is_staging_operation = True
        mock_row = Mock()
        mock_row.operation = "COPY"
        mock_row.localFile = None
        mock_row.presignedUrl = "https://example.com/copy"
        mock_row.headers = "{}"
        mock_result_set.fetchone.return_value = mock_row
        cursor.active_result_set = mock_result_set

        with pytest.raises(client.ProgrammingError) as excinfo:
            cursor._handle_staging_operation(staging_allowed_local_path="/tmp")
        assert "is not supported" in str(excinfo.value)
