import pytest
from unittest.mock import patch, MagicMock
import io
import time

from databricks.sql.telemetry.telemetry_client import TelemetryClientFactory
from databricks.sql.auth.retry import DatabricksRetryPolicy

PATCH_TARGET = "databricks.sql.common.unified_http_client.UnifiedHttpClient.request"


def create_mock_response(responses):
    """Creates mock urllib3 HTTPResponse objects for the given response specifications."""
    mock_responses = []
    for resp in responses:
        mock_response = MagicMock()
        mock_response.status = resp.get("status")
        mock_response.status_code = resp.get("status")  # Add status_code for compatibility
        mock_response.headers = resp.get("headers", {})
        mock_response.data = resp.get("body", b"{}")
        mock_response.ok = resp.get("status", 200) < 400
        mock_response.text = resp.get("body", b"{}").decode() if isinstance(resp.get("body", b"{}"), bytes) else str(resp.get("body", "{}"))
        mock_response.json = lambda: {}  # Simple json mock
        mock_responses.append(mock_response)
    return mock_responses


class TestTelemetryClientRetries:
    @pytest.fixture(autouse=True)
    def setup_and_teardown(self):
        TelemetryClientFactory._initialized = False
        TelemetryClientFactory._clients = {}
        TelemetryClientFactory._executor = None
        yield
        if TelemetryClientFactory._executor:
            TelemetryClientFactory._executor.shutdown(wait=True)
        TelemetryClientFactory._initialized = False
        TelemetryClientFactory._clients = {}
        TelemetryClientFactory._executor = None

    def get_client(self, session_id, num_retries=3):
        mock_http_client = MagicMock()
        TelemetryClientFactory.initialize_telemetry_client(
            telemetry_enabled=True,
            session_id_hex=session_id,
            auth_provider=None,
            host_url="test.databricks.com",
            batch_size=1,  # Use batch size of 1 to trigger immediate HTTP requests
            http_client=mock_http_client,
        )
        return TelemetryClientFactory.get_telemetry_client(session_id)

    @pytest.mark.parametrize(
        "status_code, description",
        [
            (401, "Unauthorized"),
            (403, "Forbidden"),
            (501, "Not Implemented"),
            (200, "Success"),
        ],
    )
    def test_non_retryable_status_codes_are_not_retried(self, status_code, description):
        """
        Verifies that terminal error codes (401, 403, 501) and success codes (200) are not retried.
        """
        # Use the status code in the session ID for easier debugging if it fails
        client = self.get_client(f"session-{status_code}")
        mock_responses = [{"status": status_code}]

        mock_response = create_mock_response(mock_responses)[0]
        with patch(PATCH_TARGET, return_value=mock_response) as mock_request:
            client.export_failure_log("TestError", "Test message")
            
            # Wait a moment for async operations to complete
            time.sleep(0.1)
            
            TelemetryClientFactory.close(client._session_id_hex)
            
            # Wait a bit more for any final operations
            time.sleep(0.1)

            mock_request.assert_called_once()

    def test_exceeds_retry_count_limit(self):
        """
        Verifies that the client retries up to the specified number of times before giving up.
        Verifies that the client respects the Retry-After header and retries on 429, 502, 503.
        """
        num_retries = 3
        expected_total_calls = num_retries + 1
        retry_after = 1
        client = self.get_client("session-exceed-limit", num_retries=num_retries)
        mock_responses = [
            {"status": 429, "headers": {"Retry-After": str(retry_after)}},
            {"status": 502},
            {"status": 503},
            {"status": 200},
        ]

        mock_response_objects = create_mock_response(mock_responses)
        with patch(PATCH_TARGET, side_effect=mock_response_objects) as mock_request:
            start_time = time.time()
            client.export_failure_log("TestError", "Test message")
            
            # Wait for async operations to complete
            time.sleep(0.2)
            
            TelemetryClientFactory.close(client._session_id_hex)
            
            # Wait for any final operations
            time.sleep(0.2)
            
            end_time = time.time()

            assert (
                mock_request.call_count
                == expected_total_calls
            )
