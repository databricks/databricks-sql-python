import pytest
from unittest.mock import patch, MagicMock
import io

from databricks.sql.telemetry.telemetry_client import TelemetryClientFactory
from databricks.sql.telemetry.models.event import DriverConnectionParameters, HostDetails, DatabricksClientType
from databricks.sql.telemetry.models.enums import AuthMech
from databricks.sql.auth.retry import DatabricksRetryPolicy

PATCH_TARGET = 'urllib3.connectionpool.HTTPSConnectionPool._get_conn'

def create_mock_conn(responses):
    """Creates a mock connection object whose getresponse() method yields a series of responses."""
    mock_conn = MagicMock()
    mock_http_responses = []
    for resp in responses:
        mock_http_response = MagicMock()
        mock_http_response.status = resp.get("status")
        mock_http_response.headers = resp.get("headers", {})
        body = resp.get("body", b'{}')
        mock_http_response.fp = io.BytesIO(body)
        def release():
            mock_http_response.fp.close()
        mock_http_response.release_conn = release
        mock_http_responses.append(mock_http_response)
    mock_conn.getresponse.side_effect = mock_http_responses
    return mock_conn

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
        """
        Configures a client with a specific number of retries.
        """
        TelemetryClientFactory.initialize_telemetry_client(
            telemetry_enabled=True,
            session_id_hex=session_id,
            auth_provider=None,
            host_url="test.databricks.com",
        )
        client = TelemetryClientFactory.get_telemetry_client(session_id)
        
        retry_policy = DatabricksRetryPolicy(
            delay_min=0.01,
            delay_max=0.02,
            stop_after_attempts_duration=2.0,
            stop_after_attempts_count=num_retries, 
            delay_default=0.1,
            force_dangerous_codes=[],
            urllib3_kwargs={'total': num_retries}
        )
        adapter = client._session.adapters.get("https://")
        adapter.max_retries = retry_policy
        return client, adapter

    def test_success_no_retry(self):
        client, _ = self.get_client("session-success")
        params = DriverConnectionParameters(
            http_path="test-path", mode=DatabricksClientType.THRIFT,
            host_info=HostDetails(host_url="test.databricks.com", port=443),
            auth_mech=AuthMech.PAT
        )
        mock_responses = [{"status": 200}]
        
        with patch(PATCH_TARGET, return_value=create_mock_conn(mock_responses)) as mock_get_conn:
            client.export_initial_telemetry_log(params, "test-agent")
            TelemetryClientFactory.close(client._session_id_hex)
            
            mock_get_conn.return_value.getresponse.assert_called_once()
        client, _ = self.get_client("session-retry-once", num_retries=1)
        mock_responses = [{"status": 503}, {"status": 200}]
        
        with patch(PATCH_TARGET, return_value=create_mock_conn(mock_responses)) as mock_get_conn:
            client.export_failure_log("TestError", "Test message")
            TelemetryClientFactory.close(client._session_id_hex)
            
            assert mock_get_conn.return_value.getresponse.call_count == 2

    @pytest.mark.parametrize(
    "status_code, description",
    [
        (401, "Unauthorized"),
        (403, "Forbidden"),
        (501, "Not Implemented"),
    ],
    )
    def test_non_retryable_status_codes_are_not_retried(self, status_code, description):
        """
        Verifies that terminal error codes (401, 403, 501, etc.) are not retried.
        """
        # Use the status code in the session ID for easier debugging if it fails
        client, _ = self.get_client(f"session-{status_code}")
        mock_responses = [{"status": status_code}]

        with patch(PATCH_TARGET, return_value=create_mock_conn(mock_responses)) as mock_get_conn:
            client.export_failure_log("TestError", "Test message")
            TelemetryClientFactory.close(client._session_id_hex)

            mock_get_conn.return_value.getresponse.assert_called_once()

    def test_respects_retry_after_header(self):
        client, _ = self.get_client("session-retry-after", num_retries=1)
        mock_responses = [{"status": 429, "headers": {'Retry-After': '1'}}, {"status": 200}]
        
        with patch(PATCH_TARGET, return_value=create_mock_conn(mock_responses)) as mock_get_conn:
            client.export_failure_log("TestError", "Test message")
            TelemetryClientFactory.close(client._session_id_hex)
            
            assert mock_get_conn.return_value.getresponse.call_count == 2

    def test_exceeds_retry_count_limit(self):
        num_retries = 3
        expected_total_calls = num_retries + 1 
        client, _ = self.get_client("session-exceed-limit", num_retries=num_retries)
        mock_responses = [{"status": 503}] * expected_total_calls
        
        with patch(PATCH_TARGET, return_value=create_mock_conn(mock_responses)) as mock_get_conn:
            client.export_failure_log("TestError", "Test message")
            TelemetryClientFactory.close(client._session_id_hex)
            
            assert mock_get_conn.return_value.getresponse.call_count == expected_total_calls