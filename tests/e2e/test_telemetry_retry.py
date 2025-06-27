# tests/e2e/test_telemetry_retry.py

import pytest
import logging
from unittest.mock import patch, MagicMock
from functools import wraps
import time
from concurrent.futures import Future

# Imports for the code being tested
from databricks.sql.telemetry.telemetry_client import TelemetryClientFactory
from databricks.sql.telemetry.models.event import DriverConnectionParameters, HostDetails, DatabricksClientType
from databricks.sql.telemetry.models.enums import AuthMech
from databricks.sql.auth.retry import DatabricksRetryPolicy, CommandType

# Imports for mocking the network layer correctly
from urllib3.connectionpool import HTTPSConnectionPool
from urllib3.exceptions import MaxRetryError
from requests.exceptions import ConnectionError as RequestsConnectionError

PATCH_TARGET = 'urllib3.connectionpool.HTTPSConnectionPool._get_conn'

# Helper to create a mock that looks and acts like a urllib3.response.HTTPResponse.
def create_urllib3_response(status, headers=None, body=b'{}'):
    """Create a proper mock response that simulates urllib3's HTTPResponse"""
    mock_response = MagicMock()
    mock_response.status = status
    mock_response.headers = headers or {}
    mock_response.msg = headers or {}  # For urllib3~=1.0 compatibility
    mock_response.data = body
    mock_response.read.return_value = body
    mock_response.get_redirect_location.return_value = False
    mock_response.closed = False
    mock_response.isclosed.return_value = False
    return mock_response

@pytest.mark.usefixtures("caplog")
class TestTelemetryClientRetries:
    """
    Test suite for verifying the retry mechanism of the TelemetryClient.
    This suite patches the low-level urllib3 connection to correctly
    trigger and test the retry logic configured in the requests adapter.
    """

    @pytest.fixture(autouse=True)
    def setup_and_teardown(self, caplog):
        caplog.set_level(logging.DEBUG)
        TelemetryClientFactory._initialized = False
        TelemetryClientFactory._clients = {}
        TelemetryClientFactory._executor = None
        yield
        if TelemetryClientFactory._executor:
            TelemetryClientFactory._executor.shutdown(wait=True)
        TelemetryClientFactory._initialized = False
        TelemetryClientFactory._clients = {}
        TelemetryClientFactory._executor = None

    def get_client(self, session_id, total_retries=3):
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
            stop_after_attempts_count=total_retries,
            delay_default=0.1,
            force_dangerous_codes=[],
            urllib3_kwargs={'total': total_retries}
        )
        adapter = client._session.adapters.get("https://")
        adapter.max_retries = retry_policy
        return client, adapter

    def wait_for_async_request(self, timeout=2.0):
        """Wait for async telemetry request to complete"""
        start_time = time.time()
        while time.time() - start_time < timeout:
            if TelemetryClientFactory._executor and TelemetryClientFactory._executor._threads:
                # Wait a bit more for threads to complete
                time.sleep(0.1)
            else:
                break
        time.sleep(0.1)  # Extra buffer for completion

    def test_success_no_retry(self):
        client, _ = self.get_client("session-success")
        params = DriverConnectionParameters(
            http_path="test-path",
            mode=DatabricksClientType.THRIFT,
            host_info=HostDetails(host_url="test.databricks.com", port=443),
            auth_mech=AuthMech.PAT
        )
        with patch(PATCH_TARGET) as mock_get_conn:
            mock_get_conn.return_value.getresponse.return_value = create_urllib3_response(200)
            
            client.export_initial_telemetry_log(params, "test-agent")
            self.wait_for_async_request()
            TelemetryClientFactory.close(client._session_id_hex)
            
            mock_get_conn.return_value.getresponse.assert_called_once()

    def test_retry_on_503_then_succeeds(self):
        client, _ = self.get_client("session-retry-once")
        with patch(PATCH_TARGET) as mock_get_conn:
            mock_get_conn.return_value.getresponse.side_effect = [
                create_urllib3_response(503),
                create_urllib3_response(200),
            ]
            
            client.export_failure_log("TestError", "Test message")
            self.wait_for_async_request()
            TelemetryClientFactory.close(client._session_id_hex)
            
            assert mock_get_conn.return_value.getresponse.call_count == 2

    def test_respects_retry_after_header(self, caplog):
        client, _ = self.get_client("session-retry-after")
        with patch(PATCH_TARGET) as mock_get_conn:
            mock_get_conn.return_value.getresponse.side_effect = [
                create_urllib3_response(429, headers={'Retry-After': '1'}),  # Use integer seconds to avoid parsing issues
                create_urllib3_response(200)
            ]
            
            client.export_failure_log("TestError", "Test message")
            self.wait_for_async_request()
            TelemetryClientFactory.close(client._session_id_hex)
            
            # Check that the request was retried (should be 2 calls: initial + 1 retry)
            assert mock_get_conn.return_value.getresponse.call_count == 2
            assert "Retrying after" in caplog.text

    def test_exceeds_retry_count_limit(self, caplog):
        client, _ = self.get_client("session-exceed-limit", total_retries=3)
        expected_call_count = 4
        with patch(PATCH_TARGET) as mock_get_conn:
            mock_get_conn.return_value.getresponse.return_value = create_urllib3_response(503)
            
            client.export_failure_log("TestError", "Test message")
            self.wait_for_async_request()
            TelemetryClientFactory.close(client._session_id_hex)
            
            assert mock_get_conn.return_value.getresponse.call_count == expected_call_count
            assert "Telemetry request failed with exception" in caplog.text
            assert "Max retries exceeded" in caplog.text

    def test_no_retry_on_401_unauthorized(self, caplog):
        """Test that 401 responses are not retried (per retry policy)"""
        client, _ = self.get_client("session-401")
        with patch(PATCH_TARGET) as mock_get_conn:
            mock_get_conn.return_value.getresponse.return_value = create_urllib3_response(401)
            
            client.export_failure_log("TestError", "Test message")
            self.wait_for_async_request()
            TelemetryClientFactory.close(client._session_id_hex)
            
            # 401 should not be retried based on the retry policy
            mock_get_conn.return_value.getresponse.assert_called_once()
            assert "Telemetry request failed with status code: 401" in caplog.text

    def test_retries_on_400_bad_request(self, caplog):
        """Test that 400 responses are retried (this is the current behavior for telemetry)"""
        client, _ = self.get_client("session-400")
        with patch(PATCH_TARGET) as mock_get_conn:
            mock_get_conn.return_value.getresponse.return_value = create_urllib3_response(400)
            
            client.export_failure_log("TestError", "Test message")
            self.wait_for_async_request()
            TelemetryClientFactory.close(client._session_id_hex)
            
            # Based on the logs, 400 IS being retried (this is the actual behavior for CommandType.OTHER)
            expected_call_count = 4  # total + 1 (initial + 3 retries)
            assert mock_get_conn.return_value.getresponse.call_count == expected_call_count
            assert "Telemetry request failed with exception" in caplog.text
            assert "Max retries exceeded" in caplog.text

    def test_no_retry_on_403_forbidden(self, caplog):
        """Test that 403 responses are not retried (per retry policy)"""
        client, _ = self.get_client("session-403")
        with patch(PATCH_TARGET) as mock_get_conn:
            mock_get_conn.return_value.getresponse.return_value = create_urllib3_response(403)
            
            client.export_failure_log("TestError", "Test message")
            self.wait_for_async_request()
            TelemetryClientFactory.close(client._session_id_hex)
            
            # 403 should not be retried based on the retry policy
            mock_get_conn.return_value.getresponse.assert_called_once()
            assert "Telemetry request failed with status code: 403" in caplog.text

    def test_retry_policy_command_type_is_set_to_other(self):
        client, adapter = self.get_client("session-command-type")
        
        original_send = adapter.send
        @wraps(original_send)
        def wrapper(request, **kwargs):
            assert adapter.max_retries.command_type == CommandType.OTHER
            return original_send(request, **kwargs)

        with patch.object(adapter, 'send', side_effect=wrapper, autospec=True), \
             patch(PATCH_TARGET) as mock_get_conn:
            mock_get_conn.return_value.getresponse.return_value = create_urllib3_response(200)
            
            client.export_failure_log("TestError", "Test message")
            self.wait_for_async_request()
            TelemetryClientFactory.close(client._session_id_hex)
            
            assert adapter.send.call_count == 1