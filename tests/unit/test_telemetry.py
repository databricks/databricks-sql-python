import uuid
import pytest
import requests
from unittest.mock import patch, MagicMock, call

from databricks.sql.telemetry.telemetry_client import (
    TelemetryClient,
    NOOP_TELEMETRY_CLIENT,
    initialize_telemetry_client,
    get_telemetry_client,
    close_telemetry_client,
    TelemetryHelper,
    BaseTelemetryClient
)
from databricks.sql.telemetry.models.enums import (
    AuthMech,
    DatabricksClientType,
    AuthFlow,
)
from databricks.sql.telemetry.models.event import (
    DriverConnectionParameters,
    HostDetails,
)
from databricks.sql.auth.authenticators import (
    AccessTokenAuthProvider,
    DatabricksOAuthProvider,
    ExternalAuthProvider,
)


@pytest.fixture
def noop_telemetry_client():
    """Fixture for NOOP_TELEMETRY_CLIENT."""
    return NOOP_TELEMETRY_CLIENT


@pytest.fixture
def telemetry_client_setup():
    """Fixture for TelemetryClient setup data."""
    session_id_hex = str(uuid.uuid4())
    auth_provider = AccessTokenAuthProvider("test-token")
    host_url = "test-host"
    executor = MagicMock()
    
    client = TelemetryClient(
        telemetry_enabled=True,
        session_id_hex=session_id_hex,
        auth_provider=auth_provider,
        host_url=host_url,
        executor=executor,
    )
    
    return {
        "client": client,
        "session_id_hex": session_id_hex,
        "auth_provider": auth_provider,
        "host_url": host_url,
        "executor": executor,
    }


@pytest.fixture
def telemetry_system_reset():
    """Fixture to reset telemetry system state before each test."""
    # Reset the static state before each test
    from databricks.sql.telemetry.telemetry_client import _clients, _executor, _initialized
    _clients.clear()
    if _executor:
        _executor.shutdown(wait=True)
    _executor = None
    _initialized = False
    yield
    # Cleanup after test if needed
    _clients.clear()
    if _executor:
        _executor.shutdown(wait=True)
    _executor = None
    _initialized = False


class TestNoopTelemetryClient:
    """Tests for the NOOP_TELEMETRY_CLIENT."""
   
    def test_export_initial_telemetry_log(self, noop_telemetry_client):
        """Test that export_initial_telemetry_log does nothing."""
        noop_telemetry_client.export_initial_telemetry_log(
            driver_connection_params=MagicMock(), user_agent="test"
        )

    def test_export_failure_log(self, noop_telemetry_client):
        """Test that export_failure_log does nothing."""
        noop_telemetry_client.export_failure_log(
            error_name="TestError", error_message="Test error message"
        )

    def test_close(self, noop_telemetry_client):
        """Test that close does nothing."""
        noop_telemetry_client.close()


class TestTelemetryClient:
    """Tests for the TelemetryClient class."""

    @patch("databricks.sql.telemetry.telemetry_client.TelemetryFrontendLog")
    @patch("databricks.sql.telemetry.telemetry_client.TelemetryHelper.get_driver_system_configuration")
    @patch("databricks.sql.telemetry.telemetry_client.uuid.uuid4")
    @patch("databricks.sql.telemetry.telemetry_client.time.time")
    def test_export_initial_telemetry_log(
        self, 
        mock_time, 
        mock_uuid4, 
        mock_get_driver_config, 
        mock_frontend_log,
        telemetry_client_setup
    ):
        """Test exporting initial telemetry log."""
        mock_time.return_value = 1000
        mock_uuid4.return_value = "test-uuid"
        mock_get_driver_config.return_value = "test-driver-config"
        mock_frontend_log.return_value = MagicMock()

        client = telemetry_client_setup["client"]
        host_url = telemetry_client_setup["host_url"]
        client._export_event = MagicMock()
               
        driver_connection_params = DriverConnectionParameters(
            http_path="test-path",
            mode=DatabricksClientType.THRIFT,
            host_info=HostDetails(host_url=host_url, port=443),
            auth_mech=AuthMech.PAT,
            auth_flow=None,
        )
        user_agent = "test-user-agent"
        
        client.export_initial_telemetry_log(driver_connection_params, user_agent)
        
        mock_frontend_log.assert_called_once()
        client._export_event.assert_called_once_with(mock_frontend_log.return_value)

    @patch("databricks.sql.telemetry.telemetry_client.TelemetryFrontendLog")
    @patch("databricks.sql.telemetry.telemetry_client.TelemetryHelper.get_driver_system_configuration")
    @patch("databricks.sql.telemetry.telemetry_client.DriverErrorInfo")
    @patch("databricks.sql.telemetry.telemetry_client.uuid.uuid4")
    @patch("databricks.sql.telemetry.telemetry_client.time.time")
    def test_export_failure_log(
        self, 
        mock_time, 
        mock_uuid4, 
        mock_driver_error_info,
        mock_get_driver_config, 
        mock_frontend_log,
        telemetry_client_setup
    ):
        """Test exporting failure telemetry log."""
        mock_time.return_value = 2000
        mock_uuid4.return_value = "test-error-uuid"
        mock_get_driver_config.return_value = "test-driver-config"
        mock_driver_error_info.return_value = MagicMock()
        mock_frontend_log.return_value = MagicMock()

        client = telemetry_client_setup["client"]
        client._export_event = MagicMock()
        
        client._driver_connection_params = "test-connection-params"
        client._user_agent = "test-user-agent"
        
        error_name = "TestError"
        error_message = "This is a test error message"
        
        client.export_failure_log(error_name, error_message)
        
        mock_driver_error_info.assert_called_once_with(
            error_name=error_name, 
            stack_trace=error_message
        )
        
        mock_frontend_log.assert_called_once()
        
        client._export_event.assert_called_once_with(mock_frontend_log.return_value)

    def test_export_event(self, telemetry_client_setup):
        """Test exporting an event."""
        client = telemetry_client_setup["client"]
        client._flush = MagicMock()
        
        for i in range(5):
            client._export_event(f"event-{i}")
        
        client._flush.assert_not_called()
        assert len(client._events_batch) == 5
        
        for i in range(5, 10):
            client._export_event(f"event-{i}")
        
        client._flush.assert_called_once()
        assert len(client._events_batch) == 10

    @patch("requests.post")
    def test_send_telemetry_authenticated(self, mock_post, telemetry_client_setup):
        """Test sending telemetry to the server with authentication."""
        client = telemetry_client_setup["client"]
        executor = telemetry_client_setup["executor"]
        
        events = [MagicMock(), MagicMock()]
        events[0].to_json.return_value = '{"event": "1"}'
        events[1].to_json.return_value = '{"event": "2"}'
        
        client._send_telemetry(events)
        
        executor.submit.assert_called_once()
        args, kwargs = executor.submit.call_args
        assert args[0] == requests.post
        assert kwargs["timeout"] == 10
        assert "Authorization" in kwargs["headers"]
        assert kwargs["headers"]["Authorization"] == "Bearer test-token"

    @patch("requests.post")
    def test_send_telemetry_unauthenticated(self, mock_post, telemetry_client_setup):
        """Test sending telemetry to the server without authentication."""
        host_url = telemetry_client_setup["host_url"]
        executor = telemetry_client_setup["executor"]
        
        unauthenticated_client = TelemetryClient(
            telemetry_enabled=True,
            session_id_hex=str(uuid.uuid4()),
            auth_provider=None,  # No auth provider
            host_url=host_url,
            executor=executor,
        )
        
        events = [MagicMock(), MagicMock()]
        events[0].to_json.return_value = '{"event": "1"}'
        events[1].to_json.return_value = '{"event": "2"}'
        
        unauthenticated_client._send_telemetry(events)
        
        executor.submit.assert_called_once()
        args, kwargs = executor.submit.call_args
        assert args[0] == requests.post
        assert kwargs["timeout"] == 10
        assert "Authorization" not in kwargs["headers"]  # No auth header
        assert kwargs["headers"]["Accept"] == "application/json"
        assert kwargs["headers"]["Content-Type"] == "application/json"

    def test_flush(self, telemetry_client_setup):
        """Test flushing events."""
        client = telemetry_client_setup["client"]
        client._events_batch = ["event1", "event2"]
        client._send_telemetry = MagicMock()
        
        client._flush()
        
        client._send_telemetry.assert_called_once_with(["event1", "event2"])
        assert client._events_batch == []

    def test_close(self, telemetry_client_setup):
        """Test closing the client."""
        client = telemetry_client_setup["client"]
        client._flush = MagicMock()
        
        client.close()
        
        client._flush.assert_called_once()

    @patch("requests.post")
    def test_telemetry_request_callback_success(self, mock_post, telemetry_client_setup):
        """Test successful telemetry request callback."""
        client = telemetry_client_setup["client"]
        
        mock_response = MagicMock()
        mock_response.status_code = 200
       
        mock_future = MagicMock()
        mock_future.result.return_value = mock_response
       
        client._telemetry_request_callback(mock_future)
       
        mock_future.result.assert_called_once()

    @patch("requests.post")
    def test_telemetry_request_callback_failure(self, mock_post, telemetry_client_setup):
        """Test telemetry request callback with failure"""
        client = telemetry_client_setup["client"]
        
        # Test with non-200 status code
        mock_response = MagicMock()
        mock_response.status_code = 500
        future = MagicMock()
        future.result.return_value = mock_response
        client._telemetry_request_callback(future)

        # Test with exception
        future = MagicMock()
        future.result.side_effect = Exception("Test error")
        client._telemetry_request_callback(future)

    def test_telemetry_client_exception_handling(self, telemetry_client_setup):
        """Test exception handling in telemetry client methods."""
        client = telemetry_client_setup["client"]
        
        # Test export_initial_telemetry_log with exception
        with patch.object(client, '_export_event', side_effect=Exception("Test error")):
            # Should not raise exception
            client.export_initial_telemetry_log(MagicMock(), "test-agent")
        
        # Test export_failure_log with exception
        with patch.object(client, '_export_event', side_effect=Exception("Test error")):
            # Should not raise exception
            client.export_failure_log("TestError", "Test error message")
        
        # Test _send_telemetry with exception
        with patch.object(client._executor, 'submit', side_effect=Exception("Test error")):
            # Should not raise exception
            client._send_telemetry([MagicMock()])

    def test_send_telemetry_thread_pool_failure(self, telemetry_client_setup):
        """Test handling of thread pool submission failure"""
        client = telemetry_client_setup["client"]
        client._executor.submit.side_effect = Exception("Thread pool error")      
        event = MagicMock()
        client._send_telemetry([event])

    def test_base_telemetry_client_abstract_methods(self):
        """Test that BaseTelemetryClient cannot be instantiated without implementing all abstract methods"""
        class TestBaseClient(BaseTelemetryClient):
            pass

        with pytest.raises(TypeError):
            TestBaseClient()  # Can't instantiate abstract class


class TestTelemetrySystem:
    """Tests for the telemetry system functions."""

    def test_initialize_telemetry_client_enabled(self, telemetry_system_reset):
        """Test initializing a telemetry client when telemetry is enabled."""
        session_id_hex = "test-uuid"
        auth_provider = MagicMock()
        host_url = "test-host"
        
        initialize_telemetry_client(
            telemetry_enabled=True,
            session_id_hex=session_id_hex,
            auth_provider=auth_provider,
            host_url=host_url,
        )
        
        client = get_telemetry_client(session_id_hex)
        assert isinstance(client, TelemetryClient)
        assert client._session_id_hex == session_id_hex
        assert client._auth_provider == auth_provider
        assert client._host_url == host_url

    def test_initialize_telemetry_client_disabled(self, telemetry_system_reset):
        """Test initializing a telemetry client when telemetry is disabled."""
        session_id_hex = "test-uuid"
        initialize_telemetry_client(
            telemetry_enabled=False,
            session_id_hex=session_id_hex,
            auth_provider=MagicMock(),
            host_url="test-host",
        )
        
        client = get_telemetry_client(session_id_hex)
        assert client is NOOP_TELEMETRY_CLIENT

    def test_get_telemetry_client_nonexistent(self, telemetry_system_reset):
        """Test getting a non-existent telemetry client."""
        client = get_telemetry_client("nonexistent-uuid")
        assert client is NOOP_TELEMETRY_CLIENT

    def test_close_telemetry_client(self, telemetry_system_reset):
        """Test closing a telemetry client."""
        session_id_hex = "test-uuid"
        auth_provider = MagicMock()
        host_url = "test-host"
        
        initialize_telemetry_client(
            telemetry_enabled=True,
            session_id_hex=session_id_hex,
            auth_provider=auth_provider,
            host_url=host_url,
        )
        
        client = get_telemetry_client(session_id_hex)
        assert isinstance(client, TelemetryClient)
        
        client.close = MagicMock()
        
        close_telemetry_client(session_id_hex)
        
        client.close.assert_called_once()
        
        client = get_telemetry_client(session_id_hex)
        assert client is NOOP_TELEMETRY_CLIENT

    def test_close_telemetry_client_noop(self, telemetry_system_reset):
        """Test closing a no-op telemetry client."""
        session_id_hex = "test-uuid"
        initialize_telemetry_client(
            telemetry_enabled=False,
            session_id_hex=session_id_hex,
            auth_provider=MagicMock(),
            host_url="test-host",
        )
        
        client = get_telemetry_client(session_id_hex)
        assert client is NOOP_TELEMETRY_CLIENT
        
        client.close = MagicMock()
        
        close_telemetry_client(session_id_hex)
        
        client.close.assert_called_once()
        
        client = get_telemetry_client(session_id_hex)
        assert client is NOOP_TELEMETRY_CLIENT

    @patch("databricks.sql.telemetry.telemetry_client._handle_unhandled_exception")
    def test_global_exception_hook(self, mock_handle_exception, telemetry_system_reset):
        """Test that global exception hook is installed and handles exceptions."""
        from databricks.sql.telemetry.telemetry_client import _install_exception_hook, _handle_unhandled_exception
            
        _install_exception_hook()
        
        test_exception = ValueError("Test exception")
        _handle_unhandled_exception(type(test_exception), test_exception, None)
        
        mock_handle_exception.assert_called_once_with(type(test_exception), test_exception, None)


class TestTelemetryHelper:
    """Tests for the TelemetryHelper class."""

    def test_get_driver_system_configuration(self):
        """Test getting driver system configuration."""
        config = TelemetryHelper.get_driver_system_configuration()
          
        assert isinstance(config.driver_name, str)
        assert isinstance(config.driver_version, str)
        assert isinstance(config.runtime_name, str)
        assert isinstance(config.runtime_vendor, str)
        assert isinstance(config.runtime_version, str)
        assert isinstance(config.os_name, str)
        assert isinstance(config.os_version, str)
        assert isinstance(config.os_arch, str)
        assert isinstance(config.locale_name, str)
        assert isinstance(config.char_set_encoding, str)
        
        assert config.driver_name == "Databricks SQL Python Connector"
        assert "Python" in config.runtime_name
        assert config.runtime_vendor in ["CPython", "PyPy", "Jython", "IronPython"]
        assert config.os_name in ["Darwin", "Linux", "Windows"]
        
        # Verify caching behavior
        config2 = TelemetryHelper.get_driver_system_configuration()
        assert config is config2  # Should return same instance

    def test_get_auth_mechanism(self):
        """Test getting auth mechanism for different auth providers."""
        # Test PAT auth
        pat_auth = AccessTokenAuthProvider("test-token")
        assert TelemetryHelper.get_auth_mechanism(pat_auth) == AuthMech.PAT
        
        # Test OAuth auth
        oauth_auth = MagicMock(spec=DatabricksOAuthProvider)
        assert TelemetryHelper.get_auth_mechanism(oauth_auth) == AuthMech.DATABRICKS_OAUTH
        
        # Test External auth
        external_auth = MagicMock(spec=ExternalAuthProvider)
        assert TelemetryHelper.get_auth_mechanism(external_auth) == AuthMech.EXTERNAL_AUTH
        
        # Test None auth provider
        assert TelemetryHelper.get_auth_mechanism(None) is None
        
        # Test unknown auth provider
        unknown_auth = MagicMock()
        assert TelemetryHelper.get_auth_mechanism(unknown_auth) == AuthMech.CLIENT_CERT

    def test_get_auth_flow(self):
        """Test getting auth flow for different OAuth providers."""
        # Test OAuth with existing tokens
        oauth_with_tokens = MagicMock(spec=DatabricksOAuthProvider)
        oauth_with_tokens._access_token = "test-access-token"
        oauth_with_tokens._refresh_token = "test-refresh-token"
        assert TelemetryHelper.get_auth_flow(oauth_with_tokens) == AuthFlow.TOKEN_PASSTHROUGH
        
        # Test OAuth with browser-based auth
        oauth_with_browser = MagicMock(spec=DatabricksOAuthProvider)
        oauth_with_browser._access_token = None
        oauth_with_browser._refresh_token = None
        oauth_with_browser.oauth_manager = MagicMock()
        assert TelemetryHelper.get_auth_flow(oauth_with_browser) == AuthFlow.BROWSER_BASED_AUTHENTICATION
        
        # Test non-OAuth provider
        pat_auth = AccessTokenAuthProvider("test-token")
        assert TelemetryHelper.get_auth_flow(pat_auth) is None
        
        # Test None auth provider
        assert TelemetryHelper.get_auth_flow(None) is None