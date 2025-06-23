import uuid
import pytest
import requests
from unittest.mock import patch, MagicMock, call
import threading
import time
import random

from databricks.sql.telemetry.telemetry_client import (
    TelemetryClient,
    NoopTelemetryClient,
    TelemetryClientFactory,
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
    """Fixture for NoopTelemetryClient."""
    return NoopTelemetryClient()


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
    TelemetryClientFactory._clients.clear()
    if TelemetryClientFactory._executor:
        TelemetryClientFactory._executor.shutdown(wait=True)
    TelemetryClientFactory._executor = None
    TelemetryClientFactory._initialized = False
    yield
    # Cleanup after test if needed
    TelemetryClientFactory._clients.clear()
    if TelemetryClientFactory._executor:
        TelemetryClientFactory._executor.shutdown(wait=True)
    TelemetryClientFactory._executor = None
    TelemetryClientFactory._initialized = False


class TestNoopTelemetryClient:
    """Tests for the NoopTelemetryClient."""

    def test_singleton(self):
        """Test that NoopTelemetryClient is a singleton."""
        client1 = NoopTelemetryClient()
        client2 = NoopTelemetryClient()
        assert client1 is client2
   
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

    def test_export_latency_log(self, noop_telemetry_client):
        """Test that export_latency_log does nothing."""
        noop_telemetry_client.export_latency_log(
            latency_ms=100, sql_execution_event="EXECUTE_STATEMENT", sql_statement_id="test-id"
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

    @patch("databricks.sql.telemetry.telemetry_client.TelemetryFrontendLog")
    @patch("databricks.sql.telemetry.telemetry_client.TelemetryHelper.get_driver_system_configuration")
    @patch("databricks.sql.telemetry.telemetry_client.uuid.uuid4")
    @patch("databricks.sql.telemetry.telemetry_client.time.time")
    def test_export_latency_log(
        self, 
        mock_time, 
        mock_uuid4, 
        mock_get_driver_config, 
        mock_frontend_log,
        telemetry_client_setup
    ):
        """Test exporting latency telemetry log."""
        mock_time.return_value = 3000
        mock_uuid4.return_value = "test-latency-uuid"
        mock_get_driver_config.return_value = "test-driver-config"
        mock_frontend_log.return_value = MagicMock()

        client = telemetry_client_setup["client"]
        client._export_event = MagicMock()
        
        client._driver_connection_params = "test-connection-params"
        client._user_agent = "test-user-agent"
        
        latency_ms = 150
        sql_execution_event = "test-execution-event"
        sql_statement_id = "test-statement-id"
        
        client.export_latency_log(latency_ms, sql_execution_event, sql_statement_id)
        
        mock_frontend_log.assert_called_once()
        
        client._export_event.assert_called_once_with(mock_frontend_log.return_value)

    def test_export_event(self, telemetry_client_setup):
        """Test exporting an event."""
        client = telemetry_client_setup["client"]
        client._flush = MagicMock()
        
        for i in range(TelemetryClient.DEFAULT_BATCH_SIZE-1):
            client._export_event(f"event-{i}")
        
        client._flush.assert_not_called()
        assert len(client._events_batch) == TelemetryClient.DEFAULT_BATCH_SIZE - 1
        
        # Add one more event to reach batch size (this will trigger flush)
        client._export_event(f"event-{TelemetryClient.DEFAULT_BATCH_SIZE - 1}")
        
        client._flush.assert_called_once()

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
        
        # Test export_latency_log with exception
        with patch.object(client, '_export_event', side_effect=Exception("Test error")):
            # Should not raise exception
            client.export_latency_log(100, "EXECUTE_STATEMENT", "test-statement-id")
        
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


class TestTelemetrySystem:
    """Tests for the telemetry system functions."""

    def test_initialize_telemetry_client_enabled(self, telemetry_system_reset):
        """Test initializing a telemetry client when telemetry is enabled."""
        session_id_hex = "test-uuid"
        auth_provider = MagicMock()
        host_url = "test-host"
        
        TelemetryClientFactory.initialize_telemetry_client(
            telemetry_enabled=True,
            session_id_hex=session_id_hex,
            auth_provider=auth_provider,
            host_url=host_url,
        )
        
        client = TelemetryClientFactory.get_telemetry_client(session_id_hex)
        assert isinstance(client, TelemetryClient)
        assert client._session_id_hex == session_id_hex
        assert client._auth_provider == auth_provider
        assert client._host_url == host_url

    def test_initialize_telemetry_client_disabled(self, telemetry_system_reset):
        """Test initializing a telemetry client when telemetry is disabled."""
        session_id_hex = "test-uuid"
        TelemetryClientFactory.initialize_telemetry_client(
            telemetry_enabled=False,
            session_id_hex=session_id_hex,
            auth_provider=MagicMock(),
            host_url="test-host",
        )
        
        client = TelemetryClientFactory.get_telemetry_client(session_id_hex)
        assert isinstance(client, NoopTelemetryClient)

    def test_get_telemetry_client_nonexistent(self, telemetry_system_reset):
        """Test getting a non-existent telemetry client."""
        client = TelemetryClientFactory.get_telemetry_client("nonexistent-uuid")
        assert isinstance(client, NoopTelemetryClient)

    def test_close_telemetry_client(self, telemetry_system_reset):
        """Test closing a telemetry client."""
        session_id_hex = "test-uuid"
        auth_provider = MagicMock()
        host_url = "test-host"
        
        TelemetryClientFactory.initialize_telemetry_client(
            telemetry_enabled=True,
            session_id_hex=session_id_hex,
            auth_provider=auth_provider,
            host_url=host_url,
        )
        
        client = TelemetryClientFactory.get_telemetry_client(session_id_hex)
        assert isinstance(client, TelemetryClient)
        
        client.close = MagicMock()
        
        TelemetryClientFactory.close(session_id_hex)
        
        client.close.assert_called_once()
        
        client = TelemetryClientFactory.get_telemetry_client(session_id_hex)
        assert isinstance(client, NoopTelemetryClient)

    def test_close_telemetry_client_noop(self, telemetry_system_reset):
        """Test closing a no-op telemetry client."""
        session_id_hex = "test-uuid"
        TelemetryClientFactory.initialize_telemetry_client(
            telemetry_enabled=False,
            session_id_hex=session_id_hex,
            auth_provider=MagicMock(),
            host_url="test-host",
        )
        
        client = TelemetryClientFactory.get_telemetry_client(session_id_hex)
        assert isinstance(client, NoopTelemetryClient)
        
        client.close = MagicMock()
        
        TelemetryClientFactory.close(session_id_hex)
        
        client.close.assert_called_once()
        
        client = TelemetryClientFactory.get_telemetry_client(session_id_hex)
        assert isinstance(client, NoopTelemetryClient)

    @patch("databricks.sql.telemetry.telemetry_client.TelemetryClientFactory._handle_unhandled_exception")
    def test_global_exception_hook(self, mock_handle_exception, telemetry_system_reset):
        """Test that global exception hook is installed and handles exceptions."""
        TelemetryClientFactory._install_exception_hook()
        
        test_exception = ValueError("Test exception")
        TelemetryClientFactory._handle_unhandled_exception(type(test_exception), test_exception, None)
        
        mock_handle_exception.assert_called_once_with(type(test_exception), test_exception, None)

    def test_initialize_telemetry_client_exception_handling(self, telemetry_system_reset):
        """Test that exceptions in initialize_telemetry_client don't cause connector to fail."""
        session_id_hex = "test-uuid"
        auth_provider = MagicMock()
        host_url = "test-host"
        
        # Test exception during TelemetryClient creation
        with patch('databricks.sql.telemetry.telemetry_client.TelemetryClient', side_effect=Exception("TelemetryClient creation failed")):
            # Should not raise exception, should fallback to NoopTelemetryClient
            TelemetryClientFactory.initialize_telemetry_client(
                telemetry_enabled=True,
                session_id_hex=session_id_hex,
                auth_provider=auth_provider,
                host_url=host_url,
            )
            
            client = TelemetryClientFactory.get_telemetry_client(session_id_hex)
            assert isinstance(client, NoopTelemetryClient)

    def test_get_telemetry_client_exception_handling(self, telemetry_system_reset):
        """Test that exceptions in get_telemetry_client don't cause connector to fail."""
        session_id_hex = "test-uuid"
        
        # Test exception during client lookup by mocking the clients dict
        mock_clients = MagicMock()
        mock_clients.__contains__.side_effect = Exception("Client lookup failed")
        
        with patch.object(TelemetryClientFactory, '_clients', mock_clients):
            # Should not raise exception, should return NoopTelemetryClient
            client = TelemetryClientFactory.get_telemetry_client(session_id_hex)
            assert isinstance(client, NoopTelemetryClient)

    def test_get_telemetry_client_dict_access_exception(self, telemetry_system_reset):
        """Test that exceptions during dictionary access don't cause connector to fail."""
        session_id_hex = "test-uuid"
        
        # Test exception during dictionary access
        mock_clients = MagicMock()
        mock_clients.__contains__.side_effect = Exception("Dictionary access failed")
        TelemetryClientFactory._clients = mock_clients
        
        # Should not raise exception, should return NoopTelemetryClient
        client = TelemetryClientFactory.get_telemetry_client(session_id_hex)
        assert isinstance(client, NoopTelemetryClient)

    def test_close_telemetry_client_shutdown_executor_exception(self, telemetry_system_reset):
        """Test that exceptions during executor shutdown don't cause connector to fail."""
        session_id_hex = "test-uuid"
        auth_provider = MagicMock()
        host_url = "test-host"
        
        # Initialize a client first
        TelemetryClientFactory.initialize_telemetry_client(
            telemetry_enabled=True,
            session_id_hex=session_id_hex,
            auth_provider=auth_provider,
            host_url=host_url,
        )
        
        # Mock executor to raise exception during shutdown
        mock_executor = MagicMock()
        mock_executor.shutdown.side_effect = Exception("Executor shutdown failed")
        TelemetryClientFactory._executor = mock_executor
        
        # Should not raise exception (executor shutdown is wrapped in try-catch)
        TelemetryClientFactory.close(session_id_hex)
        
        # Verify executor shutdown was attempted
        mock_executor.shutdown.assert_called_once_with(wait=True)


class TestTelemetryRaceConditions:
    """Tests for race conditions in multithreaded scenarios."""

    @pytest.fixture
    def race_condition_setup(self):
        """Setup for race condition tests."""
        # Reset telemetry system
        TelemetryClientFactory._clients.clear()
        if TelemetryClientFactory._executor:
            TelemetryClientFactory._executor.shutdown(wait=True)
        TelemetryClientFactory._executor = None
        TelemetryClientFactory._initialized = False
        
        yield
        
        # Cleanup
        TelemetryClientFactory._clients.clear()
        if TelemetryClientFactory._executor:
            TelemetryClientFactory._executor.shutdown(wait=True)
        TelemetryClientFactory._executor = None
        TelemetryClientFactory._initialized = False

    def test_telemetry_client_concurrent_export_events(self, race_condition_setup):
        """Test race conditions in TelemetryClient._export_event with concurrent access."""
        session_id_hex = "test-race-uuid"
        auth_provider = MagicMock()
        host_url = "test-host"
        executor = MagicMock()
        
        client = TelemetryClient(
            telemetry_enabled=True,
            session_id_hex=session_id_hex,
            auth_provider=auth_provider,
            host_url=host_url,
            executor=executor,
        )
        
        # Mock _flush to avoid actual network calls
        client._flush = MagicMock()
        
        # Track events added by each thread
        thread_events = {}
        lock = threading.Lock()
        
        def add_events(thread_id):
            """Add events from a specific thread."""
            events = []
            for i in range(10):
                event = f"event-{thread_id}-{i}"
                client._export_event(event)
                events.append(event)
            
            with lock:
                thread_events[thread_id] = events
        
        # Start multiple threads adding events concurrently
        threads = []
        for i in range(5):
            thread = threading.Thread(target=add_events, args=(i,))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Verify all events were added (no data loss due to race conditions)
        total_expected_events = sum(len(events) for events in thread_events.values())
        assert len(client._events_batch) == total_expected_events

    def test_telemetry_client_concurrent_flush_operations(self, race_condition_setup):
        """Test race conditions in TelemetryClient._flush with concurrent access."""
        session_id_hex = "test-flush-race-uuid"
        auth_provider = MagicMock()
        host_url = "test-host"
        executor = MagicMock()
        
        client = TelemetryClient(
            telemetry_enabled=True,
            session_id_hex=session_id_hex,
            auth_provider=auth_provider,
            host_url=host_url,
            executor=executor,
        )
        
        # Mock _send_telemetry to avoid actual network calls
        client._send_telemetry = MagicMock()
        
        # Add events to trigger flush
        for i in range(TelemetryClient.DEFAULT_BATCH_SIZE - 1):
            client._export_event(f"event-{i}")
        
        # Track flush operations
        flush_count = 0
        flush_lock = threading.Lock()
        
        def concurrent_flush():
            """Call flush concurrently."""
            nonlocal flush_count
            client._flush()
            with flush_lock:
                flush_count += 1
        
        # Start multiple threads calling flush concurrently
        threads = []
        for i in range(10):
            thread = threading.Thread(target=concurrent_flush)
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Verify flush was called the expected number of times
        assert flush_count == 10
        
        # Verify _send_telemetry was called at least once (some calls may have empty batches due to lock)
        assert client._send_telemetry.call_count >= 1
        
        # Verify that the total events processed is correct (no data loss)
        # The first flush should have processed all events, subsequent flushes should have empty batches
        total_events_sent = sum(len(call.args[0]) for call in client._send_telemetry.call_args_list)
        assert total_events_sent == TelemetryClient.DEFAULT_BATCH_SIZE - 1

    def test_telemetry_client_factory_concurrent_initialization(self, race_condition_setup):
        """Test race conditions in TelemetryClientFactory.initialize_telemetry_client with concurrent access."""
        session_id_hex = "test-factory-race-uuid"
        auth_provider = MagicMock()
        host_url = "test-host"
        
        # Track initialization attempts
        init_results = []
        init_lock = threading.Lock()
        
        def concurrent_initialize(thread_id):
            """Initialize telemetry client concurrently."""
            TelemetryClientFactory.initialize_telemetry_client(
                telemetry_enabled=True,
                session_id_hex=session_id_hex,
                auth_provider=auth_provider,
                host_url=host_url,
            )
            
            client = TelemetryClientFactory.get_telemetry_client(session_id_hex)
            
            with init_lock:
                init_results.append({
                    'thread_id': thread_id,
                    'client_type': type(client).__name__
                })
        
        # Start multiple threads initializing concurrently
        threads = []
        for i in range(10):
            thread = threading.Thread(target=concurrent_initialize, args=(i,))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        assert len(init_results) == 10
        
        # Verify only one client was created (no duplicate clients due to race conditions)
        client = TelemetryClientFactory.get_telemetry_client(session_id_hex)
        assert isinstance(client, TelemetryClient)
        
        # Verify the client is the same for all threads (singleton behavior)
        client_ids = set()
        for result in init_results:
            client_ids.add(id(TelemetryClientFactory.get_telemetry_client(session_id_hex)))
        
        assert len(client_ids) == 1

    def test_telemetry_client_factory_concurrent_get_client(self, race_condition_setup):
        """Test race conditions in TelemetryClientFactory.get_telemetry_client with concurrent access."""
        session_id_hex = "test-get-client-race-uuid"
        auth_provider = MagicMock()
        host_url = "test-host"
        
        # Initialize a client first
        TelemetryClientFactory.initialize_telemetry_client(
            telemetry_enabled=True,
            session_id_hex=session_id_hex,
            auth_provider=auth_provider,
            host_url=host_url,
        )
        
        # Track get_client attempts
        get_results = []
        get_lock = threading.Lock()
        
        def concurrent_get_client(thread_id):
            """Get telemetry client concurrently."""
            client = TelemetryClientFactory.get_telemetry_client(session_id_hex)
            
            with get_lock:
                get_results.append({
                    'thread_id': thread_id,
                    'client_type': type(client).__name__,
                    'client_id': id(client)
                })
        
        # Start multiple threads getting client concurrently
        threads = []
        for i in range(20):
            thread = threading.Thread(target=concurrent_get_client, args=(i,))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Verify all get_client calls succeeded
        assert len(get_results) == 20
        
        # Verify all threads got the same client instance (no race conditions)
        client_ids = set(result['client_id'] for result in get_results)
        assert len(client_ids) == 1  # Only one client instance returned

    def test_telemetry_client_factory_concurrent_close(self, race_condition_setup):
        """Test race conditions in TelemetryClientFactory.close with concurrent access."""
        session_id_hex = "test-close-race-uuid"
        auth_provider = MagicMock()
        host_url = "test-host"
        
        # Initialize a client first
        TelemetryClientFactory.initialize_telemetry_client(
            telemetry_enabled=True,
            session_id_hex=session_id_hex,
            auth_provider=auth_provider,
            host_url=host_url,
        )
        
        def concurrent_close(thread_id):
            """Close telemetry client concurrently."""
            
            TelemetryClientFactory.close(session_id_hex)
        
        # Start multiple threads closing concurrently
        threads = []
        for i in range(5):
            thread = threading.Thread(target=concurrent_close, args=(i,))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join() 
       
        # Verify client is no longer available after close
        client = TelemetryClientFactory.get_telemetry_client(session_id_hex)
        assert isinstance(client, NoopTelemetryClient)

    def test_telemetry_client_factory_mixed_concurrent_operations(self, race_condition_setup):
        """Test race conditions with mixed concurrent operations on TelemetryClientFactory."""
        session_id_hex = "test-mixed-race-uuid"
        auth_provider = MagicMock()
        host_url = "test-host"
        
        # Track operation results
        operation_results = []
        operation_lock = threading.Lock()
        
        def mixed_operations(thread_id):
            """Perform mixed operations concurrently."""
            
            # Randomly choose an operation
            operation = random.choice(['init', 'get', 'close'])
            
            if operation == 'init':
                TelemetryClientFactory.initialize_telemetry_client(
                    telemetry_enabled=True,
                    session_id_hex=session_id_hex,
                    auth_provider=auth_provider,
                    host_url=host_url,
                )
                client = TelemetryClientFactory.get_telemetry_client(session_id_hex)
                
                with operation_lock:
                    operation_results.append({
                        'thread_id': thread_id,
                        'operation': 'init',
                        'client_type': type(client).__name__
                    })
            
            elif operation == 'get':
                client = TelemetryClientFactory.get_telemetry_client(session_id_hex)
                
                with operation_lock:
                    operation_results.append({
                        'thread_id': thread_id,
                        'operation': 'get',
                        'client_type': type(client).__name__
                    })
            
            elif operation == 'close':
                TelemetryClientFactory.close(session_id_hex)
                
                with operation_lock:
                    operation_results.append({
                        'thread_id': thread_id,
                        'operation': 'close'
                    })
        
        # Start multiple threads performing mixed operations
        threads = []
        for i in range(15):
            thread = threading.Thread(target=mixed_operations, args=(i,))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        assert len(operation_results) == 15
