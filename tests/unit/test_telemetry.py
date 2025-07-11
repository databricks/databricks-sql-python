import uuid
import pytest
import requests
from unittest.mock import patch, MagicMock
import threading
import random
import time
from concurrent.futures import ThreadPoolExecutor

from databricks.sql.telemetry.telemetry_client import (
    TelemetryClient,
    NoopTelemetryClient,
    TelemetryClientFactory,
    TelemetryHelper,
)
from databricks.sql.telemetry.models.enums import AuthMech, AuthFlow
from databricks.sql.auth.authenticators import (
    AccessTokenAuthProvider,
    DatabricksOAuthProvider,
    ExternalAuthProvider,
)


@pytest.fixture
def mock_telemetry_client():
    """Create a mock telemetry client for testing."""
    session_id = str(uuid.uuid4())
    auth_provider = AccessTokenAuthProvider("test-token")
    executor = MagicMock()
    
    return TelemetryClient(
        telemetry_enabled=True,
        session_id_hex=session_id,
        auth_provider=auth_provider,
        host_url="test-host.com",
        executor=executor,
    )


class TestNoopTelemetryClient:
    """Tests for NoopTelemetryClient - should do nothing safely."""

    def test_noop_client_behavior(self):
        """Test that NoopTelemetryClient is a singleton and all methods are safe no-ops."""
        # Test singleton behavior
        client1 = NoopTelemetryClient()
        client2 = NoopTelemetryClient()
        assert client1 is client2
        
        # Test that all methods can be called without exceptions
        client1.export_initial_telemetry_log(MagicMock(), "test-agent")
        client1.export_failure_log("TestError", "Test message")
        client1.export_latency_log(100, "EXECUTE_STATEMENT", "test-id")
        client1.close()


class TestTelemetryClient:
    """Tests for actual telemetry client functionality and flows."""

    def test_event_batching_and_flushing_flow(self, mock_telemetry_client):
        """Test the complete event batching and flushing flow."""
        client = mock_telemetry_client
        client._batch_size = 3  # Small batch for testing
        
        # Mock the network call
        with patch.object(client, '_send_telemetry') as mock_send:
            # Add events one by one - should not flush yet
            client._export_event("event1")
            client._export_event("event2")
            mock_send.assert_not_called()
            assert len(client._events_batch) == 2
            
            # Third event should trigger flush
            client._export_event("event3")
            mock_send.assert_called_once()
            assert len(client._events_batch) == 0  # Batch cleared after flush
    
    @patch('requests.post')
    def test_network_request_flow(self, mock_post, mock_telemetry_client):
        """Test the complete network request flow with authentication."""
        mock_post.return_value.status_code = 200
        client = mock_telemetry_client
        
        # Create mock events
        mock_events = [MagicMock() for _ in range(2)]
        for i, event in enumerate(mock_events):
            event.to_json.return_value = f'{{"event": "{i}"}}'
        
        # Send telemetry
        client._send_telemetry(mock_events)
        
        # Verify request was submitted to executor
        client._executor.submit.assert_called_once()
        args, kwargs = client._executor.submit.call_args
        
        # Verify correct function and URL
        assert args[0] == requests.post
        assert args[1] == 'https://test-host.com/telemetry-ext'
        assert kwargs['headers']['Authorization'] == 'Bearer test-token'
        
        # Verify request body structure
        request_data = kwargs['data']
        assert '"uploadTime"' in request_data
        assert '"protoLogs"' in request_data

    def test_telemetry_logging_flows(self, mock_telemetry_client):
        """Test all telemetry logging methods work end-to-end."""
        client = mock_telemetry_client
        
        with patch.object(client, '_export_event') as mock_export:
            # Test initial log
            client.export_initial_telemetry_log(MagicMock(), "test-agent")
            assert mock_export.call_count == 1
            
            # Test failure log
            client.export_failure_log("TestError", "Error message")
            assert mock_export.call_count == 2
            
            # Test latency log
            client.export_latency_log(150, "EXECUTE_STATEMENT", "stmt-123")
            assert mock_export.call_count == 3

    def test_error_handling_resilience(self, mock_telemetry_client):
        """Test that telemetry errors don't break the client."""
        client = mock_telemetry_client
        
        # Test that exceptions in telemetry don't propagate
        with patch.object(client, '_export_event', side_effect=Exception("Test error")):
            # These should not raise exceptions
            client.export_initial_telemetry_log(MagicMock(), "test-agent")
            client.export_failure_log("TestError", "Error message")
            client.export_latency_log(100, "EXECUTE_STATEMENT", "stmt-123")
        
        # Test executor submission failure
        client._executor.submit.side_effect = Exception("Thread pool error")
        client._send_telemetry([MagicMock()])  # Should not raise


class TestTelemetryHelper:
    """Tests for TelemetryHelper utility functions."""

    def test_system_configuration_caching(self):
        """Test that system configuration is cached and contains expected data."""
        config1 = TelemetryHelper.get_driver_system_configuration()
        config2 = TelemetryHelper.get_driver_system_configuration()
        
        # Should be cached (same instance)
        assert config1 is config2

    def test_auth_mechanism_detection(self):
        """Test authentication mechanism detection for different providers."""
        test_cases = [
            (AccessTokenAuthProvider("token"), AuthMech.PAT),
            (MagicMock(spec=DatabricksOAuthProvider), AuthMech.OAUTH),
            (MagicMock(spec=ExternalAuthProvider), AuthMech.OTHER),
            (MagicMock(), AuthMech.OTHER),  # Unknown provider
            (None, None),
        ]
        
        for provider, expected in test_cases:
            assert TelemetryHelper.get_auth_mechanism(provider) == expected

    def test_auth_flow_detection(self):
        """Test authentication flow detection for OAuth providers."""
        # OAuth with existing tokens
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


class TestTelemetryFactory:
    """Tests for TelemetryClientFactory lifecycle and management."""

    @pytest.fixture(autouse=True)
    def telemetry_system_reset(self):
        """Reset telemetry system state before each test."""
        TelemetryClientFactory._clients.clear()
        if TelemetryClientFactory._executor:
            TelemetryClientFactory._executor.shutdown(wait=True)
        TelemetryClientFactory._executor = None
        TelemetryClientFactory._initialized = False
        yield
        TelemetryClientFactory._clients.clear()
        if TelemetryClientFactory._executor:
            TelemetryClientFactory._executor.shutdown(wait=True)
        TelemetryClientFactory._executor = None
        TelemetryClientFactory._initialized = False

    def test_client_lifecycle_flow(self):
        """Test complete client lifecycle: initialize -> use -> close."""
        session_id_hex = "test-session"
        auth_provider = AccessTokenAuthProvider("token")
        
        # Initialize enabled client
        TelemetryClientFactory.initialize_telemetry_client(
            telemetry_enabled=True,
            session_id_hex=session_id_hex,
            auth_provider=auth_provider,
            host_url="test-host.com"
        )
        
        client = TelemetryClientFactory.get_telemetry_client(session_id_hex)
        assert isinstance(client, TelemetryClient)
        assert client._session_id_hex == session_id_hex
        
        # Close client
        with patch.object(client, 'close') as mock_close:
            TelemetryClientFactory.close(session_id_hex)
            mock_close.assert_called_once()
        
        # Should get NoopTelemetryClient after close
        client = TelemetryClientFactory.get_telemetry_client(session_id_hex)
        assert isinstance(client, NoopTelemetryClient)

    def test_disabled_telemetry_flow(self):
        """Test that disabled telemetry uses NoopTelemetryClient."""
        session_id_hex = "test-session"
        
        TelemetryClientFactory.initialize_telemetry_client(
            telemetry_enabled=False,
            session_id_hex=session_id_hex,
            auth_provider=None,
            host_url="test-host.com"
        )
        
        client = TelemetryClientFactory.get_telemetry_client(session_id_hex)
        assert isinstance(client, NoopTelemetryClient)

    def test_factory_error_handling(self):
        """Test that factory errors fall back to NoopTelemetryClient."""
        session_id = "test-session"
        
        # Simulate initialization error
        with patch('databricks.sql.telemetry.telemetry_client.TelemetryClient', 
                  side_effect=Exception("Init error")):
            TelemetryClientFactory.initialize_telemetry_client(
                telemetry_enabled=True,
                session_id_hex=session_id,
                auth_provider=AccessTokenAuthProvider("token"),
                host_url="test-host.com"
            )
        
        # Should fall back to NoopTelemetryClient
        client = TelemetryClientFactory.get_telemetry_client(session_id)
        assert isinstance(client, NoopTelemetryClient)

    def test_factory_shutdown_flow(self):
        """Test factory shutdown when last client is removed."""
        session1 = "session-1"
        session2 = "session-2"
        
        # Initialize multiple clients
        for session in [session1, session2]:
            TelemetryClientFactory.initialize_telemetry_client(
                telemetry_enabled=True,
                session_id_hex=session,
                auth_provider=AccessTokenAuthProvider("token"),
                host_url="test-host.com"
            )
        
        # Factory should be initialized
        assert TelemetryClientFactory._initialized is True
        assert TelemetryClientFactory._executor is not None
        
        # Close first client - factory should stay initialized
        TelemetryClientFactory.close(session1)
        assert TelemetryClientFactory._initialized is True
        
        # Close second client - factory should shut down
        TelemetryClientFactory.close(session2)
        assert TelemetryClientFactory._initialized is False
        assert TelemetryClientFactory._executor is None

# A helper function to run a target in multiple threads and wait for them.
def run_in_threads(target, num_threads, pass_index=False):
    """Creates, starts, and joins a specified number of threads.
    
    Args:
        target: The function to run in each thread
        num_threads: Number of threads to create
        pass_index: If True, passes the thread index (0, 1, 2, ...) as first argument
    """
    threads = [
        threading.Thread(target=target, args=(i,) if pass_index else ())
        for i in range(num_threads)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

class TestTelemetryRaceConditions:
    """Tests for race conditions in multithreaded scenarios."""

    @pytest.fixture(autouse=True)
    def clean_factory(self):
        """A fixture to automatically reset the factory's state before each test."""
        # Clean up at the start of each test
        if TelemetryClientFactory._executor:
            TelemetryClientFactory._executor.shutdown(wait=True)
        TelemetryClientFactory._clients.clear()
        TelemetryClientFactory._executor = None
        TelemetryClientFactory._initialized = False

        yield

        # Clean up at the end of each test
        if TelemetryClientFactory._executor:
            TelemetryClientFactory._executor.shutdown(wait=True)
        TelemetryClientFactory._clients.clear()
        TelemetryClientFactory._executor = None
        TelemetryClientFactory._initialized = False

    def test_factory_concurrent_initialization_of_DIFFERENT_clients(self):
        """
        Tests that multiple threads creating DIFFERENT clients concurrently
        share a single ThreadPoolExecutor and all clients are created successfully.
        """
        num_threads = 20

        def create_client(thread_id):
            TelemetryClientFactory.initialize_telemetry_client(
                telemetry_enabled=True,
                session_id_hex=f"session_{thread_id}",
                auth_provider=None,
                host_url="test-host",
            )

        run_in_threads(create_client, 20, pass_index=True)

        # ASSERT: The factory was properly initialized
        assert TelemetryClientFactory._initialized is True
        assert TelemetryClientFactory._executor is not None
        assert isinstance(TelemetryClientFactory._executor, ThreadPoolExecutor)

        # ASSERT: All clients were successfully created
        assert len(TelemetryClientFactory._clients) == num_threads

        # ASSERT: All TelemetryClient instances share the same executor
        telemetry_clients = [
            client for client in TelemetryClientFactory._clients.values()
            if isinstance(client, TelemetryClient)
        ]
        assert len(telemetry_clients) == num_threads

        shared_executor = TelemetryClientFactory._executor
        for client in telemetry_clients:
            assert client._executor is shared_executor

    def test_factory_concurrent_initialization_of_SAME_client(self):
        """
        Tests that multiple threads trying to initialize the SAME client
        result in only one client instance being created.
        """
        session_id = "shared-session"
        num_threads = 20

        def create_same_client():
            TelemetryClientFactory.initialize_telemetry_client(
                telemetry_enabled=True,
                session_id_hex=session_id,
                auth_provider=None,
                host_url="test-host",
            )

        run_in_threads(create_same_client, num_threads)

        # ASSERT: Only one client was created in the factory.
        assert len(TelemetryClientFactory._clients) == 1
        client = TelemetryClientFactory.get_telemetry_client(session_id)
        assert isinstance(client, TelemetryClient)

    def test_client_concurrent_event_export(self):
        """
        Tests that no events are lost when multiple threads call _export_event
        on the same client instance concurrently.
        """
        client = TelemetryClient(True, "session-1", None, "host", MagicMock())
        # Mock _flush to prevent auto-flushing when batch size threshold is reached
        original_flush = client._flush
        client._flush = MagicMock()

        num_threads = 5
        events_per_thread = 10

        def add_events():
            for i in range(events_per_thread):
                client._export_event(f"event-{i}")

        run_in_threads(add_events, num_threads)

        # ASSERT: The batch contains all events from all threads, none were lost.
        total_expected_events = num_threads * events_per_thread
        assert len(client._events_batch) == total_expected_events

        # Restore original flush method for cleanup
        client._flush = original_flush

    def test_client_concurrent_flush(self):
        """
        Tests that if multiple threads trigger _flush at the same time,
        the underlying send operation is only called once for the batch.
        """
        client = TelemetryClient(True, "session-1", None, "host", MagicMock())
        client._send_telemetry = MagicMock()

        # Pre-fill the batch so there's something to flush
        client._events_batch = ["event"] * 5

        def call_flush():
            client._flush()

        run_in_threads(call_flush, 10)

        # ASSERT: The send operation was called exactly once.
        # This proves the lock prevents multiple threads from sending the same batch.
        client._send_telemetry.assert_called_once()
        # ASSERT: The event batch is now empty.
        assert len(client._events_batch) == 0

    def test_factory_concurrent_create_and_close(self):
        """
        Tests that concurrently creating and closing different clients
        doesn't corrupt the factory state and correctly shuts down the executor.
        """
        num_ops = 50

        def create_and_close_client(i):
            session_id = f"session_{i}"
            TelemetryClientFactory.initialize_telemetry_client(
                telemetry_enabled=True, session_id_hex=session_id, auth_provider=None, host_url="host"
            )
            # Small sleep to increase chance of interleaving operations
            time.sleep(random.uniform(0, 0.01))
            TelemetryClientFactory.close(session_id)

        run_in_threads(create_and_close_client, num_ops, pass_index=True)

        # ASSERT: After all operations, the factory should be empty and reset.
        assert not TelemetryClientFactory._clients
        assert TelemetryClientFactory._executor is None
        assert not TelemetryClientFactory._initialized