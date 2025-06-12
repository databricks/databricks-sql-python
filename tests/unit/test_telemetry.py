import uuid
import pytest
import requests
from unittest.mock import patch, MagicMock, call

from databricks.sql.telemetry.telemetry_client import (
    TelemetryClient,
    NoopTelemetryClient,
    TelemetryClientFactory,
)
from databricks.sql.telemetry.models.enums import (
    AuthMech,
    DatabricksClientType,
)
from databricks.sql.telemetry.models.event import (
    DriverConnectionParameters,
    HostDetails,
)
from databricks.sql.auth.authenticators import (
    AccessTokenAuthProvider,
)


@pytest.fixture
def noop_telemetry_client():
    """Fixture for NoopTelemetryClient."""
    return NoopTelemetryClient()


@pytest.fixture
def telemetry_client_setup():
    """Fixture for TelemetryClient setup data."""
    connection_uuid = str(uuid.uuid4())
    auth_provider = AccessTokenAuthProvider("test-token")
    host_url = "test-host"
    executor = MagicMock()
    
    client = TelemetryClient(
        telemetry_enabled=True,
        connection_uuid=connection_uuid,
        auth_provider=auth_provider,
        host_url=host_url,
        executor=executor,
    )
    
    return {
        "client": client,
        "connection_uuid": connection_uuid,
        "auth_provider": auth_provider,
        "host_url": host_url,
        "executor": executor,
    }


@pytest.fixture
def telemetry_factory_reset():
    """Fixture to reset TelemetryClientFactory state before each test."""
    # Reset the static class state before each test
    TelemetryClientFactory._clients = {}
    TelemetryClientFactory._executor = None
    TelemetryClientFactory._initialized = False
    yield
    # Cleanup after test if needed
    TelemetryClientFactory._clients = {}
    if TelemetryClientFactory._executor:
        TelemetryClientFactory._executor.shutdown(wait=True)
        TelemetryClientFactory._executor = None
    TelemetryClientFactory._initialized = False


class TestNoopTelemetryClient:
    """Tests for the NoopTelemetryClient class."""

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

    def test_close(self, noop_telemetry_client):
        """Test that close does nothing."""
        noop_telemetry_client.close()


class TestTelemetryClient:
    """Tests for the TelemetryClient class."""

    @patch("databricks.sql.telemetry.telemetry_client.TelemetryFrontendLog")
    @patch("databricks.sql.telemetry.telemetry_client.TelemetryHelper.getDriverSystemConfiguration")
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
        client.export_event = MagicMock()
               
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
        client.export_event.assert_called_once_with(mock_frontend_log.return_value)

    @patch("databricks.sql.telemetry.telemetry_client.TelemetryFrontendLog")
    @patch("databricks.sql.telemetry.telemetry_client.TelemetryHelper.getDriverSystemConfiguration")
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
        client.export_event = MagicMock()
        
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
        
        client.export_event.assert_called_once_with(mock_frontend_log.return_value)

    def test_export_event(self, telemetry_client_setup):
        """Test exporting an event."""
        client = telemetry_client_setup["client"]
        client.flush = MagicMock()
        
        for i in range(5):
            client.export_event(f"event-{i}")
        
        client.flush.assert_not_called()
        assert len(client._events_batch) == 5
        
        for i in range(5, 10):
            client.export_event(f"event-{i}")
        
        client.flush.assert_called_once()
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
            connection_uuid=str(uuid.uuid4()),
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
        
        client.flush()
        
        client._send_telemetry.assert_called_once_with(["event1", "event2"])
        assert client._events_batch == []

    @patch("databricks.sql.telemetry.telemetry_client.TelemetryClientFactory")
    def test_close(self, mock_factory_class, telemetry_client_setup):
        """Test closing the client."""
        client = telemetry_client_setup["client"]
        connection_uuid = telemetry_client_setup["connection_uuid"]
        client.flush = MagicMock()
        
        client.close()
        
        client.flush.assert_called_once()
        mock_factory_class.close.assert_called_once_with(connection_uuid)


class TestTelemetryClientFactory:
    """Tests for the TelemetryClientFactory static class."""

    @patch("databricks.sql.telemetry.telemetry_client.TelemetryClient")
    def test_initialize_telemetry_client_enabled(self, mock_client_class, telemetry_factory_reset):
        """Test initializing a telemetry client when telemetry is enabled."""
        connection_uuid = "test-uuid"
        auth_provider = MagicMock()
        host_url = "test-host"
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        
        TelemetryClientFactory.initialize_telemetry_client(
            telemetry_enabled=True,
            connection_uuid=connection_uuid,
            auth_provider=auth_provider,
            host_url=host_url,
        )
        
        # Verify a new client was created and stored
        mock_client_class.assert_called_once_with(
            telemetry_enabled=True,
            connection_uuid=connection_uuid,
            auth_provider=auth_provider,
            host_url=host_url,
            executor=TelemetryClientFactory._executor,
        )
        assert TelemetryClientFactory._clients[connection_uuid] == mock_client
        
        # Call again with the same connection_uuid
        client2 = TelemetryClientFactory.get_telemetry_client(connection_uuid=connection_uuid)
        
        # Verify the same client was returned and no new client was created
        assert client2 == mock_client
        mock_client_class.assert_called_once()  # Still only called once

    def test_initialize_telemetry_client_disabled(self, telemetry_factory_reset):
        """Test initializing a telemetry client when telemetry is disabled."""
        connection_uuid = "test-uuid"
        TelemetryClientFactory.initialize_telemetry_client(
            telemetry_enabled=False,
            connection_uuid=connection_uuid,
            auth_provider=MagicMock(),
            host_url="test-host",
        )
        
        # Verify a NoopTelemetryClient was stored
        assert isinstance(TelemetryClientFactory._clients[connection_uuid], NoopTelemetryClient)

        client2 = TelemetryClientFactory.get_telemetry_client(connection_uuid)
        assert isinstance(client2, NoopTelemetryClient)

    def test_get_telemetry_client_existing(self, telemetry_factory_reset):
        """Test getting an existing telemetry client."""
        connection_uuid = "test-uuid"
        mock_client = MagicMock()
        TelemetryClientFactory._clients[connection_uuid] = mock_client
        
        client = TelemetryClientFactory.get_telemetry_client(connection_uuid)
        
        assert client == mock_client

    def test_get_telemetry_client_nonexistent(self, telemetry_factory_reset):
        """Test getting a non-existent telemetry client."""
        client = TelemetryClientFactory.get_telemetry_client("nonexistent-uuid")
        
        assert isinstance(client, NoopTelemetryClient)

    @patch("databricks.sql.telemetry.telemetry_client.ThreadPoolExecutor")
    @patch("databricks.sql.telemetry.telemetry_client.TelemetryClient")
    def test_close(self, mock_client_class, mock_executor_class, telemetry_factory_reset):
        """Test that factory reinitializes properly after complete shutdown."""
        connection_uuid1 = "test-uuid1"
        mock_executor1 = MagicMock()
        mock_client1 = MagicMock()
        mock_executor_class.return_value = mock_executor1
        mock_client_class.return_value = mock_client1
        
        TelemetryClientFactory._clients[connection_uuid1] = mock_client1
        TelemetryClientFactory._executor = mock_executor1
        TelemetryClientFactory._initialized = True
        
        TelemetryClientFactory.close(connection_uuid1)
        
        assert TelemetryClientFactory._clients == {}
        assert TelemetryClientFactory._executor is None
        assert TelemetryClientFactory._initialized is False
        mock_executor1.shutdown.assert_called_once_with(wait=True)
        
        # Now create a new client - this should reinitialize the factory
        connection_uuid2 = "test-uuid2"
        mock_executor2 = MagicMock()
        mock_client2 = MagicMock()
        mock_executor_class.return_value = mock_executor2
        mock_client_class.return_value = mock_client2
        
        TelemetryClientFactory.initialize_telemetry_client(
            telemetry_enabled=True,
            connection_uuid=connection_uuid2,
            auth_provider=MagicMock(),
            host_url="test-host",
        )
        
        # Verify factory was reinitialized
        assert TelemetryClientFactory._initialized is True
        assert TelemetryClientFactory._executor is not None
        assert TelemetryClientFactory._executor == mock_executor2
        assert connection_uuid2 in TelemetryClientFactory._clients
        assert TelemetryClientFactory._clients[connection_uuid2] == mock_client2
        
        # Verify new ThreadPoolExecutor was created
        assert mock_executor_class.call_count == 1