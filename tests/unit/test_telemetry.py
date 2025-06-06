import unittest
import uuid
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

class TestNoopTelemetryClient(unittest.TestCase):
    """Tests for the NoopTelemetryClient class."""

    def test_singleton(self):
        """Test that NoopTelemetryClient is a singleton."""
        client1 = NoopTelemetryClient()
        client2 = NoopTelemetryClient()
        self.assertIs(client1, client2)

    def test_export_initial_telemetry_log(self):
        """Test that export_initial_telemetry_log does nothing."""
        client = NoopTelemetryClient()
        client.export_initial_telemetry_log()

    def test_close(self):
        """Test that close does nothing."""
        client = NoopTelemetryClient()
        client.close()


class TestTelemetryClient(unittest.TestCase):
    """Tests for the TelemetryClient class."""

    def setUp(self):
        """Set up test fixtures."""
        self.connection_uuid = str(uuid.uuid4())
        self.auth_provider = AccessTokenAuthProvider("test-token")
        self.user_agent = "test-user-agent"
        self.host_url = "test-host"
        self.driver_connection_params = DriverConnectionParameters(
            http_path="test-path",
            mode=DatabricksClientType.THRIFT,
            host_info=HostDetails(host_url=self.host_url, port=443),
            auth_mech=AuthMech.PAT,
            auth_flow=None,
        )
        self.executor = MagicMock()
        
        self.client = TelemetryClient(
            telemetry_enabled=True,
            connection_uuid=self.connection_uuid,
            auth_provider=self.auth_provider,
            user_agent=self.user_agent,
            driver_connection_params=self.driver_connection_params,
            executor=self.executor,
        )

    @patch("databricks.sql.telemetry.telemetry_client.TelemetryFrontendLog")
    @patch("databricks.sql.telemetry.telemetry_client.TelemetryHelper.getDriverSystemConfiguration")
    @patch("databricks.sql.telemetry.telemetry_client.uuid.uuid4")
    @patch("databricks.sql.telemetry.telemetry_client.time.time")
    def test_export_initial_telemetry_log(self, mock_time, mock_uuid4, mock_get_driver_config, mock_frontend_log):
        """Test exporting initial telemetry log."""
        mock_time.return_value = 1000
        mock_uuid4.return_value = "test-uuid"
        mock_get_driver_config.return_value = "test-driver-config"
        mock_frontend_log.return_value = MagicMock()
 
        self.client.export_event = MagicMock()
        
        self.client.export_initial_telemetry_log()
        
        mock_frontend_log.assert_called_once()
        self.client.export_event.assert_called_once_with(mock_frontend_log.return_value)

    def test_export_event(self):
        """Test exporting an event."""
        self.client.flush = MagicMock()
        
        for i in range(5):
            self.client.export_event(f"event-{i}")
        
        self.client.flush.assert_not_called()
        self.assertEqual(len(self.client.events_batch), 5)
        
        for i in range(5, 10):
            self.client.export_event(f"event-{i}")
        
        self.client.flush.assert_called_once()
        self.assertEqual(len(self.client.events_batch), 10)

    @patch("requests.post")
    def test_send_telemetry_authenticated(self, mock_post):
        """Test sending telemetry to the server with authentication."""
        events = [MagicMock(), MagicMock()]
        events[0].to_json.return_value = '{"event": "1"}'
        events[1].to_json.return_value = '{"event": "2"}'
        
        self.client._send_telemetry(events)
        
        self.executor.submit.assert_called_once()
        args, kwargs = self.executor.submit.call_args
        self.assertEqual(args[0], requests.post)
        self.assertEqual(kwargs["timeout"], 10)
        self.assertIn("Authorization", kwargs["headers"])
        self.assertEqual(kwargs["headers"]["Authorization"], "Bearer test-token")

    @patch("requests.post")
    def test_send_telemetry_unauthenticated(self, mock_post):
        """Test sending telemetry to the server without authentication."""
        unauthenticated_client = TelemetryClient(
            telemetry_enabled=True,
            connection_uuid=str(uuid.uuid4()),
            auth_provider=None,  # No auth provider
            user_agent=self.user_agent,
            driver_connection_params=self.driver_connection_params,
            executor=self.executor,
        )
        
        events = [MagicMock(), MagicMock()]
        events[0].to_json.return_value = '{"event": "1"}'
        events[1].to_json.return_value = '{"event": "2"}'
        
        unauthenticated_client._send_telemetry(events)
        
        self.executor.submit.assert_called_once()
        args, kwargs = self.executor.submit.call_args
        self.assertEqual(args[0], requests.post)
        self.assertEqual(kwargs["timeout"], 10)
        self.assertNotIn("Authorization", kwargs["headers"])  # No auth header
        self.assertEqual(kwargs["headers"]["Accept"], "application/json")
        self.assertEqual(kwargs["headers"]["Content-Type"], "application/json")

    def test_flush(self):
        """Test flushing events."""
        self.client.events_batch = ["event1", "event2"]
        self.client._send_telemetry = MagicMock()
        
        self.client.flush()
        
        self.client._send_telemetry.assert_called_once_with(["event1", "event2"])
        self.assertEqual(self.client.events_batch, [])

    @patch("databricks.sql.telemetry.telemetry_client.telemetry_client_factory")
    def test_close(self, mock_factory):
        """Test closing the client."""
        self.client.flush = MagicMock()
        
        self.client.close()
        
        self.client.flush.assert_called_once()
        mock_factory.close.assert_called_once_with(self.connection_uuid)


class TestTelemetryClientFactory(unittest.TestCase):
    """Tests for the TelemetryClientFactory class."""

    def setUp(self):
        """Set up test fixtures."""
        TelemetryClientFactory._instance = None
        self.factory = TelemetryClientFactory()

    def test_singleton(self):
        """Test that TelemetryClientFactory is a singleton."""
        factory1 = TelemetryClientFactory()
        factory2 = TelemetryClientFactory()
        self.assertIs(factory1, factory2)

    @patch("databricks.sql.telemetry.telemetry_client.TelemetryClient")
    def test_get_telemetry_client_enabled(self, mock_client_class):
        """Test getting a telemetry client when telemetry is enabled."""
        connection_uuid = "test-uuid"
        auth_provider = MagicMock()
        user_agent = "test-user-agent"
        driver_connection_params = MagicMock()
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        
        client = self.factory.initialize_telemetry_client(
            telemetry_enabled=True,
            connection_uuid=connection_uuid,
            auth_provider=auth_provider,
            user_agent=user_agent,
            driver_connection_params=driver_connection_params,
        )
        
        # Verify a new client was created and stored
        mock_client_class.assert_called_once_with(
            telemetry_enabled=True,
            connection_uuid=connection_uuid,
            auth_provider=auth_provider,
            user_agent=user_agent,
            driver_connection_params=driver_connection_params,
            executor=self.factory.executor,
        )
        self.assertEqual(client, mock_client)
        self.assertEqual(self.factory._clients[connection_uuid], mock_client)
        
        # Call again with the same connection_uuid
        client2 = self.factory.get_telemetry_client(connection_uuid=connection_uuid)
        
        # Verify the same client was returned and no new client was created
        self.assertEqual(client2, mock_client)
        mock_client_class.assert_called_once()  # Still only called once

    def test_get_telemetry_client_disabled(self):
        """Test getting a telemetry client when telemetry is disabled."""
        client = self.factory.initialize_telemetry_client(
            telemetry_enabled=False,
            connection_uuid="test-uuid",
            auth_provider=MagicMock(),
            user_agent="test-user-agent",
            driver_connection_params=MagicMock(),
        )
        
        # Verify a NoopTelemetryClient was returned
        self.assertIsInstance(client, NoopTelemetryClient)
        self.assertEqual(self.factory._clients, {})  # No client was stored

        client2 = self.factory.get_telemetry_client("test-uuid")
        self.assertIsInstance(client2, NoopTelemetryClient)

    @patch("databricks.sql.telemetry.telemetry_client.ThreadPoolExecutor")
    def test_close(self, mock_executor_class):
        """Test closing a client."""
        connection_uuid = "test-uuid"
        self.factory._clients[connection_uuid] = MagicMock()
        mock_executor = MagicMock()
        self.factory.executor = mock_executor
        
        self.factory.close(connection_uuid)
        
        # Verify the client was removed
        self.assertEqual(self.factory._clients, {})
        
        # Verify the executor was shutdown
        mock_executor.shutdown.assert_called_once_with(wait=True)
        
        # Add another client and close it
        connection_uuid2 = "test-uuid2"
        self.factory._clients[connection_uuid2] = MagicMock()
        self.factory.close(connection_uuid2)
        
        # Verify the executor was shutdown again
        self.assertEqual(mock_executor.shutdown.call_count, 2)


if __name__ == "__main__":
    unittest.main()