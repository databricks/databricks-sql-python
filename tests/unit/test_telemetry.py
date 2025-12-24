import uuid
import pytest
from unittest.mock import patch, MagicMock
import json
from dataclasses import asdict

from databricks.sql.telemetry.telemetry_client import (
    TelemetryClient,
    NoopTelemetryClient,
    TelemetryClientFactory,
    TelemetryHelper,
)
from databricks.sql.common.feature_flag import (
    FeatureFlagsContextFactory,
    FeatureFlagsContext,
)
from databricks.sql.telemetry.models.enums import AuthMech, AuthFlow, DatabricksClientType
from databricks.sql.telemetry.models.event import (
    TelemetryEvent,
    DriverConnectionParameters,
    DriverSystemConfiguration,
    SqlExecutionEvent,
    DriverErrorInfo,
    DriverVolumeOperation,
    HostDetails,
)
from databricks.sql.auth.authenticators import (
    AccessTokenAuthProvider,
    DatabricksOAuthProvider,
    ExternalAuthProvider,
)
from databricks import sql


@pytest.fixture
def mock_telemetry_client():
    """Create a mock telemetry client for testing."""
    session_id = str(uuid.uuid4())
    auth_provider = AccessTokenAuthProvider("test-token")
    executor = MagicMock()
    client_context = MagicMock()

    # Patch the _setup_pool_manager method to avoid SSL file loading
    with patch(
        "databricks.sql.common.unified_http_client.UnifiedHttpClient._setup_pool_managers"
    ):
        return TelemetryClient(
            telemetry_enabled=True,
            session_id_hex=session_id,
            auth_provider=auth_provider,
            host_url="test-host.com",
            executor=executor,
            batch_size=TelemetryClientFactory.DEFAULT_BATCH_SIZE,
            client_context=client_context,
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
        with patch.object(client, "_send_telemetry") as mock_send:
            # Add events one by one - should not flush yet
            client._export_event("event1")
            client._export_event("event2")
            mock_send.assert_not_called()
            assert client._events_queue.qsize() == 2

            # Third event should trigger flush
            client._export_event("event3")
            mock_send.assert_called_once()
            assert client._events_queue.qsize() == 0  # Queue cleared after flush

    @patch("databricks.sql.common.unified_http_client.UnifiedHttpClient.request")
    def test_network_request_flow(self, mock_http_request, mock_telemetry_client):
        """Test the complete network request flow with authentication."""
        # Mock response for unified HTTP client
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.status_code = 200
        mock_http_request.return_value = mock_response

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
        assert args[0] == client._send_with_unified_client
        assert args[1] == "https://test-host.com/telemetry-ext"
        assert kwargs["headers"]["Authorization"] == "Bearer test-token"

        # Verify request body structure
        request_data = kwargs["data"]
        assert '"uploadTime"' in request_data
        assert '"protoLogs"' in request_data

    def test_telemetry_logging_flows(self, mock_telemetry_client):
        """Test all telemetry logging methods work end-to-end."""
        client = mock_telemetry_client

        with patch.object(client, "_export_event") as mock_export:
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
        with patch.object(client, "_export_event", side_effect=Exception("Test error")):
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
        assert (
            TelemetryHelper.get_auth_flow(oauth_with_tokens)
            == AuthFlow.TOKEN_PASSTHROUGH
        )

        # Test OAuth with browser-based auth
        oauth_with_browser = MagicMock(spec=DatabricksOAuthProvider)
        oauth_with_browser._access_token = None
        oauth_with_browser._refresh_token = None
        oauth_with_browser.oauth_manager = MagicMock()
        assert (
            TelemetryHelper.get_auth_flow(oauth_with_browser)
            == AuthFlow.BROWSER_BASED_AUTHENTICATION
        )

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
        client_context = MagicMock()

        # Initialize enabled client
        with patch(
            "databricks.sql.common.unified_http_client.UnifiedHttpClient._setup_pool_managers"
        ):
            TelemetryClientFactory.initialize_telemetry_client(
                telemetry_enabled=True,
                session_id_hex=session_id_hex,
                auth_provider=auth_provider,
                host_url="test-host.com",
                batch_size=TelemetryClientFactory.DEFAULT_BATCH_SIZE,
                client_context=client_context,
            )

            client = TelemetryClientFactory.get_telemetry_client("test-host.com")
            assert isinstance(client, TelemetryClient)
            assert client._session_id_hex == session_id_hex

            # Close client
            with patch.object(client, "close") as mock_close:
                TelemetryClientFactory.close(host_url="test-host.com")
                mock_close.assert_called_once()

            # Should get NoopTelemetryClient after close

    def test_disabled_telemetry_creates_noop_client(self):
        """Test that disabled telemetry creates NoopTelemetryClient."""
        session_id_hex = "test-session"
        client_context = MagicMock()

        TelemetryClientFactory.initialize_telemetry_client(
            telemetry_enabled=False,
            session_id_hex=session_id_hex,
            auth_provider=None,
            host_url="test-host.com",
            batch_size=TelemetryClientFactory.DEFAULT_BATCH_SIZE,
            client_context=client_context,
        )

        client = TelemetryClientFactory.get_telemetry_client("test-host.com")
        assert isinstance(client, NoopTelemetryClient)

    def test_factory_error_handling(self):
        """Test that factory errors fall back to NoopTelemetryClient."""
        session_id = "test-session"
        client_context = MagicMock()

        # Simulate initialization error
        with patch(
            "databricks.sql.telemetry.telemetry_client.TelemetryClient",
            side_effect=Exception("Init error"),
        ):
            TelemetryClientFactory.initialize_telemetry_client(
                telemetry_enabled=True,
                session_id_hex=session_id,
                auth_provider=AccessTokenAuthProvider("token"),
                host_url="test-host.com",
                batch_size=TelemetryClientFactory.DEFAULT_BATCH_SIZE,
                client_context=client_context,
            )

        # Should fall back to NoopTelemetryClient
        client = TelemetryClientFactory.get_telemetry_client("test-host.com")
        assert isinstance(client, NoopTelemetryClient)

    def test_factory_shutdown_flow(self):
        """Test factory shutdown when last client is removed."""
        session1 = "session-1"
        session2 = "session-2"
        client_context = MagicMock()

        # Initialize multiple clients
        with patch(
            "databricks.sql.common.unified_http_client.UnifiedHttpClient._setup_pool_managers"
        ):
            for session in [session1, session2]:
                TelemetryClientFactory.initialize_telemetry_client(
                    telemetry_enabled=True,
                    session_id_hex=session,
                    auth_provider=AccessTokenAuthProvider("token"),
                    host_url="test-host.com",
                    batch_size=TelemetryClientFactory.DEFAULT_BATCH_SIZE,
                    client_context=client_context,
                )

            # Factory should be initialized
            assert TelemetryClientFactory._initialized is True
            assert TelemetryClientFactory._executor is not None

            # Close first client - factory should stay initialized
            TelemetryClientFactory.close(host_url="test-host.com")
            assert TelemetryClientFactory._initialized is True

            # Close second client - factory should shut down
            TelemetryClientFactory.close(host_url="test-host.com")
            assert TelemetryClientFactory._initialized is False
            assert TelemetryClientFactory._executor is None

    @patch(
        "databricks.sql.telemetry.telemetry_client.TelemetryClient.export_failure_log"
    )
    @patch("databricks.sql.client.Session")
    def test_connection_failure_sends_correct_telemetry_payload(
        self, mock_session, mock_export_failure_log
    ):
        """
        Verify that a connection failure constructs and sends the correct
        telemetry payload via _send_telemetry.
        """

        error_message = "Could not connect to host"
        # Set up the mock to create a session instance first, then make open() fail
        mock_session_instance = MagicMock()
        mock_session_instance.is_open = False  # Ensure cleanup is safe
        mock_session_instance.open.side_effect = Exception(error_message)
        mock_session.return_value = mock_session_instance

        try:
            sql.connect(server_hostname="test-host", http_path="/test-path")
        except Exception as e:
            assert str(e) == error_message

        mock_export_failure_log.assert_called_once()
        call_arguments = mock_export_failure_log.call_args
        assert call_arguments[0][0] == "Exception"
        assert call_arguments[0][1] == error_message


@patch("databricks.sql.client.Session")
class TestTelemetryFeatureFlag:
    """Tests the interaction between the telemetry feature flag and connection parameters."""

    def teardown_method(self):
        """Clean up telemetry factory state after each test to prevent test pollution."""
        from databricks.sql.common.feature_flag import FeatureFlagsContextFactory

        TelemetryClientFactory._clients.clear()
        FeatureFlagsContextFactory._context_map.clear()

    def _mock_ff_response(self, mock_http_request, enabled: bool):
        """Helper method to mock feature flag response for unified HTTP client."""
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.status_code = 200  # Compatibility attribute
        payload = {
            "flags": [
                {
                    "name": "databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForPythonDriver",
                    "value": str(enabled).lower(),
                }
            ],
            "ttl_seconds": 3600,
        }
        mock_response.json.return_value = payload
        mock_response.data = json.dumps(payload).encode()
        mock_http_request.return_value = mock_response

    @patch("databricks.sql.common.unified_http_client.UnifiedHttpClient.request")
    def test_telemetry_enabled_when_flag_is_true(self, mock_http_request, MockSession):
        """Telemetry should be ON when enable_telemetry=True and server flag is 'true'."""
        self._mock_ff_response(mock_http_request, enabled=True)
        mock_session_instance = MockSession.return_value
        mock_session_instance.guid_hex = "test-session-ff-true"
        mock_session_instance.host = "test-host"  # Set host for telemetry client lookup
        mock_session_instance.auth_provider = AccessTokenAuthProvider("token")
        mock_session_instance.is_open = (
            False  # Connection starts closed for test cleanup
        )

        # Set up mock HTTP client on the session
        mock_http_client = MagicMock()
        mock_http_client.request = mock_http_request
        mock_session_instance.http_client = mock_http_client

        conn = sql.client.Connection(
            server_hostname="test",
            http_path="test",
            access_token="test",
            enable_telemetry=True,
        )

        assert conn.telemetry_enabled is True
        mock_http_request.assert_called_once()
        client = TelemetryClientFactory.get_telemetry_client("test-host")
        assert isinstance(client, TelemetryClient)

    @patch("databricks.sql.common.unified_http_client.UnifiedHttpClient.request")
    def test_telemetry_disabled_when_flag_is_false(
        self, mock_http_request, MockSession
    ):
        """Telemetry should be OFF when enable_telemetry=True but server flag is 'false'."""
        self._mock_ff_response(mock_http_request, enabled=False)
        mock_session_instance = MockSession.return_value
        mock_session_instance.guid_hex = "test-session-ff-false"
        mock_session_instance.host = "test-host"  # Set host for telemetry client lookup
        mock_session_instance.auth_provider = AccessTokenAuthProvider("token")
        mock_session_instance.is_open = (
            False  # Connection starts closed for test cleanup
        )

        # Set up mock HTTP client on the session
        mock_http_client = MagicMock()
        mock_http_client.request = mock_http_request
        mock_session_instance.http_client = mock_http_client

        conn = sql.client.Connection(
            server_hostname="test",
            http_path="test",
            access_token="test",
            enable_telemetry=True,
        )

        assert conn.telemetry_enabled is False
        mock_http_request.assert_called_once()
        client = TelemetryClientFactory.get_telemetry_client("test-host")
        assert isinstance(client, NoopTelemetryClient)

    @patch("databricks.sql.common.unified_http_client.UnifiedHttpClient.request")
    def test_telemetry_disabled_when_flag_request_fails(
        self, mock_http_request, MockSession
    ):
        """Telemetry should default to OFF if the feature flag network request fails."""
        mock_http_request.side_effect = Exception("Network is down")
        mock_session_instance = MockSession.return_value
        mock_session_instance.guid_hex = "test-session-ff-fail"
        mock_session_instance.host = "test-host"  # Set host for telemetry client lookup
        mock_session_instance.auth_provider = AccessTokenAuthProvider("token")
        mock_session_instance.is_open = (
            False  # Connection starts closed for test cleanup
        )

        # Set up mock HTTP client on the session
        mock_http_client = MagicMock()
        mock_http_client.request = mock_http_request
        mock_session_instance.http_client = mock_http_client

        conn = sql.client.Connection(
            server_hostname="test",
            http_path="test",
            access_token="test",
            enable_telemetry=True,
        )

        assert conn.telemetry_enabled is False
        mock_http_request.assert_called_once()
        client = TelemetryClientFactory.get_telemetry_client("test-host")
        assert isinstance(client, NoopTelemetryClient)


class TestTelemetryEventModels:
    """Tests for telemetry event model data structures and JSON serialization."""

    def test_host_details_serialization(self):
        """Test HostDetails model serialization."""
        host = HostDetails(host_url="test-host.com", port=443)
        
        # Test JSON string generation
        json_str = host.to_json()
        assert isinstance(json_str, str)
        parsed = json.loads(json_str)
        assert parsed["host_url"] == "test-host.com"
        assert parsed["port"] == 443

    def test_driver_connection_parameters_all_fields(self):
        """Test DriverConnectionParameters with all fields populated."""
        host_info = HostDetails(host_url="workspace.databricks.com", port=443)
        proxy_info = HostDetails(host_url="proxy.company.com", port=8080)
        cf_proxy_info = HostDetails(host_url="cf-proxy.company.com", port=8080)
        
        params = DriverConnectionParameters(
            http_path="/sql/1.0/warehouses/abc123",
            mode=DatabricksClientType.SEA,
            host_info=host_info,
            auth_mech=AuthMech.OAUTH,
            auth_flow=AuthFlow.BROWSER_BASED_AUTHENTICATION,
            socket_timeout=30000,
            azure_workspace_resource_id="/subscriptions/test/resourceGroups/test",
            azure_tenant_id="tenant-123",
            use_proxy=True,
            use_system_proxy=True,
            proxy_host_info=proxy_info,
            use_cf_proxy=False,
            cf_proxy_host_info=cf_proxy_info,
            non_proxy_hosts=["localhost", "127.0.0.1"],
            allow_self_signed_support=False,
            use_system_trust_store=True,
            enable_arrow=True,
            enable_direct_results=True,
            enable_sea_hybrid_results=True,
            http_connection_pool_size=100,
            rows_fetched_per_block=100000,
            async_poll_interval_millis=2000,
            support_many_parameters=True,
            enable_complex_datatype_support=True,
            allowed_volume_ingestion_paths="/Volumes/catalog/schema/volume",
            query_tags="team:engineering,project:telemetry",
        )
        
        # Serialize to JSON and parse back
        json_str = params.to_json()
        json_dict = json.loads(json_str)
        
        # Verify all new fields are in JSON
        assert json_dict["http_path"] == "/sql/1.0/warehouses/abc123"
        assert json_dict["mode"] == "SEA"
        assert json_dict["host_info"]["host_url"] == "workspace.databricks.com"
        assert json_dict["auth_mech"] == "OAUTH"
        assert json_dict["auth_flow"] == "BROWSER_BASED_AUTHENTICATION"
        assert json_dict["socket_timeout"] == 30000
        assert json_dict["azure_workspace_resource_id"] == "/subscriptions/test/resourceGroups/test"
        assert json_dict["azure_tenant_id"] == "tenant-123"
        assert json_dict["use_proxy"] is True
        assert json_dict["use_system_proxy"] is True
        assert json_dict["proxy_host_info"]["host_url"] == "proxy.company.com"
        assert json_dict["use_cf_proxy"] is False
        assert json_dict["cf_proxy_host_info"]["host_url"] == "cf-proxy.company.com"
        assert json_dict["non_proxy_hosts"] == ["localhost", "127.0.0.1"]
        assert json_dict["allow_self_signed_support"] is False
        assert json_dict["use_system_trust_store"] is True
        assert json_dict["enable_arrow"] is True
        assert json_dict["enable_direct_results"] is True
        assert json_dict["enable_sea_hybrid_results"] is True
        assert json_dict["http_connection_pool_size"] == 100
        assert json_dict["rows_fetched_per_block"] == 100000
        assert json_dict["async_poll_interval_millis"] == 2000
        assert json_dict["support_many_parameters"] is True
        assert json_dict["enable_complex_datatype_support"] is True
        assert json_dict["allowed_volume_ingestion_paths"] == "/Volumes/catalog/schema/volume"
        assert json_dict["query_tags"] == "team:engineering,project:telemetry"

    def test_driver_connection_parameters_minimal_fields(self):
        """Test DriverConnectionParameters with only required fields."""
        host_info = HostDetails(host_url="workspace.databricks.com", port=443)
        
        params = DriverConnectionParameters(
            http_path="/sql/1.0/warehouses/abc123",
            mode=DatabricksClientType.THRIFT,
            host_info=host_info,
        )
        
        # Note: to_json() filters out None values, so we need to check asdict for complete structure
        json_str = params.to_json()
        json_dict = json.loads(json_str)
        
        # Required fields should be present
        assert json_dict["http_path"] == "/sql/1.0/warehouses/abc123"
        assert json_dict["mode"] == "THRIFT"
        assert json_dict["host_info"]["host_url"] == "workspace.databricks.com"
        
        # Optional fields with None are filtered out by to_json()
        # This is expected behavior - None values are excluded from JSON output

    def test_driver_system_configuration_serialization(self):
        """Test DriverSystemConfiguration model serialization."""
        sys_config = DriverSystemConfiguration(
            driver_name="Databricks SQL Connector for Python",
            driver_version="3.0.0",
            runtime_name="CPython",
            runtime_version="3.11.0",
            runtime_vendor="Python Software Foundation",
            os_name="Darwin",
            os_version="23.0.0",
            os_arch="arm64",
            char_set_encoding="utf-8",
            locale_name="en_US",
            client_app_name="MyApp",
        )
        
        json_str = sys_config.to_json()
        json_dict = json.loads(json_str)
        
        assert json_dict["driver_name"] == "Databricks SQL Connector for Python"
        assert json_dict["driver_version"] == "3.0.0"
        assert json_dict["runtime_name"] == "CPython"
        assert json_dict["runtime_version"] == "3.11.0"
        assert json_dict["runtime_vendor"] == "Python Software Foundation"
        assert json_dict["os_name"] == "Darwin"
        assert json_dict["os_version"] == "23.0.0"
        assert json_dict["os_arch"] == "arm64"
        assert json_dict["locale_name"] == "en_US"
        assert json_dict["char_set_encoding"] == "utf-8"
        assert json_dict["client_app_name"] == "MyApp"

    def test_telemetry_event_complete_serialization(self):
        """Test complete TelemetryEvent serialization with all nested objects."""
        host_info = HostDetails(host_url="workspace.databricks.com", port=443)
        proxy_info = HostDetails(host_url="proxy.company.com", port=8080)
        
        connection_params = DriverConnectionParameters(
            http_path="/sql/1.0/warehouses/abc123",
            mode=DatabricksClientType.SEA,
            host_info=host_info,
            auth_mech=AuthMech.OAUTH,
            use_proxy=True,
            proxy_host_info=proxy_info,
            enable_arrow=True,
            rows_fetched_per_block=100000,
        )
        
        sys_config = DriverSystemConfiguration(
            driver_name="Databricks SQL Connector for Python",
            driver_version="3.0.0",
            runtime_name="CPython",
            runtime_version="3.11.0",
            runtime_vendor="Python Software Foundation",
            os_name="Darwin",
            os_version="23.0.0",
            os_arch="arm64",
            char_set_encoding="utf-8",
        )
        
        error_info = DriverErrorInfo(
            error_name="ConnectionError",
            stack_trace="Traceback...",
        )
        
        event = TelemetryEvent(
            session_id="test-session-123",
            sql_statement_id="test-stmt-456",
            operation_latency_ms=1500,
            auth_type="OAUTH",
            system_configuration=sys_config,
            driver_connection_params=connection_params,
            error_info=error_info,
        )
        
        # Test JSON serialization
        json_str = event.to_json()
        assert isinstance(json_str, str)
        
        # Parse and verify structure
        parsed = json.loads(json_str)
        assert parsed["session_id"] == "test-session-123"
        assert parsed["sql_statement_id"] == "test-stmt-456"
        assert parsed["operation_latency_ms"] == 1500
        assert parsed["auth_type"] == "OAUTH"
        
        # Verify nested objects
        assert parsed["system_configuration"]["driver_name"] == "Databricks SQL Connector for Python"
        assert parsed["driver_connection_params"]["http_path"] == "/sql/1.0/warehouses/abc123"
        assert parsed["driver_connection_params"]["use_proxy"] is True
        assert parsed["driver_connection_params"]["proxy_host_info"]["host_url"] == "proxy.company.com"
        assert parsed["error_info"]["error_name"] == "ConnectionError"

    def test_json_serialization_excludes_none_values(self):
        """Test that JSON serialization properly excludes None values."""
        host_info = HostDetails(host_url="workspace.databricks.com", port=443)
        
        params = DriverConnectionParameters(
            http_path="/sql/1.0/warehouses/abc123",
            mode=DatabricksClientType.SEA,
            host_info=host_info,
            # All optional fields left as None
        )
        
        json_str = params.to_json()
        parsed = json.loads(json_str)
        
        # Required fields present
        assert parsed["http_path"] == "/sql/1.0/warehouses/abc123"
        
        # None values should be EXCLUDED from JSON (not included as null)
        # This is the behavior of JsonSerializableMixin
        assert "auth_mech" not in parsed
        assert "azure_tenant_id" not in parsed
        assert "proxy_host_info" not in parsed


@patch("databricks.sql.client.Session")
@patch("databricks.sql.common.unified_http_client.UnifiedHttpClient._setup_pool_managers")
class TestConnectionParameterTelemetry:
    """Tests for connection parameter population in telemetry."""

    def test_connection_with_proxy_populates_telemetry(self, mock_setup_pools, mock_session):
        """Test that proxy configuration is captured in telemetry."""
        mock_session_instance = MagicMock()
        mock_session_instance.guid_hex = "test-session-proxy"
        mock_session_instance.auth_provider = AccessTokenAuthProvider("token")
        mock_session_instance.is_open = False
        mock_session_instance.use_sea = True
        mock_session_instance.port = 443
        mock_session_instance.host = "workspace.databricks.com"
        mock_session.return_value = mock_session_instance
        
        with patch("databricks.sql.telemetry.telemetry_client.TelemetryClient.export_initial_telemetry_log") as mock_export:
            conn = sql.connect(
                server_hostname="workspace.databricks.com",
                http_path="/sql/1.0/warehouses/test",
                access_token="test-token",
                enable_telemetry=True,
                force_enable_telemetry=True,
            )
            
            # Verify export was called
            mock_export.assert_called_once()
            call_args = mock_export.call_args
            
            # Extract driver_connection_params
            driver_params = call_args.kwargs.get("driver_connection_params")
            assert driver_params is not None
            assert isinstance(driver_params, DriverConnectionParameters)
            
            # Verify fields are populated
            assert driver_params.http_path == "/sql/1.0/warehouses/test"
            assert driver_params.mode == DatabricksClientType.SEA
            assert driver_params.host_info.host_url == "workspace.databricks.com"
            assert driver_params.host_info.port == 443

    def test_connection_with_azure_params_populates_telemetry(self, mock_setup_pools, mock_session):
        """Test that Azure-specific parameters are captured in telemetry."""
        mock_session_instance = MagicMock()
        mock_session_instance.guid_hex = "test-session-azure"
        mock_session_instance.auth_provider = AccessTokenAuthProvider("token")
        mock_session_instance.is_open = False
        mock_session_instance.use_sea = False
        mock_session_instance.port = 443
        mock_session_instance.host = "workspace.azuredatabricks.net"
        mock_session.return_value = mock_session_instance
        
        with patch("databricks.sql.telemetry.telemetry_client.TelemetryClient.export_initial_telemetry_log") as mock_export:
            conn = sql.connect(
                server_hostname="workspace.azuredatabricks.net",
                http_path="/sql/1.0/warehouses/test",
                access_token="test-token",
                azure_workspace_resource_id="/subscriptions/test/resourceGroups/test",
                azure_tenant_id="tenant-123",
                enable_telemetry=True,
                force_enable_telemetry=True,
            )
            
            mock_export.assert_called_once()
            driver_params = mock_export.call_args.kwargs.get("driver_connection_params")
            
            # Verify Azure fields
            assert driver_params.azure_workspace_resource_id == "/subscriptions/test/resourceGroups/test"
            assert driver_params.azure_tenant_id == "tenant-123"

    def test_connection_populates_arrow_and_performance_params(self, mock_setup_pools, mock_session):
        """Test that Arrow and performance parameters are captured in telemetry."""
        mock_session_instance = MagicMock()
        mock_session_instance.guid_hex = "test-session-perf"
        mock_session_instance.auth_provider = AccessTokenAuthProvider("token")
        mock_session_instance.is_open = False
        mock_session_instance.use_sea = True
        mock_session_instance.port = 443
        mock_session_instance.host = "workspace.databricks.com"
        mock_session.return_value = mock_session_instance
        
        with patch("databricks.sql.telemetry.telemetry_client.TelemetryClient.export_initial_telemetry_log") as mock_export:
            # Import pyarrow availability check
            try:
                import pyarrow
                arrow_available = True
            except ImportError:
                arrow_available = False
            
            conn = sql.connect(
                server_hostname="workspace.databricks.com",
                http_path="/sql/1.0/warehouses/test",
                access_token="test-token",
                pool_maxsize=200,
                enable_telemetry=True,
                force_enable_telemetry=True,
            )
            
            mock_export.assert_called_once()
            driver_params = mock_export.call_args.kwargs.get("driver_connection_params")
            
            # Verify performance fields
            assert driver_params.enable_arrow == arrow_available
            assert driver_params.enable_direct_results is True
            assert driver_params.http_connection_pool_size == 200
            assert driver_params.rows_fetched_per_block == 100000  # DEFAULT_ARRAY_SIZE
            assert driver_params.async_poll_interval_millis == 2000
            assert driver_params.support_many_parameters is True

    def test_cf_proxy_fields_default_to_false_none(self, mock_setup_pools, mock_session):
        """Test that CloudFlare proxy fields default to False/None (not yet supported)."""
        mock_session_instance = MagicMock()
        mock_session_instance.guid_hex = "test-session-cfproxy"
        mock_session_instance.auth_provider = AccessTokenAuthProvider("token")
        mock_session_instance.is_open = False
        mock_session_instance.use_sea = True
        mock_session_instance.port = 443
        mock_session_instance.host = "workspace.databricks.com"
        mock_session.return_value = mock_session_instance
        
        with patch("databricks.sql.telemetry.telemetry_client.TelemetryClient.export_initial_telemetry_log") as mock_export:
            conn = sql.connect(
                server_hostname="workspace.databricks.com",
                http_path="/sql/1.0/warehouses/test",
                access_token="test-token",
                enable_telemetry=True,
                force_enable_telemetry=True,
            )
            
            mock_export.assert_called_once()
            driver_params = mock_export.call_args.kwargs.get("driver_connection_params")

            # CF proxy not yet supported - should be False/None
            assert driver_params.use_cf_proxy is False
            assert driver_params.cf_proxy_host_info is None


class TestFeatureFlagsContextFactory:
    """Tests for FeatureFlagsContextFactory host-level caching."""

    @pytest.fixture(autouse=True)
    def reset_factory(self):
        """Reset factory state before/after each test."""
        FeatureFlagsContextFactory._context_map.clear()
        if FeatureFlagsContextFactory._executor:
            FeatureFlagsContextFactory._executor.shutdown(wait=False)
        FeatureFlagsContextFactory._executor = None
        yield
        FeatureFlagsContextFactory._context_map.clear()
        if FeatureFlagsContextFactory._executor:
            FeatureFlagsContextFactory._executor.shutdown(wait=False)
        FeatureFlagsContextFactory._executor = None

    @pytest.mark.parametrize(
        "hosts,expected_contexts",
        [
            (["host1.com", "host1.com"], 1),  # Same host shares context
            (["host1.com", "host2.com"], 2),  # Different hosts get separate contexts
            (["host1.com", "host1.com", "host2.com"], 2),  # Mixed scenario
        ],
    )
    def test_host_level_caching(self, hosts, expected_contexts):
        """Test that contexts are cached by host correctly."""
        contexts = []
        for host in hosts:
            conn = MagicMock()
            conn.session.host = host
            conn.session.http_client = MagicMock()
            contexts.append(FeatureFlagsContextFactory.get_instance(conn))

        assert len(FeatureFlagsContextFactory._context_map) == expected_contexts
        if expected_contexts == 1:
            assert all(ctx is contexts[0] for ctx in contexts)

    def test_remove_instance_and_executor_cleanup(self):
        """Test removal uses host key and cleans up executor when empty."""
        conn1 = MagicMock()
        conn1.session.host = "host1.com"
        conn1.session.http_client = MagicMock()

        conn2 = MagicMock()
        conn2.session.host = "host2.com"
        conn2.session.http_client = MagicMock()

        FeatureFlagsContextFactory.get_instance(conn1)
        FeatureFlagsContextFactory.get_instance(conn2)
        assert FeatureFlagsContextFactory._executor is not None

        FeatureFlagsContextFactory.remove_instance(conn1)
        assert len(FeatureFlagsContextFactory._context_map) == 1
        assert FeatureFlagsContextFactory._executor is not None

        FeatureFlagsContextFactory.remove_instance(conn2)
        assert len(FeatureFlagsContextFactory._context_map) == 0
        assert FeatureFlagsContextFactory._executor is None
