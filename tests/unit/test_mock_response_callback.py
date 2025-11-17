"""
Test that mock responses from CircuitBreakerTelemetryPushClient work correctly
with the telemetry callback that parses the response.
"""

import json
import pytest
from unittest.mock import Mock, patch
from concurrent.futures import Future

from databricks.sql.telemetry.telemetry_push_client import (
    CircuitBreakerTelemetryPushClient,
    TelemetryPushClient,
)
from databricks.sql.telemetry.models.endpoint_models import TelemetryResponse
from databricks.sql.telemetry.circuit_breaker_manager import CircuitBreakerManager


class TestMockResponseWithCallback:
    """Test that mock responses work with telemetry callback processing."""

    def setup_method(self):
        """Set up test fixtures."""
        CircuitBreakerManager._instances.clear()

    def teardown_method(self):
        """Clean up after tests."""
        CircuitBreakerManager._instances.clear()

    def test_mock_response_data_structure(self):
        """Test that mock response has valid JSON structure."""
        mock_delegate = Mock(spec=TelemetryPushClient)
        client = CircuitBreakerTelemetryPushClient(
            mock_delegate, "test-host.example.com"
        )

        # Get a mock response
        mock_response = client._create_mock_success_response()

        # Verify properties exist
        assert mock_response.status == 200
        assert mock_response.data is not None

        # Verify data is bytes
        assert isinstance(mock_response.data, bytes)

        # Verify data can be decoded
        decoded_data = mock_response.data.decode()
        assert decoded_data is not None

        # Verify data is valid JSON
        parsed_data = json.loads(decoded_data)
        assert isinstance(parsed_data, dict)

        # Verify JSON has all required TelemetryResponse fields
        assert "numProtoSuccess" in parsed_data
        assert "numSuccess" in parsed_data
        assert "numRealtimeSuccess" in parsed_data
        assert "errors" in parsed_data
        
        # Verify field values
        assert parsed_data["numProtoSuccess"] == 0
        assert parsed_data["numSuccess"] == 0
        assert parsed_data["numRealtimeSuccess"] == 0
        assert parsed_data["errors"] == []

    def test_mock_response_with_telemetry_response_model(self):
        """Test that mock response JSON can be parsed into TelemetryResponse model."""
        mock_delegate = Mock(spec=TelemetryPushClient)
        client = CircuitBreakerTelemetryPushClient(
            mock_delegate, "test-host.example.com"
        )

        # Get a mock response
        mock_response = client._create_mock_success_response()

        # Simulate what _telemetry_request_callback does
        response_data = json.loads(mock_response.data.decode())

        # Try to create TelemetryResponse - this will fail if schema doesn't match
        try:
            telemetry_response = TelemetryResponse(**response_data)

            # Verify all fields in the response object
            assert telemetry_response.numProtoSuccess == 0
            assert telemetry_response.numSuccess == 0
            assert telemetry_response.numRealtimeSuccess == 0
            assert telemetry_response.errors == []

        except TypeError as e:
            pytest.fail(
                f"Mock response JSON doesn't match TelemetryResponse schema: {e}"
            )

    def test_mock_response_in_callback_simulation(self):
        """Test that mock response works in simulated callback flow."""
        mock_delegate = Mock(spec=TelemetryPushClient)
        client = CircuitBreakerTelemetryPushClient(
            mock_delegate, "test-host.example.com"
        )

        # Get a mock response
        mock_response = client._create_mock_success_response()

        # Create a future with the mock response (simulate async callback)
        future = Future()
        future.set_result(mock_response)

        # Simulate what _telemetry_request_callback does
        response = future.result()

        # Check if response is successful (200-299 range)
        is_success = 200 <= response.status < 300
        assert is_success is True

        # Parse JSON response (same as callback does)
        response_data = json.loads(response.data.decode()) if response.data else {}

        # Create TelemetryResponse (this is where it would fail if schema is wrong)
        try:
            telemetry_response = TelemetryResponse(**response_data)

            # Verify all response fields were parsed correctly
            assert telemetry_response.numProtoSuccess == 0
            assert telemetry_response.numSuccess == 0
            assert telemetry_response.numRealtimeSuccess == 0
            assert len(telemetry_response.errors) == 0

        except TypeError as e:
            pytest.fail(
                f"Mock response failed in callback simulation. Missing fields: {e}"
            )

    def test_mock_response_close_method(self):
        """Test that mock response has close() method that doesn't crash."""
        mock_delegate = Mock(spec=TelemetryPushClient)
        client = CircuitBreakerTelemetryPushClient(
            mock_delegate, "test-host.example.com"
        )

        mock_response = client._create_mock_success_response()

        # Verify close() method exists and doesn't raise
        try:
            mock_response.close()
        except Exception as e:
            pytest.fail(f"Mock response close() method raised exception: {e}")

