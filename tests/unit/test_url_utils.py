"""Tests for URL utility functions."""
import pytest
from databricks.sql.common.url_utils import normalize_host_with_protocol


class TestNormalizeHostWithProtocol:
    """Tests for normalize_host_with_protocol function."""

    @pytest.mark.parametrize("input_host,expected_output", [
        # Hostname without protocol - should add https://
        ("myserver.com", "https://myserver.com"),
        ("workspace.databricks.com", "https://workspace.databricks.com"),
        
        # Hostname with https:// - should not duplicate
        ("https://myserver.com", "https://myserver.com"),
        ("https://workspace.databricks.com", "https://workspace.databricks.com"),
        
        # Hostname with http:// - should preserve
        ("http://localhost", "http://localhost"),
        ("http://myserver.com:8080", "http://myserver.com:8080"),
        
        # Hostname with port numbers
        ("myserver.com:443", "https://myserver.com:443"),
        ("https://myserver.com:443", "https://myserver.com:443"),
        ("http://localhost:8080", "http://localhost:8080"),
        
        # Trailing slash - should be removed
        ("myserver.com/", "https://myserver.com"),
        ("https://myserver.com/", "https://myserver.com"),
        ("http://localhost/", "http://localhost"),
        
        # Real Databricks patterns
        ("adb-123456.azuredatabricks.net", "https://adb-123456.azuredatabricks.net"),
        ("https://adb-123456.azuredatabricks.net", "https://adb-123456.azuredatabricks.net"),
        ("https://adb-123456.azuredatabricks.net/", "https://adb-123456.azuredatabricks.net"),
        ("workspace.databricks.com/", "https://workspace.databricks.com"),
    ])
    def test_normalize_host_with_protocol(self, input_host, expected_output):
        """Test host normalization with various input formats."""
        assert normalize_host_with_protocol(input_host) == expected_output

