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
        
        # Case-insensitive protocol handling - should normalize to lowercase
        ("HTTPS://myserver.com", "https://myserver.com"),
        ("HTTP://myserver.com", "http://myserver.com"),
        ("HttPs://workspace.databricks.com", "https://workspace.databricks.com"),
        ("HtTp://localhost:8080", "http://localhost:8080"),
        ("HTTPS://MYSERVER.COM", "https://MYSERVER.COM"),  # Only protocol lowercased
        
        # Case-insensitive with trailing slashes
        ("HTTPS://myserver.com/", "https://myserver.com"),
        ("HTTP://localhost:8080/", "http://localhost:8080"),
        ("HttPs://workspace.databricks.com//", "https://workspace.databricks.com"),
        
        # Mixed case protocols with ports
        ("HTTPS://myserver.com:443", "https://myserver.com:443"),
        ("HtTp://myserver.com:8080", "http://myserver.com:8080"),
        
        # Case preservation - only protocol lowercased, hostname case preserved
        ("HTTPS://MyServer.DataBricks.COM", "https://MyServer.DataBricks.COM"),
        ("HttPs://CamelCase.Server.com", "https://CamelCase.Server.com"),
        ("HTTP://UPPERCASE.COM:8080", "http://UPPERCASE.COM:8080"),
    ])
    def test_normalize_host_with_protocol(self, input_host, expected_output):
        """Test host normalization with various input formats."""
        result = normalize_host_with_protocol(input_host)
        assert result == expected_output
        
        # Additional assertion: verify protocol is always lowercase
        assert result.startswith("https://") or result.startswith("http://")

    @pytest.mark.parametrize("invalid_host", [
        None,
        "",
        "   ",  # Whitespace only
    ])
    def test_normalize_host_with_protocol_raises_on_invalid_input(self, invalid_host):
        """Test that function raises ValueError for None or empty host."""
        with pytest.raises(ValueError, match="Host cannot be None or empty"):
            normalize_host_with_protocol(invalid_host)

