"""Tests for URL utility functions."""
import pytest
from databricks.sql.common.url_utils import normalize_host_with_protocol


class TestNormalizeHostWithProtocol:
    """Tests for normalize_host_with_protocol function."""

    @pytest.mark.parametrize(
        "input_url,expected_output",
        [
            ("myserver.com", "https://myserver.com"),  # Add https://
            ("https://myserver.com", "https://myserver.com"),  # No duplicate
            ("http://localhost:8080", "http://localhost:8080"),  # Preserve http://
            ("myserver.com:443", "https://myserver.com:443"),  # With port
            ("myserver.com/", "https://myserver.com"),  # Remove trailing slash
            ("https://myserver.com///", "https://myserver.com"),  # Multiple slashes
            ("HTTPS://MyServer.COM", "https://MyServer.COM"),  # Case handling
        ],
    )
    def test_normalize_host_with_protocol(self, input_url, expected_output):
        """Test host normalization with various input formats."""
        result = normalize_host_with_protocol(input_url)
        assert result == expected_output

        # Additional assertions
        assert result.startswith("https://") or result.startswith("http://")
        assert not result.endswith("/")

    @pytest.mark.parametrize(
        "invalid_host",
        [
            None,
            "",
            "   ",  # Whitespace only
        ],
    )
    def test_normalize_host_with_protocol_raises_on_invalid_input(self, invalid_host):
        """Test that function raises ValueError for None or empty host."""
        with pytest.raises(ValueError, match="Host cannot be None or empty"):
            normalize_host_with_protocol(invalid_host)
