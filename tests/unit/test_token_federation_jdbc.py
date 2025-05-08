#!/usr/bin/env python3

"""
Unit tests for the JDBC-style token refresh in Databricks SQL connector.

This test verifies that the token federation implementation follows the JDBC driver's approach
of getting a fresh external token before exchanging it for a Databricks token during refresh.
"""

import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone, timedelta

from databricks.sql.auth.token_federation import (
    DatabricksTokenFederationProvider,
    Token
)


class RefreshingCredentialsProvider:
    """
    A credentials provider that returns different tokens on each call.
    This simulates providers like Azure AD that can refresh their tokens.
    """
    
    def __init__(self):
        self.call_count = 0
    
    def auth_type(self):
        return "bearer"
    
    def __call__(self, *args, **kwargs):
        def get_headers():
            self.call_count += 1
            # Return a different token each time to simulate fresh tokens
            return {"Authorization": f"Bearer fresh_token_{self.call_count}"}
        return get_headers


class TestJdbcStyleTokenRefresh(unittest.TestCase):
    """Tests for the JDBC-style token refresh implementation."""
    
    @patch('databricks.sql.auth.token_federation.DatabricksTokenFederationProvider._parse_jwt_claims')
    @patch('databricks.sql.auth.token_federation.DatabricksTokenFederationProvider._exchange_token')
    @patch('databricks.sql.auth.token_federation.DatabricksTokenFederationProvider._is_same_host')
    def test_refresh_gets_fresh_token(self, mock_is_same_host, mock_exchange_token, mock_parse_jwt):
        """Test that token refresh first gets a fresh external token."""
        # Set up mocks
        mock_parse_jwt.return_value = {"iss": "https://login.microsoftonline.com/tenant"}
        mock_is_same_host.return_value = False
        
        # Create a credentials provider that returns different tokens on each call
        refreshing_provider = RefreshingCredentialsProvider()
        
        # Set up the token federation provider
        federation_provider = DatabricksTokenFederationProvider(
            refreshing_provider, "example.com", "client_id"
        )
        
        # Set up mock for token exchange
        future_time = datetime.now(tz=timezone.utc) + timedelta(hours=1)
        mock_exchange_token.return_value = Token(
            "exchanged_token_1", "Bearer", expiry=future_time
        )
        
        # First call to get initial headers and token
        headers_factory = federation_provider()
        headers = headers_factory()
        
        # Verify the first exchange happened
        mock_exchange_token.assert_called_with("fresh_token_1", "azure")
        self.assertEqual(headers["Authorization"], "Bearer exchanged_token_1")
        self.assertEqual(refreshing_provider.call_count, 1)
        
        # Reset the mock to track the next call
        mock_exchange_token.reset_mock()
        
        # Now simulate an approaching expiry
        near_expiry = datetime.now(tz=timezone.utc) + timedelta(minutes=4)
        federation_provider.last_exchanged_token = Token(
            "exchanged_token_1", "Bearer", expiry=near_expiry
        )
        federation_provider.last_external_token = "fresh_token_1"
        
        # Set up the mock to return a different token for the refresh
        mock_exchange_token.return_value = Token(
            "exchanged_token_2", "Bearer", expiry=future_time
        )
        
        # Make a second call which should trigger refresh
        headers = headers_factory()
        
        # With JDBC-style implementation:
        # 1. Should call credentials provider again to get fresh token
        self.assertEqual(refreshing_provider.call_count, 2)
        
        # 2. Should exchange the FRESH token (fresh_token_2), not the stored one (fresh_token_1)
        mock_exchange_token.assert_called_once_with("fresh_token_2", "azure")
        
        # 3. Should return headers with the new Databricks token
        self.assertEqual(headers["Authorization"], "Bearer exchanged_token_2")


if __name__ == "__main__":
    unittest.main() 