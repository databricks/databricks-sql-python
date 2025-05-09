#!/usr/bin/env python3

"""
Unit tests for token federation functionality in the Databricks SQL connector.
"""

import unittest
from unittest.mock import patch, MagicMock
import json
from datetime import datetime, timezone, timedelta

from databricks.sql.auth.token_federation import (
    Token,
    DatabricksTokenFederationProvider,
    SimpleCredentialsProvider,
    create_token_federation_provider,
    TOKEN_REFRESH_BUFFER_SECONDS
)


class TestToken(unittest.TestCase):
    """Tests for the Token class."""

    def test_token_initialization(self):
        """Test Token initialization."""
        token = Token("access_token_value", "Bearer", "refresh_token_value")
        self.assertEqual(token.access_token, "access_token_value")
        self.assertEqual(token.token_type, "Bearer")
        self.assertEqual(token.refresh_token, "refresh_token_value")
        
    def test_token_is_expired(self):
        """Test Token is_expired method."""
        # Token with expiry in the past
        past = datetime.now(tz=timezone.utc) - timedelta(hours=1)
        token = Token("access_token", "Bearer", expiry=past)
        self.assertTrue(token.is_expired())
        
        # Token with expiry in the future
        future = datetime.now(tz=timezone.utc) + timedelta(hours=1)
        token = Token("access_token", "Bearer", expiry=future)
        self.assertFalse(token.is_expired())
    
    def test_token_needs_refresh(self):
        """Test Token needs_refresh method."""
        # Token with expiry in the past
        past = datetime.now(tz=timezone.utc) - timedelta(hours=1)
        token = Token("access_token", "Bearer", expiry=past)
        self.assertTrue(token.needs_refresh())
        
        # Token with expiry in the near future (within refresh buffer)
        near_future = datetime.now(tz=timezone.utc) + timedelta(seconds=TOKEN_REFRESH_BUFFER_SECONDS - 60)
        token = Token("access_token", "Bearer", expiry=near_future)
        self.assertTrue(token.needs_refresh())
        
        # Token with expiry far in the future
        far_future = datetime.now(tz=timezone.utc) + timedelta(seconds=TOKEN_REFRESH_BUFFER_SECONDS + 60)
        token = Token("access_token", "Bearer", expiry=far_future)
        self.assertFalse(token.needs_refresh())


class TestSimpleCredentialsProvider(unittest.TestCase):
    """Tests for the SimpleCredentialsProvider class."""
    
    def test_simple_credentials_provider(self):
        """Test SimpleCredentialsProvider."""
        provider = SimpleCredentialsProvider("token_value", "Bearer", "custom_auth_type")
        self.assertEqual(provider.auth_type(), "custom_auth_type")
        
        header_factory = provider()
        headers = header_factory()
        self.assertEqual(headers, {"Authorization": "Bearer token_value"})


class TestTokenFederationProvider(unittest.TestCase):
    """Tests for the DatabricksTokenFederationProvider class."""
    
    def test_host_property(self):
        """Test the host property of DatabricksTokenFederationProvider."""
        creds_provider = SimpleCredentialsProvider("token")
        federation_provider = DatabricksTokenFederationProvider(
            creds_provider, "example.com", "client_id"
        )
        self.assertEqual(federation_provider.host, "example.com")
        self.assertEqual(federation_provider.hostname, "example.com")
    
    @patch('databricks.sql.auth.token_federation.requests.get')
    @patch('databricks.sql.auth.token_federation.get_oauth_endpoints')
    def test_init_oidc_discovery(self, mock_get_endpoints, mock_requests_get):
        """Test _init_oidc_discovery method."""
        # Mock the get_oauth_endpoints function
        mock_endpoints = MagicMock()
        mock_endpoints.get_openid_config_url.return_value = "https://example.com/openid-config"
        mock_get_endpoints.return_value = mock_endpoints
        
        # Mock the requests.get response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"token_endpoint": "https://example.com/token"}
        mock_requests_get.return_value = mock_response
        
        # Create the provider
        creds_provider = SimpleCredentialsProvider("token")
        federation_provider = DatabricksTokenFederationProvider(
            creds_provider, "example.com", "client_id"
        )
        
        # Call the method
        federation_provider._init_oidc_discovery()
        
        # Check if the token endpoint was set correctly
        self.assertEqual(federation_provider.token_endpoint, "https://example.com/token")
        
        # Test fallback when discovery fails
        mock_requests_get.side_effect = Exception("Connection error")
        federation_provider.token_endpoint = None
        federation_provider._init_oidc_discovery()
        self.assertEqual(federation_provider.token_endpoint, "https://example.com/oidc/v1/token")
    
    @patch('databricks.sql.auth.token_federation.DatabricksTokenFederationProvider._parse_jwt_claims')
    @patch('databricks.sql.auth.token_federation.DatabricksTokenFederationProvider._exchange_token')
    @patch('databricks.sql.auth.token_federation.DatabricksTokenFederationProvider._is_same_host')
    @patch('databricks.sql.auth.token_federation.DatabricksTokenFederationProvider._detect_idp_from_claims')
    def test_token_refresh(self, mock_detect_idp, mock_is_same_host, mock_exchange_token, mock_parse_jwt):
        """Test token refresh functionality for approaching expiry."""
        # Set up mocks
        mock_parse_jwt.return_value = {"iss": "https://login.microsoftonline.com/tenant"}
        mock_is_same_host.return_value = False
        mock_detect_idp.return_value = "azure"
        
        # Create mock credentials provider that can return different tokens for different calls
        mock_creds_provider = MagicMock()
        
        # First call returns initial_token, second call returns fresh_token
        initial_headers = {"Authorization": "Bearer initial_token"}
        fresh_headers = {"Authorization": "Bearer fresh_token"}
        
        # Set up initial header factory
        initial_header_factory = MagicMock()
        initial_header_factory.return_value = initial_headers
        
        # Set up fresh header factory for second call
        fresh_header_factory = MagicMock()
        fresh_header_factory.return_value = fresh_headers
        
        # Configure the mock to return factories
        mock_creds_provider.side_effect = [initial_header_factory, fresh_header_factory]
        
        # Set up the token federation provider
        federation_provider = DatabricksTokenFederationProvider(
            mock_creds_provider, "example.com", "client_id"
        )
        
        # Mock the token exchange to return a known token
        future_time = datetime.now(tz=timezone.utc) + timedelta(hours=1)
        mock_exchange_token.return_value = Token(
            "exchanged_token_1", "Bearer", expiry=future_time
        )
        
        # First call to get initial headers and token - this should trigger an exchange
        headers_factory = federation_provider()
        headers = headers_factory()
        
        # Verify the exchange happened with the initial token
        mock_exchange_token.assert_called_with("initial_token", "azure")
        self.assertEqual(headers["Authorization"], "Bearer exchanged_token_1")
        
        # Reset the mocks to track the next call
        mock_exchange_token.reset_mock()
        mock_creds_provider.reset_mock()
        mock_creds_provider.return_value = fresh_header_factory
        
        # Now simulate an approaching expiry
        near_expiry = datetime.now(tz=timezone.utc) + timedelta(seconds=TOKEN_REFRESH_BUFFER_SECONDS - 60)
        federation_provider.last_exchanged_token = Token(
            "exchanged_token_1", "Bearer", expiry=near_expiry
        )
        federation_provider.last_external_token = "initial_token"
        
        # Set up the mock to return a different token for the refresh
        mock_exchange_token.return_value = Token(
            "exchanged_token_2", "Bearer", expiry=future_time
        )
        
        # Make a second call which should trigger refresh
        headers = headers_factory()
        
        # Verify a fresh token was requested from the credentials provider
        # and the exchange was performed with the fresh token
        mock_exchange_token.assert_called_once_with("fresh_token", "azure")
        
        # Verify the headers contain the new token
        self.assertEqual(headers["Authorization"], "Bearer exchanged_token_2")


class TestTokenFederationFactory(unittest.TestCase):
    """Tests for the token federation factory function."""
    
    def test_create_token_federation_provider(self):
        """Test create_token_federation_provider function."""
        provider = create_token_federation_provider(
            "token_value", "example.com", "client_id", "Bearer"
        )
        
        self.assertIsInstance(provider, DatabricksTokenFederationProvider)
        self.assertEqual(provider.hostname, "example.com")
        self.assertEqual(provider.identity_federation_client_id, "client_id")
        
        # Test that the underlying credentials provider was set up correctly
        self.assertEqual(provider.credentials_provider.token, "token_value")
        self.assertEqual(provider.credentials_provider.token_type, "Bearer")


if __name__ == "__main__":
    unittest.main() 