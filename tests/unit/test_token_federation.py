import unittest
from unittest.mock import Mock, MagicMock, patch, call
import json
import jwt
from datetime import datetime, timedelta
import requests

from databricks.sql.auth.token_federation import TokenFederationProvider, ExternalTokenProvider
from databricks.sql.auth.authenticators import AccessTokenAuthProvider


class TestTokenFederationProvider(unittest.TestCase):
    
    def setUp(self):
        self.hostname = "https://test.databricks.com/"
        self.external_provider = Mock()
        self.http_client = Mock()
        self.identity_federation_client_id = "test-client-id"
        
        self.provider = TokenFederationProvider(
            hostname=self.hostname,
            external_provider=self.external_provider,
            http_client=self.http_client,
            identity_federation_client_id=self.identity_federation_client_id,
        )
        
    def test_normalize_hostname(self):
        """Test hostname normalization."""
        test_cases = [
            ("test.databricks.com", "https://test.databricks.com/"),
            ("https://test.databricks.com", "https://test.databricks.com/"),
            ("https://test.databricks.com/", "https://test.databricks.com/"),
            ("test.databricks.com/", "https://test.databricks.com/"),
        ]
        
        for input_hostname, expected in test_cases:
            provider = TokenFederationProvider(
                hostname=input_hostname,
                external_provider=self.external_provider,
            )
            self.assertEqual(provider.hostname, expected)
            
    def test_extract_token_from_header(self):
        """Test extraction of token from Authorization header."""
        # Valid header
        token_type, access_token = self.provider._extract_token_from_header(
            "Bearer test-token-123"
        )
        self.assertEqual(token_type, "Bearer")
        self.assertEqual(access_token, "test-token-123")
        
        # Invalid header - missing token
        with self.assertRaises(ValueError):
            self.provider._extract_token_from_header("Bearer")
            
        # Invalid header - empty
        with self.assertRaises(ValueError):
            self.provider._extract_token_from_header("")
            
    def test_is_same_host(self):
        """Test host comparison."""
        test_cases = [
            ("https://test.databricks.com", "https://test.databricks.com", True),
            ("https://test.databricks.com", "https://test.databricks.com:443", False),
            ("https://test1.databricks.com", "https://test2.databricks.com", False),
            ("https://login.microsoftonline.com", "https://test.databricks.com", False),
        ]
        
        for url1, url2, expected in test_cases:
            result = self.provider._is_same_host(url1, url2)
            self.assertEqual(result, expected)
            
    def test_should_exchange_token_different_issuer(self):
        """Test that token exchange is triggered for different issuer."""
        # Create a mock JWT with different issuer
        token_payload = {
            "iss": "https://login.microsoftonline.com/tenant-id/",
            "aud": "databricks",
            "exp": int((datetime.now() + timedelta(hours=1)).timestamp()),
        }
        
        with patch("jwt.decode", return_value=token_payload):
            result = self.provider._should_exchange_token("mock-jwt-token")
            self.assertTrue(result)
            
    def test_should_exchange_token_same_issuer(self):
        """Test that token exchange is not triggered for same issuer."""
        # Create a mock JWT with same issuer
        token_payload = {
            "iss": "https://test.databricks.com",
            "aud": "databricks",
            "exp": int((datetime.now() + timedelta(hours=1)).timestamp()),
        }
        
        with patch("jwt.decode", return_value=token_payload):
            result = self.provider._should_exchange_token("mock-jwt-token")
            self.assertFalse(result)
            
    def test_exchange_token_success(self):
        """Test successful token exchange."""
        access_token = "external-token-123"
        
        # Mock successful response (UnifiedHttpClient style)
        mock_response = Mock()
        mock_response.data = json.dumps({
            "access_token": "databricks-token-456",
            "token_type": "Bearer",
            "expires_in": 3600,
        }).encode('utf-8')
        mock_response.status = 200
        
        self.http_client.request.return_value = mock_response
        
        result = self.provider._exchange_token(access_token)
        
        # Verify the request
        self.http_client.request.assert_called_once()
        call_args = self.http_client.request.call_args
        
        # Check method and URL (HttpMethod.POST is first arg, URL is second)
        from databricks.sql.common.http import HttpMethod
        self.assertEqual(call_args[0][0], HttpMethod.POST)
        self.assertEqual(call_args[1]["url"], f"{self.hostname}oidc/v1/token")
        
        # Check body contains expected parameters
        from urllib.parse import parse_qs
        body = call_args[1]["body"]
        parsed_body = parse_qs(body)
        
        # Verify all expected params are in the body
        self.assertEqual(parsed_body["grant_type"][0], "urn:ietf:params:oauth:grant-type:token-exchange")
        self.assertEqual(parsed_body["subject_token"][0], access_token)
        self.assertEqual(parsed_body["subject_token_type"][0], "urn:ietf:params:oauth:token-type:jwt")
        self.assertEqual(parsed_body["scope"][0], "sql")
        self.assertEqual(parsed_body["return_original_token_if_authenticated"][0], "true")
        self.assertEqual(parsed_body["client_id"][0], self.identity_federation_client_id)
        
        # Check result
        self.assertEqual(result["access_token"], "databricks-token-456")
        self.assertEqual(result["token_type"], "Bearer")
        self.assertEqual(result["expires_in"], 3600)
        
    def test_exchange_token_failure(self):
        """Test token exchange failure handling."""
        access_token = "external-token-123"
        
        # Mock failed response
        mock_response = Mock()
        mock_response.data = b'{"error": "invalid_request"}'
        mock_response.status = 400
        self.http_client.request.return_value = mock_response
        
        # Should not raise, but should return None or handle gracefully
        # The actual implementation should handle this
        with self.assertRaises(KeyError):  # Will raise KeyError due to missing access_token
            self.provider._exchange_token(access_token)
            
    def test_add_headers_with_token_exchange(self):
        """Test adding headers with token exchange."""
        # Setup external provider to return a token
        self.external_provider.add_headers = Mock(
            side_effect=lambda headers: headers.update({
                "Authorization": "Bearer external-token-123"
            })
        )
        
        # Mock JWT decode to indicate different issuer
        token_payload = {
            "iss": "https://login.microsoftonline.com/tenant-id/",
            "aud": "databricks",
            "exp": int((datetime.now() + timedelta(hours=1)).timestamp()),
        }
        
        # Mock successful token exchange
        mock_response = Mock()
        mock_response.data = json.dumps({
            "access_token": "databricks-token-456",
            "token_type": "Bearer",
            "expires_in": 3600,
        }).encode('utf-8')
        mock_response.status = 200
        self.http_client.request.return_value = mock_response
        
        with patch("jwt.decode", return_value=token_payload):
            headers = {}
            self.provider.add_headers(headers)
            
            # Should have the exchanged token
            self.assertEqual(headers["Authorization"], "Bearer databricks-token-456")
            
    def test_add_headers_without_token_exchange(self):
        """Test adding headers without token exchange (same issuer)."""
        # Setup external provider to return a token
        self.external_provider.add_headers = Mock(
            side_effect=lambda headers: headers.update({
                "Authorization": "Bearer external-token-123"
            })
        )
        
        # Mock JWT decode to indicate same issuer
        token_payload = {
            "iss": "https://test.databricks.com",
            "aud": "databricks",
            "exp": int((datetime.now() + timedelta(hours=1)).timestamp()),
        }
        
        with patch("jwt.decode", return_value=token_payload):
            headers = {}
            self.provider.add_headers(headers)
            
            # Should have the original external token
            self.assertEqual(headers["Authorization"], "Bearer external-token-123")
            
    def test_token_caching(self):
        """Test that tokens are cached and reused."""
        # Setup external provider
        self.external_provider.add_headers = Mock(
            side_effect=lambda headers: headers.update({
                "Authorization": "Bearer external-token-123"
            })
        )
        
        # Mock JWT decode
        token_payload = {
            "iss": "https://test.databricks.com",
            "exp": int((datetime.now() + timedelta(hours=1)).timestamp()),
        }
        
        with patch("jwt.decode", return_value=token_payload):
            # First call
            headers1 = {}
            self.provider.add_headers(headers1)
            
            # Second call - should use cached token
            headers2 = {}
            self.provider.add_headers(headers2)
            
            # External provider should only be called once
            self.assertEqual(self.external_provider.add_headers.call_count, 1)
            
            # Both headers should be the same
            self.assertEqual(headers1["Authorization"], headers2["Authorization"])
            
    def test_token_cache_expiry(self):
        """Test that expired cached tokens are refreshed."""
        # Setup external provider
        call_count = [0]
        def add_headers_side_effect(headers):
            call_count[0] += 1
            headers.update({
                "Authorization": f"Bearer external-token-{call_count[0]}"
            })
            
        self.external_provider.add_headers = Mock(side_effect=add_headers_side_effect)
        
        # Mock JWT decode with short expiry
        token_payload = {
            "iss": "https://test.databricks.com",
            "exp": int((datetime.now() + timedelta(seconds=5)).timestamp()),
        }
        
        with patch("jwt.decode", return_value=token_payload):
            # First call
            headers1 = {}
            self.provider.add_headers(headers1)
            self.assertEqual(headers1["Authorization"], "Bearer external-token-1")
            
            # Expire the cache
            self.provider._cached_token_expiry = datetime.now() - timedelta(seconds=1)
            
            # Second call - should get new token
            headers2 = {}
            self.provider.add_headers(headers2)
            self.assertEqual(headers2["Authorization"], "Bearer external-token-2")
            
            # External provider should be called twice
            self.assertEqual(self.external_provider.add_headers.call_count, 2)


class TestExternalTokenProvider(unittest.TestCase):
    
    def test_add_headers(self):
        """Test adding headers from external credentials provider."""
        # Create mock credentials provider
        mock_headers = {
            "Authorization": "Bearer test-token",
            "X-Custom-Header": "custom-value",
        }
        credentials_provider = Mock(return_value=Mock(return_value=mock_headers))
        
        provider = ExternalTokenProvider(credentials_provider)
        
        # Test adding headers
        request_headers = {}
        provider.add_headers(request_headers)
        
        # Verify headers were added
        self.assertEqual(request_headers["Authorization"], "Bearer test-token")
        self.assertEqual(request_headers["X-Custom-Header"], "custom-value")
        
        # Verify credentials provider was called once
        credentials_provider.assert_called_once()
        
    def test_header_factory_cached(self):
        """Test that header factory is cached."""
        mock_headers = {"Authorization": "Bearer test-token"}
        header_factory = Mock(return_value=mock_headers)
        credentials_provider = Mock(return_value=header_factory)
        
        provider = ExternalTokenProvider(credentials_provider)
        
        # Call add_headers multiple times
        for _ in range(3):
            request_headers = {}
            provider.add_headers(request_headers)
            
        # Credentials provider should only be called once
        credentials_provider.assert_called_once()
        
        # Header factory should be called 3 times
        self.assertEqual(header_factory.call_count, 3)


if __name__ == "__main__":
    unittest.main()