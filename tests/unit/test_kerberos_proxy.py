import unittest
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# Add the src directory to the path so we can import our modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../src'))

from databricks.sql.auth.kerberos_proxy import KerberosProxyAuth, KERBEROS_AVAILABLE


class TestKerberosProxyAuth(unittest.TestCase):
    """Test cases for Kerberos proxy authentication"""
    
    @unittest.skipIf(not KERBEROS_AVAILABLE, "Kerberos libraries not available")
    @patch('databricks.sql.auth.kerberos_proxy.kerberos')
    def test_generate_proxy_auth_header_default_credentials(self, mock_kerberos):
        """Test generating Kerberos auth header with default credentials"""
        # Setup mocks
        mock_kerberos.authGSSClientInit.return_value = (1, "mock_context")
        mock_kerberos.authGSSClientStep.return_value = 1
        mock_kerberos.authGSSClientResponse.return_value = "base64encodedtoken"
        mock_kerberos.GSS_C_MUTUAL_FLAG = 1
        mock_kerberos.GSS_C_SEQUENCE_FLAG = 2
        
        # Create auth instance
        auth = KerberosProxyAuth()
        
        # Generate header
        headers = auth.generate_proxy_auth_header("proxy.example.com")
        
        # Verify
        self.assertEqual(headers["Proxy-Authorization"], "Negotiate base64encodedtoken")
        mock_kerberos.authGSSClientInit.assert_called_once_with(
            "HTTP@proxy.example.com",
            gssflags=3  # MUTUAL_FLAG | SEQUENCE_FLAG
        )
        mock_kerberos.authGSSClientStep.assert_called_once_with("mock_context", "")
        
    @unittest.skipIf(not KERBEROS_AVAILABLE, "Kerberos libraries not available")
    @patch('databricks.sql.auth.kerberos_proxy.kerberos')
    def test_generate_proxy_auth_header_with_principal(self, mock_kerberos):
        """Test generating Kerberos auth header with specific principal"""
        # Setup mocks
        mock_kerberos.authGSSClientInit.return_value = (1, "mock_context")
        mock_kerberos.authGSSClientStep.return_value = 1
        mock_kerberos.authGSSClientResponse.return_value = "base64encodedtoken"
        mock_kerberos.GSS_C_MUTUAL_FLAG = 1
        mock_kerberos.GSS_C_SEQUENCE_FLAG = 2
        
        # Create auth instance with principal
        auth = KerberosProxyAuth(principal="user@EXAMPLE.COM")
        
        # Generate header
        headers = auth.generate_proxy_auth_header("proxy.example.com")
        
        # Verify
        self.assertEqual(headers["Proxy-Authorization"], "Negotiate base64encodedtoken")
        mock_kerberos.authGSSClientInit.assert_called_once_with(
            "HTTP@proxy.example.com",
            principal="user@EXAMPLE.COM",
            gssflags=3
        )
        
    @unittest.skipIf(not KERBEROS_AVAILABLE, "Kerberos libraries not available")
    @patch('databricks.sql.auth.kerberos_proxy.kerberos')
    def test_generate_proxy_auth_header_with_delegation(self, mock_kerberos):
        """Test generating Kerberos auth header with credential delegation"""
        # Setup mocks
        mock_kerberos.authGSSClientInit.return_value = (1, "mock_context")
        mock_kerberos.authGSSClientStep.return_value = 1
        mock_kerberos.authGSSClientResponse.return_value = "base64encodedtoken"
        mock_kerberos.GSS_C_MUTUAL_FLAG = 1
        mock_kerberos.GSS_C_SEQUENCE_FLAG = 2
        mock_kerberos.GSS_C_DELEG_FLAG = 4
        
        # Create auth instance with delegation
        auth = KerberosProxyAuth(delegate=True)
        
        # Generate header
        headers = auth.generate_proxy_auth_header("proxy.example.com")
        
        # Verify delegation flag is set
        mock_kerberos.authGSSClientInit.assert_called_once_with(
            "HTTP@proxy.example.com",
            gssflags=7  # MUTUAL_FLAG | SEQUENCE_FLAG | DELEG_FLAG
        )
        
    @unittest.skipIf(not KERBEROS_AVAILABLE, "Kerberos libraries not available")
    @patch('databricks.sql.auth.kerberos_proxy.kerberos')
    def test_handle_407_response(self, mock_kerberos):
        """Test handling 407 proxy authentication response"""
        # Setup mocks
        mock_kerberos.authGSSClientStep.return_value = 1
        mock_kerberos.authGSSClientStepResult.return_value = mock_kerberos.AUTH_GSS_COMPLETE
        mock_kerberos.authGSSClientResponse.return_value = "responsetoken"
        mock_kerberos.AUTH_GSS_COMPLETE = 1
        
        # Create auth instance and set context
        auth = KerberosProxyAuth()
        auth._context = "mock_context"
        
        # Handle 407 response
        result = auth.handle_407_response("Negotiate servertoken123")
        
        # Verify
        self.assertEqual(result["Proxy-Authorization"], "Negotiate responsetoken")
        mock_kerberos.authGSSClientStep.assert_called_once_with("mock_context", "servertoken123")
        
    @unittest.skipIf(not KERBEROS_AVAILABLE, "Kerberos libraries not available")
    @patch('databricks.sql.auth.kerberos_proxy.kerberos')
    def test_cleanup(self, mock_kerberos):
        """Test cleanup of Kerberos context"""
        # Create auth instance and set context
        auth = KerberosProxyAuth()
        auth._context = "mock_context"
        
        # Call cleanup
        auth.cleanup()
        
        # Verify
        mock_kerberos.authGSSClientClean.assert_called_once_with("mock_context")
        self.assertIsNone(auth._context)
        
    @unittest.skipIf(not KERBEROS_AVAILABLE, "Kerberos libraries not available")
    @patch('databricks.sql.auth.kerberos_proxy.kerberos')
    def test_error_handling(self, mock_kerberos):
        """Test error handling in Kerberos authentication"""
        # Setup mock to raise error
        mock_kerberos.GSSError = Exception  # Define GSSError for the test
        mock_kerberos.authGSSClientInit.side_effect = Exception("GSS init failed")
        
        # Create auth instance
        auth = KerberosProxyAuth()
        
        # Verify exception is raised
        with self.assertRaises(Exception) as cm:
            auth.generate_proxy_auth_header("proxy.example.com")
        
        self.assertIn("GSS init failed", str(cm.exception))
        
    def test_kerberos_not_available(self):
        """Test behavior when Kerberos libraries are not available"""
        # Mock the KERBEROS_AVAILABLE flag
        with patch('databricks.sql.auth.kerberos_proxy.KERBEROS_AVAILABLE', False):
            # Verify ImportError is raised
            with self.assertRaises(ImportError) as cm:
                KerberosProxyAuth()
            
            self.assertIn("Kerberos proxy authentication requires", str(cm.exception))


class TestTHttpClientKerberosIntegration(unittest.TestCase):
    """Test integration of Kerberos auth with THttpClient"""
    
    @patch('databricks.sql.auth.thrift_http_client.KERBEROS_AVAILABLE', True)
    @patch('databricks.sql.auth.thrift_http_client.KerberosProxyAuth')
    @patch('urllib.request.getproxies')
    @patch('urllib.request.proxy_bypass')
    def test_thrift_client_with_kerberos_proxy(self, mock_proxy_bypass, mock_getproxies, mock_kerberos_auth):
        """Test THttpClient initialization with Kerberos proxy auth"""
        from databricks.sql.auth.thrift_http_client import THttpClient
        from databricks.sql.auth.authenticators import AuthProvider
        
        # Setup proxy mocks
        mock_getproxies.return_value = {'https': 'http://proxy.example.com:8080'}
        mock_proxy_bypass.return_value = False
        
        # Setup Kerberos auth mock
        mock_auth_instance = Mock()
        mock_auth_instance.generate_proxy_auth_header.return_value = {
            "Proxy-Authorization": "Negotiate token123"
        }
        mock_kerberos_auth.return_value = mock_auth_instance
        
        # Create THttpClient with Kerberos proxy
        auth_provider = AuthProvider()
        client = THttpClient(
            auth_provider=auth_provider,
            uri_or_host="https://databricks.example.com/sql/path",
            proxy_auth_type="kerberos",
            proxy_kerberos_service_name="HTTP",
            proxy_kerberos_principal="user@EXAMPLE.COM",
        )
        
        # Verify Kerberos auth was initialized
        mock_kerberos_auth.assert_called_once_with(
            service_name="HTTP",
            principal="user@EXAMPLE.COM",
            delegate=False,
            mutual_authentication=1  # REQUIRED
        )
        
        # Verify proxy settings
        self.assertEqual(client.proxy_uri, 'http://proxy.example.com:8080')
        self.assertEqual(client.host, 'proxy.example.com')
        self.assertEqual(client.port, 8080)
        self.assertEqual(client.realhost, 'databricks.example.com')
        
        # Test _get_proxy_headers method
        headers = client._get_proxy_headers()
        self.assertEqual(headers["Proxy-Authorization"], "Negotiate token123")
        mock_auth_instance.generate_proxy_auth_header.assert_called_once_with('proxy.example.com')


if __name__ == '__main__':
    unittest.main()