#!/usr/bin/env python3
"""
Example: Databricks SQL Connector with Proxy Authentication

This example demonstrates how to connect to Databricks through a proxy server
using different authentication methods:
1. Basic authentication (username/password in proxy URL)
2. Kerberos/Negotiate authentication
3. Default system proxy behavior

Prerequisites:
- Configure your system proxy settings (HTTP_PROXY/HTTPS_PROXY environment variables)
- For Kerberos: Ensure you have valid Kerberos tickets (kinit)
- Set your Databricks credentials in environment variables
"""

import os
from databricks import sql
import logging

# Configure logging to see proxy activity
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Uncomment for detailed debugging (shows HTTP requests/responses)
# logging.getLogger("urllib3").setLevel(logging.DEBUG)
# logging.getLogger("urllib3.connectionpool").setLevel(logging.DEBUG)

def check_proxy_environment():
    """Check if proxy environment variables are configured."""
    proxy_vars = ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy']
    configured_proxies = {var: os.environ.get(var) for var in proxy_vars if os.environ.get(var)}
    
    if configured_proxies:
        print("✓ Proxy environment variables found:")
        for var, value in configured_proxies.items():
            # Hide credentials in output for security
            safe_value = value.split('@')[-1] if '@' in value else value
            print(f"  {var}: {safe_value}")
        return True
    else:
        print("⚠ No proxy environment variables found")
        print("  Set HTTP_PROXY and/or HTTPS_PROXY if using a proxy")
        return False

def test_connection(connection_params, test_name):
    """Test a database connection with given parameters."""
    print(f"\n--- Testing {test_name} ---")
    
    try:
        with sql.connect(**connection_params) as connection:
            print("✓ Successfully connected!")
            
            with connection.cursor() as cursor:
                # Test basic query
                cursor.execute("SELECT current_user() as user, current_database() as database")
                result = cursor.fetchone()
                print(f"✓ Connected as user: {result.user}")
                print(f"✓ Default database: {result.database}")
                
                # Test a simple computation
                cursor.execute("SELECT 1 + 1 as result")
                result = cursor.fetchone()
                print(f"✓ Query result: 1 + 1 = {result.result}")
                
        return True
        
    except Exception as e:
        print(f"✗ Connection failed: {e}")
        return False

def main():
    print("Databricks SQL Connector - Proxy Authentication Examples")
    print("=" * 60)
    
    # Check proxy configuration
    has_proxy = check_proxy_environment()
    
    # Get Databricks connection parameters
    server_hostname = os.environ.get('DATABRICKS_SERVER_HOSTNAME')
    http_path = os.environ.get('DATABRICKS_HTTP_PATH')
    access_token = os.environ.get('DATABRICKS_TOKEN')
    
    if not all([server_hostname, http_path, access_token]):
        print("\n✗ Missing required environment variables:")
        print("  DATABRICKS_SERVER_HOSTNAME")
        print("  DATABRICKS_HTTP_PATH") 
        print("  DATABRICKS_TOKEN")
        return 1
    
    print(f"\nConnecting to: {server_hostname}")
    
    # Base connection parameters
    base_params = {
        'server_hostname': server_hostname,
        'http_path': http_path,
        'access_token': access_token
    }
    
    success_count = 0
    total_tests = 0
    
    # Test 1: Default proxy behavior (no _proxy_auth_method specified)
    # This uses basic auth if credentials are in proxy URL, otherwise no auth
    print("\n" + "="*60)
    print("Test 1: Default Proxy Behavior")
    print("Uses basic authentication if credentials are in proxy URL")
    total_tests += 1
    if test_connection(base_params, "Default Proxy Behavior"):
        success_count += 1
    
    # Test 2: Explicit basic authentication
    print("\n" + "="*60)
    print("Test 2: Explicit Basic Authentication")
    print("Explicitly requests basic authentication (same as default)")
    total_tests += 1
    basic_params = base_params.copy()
    basic_params['_proxy_auth_method'] = 'basic'
    if test_connection(basic_params, "Basic Proxy Authentication"):
        success_count += 1
    
    # Test 3: Kerberos/Negotiate authentication
    print("\n" + "="*60)
    print("Test 3: Kerberos/Negotiate Authentication")
    print("Uses Kerberos tickets for proxy authentication")
    print("Note: Requires valid Kerberos tickets (run 'kinit' first)")
    total_tests += 1
    kerberos_params = base_params.copy()
    kerberos_params['_proxy_auth_method'] = 'negotiate'
    if test_connection(kerberos_params, "Kerberos Proxy Authentication"):
        success_count += 1
    
    # Summary
    print(f"\n{'='*60}")
    print(f"Summary: {success_count}/{total_tests} tests passed")
    
    if success_count == total_tests:
        print("✓ All proxy authentication methods working!")
        return 0
    elif success_count > 0:
        print("⚠ Some proxy authentication methods failed")
        print("This may be normal depending on your proxy configuration")
        return 0
    else:
        print("✗ All proxy authentication methods failed")
        if not has_proxy:
            print("Consider checking your proxy configuration")
        return 1

if __name__ == "__main__":
    exit(main())
