"""
Example demonstrating Kerberos authentication for HTTP proxy connections.

This example shows how to connect to Databricks SQL through an HTTP proxy
that requires Kerberos authentication.

Prerequisites:
1. Install Kerberos dependencies:
   pip install databricks-sql-connector[kerberos]

2. Obtain a valid Kerberos ticket:
   kinit user@EXAMPLE.COM

3. Set proxy environment variables:
   export HTTPS_PROXY=http://proxy.example.com:8080
   export HTTP_PROXY=http://proxy.example.com:8080
"""

import os
from databricks import sql


def connect_with_kerberos_proxy():
    """Example: Connect using Kerberos authentication to proxy"""
    
    # Ensure proxy is configured via environment variables
    if not os.environ.get('HTTPS_PROXY'):
        print("Please set HTTPS_PROXY environment variable")
        return
    
    connection = sql.connect(
        server_hostname="your-workspace.databricks.com",
        http_path="/sql/1.0/warehouses/your-warehouse",
        access_token="your-databricks-token",
        
        # Kerberos proxy authentication settings
        _proxy_auth_type="kerberos",
        _proxy_kerberos_service_name="HTTP",  # Default service name
        # _proxy_kerberos_principal="user@EXAMPLE.COM",  # Optional: specific principal
        # _proxy_kerberos_delegate=False,  # Optional: credential delegation
        _proxy_kerberos_mutual_auth="REQUIRED",  # Options: REQUIRED, OPTIONAL, DISABLED
    )
    
    cursor = connection.cursor()
    cursor.execute("SELECT 1")
    result = cursor.fetchone()
    print(f"Query result: {result}")
    
    cursor.close()
    connection.close()


def connect_with_kerberos_proxy_custom():
    """Example: Connect using custom Kerberos settings"""
    
    connection = sql.connect(
        server_hostname="your-workspace.databricks.com",
        http_path="/sql/1.0/warehouses/your-warehouse",
        access_token="your-databricks-token",
        
        # Custom Kerberos proxy settings
        _proxy_auth_type="kerberos",
        _proxy_kerberos_service_name="HTTP",
        _proxy_kerberos_principal="dbuser@CORP.EXAMPLE.COM",  # Specific principal
        _proxy_kerberos_delegate=True,  # Enable credential delegation
        _proxy_kerberos_mutual_auth="OPTIONAL",  # Less strict mutual auth
    )
    
    cursor = connection.cursor()
    cursor.execute("SHOW TABLES")
    tables = cursor.fetchall()
    print(f"Found {len(tables)} tables")
    
    cursor.close()
    connection.close()


def connect_with_fallback():
    """Example: Try Kerberos first, fall back to basic auth"""
    
    try:
        # First try Kerberos authentication
        print("Attempting Kerberos proxy authentication...")
        connection = sql.connect(
            server_hostname="your-workspace.databricks.com",
            http_path="/sql/1.0/warehouses/your-warehouse",
            access_token="your-databricks-token",
            _proxy_auth_type="kerberos",
        )
        print("Successfully connected with Kerberos")
        
    except ImportError:
        print("Kerberos libraries not available, falling back to basic auth")
        # Fall back to basic proxy authentication
        # Set proxy with credentials in environment variable:
        # export HTTPS_PROXY=http://username:password@proxy.example.com:8080
        connection = sql.connect(
            server_hostname="your-workspace.databricks.com",
            http_path="/sql/1.0/warehouses/your-warehouse",
            access_token="your-databricks-token",
        )
        print("Connected with basic proxy auth")
    
    except Exception as e:
        print(f"Kerberos authentication failed: {e}")
        raise
    
    # Use the connection
    cursor = connection.cursor()
    cursor.execute("SELECT current_date()")
    result = cursor.fetchone()
    print(f"Current date: {result[0]}")
    
    cursor.close()
    connection.close()


if __name__ == "__main__":
    # Example 1: Basic Kerberos proxy authentication
    print("=== Example 1: Basic Kerberos Proxy ===")
    connect_with_kerberos_proxy()
    
    print("\n=== Example 2: Custom Kerberos Settings ===")
    connect_with_kerberos_proxy_custom()
    
    print("\n=== Example 3: Kerberos with Fallback ===")
    connect_with_fallback()