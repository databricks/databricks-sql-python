# Proxy Configuration Guide

This guide explains how to configure the Databricks SQL Connector for Python to work with HTTP/HTTPS proxies, including support for Kerberos authentication.

## Table of Contents
- [Basic Proxy Configuration](#basic-proxy-configuration)
- [Proxy with Basic Authentication](#proxy-with-basic-authentication)
- [Proxy with Kerberos Authentication](#proxy-with-kerberos-authentication)
- [Troubleshooting](#troubleshooting)

## Basic Proxy Configuration

The connector automatically detects proxy settings from environment variables:

```bash
# For HTTPS connections (most common)
export HTTPS_PROXY=http://proxy.example.com:8080

# For HTTP connections
export HTTP_PROXY=http://proxy.example.com:8080

# Hosts to bypass proxy
export NO_PROXY=localhost,127.0.0.1,.internal.company.com
```

Then connect normally:

```python
from databricks import sql

connection = sql.connect(
    server_hostname="your-workspace.databricks.com",
    http_path="/sql/1.0/warehouses/your-warehouse",
    access_token="your-token"
)
```

## Proxy with Basic Authentication

For proxies requiring username/password authentication, include credentials in the proxy URL:

```bash
export HTTPS_PROXY=http://username:password@proxy.example.com:8080
```

## Proxy with Kerberos Authentication

For enterprise environments using Kerberos authentication on proxies:

### Prerequisites

1. Install Kerberos dependencies:
   ```bash
   pip install databricks-sql-connector[kerberos]
   ```

2. Obtain a valid Kerberos ticket:
   ```bash
   kinit user@EXAMPLE.COM
   ```

3. Set proxy environment variables (without credentials):
   ```bash
   export HTTPS_PROXY=http://proxy.example.com:8080
   ```

### Connection with Kerberos Proxy

```python
from databricks import sql

connection = sql.connect(
    server_hostname="your-workspace.databricks.com",
    http_path="/sql/1.0/warehouses/your-warehouse",
    access_token="your-databricks-token",
    
    # Enable Kerberos proxy authentication
    _proxy_auth_type="kerberos",
    
    # Optional Kerberos settings
    _proxy_kerberos_service_name="HTTP",  # Default: "HTTP"
    _proxy_kerberos_principal="user@EXAMPLE.COM",  # Optional: uses default if not set
    _proxy_kerberos_delegate=False,  # Enable credential delegation
    _proxy_kerberos_mutual_auth="REQUIRED"  # Options: REQUIRED, OPTIONAL, DISABLED
)
```

### Kerberos Configuration Options

| Parameter | Default | Description |
|-----------|---------|-------------|
| `_proxy_auth_type` | None | Set to `"kerberos"` to enable Kerberos proxy auth |
| `_proxy_kerberos_service_name` | `"HTTP"` | Kerberos service name for the proxy |
| `_proxy_kerberos_principal` | None | Specific principal to use (uses default if not set) |
| `_proxy_kerberos_delegate` | `False` | Whether to delegate credentials to the proxy |
| `_proxy_kerberos_mutual_auth` | `"REQUIRED"` | Mutual authentication requirement level |

### Example: Custom Kerberos Settings

```python
# Using a specific service principal with delegation
connection = sql.connect(
    server_hostname="your-workspace.databricks.com",
    http_path="/sql/1.0/warehouses/your-warehouse",
    access_token="your-token",
    
    _proxy_auth_type="kerberos",
    _proxy_kerberos_service_name="HTTP",
    _proxy_kerberos_principal="dbuser@CORP.EXAMPLE.COM",
    _proxy_kerberos_delegate=True,  # Allow credential delegation
    _proxy_kerberos_mutual_auth="OPTIONAL"  # Less strict verification
)
```

## Troubleshooting

### Kerberos Authentication Issues

1. **No Kerberos ticket**:
   ```bash
   # Check if you have a valid ticket
   klist
   
   # If not, obtain one
   kinit user@EXAMPLE.COM
   ```

2. **Wrong service principal**:
   - Check with your IT team for the correct proxy service principal name
   - It's typically `HTTP@proxy.example.com` but may vary

3. **Import errors**:
   ```
   ImportError: Kerberos proxy authentication requires 'pykerberos'
   ```
   Solution: Install with `pip install databricks-sql-connector[kerberos]`

### Proxy Connection Issues

1. **Enable debug logging**:
   ```python
   import logging
   logging.basicConfig(level=logging.DEBUG)
   ```

2. **Test proxy connectivity**:
   ```bash
   # Test if proxy is reachable
   curl -x http://proxy.example.com:8080 https://www.databricks.com
   ```

3. **Verify environment variables**:
   ```python
   import os
   print(f"HTTPS_PROXY: {os.environ.get('HTTPS_PROXY')}")
   print(f"NO_PROXY: {os.environ.get('NO_PROXY')}")
   ```

### Platform-Specific Notes

- **Linux/Mac**: Uses `pykerberos` library
- **Windows**: Uses `winkerberos` library (automatically selected)
- **Docker/Containers**: Ensure Kerberos configuration files are mounted

## Security Considerations

1. **Avoid hardcoding credentials** - Use environment variables or secure credential stores
2. **Use HTTPS connections** - Even through proxies, maintain encrypted connections to Databricks
3. **Credential delegation** - Only enable `_proxy_kerberos_delegate=True` if required by your proxy
4. **Mutual authentication** - Keep `_proxy_kerberos_mutual_auth="REQUIRED"` for maximum security

## See Also

- [Kerberos Proxy Example](../examples/kerberos_proxy_auth.py)
- [Databricks SQL Connector Documentation](https://docs.databricks.com/dev-tools/python-sql-connector.html)