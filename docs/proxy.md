# Proxy Support

The Databricks SQL Connector supports connecting through HTTP and HTTPS proxy servers with various authentication methods. This feature automatically detects system proxy configuration and handles proxy authentication transparently.

## Quick Start

The connector automatically uses your system's proxy configuration when available:

```python
from databricks import sql

# Basic connection - uses system proxy automatically
with sql.connect(
    server_hostname="your-workspace.cloud.databricks.com",
    http_path="/sql/1.0/endpoints/your-endpoint-id",
    access_token="your-token"
) as connection:
    # Your queries here...
```

For advanced proxy authentication (like Kerberos), specify the authentication method:

```python
with sql.connect(
    server_hostname="your-workspace.cloud.databricks.com",
    http_path="/sql/1.0/endpoints/your-endpoint-id", 
    access_token="your-token",
    _proxy_auth_method="negotiate"  # Enable Kerberos proxy auth
) as connection:
    # Your queries here...
```

## Proxy Configuration

### Environment Variables

The connector follows standard proxy environment variable conventions:

| Variable | Description | Example |
|----------|-------------|---------|
| `HTTP_PROXY` | Proxy for HTTP requests | `http://proxy.company.com:8080` |
| `HTTPS_PROXY` | Proxy for HTTPS requests | `https://proxy.company.com:8080` |
| `NO_PROXY` | Hosts to bypass proxy | `localhost,127.0.0.1,.company.com` |

**Note**: The connector also recognizes lowercase versions (`http_proxy`, `https_proxy`, `no_proxy`).

### Proxy URL Formats

Basic proxy (no authentication):
```bash
export HTTPS_PROXY="http://proxy.company.com:8080"
```

Proxy with basic authentication:
```bash
export HTTPS_PROXY="http://username:password@proxy.company.com:8080"
```

## Authentication Methods

The connector supports multiple proxy authentication methods via the `_proxy_auth_method` parameter:

### 1. Basic Authentication (`basic` or `None`)

**Default behavior** when credentials are provided in the proxy URL or when `_proxy_auth_method="basic"` is specified.

```python
# Method 1: Credentials in proxy URL (recommended)
# Set environment: HTTPS_PROXY="http://user:pass@proxy.company.com:8080"
with sql.connect(
    server_hostname="your-workspace.com",
    http_path="/sql/1.0/endpoints/abc123",
    access_token="your-token"
    # No _proxy_auth_method needed - detected automatically
) as conn:
    pass

# Method 2: Explicit basic authentication
with sql.connect(
    server_hostname="your-workspace.com", 
    http_path="/sql/1.0/endpoints/abc123",
    access_token="your-token",
    _proxy_auth_method="basic"  # Explicit basic auth
) as conn:
    pass
```

### 2. Kerberos/Negotiate Authentication (`negotiate`)

For corporate environments using Kerberos authentication with proxy servers.

**Prerequisites:**
- Valid Kerberos tickets (run `kinit` first)
- Properly configured Kerberos environment

```python
with sql.connect(
    server_hostname="your-workspace.com",
    http_path="/sql/1.0/endpoints/abc123", 
    access_token="your-token",
    _proxy_auth_method="negotiate"  # Enable Kerberos proxy auth
) as conn:
    pass
```

**Kerberos Setup Example:**
```bash
# Obtain Kerberos tickets
kinit your-username@YOUR-DOMAIN.COM

# Set proxy (no credentials in URL for Kerberos)
export HTTPS_PROXY="http://proxy.company.com:8080"

# Run your Python script
python your_script.py
```

## Proxy Bypass

The connector respects system proxy bypass rules. Requests to hosts listed in `NO_PROXY` or system bypass lists will connect directly, bypassing the proxy.

```bash
# Bypass proxy for local and internal hosts
export NO_PROXY="localhost,127.0.0.1,*.internal.company.com,10.*"
```

## Advanced Configuration

### Per-Request Proxy Decisions

The connector automatically makes per-request decisions about proxy usage based on:

1. **System proxy configuration** - Detected from environment variables
2. **Proxy bypass rules** - Honor `NO_PROXY` and system bypass settings  
3. **Target host** - Check if the specific host should use proxy

### Connection Pooling

The connector maintains separate connection pools for direct and proxy connections, allowing efficient handling of mixed proxy/direct traffic.

### SSL/TLS with Proxy

HTTPS connections through HTTP proxies use the CONNECT method for SSL tunneling. The connector handles this automatically while preserving all SSL verification settings.

## Troubleshooting

### Common Issues

**Problem**: Connection fails with proxy-related errors
```
Solution: 
1. Verify proxy environment variables are set correctly
2. Check if proxy requires authentication
3. Ensure proxy allows CONNECT method for HTTPS
4. Test proxy connectivity with curl:
   curl -x $HTTPS_PROXY https://your-workspace.com
```

**Problem**: Kerberos authentication fails
```
Solution:
1. Verify Kerberos tickets: klist
2. Renew tickets if expired: kinit
3. Check proxy supports negotiate authentication
4. Ensure time synchronization between client and KDC
```

**Problem**: Some requests bypass proxy unexpectedly
```
Solution:
1. Check NO_PROXY environment variable
2. Review system proxy bypass settings
3. Verify the target hostname format
```

### Debug Logging

Enable detailed logging to troubleshoot proxy issues:

```python
import logging

# Enable connector debug logging
logging.basicConfig(level=logging.DEBUG)
logging.getLogger("databricks.sql").setLevel(logging.DEBUG)

# Enable urllib3 logging for HTTP details
logging.getLogger("urllib3").setLevel(logging.DEBUG)
```

### Testing Proxy Configuration

Use the provided example script to test different proxy authentication methods:

```bash
cd examples/
python proxy_authentication.py
```

This script tests:
- Default proxy behavior
- Basic authentication
- Kerberos/Negotiate authentication

## Examples

See `examples/proxy_authentication.py` for a comprehensive demonstration of proxy authentication methods.

## Implementation Details

### How Proxy Detection Works

1. **Environment Variables**: Check `HTTP_PROXY`/`HTTPS_PROXY` environment variables
2. **System Configuration**: Use Python's `urllib.request.getproxies()` to detect system settings
3. **Bypass Rules**: Honor `NO_PROXY` and `urllib.request.proxy_bypass()` rules
4. **Per-Request Logic**: Decide proxy usage for each request based on target host

### Supported Proxy Types

- **HTTP Proxies**: For both HTTP and HTTPS traffic (via CONNECT)
- **HTTPS Proxies**: Encrypted proxy connections
- **Authentication**: Basic, Negotiate/Kerberos
- **Bypass Rules**: Full support for NO_PROXY patterns

### Connection Architecture

The connector uses a unified HTTP client that maintains:
- **Direct Pool Manager**: For non-proxy connections
- **Proxy Pool Manager**: For proxy connections  
- **Per-Request Routing**: Automatic selection based on target host

This architecture ensures optimal performance and correct proxy handling across all connector operations.
