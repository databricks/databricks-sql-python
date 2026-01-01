"""
URL utility functions for the Databricks SQL connector.
"""


def normalize_host_with_protocol(host: str) -> str:
    """
    Normalize a connection hostname by ensuring it has a protocol.

    This is useful for handling cases where users may provide hostnames with or without protocols
    (common with dbt-databricks users copying URLs from their browser).

    Args:
        host: Connection hostname which may or may not include a protocol prefix (https:// or http://)
              and may or may not have a trailing slash

    Returns:
        Normalized hostname with protocol prefix and no trailing slashes

    Examples:
        normalize_host_with_protocol("myserver.com") -> "https://myserver.com"
        normalize_host_with_protocol("https://myserver.com") -> "https://myserver.com"
        normalize_host_with_protocol("HTTPS://myserver.com/") -> "https://myserver.com"
        normalize_host_with_protocol("http://localhost:8080/") -> "http://localhost:8080"

    Raises:
        ValueError: If host is None or empty string
    """
    # Handle None or empty host
    if not host or not host.strip():
        raise ValueError("Host cannot be None or empty")

    # Remove trailing slashes
    host = host.rstrip("/")

    # Add protocol if not present (case-insensitive check)
    host_lower = host.lower()
    if not host_lower.startswith("https://") and not host_lower.startswith("http://"):
        host = f"https://{host}"
    elif host_lower.startswith("https://") or host_lower.startswith("http://"):
        # Normalize protocol to lowercase
        protocol_end = host.index("://") + 3
        host = host[:protocol_end].lower() + host[protocol_end:]

    return host
