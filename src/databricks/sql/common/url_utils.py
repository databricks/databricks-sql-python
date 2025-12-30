"""
URL utility functions for the Databricks SQL connector.
"""


def normalize_host_with_protocol(host: str) -> str:
    """
    Normalize a connection hostname by ensuring it has a protocol and removing trailing slashes.

    This is useful for handling cases where users may provide hostnames with or without protocols
    (common with dbt-databricks users copying URLs from their browser).

    Args:
        host: Connection hostname which may or may not include a protocol prefix (https:// or http://)
              and may or may not have a trailing slash

    Returns:
        Normalized hostname with protocol prefix and no trailing slash

    Examples:
        normalize_host_with_protocol("myserver.com") -> "https://myserver.com"
        normalize_host_with_protocol("https://myserver.com") -> "https://myserver.com"
    """
    # Remove trailing slash
    host = host.rstrip("/")

    # Add protocol if not present
    if not host.startswith("https://") and not host.startswith("http://"):
        host = f"https://{host}"

    return host
