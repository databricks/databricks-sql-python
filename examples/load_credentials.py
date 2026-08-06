"""Load credentials from examples/credentials.env"""

import os


def load_credentials(env_path=None):
    """Read credentials.env and return a dict of key=value pairs."""
    if env_path is None:
        env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "credentials.env")

    if not os.path.exists(env_path):
        raise FileNotFoundError(
            f"Credentials file not found: {env_path}\n"
            f"Copy examples/credentials.env.example to examples/credentials.env and fill in your values."
        )

    creds = {}
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            key, _, value = line.partition("=")
            creds[key.strip()] = value.strip()

    required = ["SERVER_HOSTNAME", "HTTP_PATH", "ACCESS_TOKEN", "CATALOG", "SCHEMA"]
    missing = [k for k in required if k not in creds]
    if missing:
        raise ValueError(f"Missing required credentials: {', '.join(missing)}")

    return creds
