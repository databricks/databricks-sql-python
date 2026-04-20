"""
Detects whether the Python SQL connector is being invoked by an AI coding agent
by checking for well-known environment variables that agents set in their spawned
shell processes.

Detection only succeeds when exactly one agent environment variable is present,
to avoid ambiguous attribution when multiple agent environments overlap.

Adding a new agent requires only a new entry in KNOWN_AGENTS.

References for each environment variable:
  - ANTIGRAVITY_AGENT: Closed source. Google Antigravity sets this variable.
  - CLAUDECODE: https://github.com/anthropics/claude-code (sets CLAUDECODE=1)
  - CLINE_ACTIVE: https://github.com/cline/cline (shipped in v3.24.0)
  - CODEX_CI: https://github.com/openai/codex (part of UNIFIED_EXEC_ENV array in codex-rs)
  - CURSOR_AGENT: Closed source. Referenced in a gist by johnlindquist.
  - GEMINI_CLI: https://google-gemini.github.io/gemini-cli/docs/tools/shell.html (sets GEMINI_CLI=1)
  - OPENCODE: https://github.com/opencode-ai/opencode (sets OPENCODE=1)
"""

import os

KNOWN_AGENTS = [
    ("ANTIGRAVITY_AGENT", "antigravity"),
    ("CLAUDECODE", "claude-code"),
    ("CLINE_ACTIVE", "cline"),
    ("CODEX_CI", "codex"),
    ("CURSOR_AGENT", "cursor"),
    ("GEMINI_CLI", "gemini-cli"),
    ("OPENCODE", "opencode"),
]


def detect(env=None):
    """Detect which AI coding agent (if any) is driving the current process.

    Args:
        env: Optional dict-like object for environment variable lookup.
             Defaults to os.environ. Exists for testability.

    Returns:
        The agent product string if exactly one agent is detected,
        or an empty string otherwise.
    """
    if env is None:
        env = os.environ

    detected = [product for var, product in KNOWN_AGENTS if env.get(var)]

    if len(detected) == 1:
        return detected[0]
    return ""
