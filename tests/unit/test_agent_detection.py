import pytest
from databricks.sql.common.agent import detect, KNOWN_AGENTS


class TestAgentDetection:
    def test_detects_single_agent_claude_code(self):
        assert detect({"CLAUDECODE": "1"}) == "claude-code"

    def test_detects_single_agent_cursor(self):
        assert detect({"CURSOR_AGENT": "1"}) == "cursor"

    def test_detects_single_agent_gemini_cli(self):
        assert detect({"GEMINI_CLI": "1"}) == "gemini-cli"

    def test_detects_single_agent_cline(self):
        assert detect({"CLINE_ACTIVE": "1"}) == "cline"

    def test_detects_single_agent_codex(self):
        assert detect({"CODEX_CI": "1"}) == "codex"

    def test_detects_single_agent_opencode(self):
        assert detect({"OPENCODE": "1"}) == "opencode"

    def test_detects_single_agent_antigravity(self):
        assert detect({"ANTIGRAVITY_AGENT": "1"}) == "antigravity"

    def test_returns_empty_when_no_agent_detected(self):
        assert detect({}) == ""

    def test_returns_empty_when_multiple_agents_detected(self):
        assert detect({"CLAUDECODE": "1", "CURSOR_AGENT": "1"}) == ""

    def test_ignores_empty_env_var_values(self):
        assert detect({"CLAUDECODE": ""}) == ""

    def test_all_known_agents_are_covered(self):
        for env_var, product in KNOWN_AGENTS:
            assert detect({env_var: "1"}) == product, (
                f"Agent with env var {env_var} should be detected as {product}"
            )

    def test_defaults_to_os_environ(self, monkeypatch):
        monkeypatch.delenv("CLAUDECODE", raising=False)
        monkeypatch.delenv("CURSOR_AGENT", raising=False)
        monkeypatch.delenv("GEMINI_CLI", raising=False)
        monkeypatch.delenv("CLINE_ACTIVE", raising=False)
        monkeypatch.delenv("CODEX_CI", raising=False)
        monkeypatch.delenv("OPENCODE", raising=False)
        monkeypatch.delenv("ANTIGRAVITY_AGENT", raising=False)
        # With all agent vars cleared, detect() should return empty
        assert detect() == ""
