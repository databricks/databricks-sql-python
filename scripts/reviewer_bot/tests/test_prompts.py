from scripts.reviewer_bot.prompts import (
    SYSTEM_PROMPT, OUTPUT_SCHEMA_DOC, USER_PROMPT_TEMPLATE,
)


def test_system_prompt_mentions_severity_levels():
    for s in ("critical", "high", "medium", "low", "nit"):
        assert s in SYSTEM_PROMPT.lower()


def test_system_prompt_describes_tool_use():
    """The agentic tool-use loop must be documented in the prompt."""
    assert "read_paths" in SYSTEM_PROMPT
    assert "grep" in SYSTEM_PROMPT


def test_system_prompt_mentions_citation_options():
    assert "CONTRIBUTING.md" in SYSTEM_PROMPT
    assert "verified_against" in SYSTEM_PROMPT


def test_output_schema_doc_is_valid_json_template():
    # Must mention all the fields validate_findings.py will look for.
    for field in ("findings", "summary", "severity", "file", "line",
                  "citation", "verified_against", "body", "suggestion"):
        assert field in OUTPUT_SCHEMA_DOC


def test_user_prompt_template_has_placeholders():
    # Sanity: these placeholders must exist for run_review.py to fill.
    for ph in ("{pr_title}", "{pr_body}", "{diff}", "{repo_rules}",
               "{open_threads}"):
        assert ph in USER_PROMPT_TEMPLATE


def test_system_prompt_has_comments_vs_code_rule():
    """Anti-hallucination rule: re-reviews must locate the actual
    executing code line, not match on a fragile pattern that appears
    only inside an explanatory comment. Without this guidance, the
    bot resurrects findings that reconcile correctly closed (the
    "HEAD~1 in a comment" pattern from PR #339)."""
    # Section header must be present so future editors don't accidentally
    # delete it during prompt reflows.
    assert "Comments vs. code" in SYSTEM_PROMPT
    # Collapse whitespace so the assertions are robust to prompt
    # reflows (line-wrapping vs single-line, etc.).
    import re as _re
    collapsed = _re.sub(r"\s+", " ", SYSTEM_PROMPT)
    assert "executable code, not at a comment" in collapsed
    assert "documentation, not a bug" in collapsed


def test_system_prompt_has_anchor_rule():
    """Every Critical/High/Medium/Low finding must anchor inline. The
    LLM previously punted multi-line concerns to summary (e.g. dead-
    imports findings on PR #386 review #4379014516); the explicit
    Anchor rule section makes file:line mandatory for non-Nits, even
    when the concern spans multiple lines (anchor at the first
    relevant line). Without this guardrail, severity-tagged findings
    silently bury in the body's `Other findings` section instead of
    landing as actionable inline comments.
    """
    assert "Anchor rule" in SYSTEM_PROMPT
    import re as _re
    collapsed = _re.sub(r"\s+", " ", SYSTEM_PROMPT)
    # Must require BOTH file and line for non-Nit findings.
    assert "MUST emit BOTH `file` AND `line`" in collapsed
    # Must explicitly cover the multi-line case so the model doesn't
    # treat "spans multiple lines" as an excuse to omit the anchor.
    assert "FIRST relevant line as the anchor" in collapsed


def test_system_prompt_has_review_dimensions():
    """The prompt must give the model an ACTIVE review agenda (what to look
    for), not just suppression/formatting rules — otherwise it returns empty
    'looks good' reviews. Guards the thoroughness section against silent
    deletion."""
    assert "What to review for" in SYSTEM_PROMPT
    low = SYSTEM_PROMPT.lower()
    for axis in ("correctness", "error handling", "tests & coverage",
                 "edge cases", "contracts", "security"):
        assert axis in low, axis


def test_system_prompt_encourages_medium_low_findings():
    """Thoroughness rebalance: the prompt must invite Medium/Low findings, not
    only Critical/High — while still keeping the guardrails referenced."""
    import re as _re
    collapsed = _re.sub(r"\s+", " ", SYSTEM_PROMPT)
    assert "Calibrate, don't withhold" in collapsed
    # Guardrails must remain intact alongside the encouragement.
    assert "must not duplicate an open thread" in collapsed
    assert "not a comment pattern" in collapsed
