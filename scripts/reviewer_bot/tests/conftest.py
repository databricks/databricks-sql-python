"""Pytest fixtures for reviewer_bot unit tests."""
import pytest


@pytest.fixture
def simple_diff() -> str:
    """Two-file unified diff: one file with added lines 10-12, one file
    with both added (5) and deleted (around 8) lines. Used by the
    diff-position parser tests."""
    return """\
diff --git a/src/foo.py b/src/foo.py
index abc123..def456 100644
--- a/src/foo.py
+++ b/src/foo.py
@@ -8,3 +8,6 @@ def existing():
     pass


+def added_function():
+    print("hello")
+    return 1
diff --git a/src/bar.py b/src/bar.py
index 111..222 100644
--- a/src/bar.py
+++ b/src/bar.py
@@ -3,5 +3,5 @@
 line3
 line4
-line5_old
+line5_new
 line6
 line7
"""


@pytest.fixture
def fake_repo(tmp_path):
    """Build a fake repo on disk: CLAUDE.md at root, CLAUDE.md for
    csharp and rust drivers, and a specs/README.md."""
    (tmp_path / "CLAUDE.md").write_text("# Root rules\nRule A\n")

    (tmp_path / "tests" / "csharp").mkdir(parents=True)
    (tmp_path / "tests" / "csharp" / "CLAUDE.md").write_text(
        "# csharp rules\nRule B\n"
    )

    (tmp_path / "tests" / "rust").mkdir(parents=True)
    (tmp_path / "tests" / "rust" / "CLAUDE.md").write_text(
        "# rust rules\nRule C\n"
    )

    # comparator-tests intentionally has NO CLAUDE.md
    (tmp_path / "tests" / "comparator-tests").mkdir(parents=True)

    (tmp_path / "specs").mkdir()
    (tmp_path / "specs" / "README.md").write_text("# Specs README\n")

    # .claude/ layout post-#320 restructure: commands/ + knowledge/ (rules),
    # plans/ excluded. See gather_context.aggregate_repo_rules docstring.
    claude_dir = tmp_path / ".claude"
    commands_dir = claude_dir / "commands"
    commands_dir.mkdir(parents=True)
    (commands_dir / "writeSpec.md").write_text(
        "# Write spec\nWrite spec rules\n"
    )
    (commands_dir / "implTest.md").write_text(
        "# Impl test\nImpl test rules\n"
    )
    # nested namespaced subdir (e.g., /audit:compliance) — must also be picked up
    audit_dir = commands_dir / "audit"
    audit_dir.mkdir()
    (audit_dir / "compliance.md").write_text(
        "# Audit compliance\nCompliance rules\n"
    )
    knowledge_dir = claude_dir / "knowledge"
    knowledge_dir.mkdir()
    (knowledge_dir / "learning-log.md").write_text(
        "# Learning log\nDated entries\n"
    )
    # .claude/plans/ is intentionally NOT aggregated — historical/in-flight drafts.
    plans_dir = claude_dir / "plans"
    plans_dir.mkdir()
    (plans_dir / "historical-design.md").write_text(
        "# Historical design\nShould NOT be included\n"
    )

    return tmp_path


@pytest.fixture
def fake_driver_tree(tmp_path):
    """Fake adbc-drivers/databricks layout with csharp/src/."""
    src = tmp_path / "csharp" / "src"
    src.mkdir(parents=True)

    auth = src / "Authentication"
    auth.mkdir()
    (auth / "AuthHandler.cs").write_text("// auth handler\n" * 10)
    (auth / "TokenManager.cs").write_text("// token manager\n" * 10)

    other = src / "Statements"
    other.mkdir()
    (other / "StatementExecutor.cs").write_text("// statement\n" * 10)

    return tmp_path
