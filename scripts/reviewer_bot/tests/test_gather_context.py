from scripts.reviewer_bot.gather_context import parse_diff_positions


def test_parse_diff_positions_added_lines(simple_diff):
    positions = parse_diff_positions(simple_diff)

    # foo.py: added 3 lines at the bottom of the hunk (lines 11, 12, 13
    # on the RIGHT side, because the hunk header says @@ -8,3 +8,6 @@
    # meaning "old lines start at 8 (3 lines), new lines start at 8 (6
    # lines)" — so new lines are 8, 9, 10, 11, 12, 13. The first three
    # are context (`pass`, blank, blank); the last three are added.
    assert "src/foo.py" in positions
    assert positions["src/foo.py"] == {11, 12, 13}


def test_parse_diff_positions_replaced_line(simple_diff):
    positions = parse_diff_positions(simple_diff)

    # bar.py: hunk @@ -3,5 +3,5 @@. One line replaced (line 5 on RIGHT).
    assert "src/bar.py" in positions
    assert positions["src/bar.py"] == {5}


def test_parse_diff_positions_empty():
    assert parse_diff_positions("") == {}


def test_parse_diff_positions_ignores_no_newline_meta_line():
    """The unified-diff "no newline at end of file" meta marker (literal
    backslash + space + text) is NOT a real RIGHT-side line and must NOT
    advance the right-line counter. If it did, every position reported
    AFTER such a hunk would be off by one — and inline routing would
    place comments on the wrong line."""
    # File ends with an added line that has no trailing newline. Git
    # follows the added line with `\ No newline at end of file`.
    diff = """\
diff --git a/src/x.py b/src/x.py
--- a/src/x.py
+++ b/src/x.py
@@ -1,3 +1,4 @@
 line1
 line2
 line3
+line4_no_newline
\\ No newline at end of file
"""
    positions = parse_diff_positions(diff)
    # The added line is line 4 on the RIGHT. The `\ No newline` meta
    # line MUST NOT push the counter to 5.
    assert positions == {"src/x.py": {4}}


def test_parse_diff_positions_no_newline_meta_does_not_offset_following_lines():
    """The trickier variant: two hunks in one file, the first ending with
    `\\ No newline at end of file`. The second hunk's positions must NOT
    be shifted by one — that was the bug."""
    diff = """\
diff --git a/src/x.py b/src/x.py
--- a/src/x.py
+++ b/src/x.py
@@ -1,2 +1,3 @@
 line1
+line2_no_newline
\\ No newline at end of file
@@ -10,2 +11,3 @@
 line10
+line11_new
 line12
"""
    positions = parse_diff_positions(diff)
    # Hunk 1: added line is RIGHT-side line 2.
    # Hunk 2: added line is RIGHT-side line 12 (per the hunk header
    # which restarts the counter — the bug was visible WITHIN a hunk,
    # so we also test single-hunk above).
    assert positions == {"src/x.py": {2, 12}}


def test_parse_diff_positions_ignores_pure_deletions():
    diff = """\
diff --git a/src/x.py b/src/x.py
--- a/src/x.py
+++ b/src/x.py
@@ -1,3 +1,2 @@
 keep1
-deleted_line
 keep2
"""
    positions = parse_diff_positions(diff)
    # Only deletions in this hunk → no RIGHT-side modified lines.
    assert positions.get("src/x.py", set()) == set()


from scripts.reviewer_bot.gather_context import aggregate_repo_rules


def test_aggregate_includes_contributing_md(tmp_path):
    """CONTRIBUTING.md is this repo's canonical convention source — it must be
    inlined (with its delimiter) so the reviewer can cite its rule lines."""
    (tmp_path / "CONTRIBUTING.md").write_text(
        "## Coding Style\nPEP 8 with lines up to 100 characters.\n"
    )
    result = aggregate_repo_rules(["src/databricks/sql/client.py"], repo_root=tmp_path)
    assert "=== CONTRIBUTING.md ===" in result
    assert "PEP 8 with lines up to 100 characters." in result


def test_aggregate_skips_missing_contributing_md(tmp_path):
    """No CONTRIBUTING.md — no error, empty result."""
    result = aggregate_repo_rules(["src/databricks/sql/client.py"], repo_root=tmp_path)
    assert result == ""


def test_aggregate_does_not_inline_readme(tmp_path):
    """README.md is usage docs, not review rules — not inlined (the model can
    read it via read_paths if a finding needs it)."""
    (tmp_path / "CONTRIBUTING.md").write_text("PEP 8, 100 cols.\n")
    (tmp_path / "README.md").write_text("pip install databricks-sql-connector\n")
    result = aggregate_repo_rules(["src/databricks/sql/client.py"], repo_root=tmp_path)
    assert "=== README.md ===" not in result


from scripts.reviewer_bot.gather_context import list_driver_source


def test_list_driver_source_returns_paths_with_sizes(fake_driver_tree):
    listing = list_driver_source(
        driver_root=fake_driver_tree,
        source_subpath="csharp/src/",
    )
    assert "AuthHandler.cs" in listing
    assert "TokenManager.cs" in listing
    assert "StatementExecutor.cs" in listing
    # Sizes are appended in `( N bytes)` format
    assert "bytes)" in listing
