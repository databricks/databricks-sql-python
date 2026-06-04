"""Thin wrappers around the `gh` CLI for bot workflows.

Auth comes from the GH_TOKEN env var that callers must set (usually
to a GitHub App installation token). Network errors propagate.
"""
from __future__ import annotations

import json
import subprocess
from typing import Any


def _run_gh(*args: str, paginate: bool = False) -> str:
    """Run `gh` with the given args, return stdout. On failure, raise
    RuntimeError with stderr included so it shows up in workflow logs.
    """
    cmd = ["gh", *args]
    if paginate:
        # Insert --paginate after the subcommand (e.g. `gh api --paginate ...`)
        cmd.insert(2, "--paginate")
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, check=True)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            f"gh failed (exit {e.returncode}): {' '.join(cmd)}\n"
            f"stderr: {e.stderr.strip()}"
        ) from e
    return r.stdout


def fetch_pr(repo: str, pr_number: int) -> dict[str, Any]:
    out = _run_gh(
        "pr", "view", str(pr_number), "--repo", repo, "--json",
        "title,body,url,headRefName,headRefOid,baseRefName,isCrossRepository",
    )
    return json.loads(out)


def fetch_review_comment(repo: str, comment_id: int) -> dict[str, Any]:
    out = _run_gh("api", f"repos/{repo}/pulls/comments/{comment_id}")
    return json.loads(out)


def list_pr_review_comments(repo: str, pr_number: int) -> list[dict[str, Any]]:
    out = _run_gh("api", f"repos/{repo}/pulls/{pr_number}/comments", paginate=True)
    return json.loads(out)


def post_inline_reply(repo: str, pr_number: int, in_reply_to: int, body: str) -> int:
    out = _run_gh(
        "api", "-X", "POST",
        f"repos/{repo}/pulls/{pr_number}/comments",
        "-f", f"body={body}",
        "-F", f"in_reply_to={in_reply_to}",
    )
    return json.loads(out)["id"]


def post_pr_comment(repo: str, pr_number: int, body: str) -> int:
    out = _run_gh(
        "api", "-X", "POST",
        f"repos/{repo}/issues/{pr_number}/comments",
        "-f", f"body={body}",
    )
    return json.loads(out)["id"]


def resolve_thread(repo: str, thread_id: str) -> None:
    query = (
        "mutation($id: ID!) { resolveReviewThread(input: {threadId: $id}) "
        "{ thread { id isResolved } } }"
    )
    _run_gh("api", "graphql", "-f", f"query={query}", "-f", f"id={thread_id}")
