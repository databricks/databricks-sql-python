"""Thin wrappers around `git` for bot workflows."""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Optional


def configure_user(cwd: Path, email: str, name: str) -> None:
    subprocess.run(["git", "config", "user.email", email], cwd=cwd, check=True)
    subprocess.run(["git", "config", "user.name", name], cwd=cwd, check=True)


def commit_paths(cwd: Path, paths: list[str], message: str) -> bool:
    """Stage the given paths and create a commit. Returns False if nothing staged."""
    subprocess.run(["git", "add", *paths], cwd=cwd, check=True)
    diff = subprocess.run(["git", "diff", "--cached", "--quiet"], cwd=cwd)
    if diff.returncode == 0:
        return False  # nothing staged
    if diff.returncode != 1:
        # git diff --quiet documents 0 (no diff) / 1 (diff). Anything else
        # is an error — fail loud rather than committing on a broken repo.
        raise RuntimeError(
            f"git diff --cached --quiet returned unexpected code {diff.returncode}; "
            f"refusing to commit"
        )
    subprocess.run(["git", "commit", "-m", message], cwd=cwd, check=True)
    return True


def push(
    cwd: Path,
    branch: str,
    remote: str = "origin",
    *,
    force_with_lease: bool = False,
    expected_sha: Optional[str] = None,
) -> None:
    """Push a branch to `remote`.

    Default is a plain push that fails on non-fast-forward.

    `force_with_lease=True` without `expected_sha` is unsafe in CI
    (background fetches by actions/checkout refresh the lease ref, so
    the lease check degenerates to `--force`). When you need force-push
    behavior, pass `expected_sha` — the lease will be qualified with
    `<branch>:<expected>` so it actually protects against races.
    """
    if force_with_lease and not expected_sha:
        # Fail loud rather than silently constructing the risky
        # unqualified --force-with-lease command. Callers that really
        # want to force-push must compute the expected remote SHA
        # first (via `git ls-remote`) and pass it explicitly.
        raise ValueError(
            "push(force_with_lease=True) requires expected_sha. Unqualified "
            "--force-with-lease degrades to --force in CI because background "
            "fetches refresh the lease ref. Pass the expected remote SHA or "
            "use a plain push."
        )
    cmd = ["git", "push", remote, branch]
    if force_with_lease:
        cmd.insert(2, f"--force-with-lease={branch}:{expected_sha}")
    try:
        r = subprocess.run(
            cmd, cwd=cwd, check=True,
            capture_output=True, text=True,
        )
    except subprocess.CalledProcessError as e:
        # Re-raise with the captured stderr so callers can include it
        # in their failure messages. Print to the workflow log first so
        # the trace is visible even if the caller doesn't surface stderr.
        if e.stdout:
            print(e.stdout, end="")
        if e.stderr:
            print(e.stderr, end="", file=sys.stderr)
        raise


def current_sha(cwd: Path) -> str:
    r = subprocess.run(["git", "rev-parse", "HEAD"], cwd=cwd,
                       capture_output=True, text=True, check=True)
    return r.stdout.strip()
