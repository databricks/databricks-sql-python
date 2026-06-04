"""Phase 2: gather all context the LLM needs to review the PR.

Functions in this module are pure (no network, no subprocess) where
possible, so unit tests are simple. The `gather` entry point at the
bottom orchestrates everything and is the only place that shells out.
"""
from __future__ import annotations

import re
import sys
from typing import Any, Dict, Iterable, Set, Union

# Single source of truth for the keys we expect on a thread dict lives
# next to the producer (`reconcile.REVIEW_THREAD_REQUIRED_KEYS` /
# `reconcile.ReviewThread`). Imported lazily inside the function that
# uses it so this module stays cheap to import — `reconcile` pulls in
# the markers module + GraphQL plumbing this module otherwise doesn't
# need.


_DIFF_FILE_HEADER = re.compile(r"^\+\+\+ b/(.+)$")
_HUNK_HEADER = re.compile(r"^@@ -\d+(?:,\d+)? \+(\d+)(?:,\d+)? @@")


def parse_diff_positions(diff_text: str) -> Dict[str, Set[int]]:
    """Return {file_path: set of RIGHT-side line numbers that were added
    or modified}.

    Walks the unified diff line by line. A line starting with `+` (but
    not `+++` which is the file header) is a RIGHT-side addition; track
    its line number. Context lines (` `) advance the right-side line
    counter without adding to the set. Deleted lines (`-`, not `---`)
    don't advance the right-side counter.
    """
    positions: Dict[str, Set[int]] = {}
    current_file: str | None = None
    right_line: int | None = None

    for line in diff_text.splitlines():
        # File header — start of a new file's diff
        m = _DIFF_FILE_HEADER.match(line)
        if m:
            current_file = m.group(1)
            positions.setdefault(current_file, set())
            right_line = None
            continue

        # Hunk header — initializes the right-side line counter
        m = _HUNK_HEADER.match(line)
        if m:
            right_line = int(m.group(1))
            continue

        if current_file is None or right_line is None:
            continue

        # `+++` was matched above as a file header; skip the `--- a/...`
        # variant which appears just before `+++ b/...`.
        if line.startswith("---"):
            continue

        # Unified-diff "no newline at end of file" meta marker (literal:
        # backslash + space + text). NOT a real RIGHT-side line — must
        # not advance the right-line counter, otherwise every position
        # reported AFTER such a hunk is off by one.
        if line.startswith("\\ "):
            continue

        if line.startswith("+"):
            positions[current_file].add(right_line)
            right_line += 1
        elif line.startswith("-"):
            # Deleted from the LEFT side; right-side line counter does
            # not advance.
            pass
        else:
            # Context line ` ` (or any other unhandled non-meta line).
            right_line += 1

    return positions

from pathlib import Path


def aggregate_repo_rules(changed_files: list[str], repo_root: Path) -> str:
    """Concatenate this repo's convention references so the reviewer can cite
    exact rule lines. For databricks-sql-python the canonical source is
    CONTRIBUTING.md (coding style: PEP 8 with a 100-char line limit; DCO
    sign-off). Missing files are silently skipped.

    `changed_files` is unused (kept for signature stability with the followup /
    run_review callers) — this repo has no per-path convention files to select.
    The connector's README.md is usage docs, not review rules; the model can
    read it via read_paths if a finding needs it, so it is not inlined here.
    """
    sections: list[str] = []
    for name in ("CONTRIBUTING.md",):
        path = repo_root / name
        if not path.is_file():
            continue
        sections.append(f"=== {name} ===\n{path.read_text()}")
    return "\n\n".join(sections)

_SEVERITY_NAMES = ("CRITICAL", "HIGH", "MEDIUM", "LOW", "NIT")


def _extract_severity_tag(body: str) -> str:
    """Return the severity tag (e.g. "HIGH") parsed from a posted finding
    body, or "?" when the body doesn't start with a recognizable badge.

    The posted body shape is `{badge} — {text}` where `{badge}` is one of
    `severity.BADGE`'s values like `🔴 Critical`. We tolerate optional
    leading whitespace and the HTML marker comment that some posts put
    on the first line. "?" lets the LLM still see the row in the
    open-threads list (so dedup still fires) without inventing a
    severity — the system prompt's escalation rule degrades to "skip"
    when the existing severity is unknown, which is the safe default.
    """
    if not body:
        return "?"
    # Drop HTML marker comments before scanning so a leading
    # `<!-- pr-review-bot:v1 ... -->` doesn't hide the badge.
    cleaned = re.sub(r"<!--.*?-->", "", body, flags=re.S).lstrip()
    # Match `{emoji}{ws}{NAME}` at the start (the dash/em-dash that
    # follows is not required — a body that's only the badge still
    # tells us the severity).
    m = re.match(
        r"[\U0001F534\U0001F7E0\U0001F7E1\U0001F535⚪]\s*"
        r"(?P<name>Critical|High|Medium|Low|Nit)\b",
        cleaned, flags=re.I,
    )
    if not m:
        return "?"
    return m.group("name").upper()


def _summarize_thread_root(body: str, max_chars: int = 180) -> str:
    """Compress a review-bot finding root to a one-line essence.

    Strategy:
      1. Strip the leading severity badge (🔴/🟠/🟡/🔵 + "— Severity —").
      2. Strip any HTML marker comments (`<!-- pr-review-bot:v1 ... -->`).
      3. Take the first ~max_chars of the first non-empty line.

    The result is meant to be unique-enough for the LLM to recognize a
    semantic duplicate without us shipping the full multi-paragraph
    finding body (which would balloon the prompt and add irrelevant
    detail like suggestion blocks).

    Note: severity is parsed separately by `_extract_severity_tag` and
    surfaced in the row prefix so the LLM can apply the escalation
    override the system prompt asks for. We don't include it in the
    summary line itself — the prefix already shows it.
    """
    if not body:
        return ""
    # Strip HTML marker comments
    body = re.sub(r"<!--.*?-->", "", body, flags=re.S).strip()
    # Strip leading severity emoji + the "— Severity — " preamble
    body = re.sub(r"^[\U0001F534\U0001F7E0\U0001F7E1\U0001F535⚪]\s*", "", body)
    body = re.sub(r"^(?:Critical|High|Medium|Low|Nit)\s*[—-]\s*", "", body, flags=re.I)
    # Take first non-empty line
    first_line = next(
        (ln.strip() for ln in body.splitlines() if ln.strip()),
        "",
    )
    if len(first_line) > max_chars:
        first_line = first_line[:max_chars].rstrip() + "…"
    return first_line


def format_open_threads(
    threads: list[Dict[str, Any]],
    *,
    bot_login: Union[str, Iterable[str]],
    max_threads: int = 50,
) -> str:
    """Compress open review-bot threads to a one-line-each block for the prompt.

    Each entry in `threads` is expected to match `reconcile.ReviewThread`
    (the TypedDict declared on the producer side). The contract carries
    these keys: `root_author`, `root_comment_id`, `root_path`,
    `root_line`, `root_body`, `root_created_at`. We reach into them
    directly below; if `reconcile`'s GraphQL projection drops or
    renames one of these without also updating `ReviewThread`, the
    boundary assertion below logs `::warning::` and skips the entry
    rather than silently falling through to "(no open review-bot
    threads)" or rendering `#None`.

    Filters to roots authored by `bot_login` (skips human-authored
    threads — those aren't review-bot findings the dedup gate should
    consider). Caps to the `max_threads` most recent threads to bound
    prompt cost on long-running PRs; if more exist, the bot has bigger
    problems than the dedup heuristic can fix.

    `bot_login` is matched case-insensitively against the thread root
    author's GitHub login. Accepts either a single login string or an
    iterable of logins so the caller can pass `post_review.BOT_LOGINS`
    to recognize both the current `peco-review-bot[bot]` identity AND
    the legacy `github-actions[bot]` identity that pre-app-migration
    PRs used. Without dual-login support, open legacy threads on
    migration-era PRs would be absent from the dedup prompt and the
    LLM would re-emit duplicates for those still-open findings.

    Returns "(no open review-bot threads)" when nothing matches —
    template substitution shouldn't leave an empty section.
    """
    if not threads:
        return "(no open review-bot threads)"

    if isinstance(bot_login, str):
        bot_logins_lc = frozenset({bot_login.lower()})
    else:
        bot_logins_lc = frozenset(bl.lower() for bl in bot_login)
    # Imported here (not at module top) so we don't drag the reconcile
    # module's GraphQL/markers plumbing into anything that just wants
    # `parse_diff_positions` or `aggregate_repo_rules`.
    from .reconcile import REVIEW_THREAD_REQUIRED_KEYS

    rows: list[tuple[str, str]] = []  # (created_at, formatted_line)
    for t in threads:
        # Boundary assertion: the producer (`reconcile.fetch_open_review_threads`)
        # is documented to return dicts matching the `ReviewThread` TypedDict.
        # If a refactor over there drops or renames one of the keys we read
        # below without updating `ReviewThread`, the type-checker will catch
        # it — but a runtime payload that bypasses the type-check (malformed
        # GraphQL response, monkeypatched test fixture, future caller passing
        # a different shape) would silently degrade: a missing `root_author`
        # would skip the entry, a missing `root_comment_id` would render
        # `#None`. Surface the contract drift as a `::warning::` instead.
        missing = REVIEW_THREAD_REQUIRED_KEYS - t.keys()
        if missing:
            print(
                f"::warning::format_open_threads: thread missing required "
                f"keys {sorted(missing)} — skipping. "
                f"This signals a contract drift between "
                f"reconcile.ReviewThread and the actual payload; check "
                f"`fetch_open_review_threads` for a renamed/dropped field.",
                file=sys.stderr,
            )
            continue
        if (t.get("root_author") or "").lower() not in bot_logins_lc:
            continue
        thread_id = t.get("root_comment_id")
        path = t.get("root_path") or "(no file)"
        line = t.get("root_line")
        line_str = str(line) if line is not None else "?"
        body = t.get("root_body") or ""
        # Surface the existing thread's severity in the row prefix —
        # `[#id SEVERITY]` matches the example in `prompts.SYSTEM_PROMPT`
        # under "De-duplication against existing review threads", and
        # gives the LLM the signal it needs to apply the prompt's
        # escalation override ("emit a new finding when the candidate's
        # severity is HIGHER than the existing thread's"). Without the
        # tag, the model has no reliable way to compare severities and
        # the override silently degrades to never-fires.
        severity_tag = _extract_severity_tag(body)
        essence = _summarize_thread_root(body)
        rows.append((
            t.get("root_created_at") or "",
            f"[#{thread_id} {severity_tag}] {path}:{line_str} — {essence}",
        ))

    if not rows:
        return "(no open review-bot threads)"

    # Sort newest-first so when we truncate it's the OLDEST threads that
    # get dropped. A finding that re-surfaces is more likely to match
    # against a recent thread than an ancient one.
    rows.sort(key=lambda r: r[0], reverse=True)
    if len(rows) > max_threads:
        kept = rows[:max_threads]
        dropped = len(rows) - max_threads
        lines = [line for _, line in kept]
        lines.append(
            f"\n[… {dropped} older open thread(s) omitted to cap prompt cost. "
            f"If a candidate finding matches one of these, the dedup gate will "
            f"miss it and you'll emit a duplicate — accept that risk and "
            f"don't speculate.]"
        )
        return "\n".join(lines)

    return "\n".join(line for _, line in rows)


def list_driver_source(
    *,
    driver_root: Path,
    source_subpath: str,
) -> str:
    """Return a flat directory listing of source_subpath (paths + sizes).

    No file content — content is fetched on-demand via the agent's
    read_paths / grep tools.
    """
    src_root = driver_root / source_subpath.rstrip("/")
    listing_lines: list[str] = []
    for path in sorted(src_root.rglob("*")):
        if path.is_file():
            rel = path.relative_to(driver_root)
            listing_lines.append(f"{rel}  ({path.stat().st_size} bytes)")
    return "\n".join(listing_lines)
