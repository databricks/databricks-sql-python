"""Merge the default branch into open engineer-bot PRs that conflict with it.

GitHub runs no pull request workflows on a conflicting PR, so neither bot answers
review comments until the conflict is resolved. A conflict limited to entries the
PR added under `# Unreleased` in CHANGELOG.md is resolved by keeping both sides'
entries. Any other conflict gets one comment per PR head for a maintainer.
"""

from collections import Counter
import json
import os
import re
import subprocess
import sys

BOT_NAME = 'peco-engineer-bot[bot]'
BOT_EMAIL = 'peco-engineer-bot[bot]@users.noreply.github.com'
MARKER = '<!-- engineer-bot-python:v1 sync-main head={} -->'
UNRELEASED_HEADING = '# Unreleased'
RELEASE_PREFIX = '# '
RELEASE_HEADING = re.compile('^' + re.escape(RELEASE_PREFIX) + r'\[?v?\d')
SECTION = re.compile(r'(?ms)^' + re.escape(UNRELEASED_HEADING) + r'[ \t]*\n(.*?)(?=^' + re.escape(RELEASE_PREFIX) + r'|\Z)')


def parse_unreleased(text):
    """Return unchanged text, categorized entries, notes, and insertion points."""
    match = SECTION.search(text)
    if match is None:
        return text, [], [], {}
    category, entry = '', None
    entries, notes, positions = [], [], {'': 0}
    for index, line in enumerate(match.group(1).splitlines()):
        if line.startswith('### '):
            category, entry = line, None
            if category in positions:
                return text, [], [], {}
            notes.append(line)
            positions[category] = index + 1
        elif re.match(r'[-*+] ', line) and line[2:].strip():
            entry = [line]
            entries.append((category, entry))
            positions[category] = index + 1
        elif entry is not None and (not line.strip() or line[:1].isspace()):
            entry.append(line)
            if line.strip():
                positions[category] = index + 1
        else:
            entry = None
            if line.strip():
                notes.append(line)
    outside = text[:match.start()] + text[match.end():]
    return outside, [(group, '\n'.join(lines).rstrip()) for group, lines in entries], notes, positions


def split_unreleased(text):
    return parse_unreleased(text)[:3]


def merge_changelog(base, ours, theirs):
    """Keep both sides' new entries; reject edits to existing entries or notes."""
    base_outside, base_entries, base_notes = split_unreleased(base)
    ours_outside, ours_entries, ours_notes = split_unreleased(ours)
    if ours_outside != base_outside or ours_notes != base_notes:
        return None
    unmatched = Counter(base_entries)
    added = []
    for entry in ours_entries:
        if unmatched[entry]:
            unmatched[entry] -= 1
        else:
            added.append(entry)
    if +unmatched:
        return None
    theirs_outside, theirs_entries, theirs_notes, positions = parse_unreleased(theirs)
    added = [entry for entry in added if entry not in theirs_entries]
    if not added:
        return theirs

    match = SECTION.search(theirs)
    lines = theirs.split('\n')
    if match is None:
        # A released flat changelog can start a fresh Unreleased section.
        if any(group for group, _ in added):
            return None
        first = next((i for i, line in enumerate(lines) if RELEASE_HEADING.match(line)), None)
        if first is None:
            return None
        block = '\n'.join(entry for _, entry in added).split('\n')
        lines[first:first] = [UNRELEASED_HEADING, '', *block, '']
    else:
        if any(group not in positions for group, _ in added):
            return None
        body_start = theirs[:match.start(1)].count('\n')
        for group, position in sorted(positions.items(), key=lambda item: item[1], reverse=True):
            entries = [entry for category, entry in added if category == group]
            if not entries:
                continue
            position += body_start
            block = '\n'.join(entries).split('\n')
            if position == body_start or lines[position - 1].startswith('### '):
                block.insert(0, '')
            if position == len(lines) or lines[position].strip():
                block.append('')
            lines[position:position] = block
    merged = '\n'.join(lines)
    outside, entries, notes = split_unreleased(merged)
    if (outside != theirs_outside or notes != theirs_notes
            or Counter(entries) != Counter(theirs_entries + added)):
        return None
    return merged


def git(*args, check=True):
    return subprocess.run(
        [
            'git',
            '-c',
            'core.hooksPath=/dev/null',
            '-c',
            'commit.gpgsign=false',
            '-c',
            f'user.name={BOT_NAME}',
            '-c',
            f'user.email={BOT_EMAIL}',
            *args,
        ],
        check=check,
        capture_output=True,
        text=True,
    )


def sync_pr(branch, target, target_name):
    """Merge `target` into `branch` when they conflict. Returns (status, head)."""
    git('fetch', '--no-tags', 'origin', f'refs/heads/{branch}')
    head = git('rev-parse', 'FETCH_HEAD').stdout.strip()
    if git('merge-base', '--is-ancestor', target, head, check=False).returncode == 0:
        return 'up-to-date', head
    git('checkout', '--quiet', '--detach', head)
    if git('merge', '--no-commit', '--no-ff', target, check=False).returncode == 0:
        git('merge', '--abort')
        return 'clean', head
    merged = None
    if git('diff', '--name-only', '--diff-filter=U').stdout.split() == ['CHANGELOG.md']:
        stages = [git('show', f':{stage}:CHANGELOG.md', check=False) for stage in (1, 2, 3)]
        if all(stage.returncode == 0 for stage in stages):
            merged = merge_changelog(*(stage.stdout for stage in stages))
    if merged is None:
        git('merge', '--abort')
        return 'conflict', head
    with open('CHANGELOG.md', 'w') as changelog:
        changelog.write(merged)
    git('add', 'CHANGELOG.md')
    git('commit', '--quiet', '--signoff', '-m', f'Merge {target_name} into {branch}')
    git('push', '--quiet', 'origin', f'HEAD:refs/heads/{branch}')
    return 'merged', head


def gh(*args):
    return subprocess.run(['gh', *args], check=True, capture_output=True, text=True).stdout


def comment_once(repo, number, head, target_name):
    marker = MARKER.format(head)
    if marker in gh('api', '--paginate', f'repos/{repo}/issues/{number}/comments', '--jq', '.[].body'):
        return
    body = (
        f'This PR conflicts with `{target_name}` beyond new `# Unreleased` changelog entries, '
        'so it needs a manual merge. GitHub runs no pull request workflows on a PR with conflicts, '
        'so the bots won\'t respond to review comments until this is resolved.\n\n'
        f'{marker}'
    )
    gh('pr', 'comment', str(number), '--repo', repo, '--body', body)


def main():
    repo = os.environ['REPO']
    target_name = os.environ['DEFAULT_BRANCH']
    target = git('rev-parse', f'refs/remotes/origin/{target_name}').stdout.strip()
    prs = json.loads(
        gh(
            'pr', 'list', '--repo', repo, '--label', 'engineer-bot', '--state', 'open', '--base', target_name, '--limit', '100',
            '--json', 'number,headRefName,isCrossRepository',
        )
    )
    token = os.environ.get('GH_TOKEN', '')
    failed = []
    for pr in prs:
        if pr['isCrossRepository']:
            continue
        number, branch = pr['number'], pr['headRefName']
        try:
            status, head = sync_pr(branch, target, target_name)
            if status == 'conflict':
                comment_once(repo, number, head, target_name)
        except subprocess.CalledProcessError as error:
            git('merge', '--abort', check=False)
            git('reset', '--quiet', '--hard', check=False)
            detail = (error.stderr or '').strip()
            if token:
                detail = detail.replace(token, '***')
            print(f'::warning::Could not sync PR #{number} ({branch}): {detail}')
            failed.append(number)
            continue
        print(f'PR #{number} ({branch}): {status}')
    if failed:
        sys.exit(f'::error::Could not sync PRs: {", ".join(f"#{number}" for number in failed)}')


if __name__ == '__main__':
    main()
