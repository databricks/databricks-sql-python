import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile


SCRIPT = Path(__file__).resolve().parents[1] / 'sync-engineer-bot-prs.py'
sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location('sync_engineer_bot_prs', SCRIPT)
sync = importlib.util.module_from_spec(spec)
spec.loader.exec_module(sync)
merge = sync.merge_changelog

BASE = '# Release History\n\n# 3.0.0\n\n- Released fix.\n'
RELEASED = BASE.replace('# 3.0.0', '# 3.1.0\n\n- Existing fix.\n\n# 3.0.0')


def unreleased(*lines, base=BASE):
    return base.replace('\n# ', '\n# Unreleased\n\n' + ''.join(line + '\n' for line in lines) + '\n# ', 1)


EXISTING = unreleased('- Existing fix.')
EMPTY = BASE.replace('\n# ', '\n# Unreleased\n\n# ', 1)

merge_cases = [
    # Both sides create the section, as #565 and #569 did.
    ((BASE, unreleased('- PR fix. (#494)'), unreleased('- Main fix. (#470)')),
     unreleased('- Main fix. (#470)', '- PR fix. (#494)')),
    # Continuation lines move with their entry.
    ((EXISTING, unreleased('- Existing fix.', '- PR fix.', '  Detail.'), unreleased('- Existing fix.', '- Main fix.')),
     unreleased('- Existing fix.', '- Main fix.', '- PR fix.', '  Detail.')),
    # Main released the section, so the PR's entry starts a new one.
    ((EXISTING, unreleased('- Existing fix.', '- PR fix.'), RELEASED), unreleased('- PR fix.', base=RELEASED)),
    ((BASE, unreleased('- PR fix.'), EMPTY), unreleased('- PR fix.')),
    ((BASE, unreleased('- Same fix.'), unreleased('- Same fix.')), unreleased('- Same fix.')),
    # Anything beyond appended entries is left for a maintainer.
    ((BASE, unreleased('- PR fix.').replace('Released fix', 'Edited fix'), unreleased('- Main fix.')), None),
    ((EXISTING, unreleased('- PR fix.'), unreleased('- Existing fix.', '- Main fix.')), None),
    ((BASE, unreleased('- PR fix.', 'A note.'), unreleased('- Main fix.')), None),
]

# Preserve main's releases/notes, and reject changed or removed existing entries.
merge_cases += [
    ((EXISTING, unreleased('- Existing fix.', '- PR fix.'), unreleased('- Main fix.', base=RELEASED)),
     unreleased('- Main fix.', '- PR fix.', base=RELEASED)),
    ((EXISTING, unreleased('- Changed existing fix.'), unreleased('- Main fix.')), None),
    ((EXISTING, unreleased('- PR fix.'), unreleased('- Main fix.')), None),
]

for arguments, expected in merge_cases:
    assert merge(*arguments) == expected, (arguments, merge(*arguments))


def git(root, *args):
    return subprocess.run(
        [
            'git',
            '-c',
            'core.hooksPath=/dev/null',
            '-c',
            'commit.gpgsign=false',
            '-c',
            'user.name=Test',
            '-c',
            'user.email=test@example.com',
            '-C',
            str(root),
            *args,
        ],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def commit(root, message, files):
    for name, content in files.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
    git(root, 'add', '.')
    git(root, 'commit', '-qm', message)
    return git(root, 'rev-parse', 'HEAD')


sync_cases = 0
with tempfile.TemporaryDirectory(prefix='driver-bot-sync-') as temporary:
    root = Path(temporary)
    origin = root / 'origin.git'
    git(root, 'init', '-q', '--bare', str(origin))
    git(origin, 'symbolic-ref', 'HEAD', 'refs/heads/main')
    seed = root / 'seed'
    git(root, 'clone', '-q', str(origin), str(seed))
    base = commit(seed, 'base', {'CHANGELOG.md': BASE, 'src/a.txt': 'one\n'})
    git(seed, 'push', '-q', 'origin', 'HEAD:refs/heads/main')

    branches = {
        'changelog': {'CHANGELOG.md': unreleased('- PR fix. (#494)'), 'src/b.txt': 'pr\n'},
        'code': {'src/a.txt': 'pr\n'},
        'mixed': {'CHANGELOG.md': unreleased('- PR fix.'), 'src/a.txt': 'pr\n'},
        'edited': {'CHANGELOG.md': unreleased('- PR fix.').replace('Released fix', 'Edited fix')},
        'clean': {'src/c.txt': 'pr\n'},
    }
    heads = {}
    for name, files in branches.items():
        git(seed, 'checkout', '-q', '--detach', base)
        heads[name] = commit(seed, name, files)
        git(seed, 'push', '-q', 'origin', f'HEAD:refs/heads/{name}')

    git(seed, 'checkout', '-q', '--detach', base)
    target = commit(seed, 'main', {'CHANGELOG.md': unreleased('- Main fix. (#470)'), 'src/a.txt': 'main\n'})
    git(seed, 'push', '-q', 'origin', 'HEAD:refs/heads/main')
    heads['current'] = commit(seed, 'current', {'src/d.txt': 'pr\n'})
    git(seed, 'push', '-q', 'origin', 'HEAD:refs/heads/current')

    worker = root / 'worker'
    git(root, 'clone', '-q', str(origin), str(worker))
    expected = {
        'changelog': 'merged',
        'code': 'conflict',
        'mixed': 'conflict',
        'edited': 'conflict',
        'clean': 'clean',
        'current': 'up-to-date',
    }
    previous = os.getcwd()
    os.chdir(worker)
    try:
        for name, status in expected.items():
            assert sync.sync_pr(name, target, 'main') == (status, heads[name]), name
            assert git(worker, 'status', '--porcelain') == '', name
            if status != 'merged':
                assert git(origin, 'rev-parse', f'refs/heads/{name}') == heads[name], name
            sync_cases += 1
    finally:
        os.chdir(previous)

    merged = 'refs/heads/changelog'
    assert git(origin, 'rev-list', '--parents', '-n1', merged).split()[1:] == [heads['changelog'], target]
    assert git(origin, 'show', f'{merged}:CHANGELOG.md') + '\n' == unreleased('- Main fix. (#470)', '- PR fix. (#494)')
    assert git(origin, 'show', f'{merged}:src/a.txt') == 'main'
    assert git(origin, 'show', f'{merged}:src/b.txt') == 'pr'
    message = git(origin, 'log', '-1', '--format=%B', merged)
    assert message.startswith('Merge main into changelog'), message
    assert f'Signed-off-by: {sync.BOT_NAME} <{sync.BOT_EMAIL}>' in message, message

    # main(): skips cross-repository PRs and comments once per conflicting head.
    comments = root / 'comments.json'
    comments.write_text('[]')
    fake_gh = root / 'bin' / 'gh'
    fake_gh.parent.mkdir()
    fake_gh.write_text(
        f'#!{sys.executable}\n'
        'import json, sys\n'
        f'path = {str(comments)!r}\n'
        'args = sys.argv[1:]\n'
        'comments = json.load(open(path))\n'
        'if args[:2] == ["pr", "list"]:\n'
        '    assert args[args.index("--base") + 1] == "main"\n'
        '    print(json.dumps([\n'
        '        {"number": 1, "headRefName": "changelog", "isCrossRepository": False},\n'
        '        {"number": 2, "headRefName": "code", "isCrossRepository": False},\n'
        '        {"number": 3, "headRefName": "edited", "isCrossRepository": True},\n'
        '    ]))\n'
        'elif args[0] == "api":\n'
        '    number = int(args[2].split("/")[-2])\n'
        '    print("\\n".join(body for n, body in comments if n == number))\n'
        'elif args[:2] == ["pr", "comment"]:\n'
        '    comments.append([int(args[2]), args[args.index("--body") + 1]])\n'
        '    json.dump(comments, open(path, "w"))\n'
    )
    fake_gh.chmod(0o755)
    runner = root / 'runner'
    git(root, 'clone', '-q', str(origin), str(runner))
    environment = dict(os.environ, REPO='databricks/databricks-sql-python', DEFAULT_BRANCH='main')
    environment['PATH'] = f'{fake_gh.parent}{os.pathsep}{environment["PATH"]}'
    for _ in range(2):
        run = subprocess.run(
            [sys.executable, '-I', str(SCRIPT)], cwd=runner, env=environment, capture_output=True, text=True
        )
        assert run.returncode == 0, run.stdout + run.stderr
        assert 'PR #1 (changelog): up-to-date' in run.stdout, run.stdout
        assert 'PR #2 (code): conflict' in run.stdout, run.stdout
        assert '#3' not in run.stdout, run.stdout
    posted = json.loads(comments.read_text())
    assert [number for number, _ in posted] == [2], posted
    assert sync.MARKER.format(heads['code']) in posted[0][1], posted
    assert git(origin, 'rev-parse', 'refs/heads/edited') == heads['edited']
    sync_cases += 1

print(f'Passed {len(merge_cases)} changelog merge cases and {sync_cases} branch sync cases.')
