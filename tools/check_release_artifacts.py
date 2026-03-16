"""
:mod:`check_release_artifacts` module.

Utility for auditing release artifacts for forbidden paths and untracked files.
"""

from __future__ import annotations

import re
import subprocess
import sys
import tarfile
import zipfile
from functools import cache
from pathlib import Path

# SECTION: CONSTANTS ======================================================== #


FORBIDDEN_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r'(^|/)(doc-NEW|docs-CURRENT|out|temp)(/|$)'),
    re.compile(r'(^|/)\.vscode(/|$)'),
    re.compile(r'(^|/)build(/|$)'),
    re.compile(r'(^|/)etlplus/[^/]+_DEFUNCT(/|$)'),
    re.compile(
        r'(^|/)etlplus/'
        r'(database/ddl_(NEW|ORIG)\.py|file/jinja2_NEW\.py)$',
    ),
    re.compile(
        r'(^|/)'
        r'(Makefile_ORIG|MONETIZATION\.md|export\.json|out\.json|'
        r'output\.csv|output\.json|result\.json|roadmap\.md)$',
    ),
    re.compile(r'(^|/)\.DS_Store$'),
    re.compile(r'(^|/)etlplus/enum_base\.py\.txt$'),
    re.compile(
        r'(^|/)\.github/'
        r'(FUNDING_(NEW|ORIG)\.yml|'
        r'workflows/ci_(FIXED|ORIG)\.yml\.txt)$',
    ),
    re.compile(
        r'(^|/)[^/]+'
        r'(_NEW\.py|_ORIG\.py|_FIXED\.yml\.txt|_ORIG\.yml\.txt)$',
    ),
)


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _artifact_members(path: Path) -> list[str]:
    if path.suffix == '.whl' or path.suffix == '.zip':
        with zipfile.ZipFile(path) as archive:
            return archive.namelist()
    if path.suffixes[-2:] == ['.tar', '.gz']:
        with tarfile.open(path, 'r:gz') as archive:
            return [member.name for member in archive.getmembers() if member.name]
    raise ValueError(f'Unsupported artifact type: {path}')


@cache
def _tracked_repo_files() -> set[str]:
    repo_root = Path(__file__).resolve().parents[1]
    tracked = subprocess.check_output(
        ['git', 'ls-files'],
        text=True,
        cwd=repo_root,
    )
    return set(tracked.splitlines())


def _normalized_member_paths(
    path: Path,
) -> list[str]:
    members: list[str] = []

    if path.suffix == '.whl' or path.suffix == '.zip':
        for member_name in _artifact_members(path):
            if '.dist-info/' in member_name or '.data/' in member_name:
                continue
            members.append(member_name.strip('./'))
        return members

    with tarfile.open(path, 'r:gz') as archive:
        for archive_member in archive.getmembers():
            if not archive_member.isfile() or not archive_member.name:
                continue
            normalized = archive_member.name.strip('./')
            _, separator, relative = normalized.partition('/')
            if not separator or not relative:
                continue
            if relative == 'PKG-INFO' or relative == 'setup.cfg':
                continue
            if relative.startswith('etlplus.egg-info/'):
                continue
            members.append(relative)
    return members


def _find_violations(path: Path) -> list[str]:
    violations: list[str] = []
    for normalized in _normalized_member_paths(path):
        if any(pattern.search(normalized) for pattern in FORBIDDEN_PATTERNS):
            violations.append(normalized)
    return sorted(set(violations))


def _find_untracked_members(path: Path) -> list[str]:
    tracked = _tracked_repo_files()
    return sorted(
        {member for member in _normalized_member_paths(path) if member not in tracked},
    )


# SECTION: FUNCTIONS ======================================================== #


def main(argv: list[str]) -> int:
    artifact_paths = (
        [Path(arg) for arg in argv] if argv else sorted(Path('dist').glob('*'))
    )
    if not artifact_paths:
        print('No artifacts found to audit.', file=sys.stderr)
        return 2

    exit_code = 0
    for artifact_path in artifact_paths:
        violations = _find_violations(artifact_path)
        untracked = _find_untracked_members(artifact_path)
        if not violations and not untracked:
            print(f'{artifact_path}: ok')
            continue
        exit_code = 1
        print(
            f'{artifact_path}: release artifact audit failed',
            file=sys.stderr,
        )
        if violations:
            print('  forbidden packaged paths detected', file=sys.stderr)
        for violation in violations:
            print(f'    - {violation}', file=sys.stderr)
        if untracked:
            print(
                '  packaged files missing from git tracking',
                file=sys.stderr,
            )
        for member in untracked:
            print(f'    - {member}', file=sys.stderr)
    return exit_code


# SECTION: MAIN EXECUTION =================================================== #


if __name__ == '__main__':
    raise SystemExit(main(sys.argv[1:]))
