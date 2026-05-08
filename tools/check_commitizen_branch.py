#!/usr/bin/env python3
"""Run the Commitizen branch check against commits ahead of the default branch."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
ZERO_REF = '0' * 40


def _git_stdout(*args: str) -> str:
    """Return stripped stdout for a Git command executed at the repository root."""
    completed = subprocess.run(
        ['git', *args],
        check=True,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def _resolve_default_remote_branch() -> str:
    """Return the remote-tracking default branch used as the Commitizen base."""
    try:
        return _git_stdout(
            'symbolic-ref',
            '--quiet',
            '--short',
            'refs/remotes/origin/HEAD',
        )
    except subprocess.CalledProcessError:
        return 'origin/main'


def _resolve_rev_range() -> str:
    """Return the commit range to validate for the current push."""
    from_ref = os.getenv('PRE_COMMIT_FROM_REF', '').strip()
    to_ref = os.getenv('PRE_COMMIT_TO_REF', '').strip()
    if from_ref and to_ref and from_ref != ZERO_REF and to_ref != ZERO_REF:
        return f'{from_ref}..{to_ref}'
    return f'{_resolve_default_remote_branch()}..HEAD'


def main() -> int:
    """Execute Commitizen against the resolved revision range."""
    rev_range = _resolve_rev_range()
    return subprocess.run(
        [sys.executable, '-m', 'commitizen', 'check', '--rev-range', rev_range],
        check=False,
        cwd=REPO_ROOT,
    ).returncode


if __name__ == '__main__':
    raise SystemExit(main())
