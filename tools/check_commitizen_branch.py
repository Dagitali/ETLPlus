#!/usr/bin/env python3
"""Run Commitizen against non-merge commits ahead of the default branch."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

# SECTION: CONSTANTS ======================================================== #


REPO_ROOT = Path(__file__).resolve().parents[1]
ZERO_REF = '0' * 40


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _git_stdout(
    *args: str,
) -> str:
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


def _non_merge_commits(
    rev_range: str,
) -> list[str]:
    """Return non-merge commit hashes in oldest-first order for a revision range."""
    commits = _git_stdout('rev-list', '--reverse', '--no-merges', rev_range)
    return commits.splitlines()


def _commit_message(
    commit: str,
) -> str:
    """Return the full commit message for a commit hash."""
    return _git_stdout('log', '--format=%B', '-n', '1', commit)


def _check_message(
    message: str,
) -> int:
    """Return the Commitizen status code for one commit message."""
    return subprocess.run(
        [sys.executable, '-m', 'commitizen', 'check', '--message', message],
        check=False,
        cwd=REPO_ROOT,
    ).returncode


# SECTION: FUNCTIONS ======================================================== #


def main() -> int:
    """
    Execute Commitizen against non-merge commits in the resolved range.

    Returns
    -------
    int
        A conventional POSIX exit code: zero on success, non-zero on error.
    """
    rev_range = _resolve_rev_range()
    return next(
        (
            status
            for commit in _non_merge_commits(rev_range)
            if (status := _check_message(_commit_message(commit))) != 0
        ),
        0,
    )


# SECTION: MAIN ENTRY POINT ================================================= #


if __name__ == '__main__':
    raise SystemExit(main())
