#!/usr/bin/env python3
"""Run Commitizen against non-merge commits ahead of the default branch."""

from __future__ import annotations

import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

# SECTION: CONSTANTS ======================================================== #


REPO_ROOT = Path(__file__).resolve().parents[1]
ZERO_REF = '0' * 40
LONG_LIVED_BRANCHES = frozenset({'develop', 'main'})
GITFLOW_BRANCH_SLASH_COUNT = 1


# SECTION: DATA CLASSES ===================================================== #


@dataclass(frozen=True, slots=True)
class CommitizenBranchChecker:
    """
    Validate GitFlow branch shape and non-merge commit messages.

    Attributes
    ----------
    repo_root : Path
        The repository root directory to operate within.
    zero_ref : str
        The zero commit hash used by Git to represent an empty reference.
    long_lived_branches : frozenset[str]
        A set of branch names considered long-lived, which are exempt from
        GitFlow branch name validation.
    branch_slash_count : int
        The expected number of slashes in a GitFlow branch name, which is used
        to validate the current branch name.
    """

    repo_root: Path = REPO_ROOT
    zero_ref: str = ZERO_REF
    long_lived_branches: frozenset[str] = LONG_LIVED_BRANCHES
    branch_slash_count: int = GITFLOW_BRANCH_SLASH_COUNT

    # -- Class Methods -- #

    @classmethod
    def validate_gitflow_branch_name(
        cls,
        branch: str,
        *,
        long_lived_branches: frozenset[str] = LONG_LIVED_BRANCHES,
        branch_slash_count: int = GITFLOW_BRANCH_SLASH_COUNT,
    ) -> int:
        """
        Return zero when a branch name follows the repository GitFlow shape.

        Parameters
        ----------
        branch : str
            The branch name to validate.
        long_lived_branches : frozenset[str], optional
            A set of branch names considered long-lived (default is
            ``LONG_LIVED_BRANCHES``).
        branch_slash_count : int, optional
            The expected number of slashes in a GitFlow branch name (default is
            ``GITFLOW_BRANCH_SLASH_COUNT``).

        Returns
        -------
        int
            Zero if the branch name follows the GitFlow shape, non-zero
            otherwise.
        """
        if not branch or branch in long_lived_branches:
            return 0

        if branch.count('/') == branch_slash_count:
            return 0

        print(
            f'GitFlow branch names must contain exactly one "/": {branch!r}',
            file=sys.stderr,
        )
        return 1

    # -- Instance Methods -- #

    def check_message(
        self,
        message: str,
    ) -> int:
        """
        Return the Commitizen status code for one commit message.

        Parameters
        ----------
        message : str
            The commit message to check.

        Returns
        -------
        int
            A conventional POSIX exit code: zero on success, non-zero on error.
        """
        return subprocess.run(
            [sys.executable, '-m', 'commitizen', 'check', '--message', message],
            check=False,
            cwd=self.repo_root,
        ).returncode

    def commit_message(
        self,
        commit: str,
    ) -> str:
        """
        Return the full commit message for a commit hash.

        Parameters
        ----------
        commit : str
            The commit hash to retrieve the message for.

        Returns
        -------
        str
            The full commit message for the specified commit.
        """
        return self.git_stdout('log', '--format=%B', '-n', '1', commit)

    def current_branch(self) -> str:
        """
        Return the current local branch name, or empty string for detached
        HEAD.

        Returns
        -------
        str
            The current local branch name, or empty string for detached HEAD.
        """
        return self.git_stdout('branch', '--show-current')

    def git_stdout(
        self,
        *args: str,
    ) -> str:
        """
        Return stripped stdout for a Git command executed at the repository root.

        Parameters
        ----------
        *args : str
            Arguments to pass to the Git command.

        Returns
        -------
        str
            The stripped stdout of the Git command.
        """
        completed = subprocess.run(
            ['git', *args],
            check=True,
            cwd=self.repo_root,
            capture_output=True,
            text=True,
        )
        return completed.stdout.strip()

    def non_merge_commits(
        self,
        rev_range: str,
    ) -> list[str]:
        """
        Return non-merge commit hashes in oldest-first order for a revision range.

        Parameters
        ----------
        rev_range : str
            The revision range to retrieve non-merge commits for.

        Returns
        -------
        list[str]
            A list of non-merge commit hashes in oldest-first order.
        """
        commits = self.git_stdout('rev-list', '--reverse', '--no-merges', rev_range)
        return commits.splitlines()

    def resolve_default_remote_branch(self) -> str:
        """
        Return the remote-tracking default branch used as the Commitizen base.

        Returns
        -------
        str
            The remote-tracking default branch used as the Commitizen base.
        """
        try:
            return self.git_stdout(
                'symbolic-ref',
                '--quiet',
                '--short',
                'refs/remotes/origin/HEAD',
            )
        except subprocess.CalledProcessError:
            return 'origin/main'

    def resolve_rev_range(self) -> str:
        """
        Return the commit range to validate for the current push.

        Returns
        -------
        str
            The commit range to validate for the current push.
        """
        from_ref = os.getenv('PRE_COMMIT_FROM_REF', '').strip()
        to_ref = os.getenv('PRE_COMMIT_TO_REF', '').strip()
        if (
            from_ref
            and to_ref
            and from_ref != self.zero_ref
            and to_ref != self.zero_ref
        ):
            return f'{from_ref}..{to_ref}'
        return f'{self.resolve_default_remote_branch()}..HEAD'

    def run(self) -> int:
        """
        Execute Commitizen against non-merge commits in the resolved range.

        Returns
        -------
        int
            A conventional POSIX exit code: zero on success, non-zero on error.
        """
        if (
            status := self.validate_gitflow_branch_name(
                self.current_branch(),
                long_lived_branches=self.long_lived_branches,
                branch_slash_count=self.branch_slash_count,
            )
        ) != 0:
            return status

        rev_range = self.resolve_rev_range()
        return next(
            (
                status
                for commit in self.non_merge_commits(rev_range)
                if (status := self.check_message(self.commit_message(commit))) != 0
            ),
            0,
        )


# SECTION: FUNCTIONS ======================================================== #


def main() -> int:
    """
    Execute Commitizen against non-merge commits in the resolved range.

    Returns
    -------
    int
        A conventional POSIX exit code: zero on success, non-zero on error.
    """
    return CommitizenBranchChecker().run()


# SECTION: MAIN ENTRY POINT ================================================= #


if __name__ == '__main__':
    raise SystemExit(main())
