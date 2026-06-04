"""
:mod:`tests.meta.test_m_commitizen_branch` module.

Guardrails for local Commitizen branch validation.
"""

from __future__ import annotations

import pytest
import yaml  # type: ignore[import]

import tools.check_commitizen_branch as commitizen_branch
from tests.pytest_shared_support import REPO_ROOT

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: CONSTANTS ======================================================== #

DEPENDABOT_CONFIG_PATH = REPO_ROOT / '.github' / 'dependabot.yml'
COMMITIZEN_COMMIT_TYPES = frozenset(
    {
        'build',
        'chore',
        'ci',
        'docs',
        'feat',
        'fix',
        'perf',
        'refactor',
        'style',
        'test',
    },
)

# SECTION: TESTS ============================================================ #


class TestCommitizenBranchCheck:
    """Test local Commitizen branch-check behavior."""

    def test_dependabot_commit_prefixes_match_commitizen_types(self) -> None:
        """
        Test Dependabot update commits use Commitizen-compatible types.
        """
        dependabot_config = yaml.safe_load(
            DEPENDABOT_CONFIG_PATH.read_text(encoding='utf-8'),
        )

        prefixes = {
            update['commit-message']['prefix']
            for update in dependabot_config['updates']
            if 'commit-message' in update
        }

        assert prefixes <= COMMITIZEN_COMMIT_TYPES
        assert 'deps' not in prefixes

    @pytest.mark.parametrize(
        'branch',
        [
            pytest.param('', id='detached-head'),
            pytest.param('develop', id='develop'),
            pytest.param('main', id='main'),
            pytest.param('bugfix/declare-click-runtime-dependency', id='bugfix'),
            pytest.param('feature/add-connector-metadata', id='feature'),
            pytest.param('release/v1.26.29', id='release'),
        ],
    )
    def test_gitflow_branch_names_allow_one_slash_or_long_lived_branches(
        self,
        branch: str,
    ) -> None:
        """Test the local branch-name guard allows supported GitFlow names."""
        assert (
            commitizen_branch.CommitizenBranchChecker.validate_gitflow_branch_name(
                branch,
            )
            == 0
        )

    @pytest.mark.parametrize(
        'branch',
        [
            pytest.param('feature/api/add-connector-metadata', id='two-slashes'),
            pytest.param('bugfix', id='no-slash-working-branch'),
        ],
    )
    def test_gitflow_branch_names_reject_nonstandard_slash_counts(
        self,
        capsys: pytest.CaptureFixture[str],
        branch: str,
    ) -> None:
        """Test the local branch-name guard rejects nonstandard branch shapes."""
        assert (
            commitizen_branch.CommitizenBranchChecker.validate_gitflow_branch_name(
                branch,
            )
            == 1
        )
        assert 'exactly one "/"' in capsys.readouterr().err

    def test_main_delegates_to_checker_run(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test the script entrypoint remains a thin compatibility wrapper."""

        class Checker:
            """Minimal checker stub for entrypoint delegation."""

            def run(self) -> int:
                """Return a deterministic status."""
                return 23

        monkeypatch.setattr(commitizen_branch, 'CommitizenBranchChecker', Checker)

        assert commitizen_branch.main() == 23

    def test_non_merge_commits_excludes_merge_commits(
        self,
    ) -> None:
        """
        Test that non-merge commit discovery delegates merge exclusion to Git.
        """
        calls: list[tuple[str, ...]] = []

        class Checker(commitizen_branch.CommitizenBranchChecker):
            """Checker with deterministic Git output."""

            def git_stdout(self, *args: str) -> str:
                """Capture Git args and return deterministic commits."""
                calls.append(args)
                return 'abc123\ndef456'

        assert Checker().non_merge_commits('origin/main..HEAD') == [
            'abc123',
            'def456',
        ]

        assert calls == [
            ('rev-list', '--reverse', '--no-merges', 'origin/main..HEAD'),
        ]

    def test_run_accepts_ranges_with_only_merge_commits(self) -> None:
        """
        Test that ranges with no non-merge commits pass validation.
        """

        class Checker(commitizen_branch.CommitizenBranchChecker):
            """Checker with only merge commits in range."""

            def check_message(self, message: str) -> int:
                """Raise when Commitizen validation should not run."""
                return TestCommitizenBranchCheck._unexpected_check(message)

            def current_branch(self) -> str:
                """Return a valid GitFlow branch name."""
                return 'bugfix/valid-branch-name'

            def non_merge_commits(self, rev_range: str) -> list[str]:
                """Return no non-merge commits."""
                return []

            def resolve_rev_range(self) -> str:
                """Return a deterministic revision range."""
                return 'origin/main..HEAD'

        assert Checker().run() == 0

    def test_run_returns_first_commitizen_failure(self) -> None:
        """
        Test that validation exits on the first failing non-merge commit.
        """
        checked_messages: list[str] = []

        class Checker(commitizen_branch.CommitizenBranchChecker):
            """Checker with a failing Commitizen result."""

            def check_message(self, message: str) -> int:
                """Capture messages checked by Commitizen and fail."""
                checked_messages.append(message)
                return 17

            def commit_message(self, commit: str) -> str:
                """Return deterministic commit messages."""
                return f'{commit} message'

            def current_branch(self) -> str:
                """Return a valid GitFlow branch name."""
                return 'bugfix/valid-branch-name'

            def non_merge_commits(self, rev_range: str) -> list[str]:
                """Return deterministic non-merge commits."""
                return ['abc123', 'def456']

            def resolve_rev_range(self) -> str:
                """Return a deterministic revision range."""
                return 'origin/main..HEAD'

        assert Checker().run() == 17

        assert checked_messages == ['abc123 message']

    def test_run_stops_before_commitizen_when_branch_name_is_invalid(
        self,
    ) -> None:
        """
        Test invalid GitFlow branch names fail before commit-message checks.
        """

        class Checker(commitizen_branch.CommitizenBranchChecker):
            """Checker with invalid branch state."""

            def check_message(self, message: str) -> int:
                """Raise when Commitizen validation should not run."""
                return TestCommitizenBranchCheck._unexpected_check(message)

            def current_branch(self) -> str:
                """Return an invalid GitFlow branch name."""
                return 'feature/api/add-connector-metadata'

            def resolve_rev_range(self) -> str:
                """Return a deterministic revision range."""
                return 'origin/main..HEAD'

        assert Checker().run() == 1

    def test_run_validates_non_merge_commit_messages(self) -> None:
        """
        Test that Commitizen validates only discovered non-merge commits.
        """
        checked_messages: list[str] = []
        messages = {
            'abc123': 'ci(github-actions): bump pinned actions',
            'def456': 'fix(ci): ignore merge commits in commitizen check',
        }

        class Checker(commitizen_branch.CommitizenBranchChecker):
            """Checker with deterministic commit messages."""

            def check_message(self, message: str) -> int:
                """Capture messages checked by Commitizen."""
                checked_messages.append(message)
                return 0

            def commit_message(self, commit: str) -> str:
                """Return deterministic commit messages."""
                return messages[commit]

            def current_branch(self) -> str:
                """Return a valid GitFlow branch name."""
                return 'bugfix/valid-branch-name'

            def non_merge_commits(self, rev_range: str) -> list[str]:
                """Return deterministic non-merge commits."""
                return ['abc123', 'def456']

            def resolve_rev_range(self) -> str:
                """Return a deterministic revision range."""
                return 'origin/main..HEAD'

        assert Checker().run() == 0

        assert checked_messages == list(messages.values())

    @staticmethod
    def _unexpected_check(message: str) -> int:
        """
        Raise when Commitizen validation should not run.
        """
        raise AssertionError(f'unexpected Commitizen check: {message}')
