"""
:mod:`tests.meta.test_m_commitizen_branch` module.

Guardrails for local Commitizen branch validation.
"""

from __future__ import annotations

import pytest

import tools.check_commitizen_branch as commitizen_branch

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestCommitizenBranchCheck:
    """Test local Commitizen branch-check behavior."""

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
        assert commitizen_branch._validate_gitflow_branch_name(branch) == 0

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
        assert commitizen_branch._validate_gitflow_branch_name(branch) == 1
        assert 'exactly one "/"' in capsys.readouterr().err

    def test_main_stops_before_commitizen_when_branch_name_is_invalid(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test invalid GitFlow branch names fail before commit-message checks.
        """
        monkeypatch.setattr(
            commitizen_branch,
            '_current_branch',
            lambda: 'feature/api/add-connector-metadata',
        )
        monkeypatch.setattr(
            commitizen_branch,
            '_resolve_rev_range',
            lambda: 'origin/main..HEAD',
        )
        monkeypatch.setattr(commitizen_branch, '_check_message', self._unexpected_check)

        assert commitizen_branch.main() == 1

    def test_non_merge_commits_excludes_merge_commits(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that non-merge commit discovery delegates merge exclusion to Git.
        """
        calls: list[tuple[str, ...]] = []

        def git_stdout(*args: str) -> str:
            calls.append(args)
            return 'abc123\ndef456'

        monkeypatch.setattr(commitizen_branch, '_git_stdout', git_stdout)

        assert commitizen_branch._non_merge_commits('origin/main..HEAD') == [
            'abc123',
            'def456',
        ]

        assert calls == [
            ('rev-list', '--reverse', '--no-merges', 'origin/main..HEAD'),
        ]

    def test_main_accepts_ranges_with_only_merge_commits(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that ranges with no non-merge commits pass validation.
        """
        monkeypatch.setattr(
            commitizen_branch,
            '_current_branch',
            lambda: 'bugfix/valid-branch-name',
        )
        monkeypatch.setattr(
            commitizen_branch,
            '_resolve_rev_range',
            lambda: 'origin/main..HEAD',
        )
        monkeypatch.setattr(
            commitizen_branch,
            '_non_merge_commits',
            lambda rev_range: [],
        )
        monkeypatch.setattr(commitizen_branch, '_check_message', self._unexpected_check)

        assert commitizen_branch.main() == 0

    def test_main_validates_non_merge_commit_messages(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that Commitizen validates only discovered non-merge commits.
        """
        checked_messages: list[str] = []
        messages = {
            'abc123': 'deps(github-actions): bump pinned actions',
            'def456': 'fix(ci): ignore merge commits in commitizen check',
        }

        monkeypatch.setattr(
            commitizen_branch,
            '_current_branch',
            lambda: 'bugfix/valid-branch-name',
        )
        monkeypatch.setattr(
            commitizen_branch,
            '_resolve_rev_range',
            lambda: 'origin/main..HEAD',
        )
        monkeypatch.setattr(
            commitizen_branch,
            '_non_merge_commits',
            lambda rev_range: ['abc123', 'def456'],
        )
        monkeypatch.setattr(commitizen_branch, '_commit_message', messages.__getitem__)

        def check_message(message: str) -> int:
            checked_messages.append(message)
            return 0

        monkeypatch.setattr(commitizen_branch, '_check_message', check_message)

        assert commitizen_branch.main() == 0

        assert checked_messages == list(messages.values())

    def test_main_returns_first_commitizen_failure(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that validation exits on the first failing non-merge commit.
        """
        checked_messages: list[str] = []

        monkeypatch.setattr(
            commitizen_branch,
            '_current_branch',
            lambda: 'bugfix/valid-branch-name',
        )
        monkeypatch.setattr(
            commitizen_branch,
            '_resolve_rev_range',
            lambda: 'origin/main..HEAD',
        )
        monkeypatch.setattr(
            commitizen_branch,
            '_non_merge_commits',
            lambda rev_range: ['abc123', 'def456'],
        )
        monkeypatch.setattr(
            commitizen_branch,
            '_commit_message',
            lambda commit: f'{commit} message',
        )

        def check_message(message: str) -> int:
            checked_messages.append(message)
            return 17

        monkeypatch.setattr(commitizen_branch, '_check_message', check_message)

        assert commitizen_branch.main() == 17

        assert checked_messages == ['abc123 message']

    @staticmethod
    def _unexpected_check(message: str) -> int:
        """
        Raise when Commitizen validation should not run.
        """
        raise AssertionError(f'unexpected Commitizen check: {message}')
