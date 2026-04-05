"""
:mod:`tests.unit.cli._handlers.test_u_cli_handlers_run` module.

Direct unit tests for :mod:`etlplus.cli._handlers.run`.
"""

from __future__ import annotations

import pytest

from etlplus.cli._handlers import run as run_mod

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestFailureMessage:
    """Unit tests for :func:`etlplus.cli._handlers.run._failure_message`."""

    @pytest.mark.parametrize(
        ('result', 'expected'),
        [
            pytest.param(
                'not-a-mapping',
                None,
                id='non-mapping',
            ),
            pytest.param(
                {
                    'status': 'failed',
                    'failed_jobs': ['seed', 'publish'],
                    'skipped_jobs': ['notify'],
                },
                '2 job(s) failed and 1 job(s) were skipped during DAG execution',
                id='plural-failure-and-skips',
            ),
            pytest.param(
                {
                    'status': 'partial_success',
                    'failed_jobs': 'bad-shape',
                    'skipped_jobs': None,
                },
                'DAG execution failed',
                id='fallback-message',
            ),
        ],
    )
    def test_failure_message_variants(
        self,
        result: object,
        expected: str | None,
    ) -> None:
        """
        Test that failure messaging covers non-mapping, plural, and fallback
        cases.
        """
        assert run_mod._failure_message(result) == expected
