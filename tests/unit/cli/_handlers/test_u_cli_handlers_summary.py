"""
:mod:`tests.unit.cli._handlers.test_u_cli_handlers_summary` module.

Direct unit tests for :mod:`etlplus.cli._handlers._summary`.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import cast

import pytest

from etlplus import Config
from etlplus.cli._handlers import _summary as summary_mod

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestGraphSummary:
    """Unit tests for :func:`etlplus.cli._handlers._summary.graph_summary`."""

    def test_graph_summary_rejects_duplicate_job_names(self) -> None:
        """Test that duplicate configured job names fail before DAG sorting."""
        cfg = SimpleNamespace(
            jobs=[
                SimpleNamespace(name='seed', depends_on=[]),
                SimpleNamespace(name='seed', depends_on=['seed']),
            ],
            name='pipeline',
            version='v1',
        )

        with pytest.raises(ValueError, match='Duplicate job name: seed'):
            summary_mod.graph_summary(cast(Config, cfg))

    def test_graph_summary_returns_ordered_job_payload(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that graph summary exposes ordered jobs and dependency metadata.
        """
        jobs = [
            SimpleNamespace(name='seed', depends_on=[]),
            SimpleNamespace(name='publish', depends_on=('seed',)),
        ]
        cfg = SimpleNamespace(
            jobs=jobs,
            name='pipeline',
            version='v1',
        )

        monkeypatch.setattr(summary_mod, 'topological_sort_jobs', lambda _jobs: jobs)

        assert summary_mod.graph_summary(cast(Config, cfg)) == {
            'job_count': 2,
            'name': 'pipeline',
            'ordered_jobs': ['seed', 'publish'],
            'status': 'ok',
            'version': 'v1',
            'jobs': [
                {
                    'depends_on': [],
                    'name': 'seed',
                },
                {
                    'depends_on': ['seed'],
                    'name': 'publish',
                },
            ],
        }
