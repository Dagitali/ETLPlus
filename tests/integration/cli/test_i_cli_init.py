"""
:mod:`tests.integration.cli.test_i_cli_init` module.

Integration-scope smoke tests for the ``etlplus init`` CLI command.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:  # pragma: no cover - typing helpers only
    from tests.conftest import CliInvoke
    from tests.conftest import JsonOutputParser

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestCliInit:
    """Smoke tests for the ``etlplus init`` CLI command."""

    def test_init_requires_force_to_overwrite_existing_pipeline(
        self,
        tmp_path: Path,
        cli_invoke: CliInvoke,
    ) -> None:
        """
        Test that ``etlplus init`` rejects existing scaffold files unless
        ``--force`` is used.
        """
        project_dir = tmp_path / 'starter-project'
        project_dir.mkdir()
        (project_dir / 'pipeline.yml').write_text('name: existing\n', encoding='utf-8')

        code, out, err = cli_invoke(('init', str(project_dir)))
        assert code == 1
        assert out == ''
        assert 'Scaffold file already exists' in err

    def test_init_scaffolds_runnable_starter_project(
        self,
        tmp_path: Path,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that ``etlplus init`` creates a starter project that can run
        immediately.
        """
        project_dir = tmp_path / 'starter-project'

        code, out, err = cli_invoke(('init', str(project_dir)))
        assert code == 0
        assert err == ''
        payload = parse_json_output(out)
        assert payload['status'] == 'ok'
        assert (project_dir / 'pipeline.yml').is_file()
        assert (project_dir / 'data' / 'customers.csv').is_file()

        monkeypatch.chdir(project_dir)
        code, out, err = cli_invoke(
            ('run', '--config', 'pipeline.yml', '--job', payload['job']),
        )
        assert code == 0
        assert err == ''
        run_payload = parse_json_output(out)
        assert run_payload['status'] == 'ok'
        assert (project_dir / 'temp' / 'customers.json').is_file()
