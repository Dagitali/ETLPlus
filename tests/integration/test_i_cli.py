"""
:mod:`tests.integration.test_i_cli` module.

End-to-end CLI integration test suite that exercises the ``etlplus`` command
without external dependencies. Tests rely on shared fixtures for CLI
invocation and filesystem management to maximize reuse.

Notes
-----
- Uses ``cli_invoke``/``cli_runner`` fixtures to avoid ad-hoc monkeypatching.
- Creates JSON files through ``json_file_factory`` for deterministic cleanup.
- Keeps docstrings NumPy-compliant for automated linting.
"""

from __future__ import annotations

import io
import json
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
import typer

if TYPE_CHECKING:  # pragma: no cover - typing helpers only
    from tests.conftest import CliInvoke
    from tests.conftest import CliRunner
    from tests.conftest import JsonFactory


# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.integration


# SECTION: TESTS ============================================================ #


class TestCliEndToEnd:
    """Integration test suite for :mod:`etlplus.cli`."""

    @pytest.mark.parametrize(
        ('extra_flags', 'expected_code', 'expected_message'),
        [
            pytest.param(
                ['--strict-format'],
                1,
                'Error:',
                id='strict-errors',
            ),
            pytest.param(
                [],
                0,
                'Warning:',
                id='warns-default',
            ),
        ],
    )
    def test_extract_format_feedback(
        self,
        json_file_factory: JsonFactory,
        cli_invoke: CliInvoke,
        extra_flags: list[str],
        expected_code: int,
        expected_message: str,
    ) -> None:
        """Verify ``extract`` error/warning flow with optional strict flag."""
        source = json_file_factory({'x': 1}, filename='payload.json')
        args: list[str] = [
            'extract',
            str(source),
            '--source-format',
            'json',
            *extra_flags,
        ]
        code, _out, err = cli_invoke(args)
        assert code == expected_code
        assert expected_message in err

    @pytest.mark.parametrize(
        (
            'extra_flags',
            'expected_code',
            'expected_message',
            'expect_output',
        ),
        [
            pytest.param(
                ['--strict-format'],
                1,
                'Error:',
                False,
                id='strict-errors',
            ),
            pytest.param(
                [],
                0,
                'Warning:',
                True,
                id='warns-default',
            ),
        ],
    )
    def test_load_format_feedback(
        self,
        tmp_path: Path,
        cli_invoke: CliInvoke,
        monkeypatch: pytest.MonkeyPatch,
        extra_flags: list[str],
        expected_code: int,
        expected_message: str,
        expect_output: bool,
    ) -> None:
        """
        Validate ``load`` warnings/errors and resulting output file state.
        """
        output_path = tmp_path / 'output.csv'
        monkeypatch.setattr(
            sys,
            'stdin',
            io.StringIO('{"name": "John"}'),
        )
        args: list[str] = [
            'load',
            str(output_path),
            '--target-format',
            'csv',
            *extra_flags,
        ]
        code, _out, err = cli_invoke(args)
        assert code == expected_code
        assert expected_message in err
        assert output_path.exists() is expect_output

    def test_load_target_type_conflict(
        self,
        tmp_path: Path,
        cli_runner: CliRunner,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Explicit TARGET_TYPE conflicts with ``--to`` override."""
        output_path = tmp_path / 'conflict.json'
        monkeypatch.setattr(
            sys,
            'stdin',
            io.StringIO('{"name": "Ana"}'),
        )
        with pytest.raises(typer.BadParameter) as excinfo:
            cli_runner(('load', 'file', str(output_path), '--to', 'api'))
        assert 'Do not combine --to with an explicit TARGET_TYPE.' in str(
            excinfo.value,
        )

    def test_main_extract_file(
        self,
        json_file_factory: JsonFactory,
        cli_invoke: CliInvoke,
    ) -> None:
        """Test that ``extract file`` prints the serialized payload."""
        payload = {'name': 'John', 'age': 30}
        source = json_file_factory(payload, filename='input.json')
        code, out, _err = cli_invoke(('extract', str(source)))
        assert code == 0
        assert json.loads(out) == payload

    def test_main_error_handling(
        self,
        cli_invoke: CliInvoke,
    ) -> None:
        """Test that running :func:`main` with an invalid command errors."""
        code, _out, err = cli_invoke(
            ('extract', '/nonexistent/file.json'),
        )
        assert code == 1
        assert 'Error:' in err

    def test_main_load_explicit_target_type(
        self,
        tmp_path: Path,
        cli_runner: CliRunner,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Ensure positional TARGET_TYPE raises a helpful error."""
        output_path = tmp_path / 'explicit.json'
        monkeypatch.setattr(
            sys,
            'stdin',
            io.StringIO('{"name": "Jane"}'),
        )
        with pytest.raises(typer.BadParameter) as excinfo:
            cli_runner(('load', 'file', str(output_path)))
        assert 'Legacy form' in str(excinfo.value)

    def test_main_load_file(
        self,
        tmp_path: Path,
        cli_invoke: CliInvoke,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that running :func:`main` with the ``load`` file command works.
        """
        output_path = tmp_path / 'output.json'
        monkeypatch.setattr(
            sys,
            'stdin',
            io.StringIO('{"name": "John", "age": 30}'),
        )
        code, _out, _err = cli_invoke(
            ('load', str(output_path)),
        )
        assert code == 0
        assert output_path.exists()

    def test_main_no_command(self, cli_invoke: CliInvoke) -> None:
        """Test that running :func:`main` with no command shows usage."""
        code, out, _err = cli_invoke()
        assert code == 0
        assert 'usage:' in out.lower()

    def test_main_strict_format_error(
        self,
        cli_invoke: CliInvoke,
    ) -> None:
        """
        Test ``extract`` with ``--strict-format`` rejects mismatched args.
        """
        code, _out, err = cli_invoke(
            (
                'extract',
                'data.csv',
                '--source-format',
                'csv',
                '--strict-format',
            ),
        )
        assert code == 1
        assert 'Error:' in err

    def test_main_transform_data(
        self,
        cli_invoke: CliInvoke,
    ) -> None:
        """
        Test that running :func:`main` with the ``transform`` command works.
        """
        json_data = '[{"name": "John", "age": 30}]'
        operations = '{"select": ["name"]}'
        code, out, _err = cli_invoke(
            ('transform', json_data, '--operations', operations),
        )
        assert code == 0
        output = json.loads(out)
        assert len(output) == 1 and 'age' not in output[0]

    def test_main_validate_data(
        self,
        cli_invoke: CliInvoke,
    ) -> None:
        """
        Test that running :func:`main` with the ``validate`` command works.
        """
        json_data = '{"name": "John", "age": 30}'
        code, out, _err = cli_invoke(('validate', json_data))
        assert code == 0
        assert json.loads(out)['valid'] is True
