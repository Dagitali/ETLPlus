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

import json
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:  # pragma: no cover - typing helpers only
    from tests.conftest import CliInvoke
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
            'file',
            str(source),
            '--format',
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
        extra_flags: list[str],
        expected_code: int,
        expected_message: str,
        expect_output: bool,
    ) -> None:
        """
        Validate ``load`` warnings/errors and resulting output file state.
        """
        output_path = tmp_path / 'output.csv'
        args: list[str] = [
            'load',
            '{"name": "John"}',
            'file',
            str(output_path),
            '--format',
            'csv',
            *extra_flags,
        ]
        code, _out, err = cli_invoke(args)
        assert code == expected_code
        assert expected_message in err
        assert output_path.exists() is expect_output

    def test_main_no_command(self, cli_invoke: CliInvoke) -> None:
        """Test that running :func:`main` with no command shows usage."""
        code, out, _err = cli_invoke()
        assert code == 0
        assert 'usage:' in out.lower()

    def test_main_extract_file(
        self,
        json_file_factory: JsonFactory,
        cli_invoke: CliInvoke,
    ) -> None:
        """Test that ``extract file`` prints the serialized payload."""
        payload = {'name': 'John', 'age': 30}
        source = json_file_factory(payload, filename='input.json')
        code, out, _err = cli_invoke(('extract', 'file', str(source)))
        assert code == 0
        assert json.loads(out) == payload

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

    def test_main_load_file(
        self,
        tmp_path: Path,
        cli_invoke: CliInvoke,
    ) -> None:
        """
        Test that running :func:`main` with the ``load`` file command works.
        """
        output_path = tmp_path / 'output.json'
        json_data = '{"name": "John", "age": 30}'
        code, _out, _err = cli_invoke(
            ('load', json_data, 'file', str(output_path)),
        )
        assert code == 0
        assert output_path.exists()

    def test_main_extract_with_output(
        self,
        tmp_path: Path,
        json_file_factory: JsonFactory,
        cli_invoke: CliInvoke,
    ) -> None:
        """Test extract command with ``-o`` output persistence."""
        test_data = {'name': 'John', 'age': 30}
        source = json_file_factory(test_data, filename='input.json')
        output_path = tmp_path / 'output.json'
        code, _out, _err = cli_invoke(
            (
                'extract',
                'file',
                str(source),
                '-o',
                str(output_path),
            ),
        )
        assert code == 0
        assert output_path.exists()
        assert json.loads(output_path.read_text()) == test_data

    def test_main_error_handling(
        self,
        cli_invoke: CliInvoke,
    ) -> None:
        """Test that running :func:`main` with an invalid command errors."""
        code, _out, err = cli_invoke(
            ('extract', 'file', '/nonexistent/file.json'),
        )
        assert code == 1
        assert 'Error:' in err

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
                'file',
                'data.csv',
                '--format',
                'csv',
                '--strict-format',
            ),
        )
        assert code == 1
        assert 'Error:' in err
