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

if TYPE_CHECKING:  # pragma: no cover - typing helpers only
    from tests.conftest import CliInvoke
    from tests.conftest import JsonFactory


# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.integration


# SECTION: TESTS ============================================================ #


class TestCliEndToEnd:
    """Integration test suite for :mod:`etlplus.cli`."""

    @pytest.mark.parametrize(
        'args,should_pass',
        [
            # extract: valid/invalid option placements
            (
                (
                    'extract',
                    'examples/data/sample.csv',
                    '--source-format',
                    'csv',
                ),
                True,
            ),
            (
                (
                    'extract',
                    '--source-format',
                    'csv',
                    'examples/data/sample.csv',
                ),
                True,
            ),
            (('extract',), False),
            (
                (
                    'extract',
                    'examples/data/sample.csv',
                    '--source-type',
                    'file',
                ),
                True,
            ),
            (
                (
                    'extract',
                    'examples/data/sample.csv',
                    '--source-format',
                    'badformat',
                ),
                False,
            ),
            # load: valid/invalid option placements
            (('load', 'output.csv', '--target-format', 'csv'), True),
            (('load', '--target-format', 'csv', 'output.csv'), False),
            (('load',), False),
            (('load', 'output.csv', '--target-type', 'file'), True),
            (('load', 'output.csv', '--target-format', 'badformat'), False),
            # transform: valid/invalid placements for source/target options
            (
                (
                    'transform',
                    '[{}]',
                    'output.json',
                    '--source-format',
                    'json',
                    '--target-format',
                    'json',
                    '--operations',
                    '{}',
                ),
                True,
            ),
            (
                (
                    'transform',
                    '[{}]',
                    '--source-format',
                    'json',
                    'output.json',
                    '--target-format',
                    'json',
                    '--operations',
                    '{}',
                ),
                True,
            ),
            (
                (
                    'transform',
                    '[{}]',
                    'output.json',
                    '--target-format',
                    'json',
                    '--source-format',
                    'json',
                    '--operations',
                    '{}',
                ),
                True,
            ),
            (('transform',), False),
            (
                (
                    'transform',
                    '[{}]',
                    'output.json',
                    '--source-format',
                    'badformat',
                    '--operations',
                    '{}',
                ),
                False,
            ),
        ],
    )
    def test_cli_option_order_and_required_args(
        self,
        cli_invoke,
        args,
        should_pass,
        monkeypatch: pytest.MonkeyPatch,
    ):
        """Test CLI required arguments and option order edge cases."""
        if should_pass and args and args[0] == 'load':
            monkeypatch.setattr(
                sys,
                'stdin',
                io.StringIO('[{"name": "John"}]'),
            )
        code, _out, err = cli_invoke(args)
        if should_pass:
            assert code == 0, (
                f'Expected success for args: {args}, got error: {err}'
            )
        else:
            assert code != 0, f'Expected failure for args: {args}'

    def test_extract_source_format_override(
        self,
        tmp_path: Path,
        cli_invoke: CliInvoke,
    ) -> None:
        """Explicit ``--source-format`` overrides file extension inference."""
        source = tmp_path / 'records.txt'
        source.write_text('a,b\n1,2\n')
        code, out, err = cli_invoke(
            ('extract', str(source), '--source-format', 'csv'),
        )
        assert code == 0
        assert err.strip() == ''
        payload = json.loads(out)
        assert payload[0] == {'a': '1', 'b': '2'}

    def test_load_target_format_override(
        self,
        tmp_path: Path,
        cli_invoke: CliInvoke,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """``--target-format`` controls how file targets are written."""
        output_path = tmp_path / 'output.bin'
        monkeypatch.setattr(
            sys,
            'stdin',
            io.StringIO('[{"name": "John"}]'),
        )
        code, _out, err = cli_invoke(
            ('load', str(output_path), '--target-format', 'csv'),
        )
        assert code == 0
        assert err.strip() == ''
        contents = output_path.read_text().splitlines()
        assert contents[0] == 'name'
        assert contents[1] == 'John'

    def test_validate_source_format_override(
        self,
        tmp_path: Path,
        cli_invoke: CliInvoke,
    ) -> None:
        """``validate`` accepts CSV files lacking extensions via flag."""
        source = tmp_path / 'dataset.data'
        source.write_text('id,val\n1,2\n')
        code, out, err = cli_invoke(
            ('validate', str(source), '--source-format', 'csv'),
        )
        assert code == 0
        assert err.strip() == ''
        payload = json.loads(out)
        assert payload['valid'] is True

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
