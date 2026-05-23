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

from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:  # pragma: no cover - typing helpers only
    from tests.conftest import CliInvoke
    from tests.conftest import JsonFactory
    from tests.conftest import JsonFileParser
    from tests.conftest import JsonOutputParser
    from tests.integration.pytest_integration_support import StdinText

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TYPE ALIASES ===================================================== #


type CliArgs = tuple[str, ...]


# SECTION: CONSTANTS ======================================================== #


STDIN_PERSON_LIST = '[{"name": "John"}]'

# SECTION: TESTS ============================================================ #


class TestCliEndToEnd:
    """Integration tests for :mod:`etlplus.cli`."""

    @pytest.mark.parametrize(
        'args,should_pass',
        [
            pytest.param(
                (
                    'extract',
                    'examples/data/sample.csv',
                    '--source-format',
                    'csv',
                ),
                True,
                id='extract-source-format-after-source',
            ),
            pytest.param(
                (
                    'extract',
                    '--source-format',
                    'csv',
                    'examples/data/sample.csv',
                ),
                True,
                id='extract-source-format-before-source',
            ),
            pytest.param(('extract',), False, id='extract-missing-source'),
            pytest.param(
                (
                    'extract',
                    'examples/data/sample.csv',
                    '--source-type',
                    'file',
                ),
                True,
                id='extract-source-type-file',
            ),
            pytest.param(
                (
                    'extract',
                    'examples/data/sample.csv',
                    '--source-format',
                    'badformat',
                ),
                False,
                id='extract-invalid-source-format',
            ),
            pytest.param(
                ('load', 'output.csv', '--target-format', 'csv'),
                True,
                id='load-target-format-after-target',
            ),
            pytest.param(
                ('load', '--target-format', 'csv', 'output.csv'),
                False,
                id='load-target-format-before-target',
            ),
            pytest.param(('load',), False, id='load-missing-target'),
            pytest.param(
                ('load', 'output.csv', '--target-type', 'file'),
                True,
                id='load-target-type-file',
            ),
            pytest.param(
                ('load', 'output.csv', '--target-format', 'badformat'),
                False,
                id='load-invalid-target-format',
            ),
            pytest.param(
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
                id='transform-source-format-before-target-format',
            ),
            pytest.param(
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
                id='transform-source-format-before-target',
            ),
            pytest.param(
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
                id='transform-target-format-before-source-format',
            ),
            pytest.param(
                ('transform',),
                False,
                id='transform-missing-args',
            ),
            pytest.param(
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
                id='transform-invalid-source-format',
            ),
        ],
    )
    def test_cli_option_order_and_required_args(
        self,
        cli_invoke: CliInvoke,
        args: CliArgs,
        should_pass: bool,
        stdin_text: StdinText,
    ) -> None:
        """Test CLI required-argument and option-order edge cases."""
        if should_pass and args[0] == 'load':
            stdin_text(STDIN_PERSON_LIST)
        code, _out, err = cli_invoke(args)
        if should_pass:
            assert code == 0, f'Expected success for args: {args}, got error: {err}'
        else:
            assert code != 0, f'Expected failure for args: {args}'

    def test_extract_source_format_override(
        self,
        tmp_path: Path,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
    ) -> None:
        """
        Test that ``--source-format`` overrides file extension inference.
        """
        source = tmp_path / 'records.txt'
        source.write_text('a,b\n1,2\n')
        code, out, err = cli_invoke(
            ('extract', str(source), '--source-format', 'csv'),
        )
        assert code == 0
        assert err.strip() == ''
        payload = parse_json_output(out)
        assert payload[0] == {'a': '1', 'b': '2'}

    def test_load_target_format_override(
        self,
        tmp_path: Path,
        cli_invoke: CliInvoke,
        stdin_text: StdinText,
    ) -> None:
        """
        Test that ``--target-format`` controls how file targets are written.
        """
        output_path = tmp_path / 'output.bin'
        stdin_text(STDIN_PERSON_LIST)
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
        parse_json_output: JsonOutputParser,
    ) -> None:
        """
        Test that ``validate`` accepts CSV files lacking extensions via flag.
        """
        source = tmp_path / 'dataset.data'
        source.write_text('id,val\n1,2\n')
        code, out, err = cli_invoke(
            ('validate', str(source), '--source-format', 'csv'),
        )
        assert code == 0
        assert err.strip() == ''
        payload = parse_json_output(out)
        assert payload['valid'] is True

    def test_main_extract_file(
        self,
        json_file_factory: JsonFactory,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
    ) -> None:
        """Test that ``extract file`` prints the serialized payload."""
        payload = {'name': 'John', 'age': 30}
        source = json_file_factory(payload, filename='input.json')
        code, out, _err = cli_invoke(('extract', str(source)))
        assert code == 0
        assert parse_json_output(out) == payload

    def test_main_error_handling(
        self,
        cli_invoke: CliInvoke,
    ) -> None:
        """Test invalid-command handling in :func:`main`."""
        code, _out, err = cli_invoke(
            ('extract', '/nonexistent/file.json'),
        )
        assert code == 1
        assert 'Error:' in err

    def test_main_load_file(
        self,
        tmp_path: Path,
        cli_invoke: CliInvoke,
        parse_json_file: JsonFileParser,
        stdin_text: StdinText,
    ) -> None:
        """
        Test that running :func:`main` with the ``load`` file command works.
        """
        output_path = tmp_path / 'output.json'
        stdin_text('{"name": "John", "age": 30}')
        code, _out, _err = cli_invoke(
            ('load', str(output_path)),
        )
        assert code == 0
        assert parse_json_file(output_path) == {'name': 'John', 'age': 30}

    def test_main_no_command(self, cli_invoke: CliInvoke) -> None:
        """Test that running :func:`main` with no command shows usage."""
        code, out, _err = cli_invoke()
        assert code == 0
        assert 'usage:' in out.lower()

    def test_main_transform_data(
        self,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
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
        output = parse_json_output(out)
        assert output == [{'name': 'John'}]

    def test_main_validate_data(
        self,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
    ) -> None:
        """
        Test that running :func:`main` with the ``validate`` command works.
        """
        json_data = '{"name": "John", "age": 30}'
        code, out, _err = cli_invoke(('validate', json_data))
        assert code == 0
        assert parse_json_output(out) == {
            'valid': True,
            'errors': [],
            'field_errors': {},
            'data': {'name': 'John', 'age': 30},
        }
