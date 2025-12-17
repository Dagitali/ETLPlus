"""
:mod:`tests.unit.test_u_cli` module.

Unit tests for ``etlplus.cli``.

Notes
-----
- Hermetic: no file or network I/O.
- Uses fixtures from `tests/unit/conftest.py` when needed.
"""

from __future__ import annotations

import pytest

from etlplus.cli import create_parser

# SECTION: TESTS ============================================================ #


@pytest.mark.unit
class TestCreateParser:
    """
    Unit test suite for :func:`etlplus.cli.create_parser`.

    Notes
    -----
    - Tests CLI parser creation and argument parsing for all commands.
    """

    def test_create_parser(self) -> None:
        """
        Test that the CLI parser is created and configured correctly.
        """
        parser = create_parser()
        assert parser is not None
        assert parser.prog == 'etlplus'

    @pytest.mark.parametrize(
        'cmd_args,expected_args',
        [
            (
                ['extract', 'file', '/path/to/file.json'],
                {
                    'command': 'extract',
                    'source_type': 'file',
                    'source': '/path/to/file.json',
                    'format': 'json',
                },
            ),
            (
                ['extract', 'file', '/path/to/file.csv', '--format', 'csv'],
                {
                    'command': 'extract',
                    'source_type': 'file',
                    'source': '/path/to/file.csv',
                    'format': 'csv',
                    '_format_explicit': True,
                },
            ),
            (
                ['load', '/path/to/file.json', 'file', '/path/to/output.json'],
                {
                    'command': 'load',
                    'source': '/path/to/file.json',
                    'target_type': 'file',
                    'target': '/path/to/output.json',
                },
            ),
            (
                [
                    'load',
                    '/path/to/file.json',
                    'file',
                    '/path/to/output.csv',
                    '--format',
                    'csv',
                ],
                {
                    'command': 'load',
                    'source': '/path/to/file.json',
                    'target_type': 'file',
                    'target': '/path/to/output.csv',
                    'format': 'csv',
                    '_format_explicit': True,
                },
            ),
            ([], {'command': None}),
            (
                ['transform', '/path/to/file.json'],
                {'command': 'transform', 'source': '/path/to/file.json'},
            ),
            (
                ['validate', '/path/to/file.json'],
                {'command': 'validate', 'source': '/path/to/file.json'},
            ),
        ],
    )
    def test_parser_commands(
        self,
        cmd_args: list[str],
        expected_args: dict[str, object],
    ) -> None:
        """
        Test CLI command parsing and argument mapping.

        Parameters
        ----------
        cmd_args : list[str]
            CLI arguments to parse.
        expected_args : dict[str, object]
            Expected parsed argument values.
        """
        parser = create_parser()
        args = parser.parse_args(cmd_args)
        for key, val in expected_args.items():
            assert getattr(args, key, None) == val

    def test_parser_version(self) -> None:
        """Test that the CLI parser provides version information."""
        parser = create_parser()
        with pytest.raises(SystemExit) as exc_info:
            parser.parse_args(['--version'])
        assert exc_info.value.code == 0
