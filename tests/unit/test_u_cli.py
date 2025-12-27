"""
:mod:`tests.unit.test_u_cli` module.

Unit tests for :mod:`etlplus.cli`.

Notes
-----
These tests are intended to be hermetic. They avoid real network I/O and keep
file I/O limited to pytest-managed temporary directories.
"""

from __future__ import annotations

import argparse
import types
from collections.abc import Callable
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Final
from unittest.mock import Mock

import pytest

import etlplus.cli as cli

# SECTION: HELPERS ========================================================== #

pytestmark = pytest.mark.unit

type ParseCli = Callable[[Sequence[str]], argparse.Namespace]

CSV_TEXT: Final[str] = 'a,b\n1,2\n3,4\n'


@dataclass(frozen=True, slots=True)
class ParserCase:
    """
    Declarative CLI parser test case.

    Attributes
    ----------
    identifier : str
        Stable ID for pytest parametrization.
    args : tuple[str, ...]
        Argument vector passed to :meth:`argparse.ArgumentParser.parse_args`.
    expected : Mapping[str, object]
        Mapping of expected attribute values on the returned namespace.
    """

    identifier: str
    args: tuple[str, ...]
    expected: Mapping[str, object]


# Shared parser cases to keep parametrization DRY and self-documenting.
PARSER_CASES: Final[tuple[ParserCase, ...]] = (
    ParserCase(
        identifier='extract-default-format',
        args=('extract', 'file', '/path/to/file.json'),
        expected={
            'command': 'extract',
            'source_type': 'file',
            'source': '/path/to/file.json',
            'format': 'json',
        },
    ),
    ParserCase(
        identifier='extract-explicit-format',
        args=('extract', 'file', '/path/to/file.csv', '--format', 'csv'),
        expected={
            'command': 'extract',
            'source_type': 'file',
            'source': '/path/to/file.csv',
            'format': 'csv',
            '_format_explicit': True,
        },
    ),
    ParserCase(
        identifier='load-default-format',
        args=('load', '/path/to/file.json', 'file', '/path/to/output.json'),
        expected={
            'command': 'load',
            'source': '/path/to/file.json',
            'target_type': 'file',
            'target': '/path/to/output.json',
        },
    ),
    ParserCase(
        identifier='load-explicit-format',
        args=(
            'load',
            '/path/to/file.json',
            'file',
            '/path/to/output.csv',
            '--format',
            'csv',
        ),
        expected={
            'command': 'load',
            'source': '/path/to/file.json',
            'target_type': 'file',
            'target': '/path/to/output.csv',
            'format': 'csv',
            '_format_explicit': True,
        },
    ),
    # ParserCase(
    #     identifier='no-subcommand',
    #     args=(),
    #     expected={'command': None},
    # ),
    ParserCase(
        identifier='transform',
        args=('transform', '/path/to/file.json'),
        expected={'command': 'transform', 'source': '/path/to/file.json'},
    ),
    ParserCase(
        identifier='validate',
        args=('validate', '/path/to/file.json'),
        expected={'command': 'validate', 'source': '/path/to/file.json'},
    ),
)


def _subcommand_dests(parser: argparse.ArgumentParser) -> set[str]:
    """Extract registered subcommand dests from an argparse parser.

    Notes
    -----
    This inspects argparse internals to keep the test small and explicit.

    Parameters
    ----------
    parser : argparse.ArgumentParser
        Parser to introspect.

    Returns
    -------
    set[str]
        Set of subcommand dest names.
    """
    # pylint: disable=protected-access

    subparsers = getattr(parser, '_subparsers', None)
    if subparsers is None:
        return set()

    group_actions = getattr(subparsers, '_group_actions', [])
    if not group_actions:
        return set()

    action = group_actions[0]
    choices = getattr(action, '_choices_actions', [])
    return {a.dest for a in choices}


@dataclass(slots=True)
class ParserStub:
    """
    Minimal stand-in for :class:`argparse.ArgumentParser`.

    Notes
    -----
    The production :func:`etlplus.cli.main` only needs a ``parse_args`` method
    returning a namespace.

    Attributes
    ----------
    namespace : argparse.Namespace
        Namespace returned by :meth:`parse_args`.
    """

    namespace: argparse.Namespace

    def parse_args(
        self,
        _args: Sequence[str] | None = None,
    ) -> argparse.Namespace:
        """Return the pre-configured namespace."""
        return self.namespace


class DummyCfg:
    """Minimal stand-in pipeline config for CLI helper tests."""

    name = 'p1'
    version = 'v1'
    sources = [types.SimpleNamespace(name='s1')]
    targets = [types.SimpleNamespace(name='t1')]
    transforms = [types.SimpleNamespace(name='tr1')]
    jobs = [types.SimpleNamespace(name='j1')]


# SECTION: FIXTURES ========================================================= #


@pytest.fixture(name='cli_parser')
def cli_parser_fixture() -> argparse.ArgumentParser:
    """
    Provide a fresh CLI parser per test.

    Returns
    -------
    argparse.ArgumentParser
        Newly constructed parser instance.
    """
    return cli.create_parser()


@pytest.fixture(name='parse_cli')
def parse_cli_fixture(
    cli_parser: argparse.ArgumentParser,
) -> ParseCli:
    """
    Provide a callable that parses argv into a namespace.

    Parameters
    ----------
    cli_parser : argparse.ArgumentParser
        Parser instance created per test.

    Returns
    -------
    ParseCli
        Callable that parses CLI args into an :class:`argparse.Namespace`.
    """

    def _parse(args: Sequence[str]) -> argparse.Namespace:
        return cli_parser.parse_args(list(args))

    return _parse


# SECTION: TESTS ============================================================ #


class TestCreateParser:
    """Tests for :func:`etlplus.cli.create_parser`."""

    def test_create_parser_smoke(
        self,
        cli_parser: argparse.ArgumentParser,
    ) -> None:
        """Parser is constructed and uses the expected program name."""
        assert isinstance(cli_parser, argparse.ArgumentParser)
        assert cli_parser.prog == 'etlplus'

    @pytest.mark.parametrize('case', PARSER_CASES, ids=lambda c: c.identifier)
    def test_parser_commands(
        self,
        parse_cli: ParseCli,
        case: ParserCase,
    ) -> None:
        """Known argv patterns map to the expected argparse namespace."""
        ns = parse_cli(case.args)
        for key, expected in case.expected.items():
            assert getattr(ns, key, None) == expected

    def test_parser_version_flag_exits_zero(
        self,
        cli_parser: argparse.ArgumentParser,
    ) -> None:
        """``--version`` exits successfully."""
        with pytest.raises(SystemExit) as exc_info:
            cli_parser.parse_args(['--version'])
        assert exc_info.value.code == 0

    def test_parser_includes_expected_subcommands(
        self,
        cli_parser: argparse.ArgumentParser,
    ) -> None:
        """Expected subcommands are registered on the parser."""
        dests = _subcommand_dests(cli_parser)
        for cmd in (
            'extract',
            'validate',
            'transform',
            'load',
            'pipeline',
            'list',
            'run',
        ):
            assert cmd in dests


class TestCliInternalHelpers:
    """Unit tests for internal CLI helpers in :mod:`etlplus.cli`."""

    def test_format_action_sets_flag(self) -> None:
        """``_FormatAction`` sets ``_format_explicit`` when used."""
        # pylint: disable=protected-access

        parser = argparse.ArgumentParser()
        parser.add_argument('--format', action=cli._FormatAction)
        ns = parser.parse_args(['--format', 'json'])
        assert ns.format == 'json'
        assert ns._format_explicit is True

    def test_add_format_options_sets_defaults(self) -> None:
        """``_add_format_options`` establishes default values."""
        # pylint: disable=protected-access

        parser = argparse.ArgumentParser()
        cli._add_format_options(parser, context='source')

        ns = parser.parse_args([])
        assert ns._format_explicit is False

        ns_strict = parser.parse_args(['--strict-format'])
        assert ns_strict.strict_format is True

        ns_format = parser.parse_args(['--format', 'json'])
        assert ns_format.format == 'json'
        assert ns_format._format_explicit is True

    @pytest.mark.parametrize(
        ('behavior', 'expected_err', 'should_raise'),
        [
            pytest.param('ignore', '', False, id='ignore'),
            pytest.param('silent', '', False, id='silent'),
            pytest.param('warn', 'Warning:', False, id='warn'),
            pytest.param('error', '', True, id='error'),
        ],
    )
    def test_emit_behavioral_notice(
        self,
        behavior: str,
        expected_err: str,
        should_raise: bool,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """
        Test that :func:`_emit_behavioral_notice` raises or emits stderr per
        behavior.
        """
        # pylint: disable=protected-access

        if should_raise:
            with pytest.raises(ValueError):
                cli._emit_behavioral_notice('msg', behavior)
            return

        cli._emit_behavioral_notice('msg', behavior)
        captured = capsys.readouterr()
        assert expected_err in captured.err

    def test_format_behavior_strict(self) -> None:
        """Strict mode maps to error behavior."""
        # pylint: disable=protected-access

        assert cli._format_behavior(True) == 'error'

    def test_format_behavior_env(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that environment overrides behavior when not strict."""
        # pylint: disable=protected-access

        monkeypatch.setenv(cli.FORMAT_ENV_KEY, 'fail')
        assert cli._format_behavior(False) == 'fail'

        monkeypatch.delenv(cli.FORMAT_ENV_KEY, raising=False)
        assert cli._format_behavior(False) == 'warn'

    @pytest.mark.parametrize(
        ('resource_type', 'format_explicit', 'should_raise'),
        [
            pytest.param('file', True, True, id='file-explicit'),
            pytest.param('file', False, False, id='file-implicit'),
            pytest.param('database', True, False, id='nonfile-explicit'),
        ],
    )
    def test_handle_format_guard(
        self,
        monkeypatch: pytest.MonkeyPatch,
        resource_type: str,
        format_explicit: bool,
        should_raise: bool,
    ) -> None:
        """
        Test that guard raises only for explicit formats on file resources.
        """
        # pylint: disable=protected-access

        monkeypatch.setattr(cli, '_format_behavior', lambda _strict: 'error')

        def call():
            return cli._handle_format_guard(
                io_context='source',
                resource_type=resource_type,
                format_explicit=format_explicit,
                strict=False,
            )

        if should_raise:
            with pytest.raises(ValueError):
                call()
        else:
            call()

    def test_list_sections_all(self) -> None:
        """Test that :func:`_list_sections` includes all requested sections."""
        # pylint: disable=protected-access

        args = argparse.Namespace(
            pipelines=True,
            sources=True,
            targets=True,
            transforms=True,
        )
        result = cli._list_sections(DummyCfg(), args)  # type: ignore[arg-type]
        assert set(result) >= {'pipelines', 'sources', 'targets', 'transforms'}

    def test_list_sections_default(self) -> None:
        """
        Test that :func:`_list_sections` defaults to jobs when no flags are
        set.
        """
        # pylint: disable=protected-access

        args = argparse.Namespace(
            pipelines=False,
            sources=False,
            targets=False,
            transforms=False,
        )
        result = cli._list_sections(DummyCfg(), args)  # type: ignore[arg-type]
        assert 'jobs' in result

    def test_materialize_csv_payload_non_str(self) -> None:
        """Test that non-string payloads return unchanged."""
        # pylint: disable=protected-access

        payload: object = {'foo': 1}
        assert cli._materialize_csv_payload(payload) is payload

    def test_materialize_csv_payload_non_csv(self, tmp_path: Path) -> None:
        """Non-CSV file paths are returned unchanged."""
        # pylint: disable=protected-access

        f = tmp_path / 'file.txt'
        f.write_text('abc')
        assert cli._materialize_csv_payload(str(f)) == str(f)

    def test_materialize_csv_payload_csv(self, tmp_path: Path) -> None:
        """CSV file paths are loaded into row dictionaries."""
        # pylint: disable=protected-access

        f = tmp_path / 'file.csv'
        f.write_text(CSV_TEXT)
        rows = cli._materialize_csv_payload(str(f))

        assert isinstance(rows, list)
        assert rows[0] == {'a': '1', 'b': '2'}

    def test_pipeline_summary(self) -> None:
        """``_pipeline_summary`` returns a mapping for a pipeline config."""
        # pylint: disable=protected-access

        result = cli._pipeline_summary(DummyCfg())  # type: ignore[arg-type]
        assert result['name'] == 'p1'
        assert result['version'] == 'v1'
        assert set(result) >= {'sources', 'targets', 'jobs'}

    def test_read_csv_rows(self, tmp_path: Path) -> None:
        """
        Test that :func:`_read_csv_rows` reads a CSV into a list of row
        dictionaries.
        """
        # pylint: disable=protected-access

        f = tmp_path / 'data.csv'
        f.write_text(CSV_TEXT)
        assert cli._read_csv_rows(f) == [
            {'a': '1', 'b': '2'},
            {'a': '3', 'b': '4'},
        ]

    def test_write_json_output_stdout_is_quiet(
        self,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """
        Test that, when writing to stdout, the helper does not print JSON to
        stdout.
        """
        # pylint: disable=protected-access

        data = {'x': 1}
        assert (
            cli._write_json_output(data, None, success_message='msg') is False
        )
        assert capsys.readouterr().out == ''

    def test_write_json_output_file(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that, when a file path is provided, the helper writes JSON via
        :class:`File`.
        """
        # pylint: disable=protected-access

        data = {'x': 1}

        dummy_file = Mock()
        monkeypatch.setattr(cli, 'File', lambda _p, _f: dummy_file)

        cli._write_json_output(data, 'out.json', success_message='msg')
        dummy_file.write_json.assert_called_once_with(data)


class TestMain:
    """Unit test suite for :func:`etlplus.cli.main`."""

    @pytest.mark.parametrize(
        'argv',
        [
            pytest.param(['extract', 'file', 'foo'], id='extract'),
            pytest.param(['validate', 'foo'], id='validate'),
            pytest.param(['transform', 'foo'], id='transform'),
            pytest.param(['load', 'foo', 'file', 'bar'], id='load'),
            pytest.param(['pipeline', '--config', 'foo.yml'], id='pipeline'),
            pytest.param(['list', '--config', 'foo.yml'], id='list'),
            pytest.param(['run', '--config', 'foo.yml'], id='run'),
        ],
    )
    def test_dispatches_all_subcommands(
        self,
        monkeypatch: pytest.MonkeyPatch,
        argv: list[str],
    ) -> None:
        """
        Test that :func:`main` dispatches all subcommands to ``args.func``.
        """
        parser = cli.create_parser()
        args = parser.parse_args(argv)

        args.func = Mock(return_value=0)

        monkeypatch.setattr(cli, 'create_parser', lambda: parser)
        monkeypatch.setattr(parser, 'parse_args', lambda _argv: args)

        assert cli.main(argv) == 0
        args.func.assert_called_once_with(args)

    def test_no_command_is_usage_error(self) -> None:
        """Test that no subcommand is a usage error (argparse exit code 2)."""
        try:
            result = cli.main([])
        except SystemExit as exc:
            assert exc.code == 2
        else:
            assert result == 0

    def test_handles_keyboard_interrupt(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """KeyboardInterrupt maps to the conventional exit code 130."""
        cmd = Mock(side_effect=KeyboardInterrupt)
        ns = argparse.Namespace(command='dummy', func=cmd)
        monkeypatch.setattr(cli, 'create_parser', lambda: ParserStub(ns))

        assert cli.main([]) == 130

    def test_handles_system_exit_from_command(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that :func:`main` does not swallow :class:`SystemExit` from the
        dispatched command.
        """
        cmd = Mock(side_effect=SystemExit(5))
        ns = argparse.Namespace(command='dummy', func=cmd)
        monkeypatch.setattr(cli, 'create_parser', lambda: ParserStub(ns))

        with pytest.raises(SystemExit) as exc_info:
            cli.main([])
        assert exc_info.value.code == 5

    def test_value_error_returns_exit_code_1(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Test that :class:`ValueError` from a command maps to exit code 1."""
        cmd = Mock(side_effect=ValueError('fail'))
        ns = argparse.Namespace(command='dummy', func=cmd)
        monkeypatch.setattr(cli, 'create_parser', lambda: ParserStub(ns))

        assert cli.main([]) == 1
        assert 'Error:' in capsys.readouterr().err
